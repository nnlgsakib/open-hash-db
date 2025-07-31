package bootnode

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"openhashdb/network/mtr"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
)

// Constants
const (
	ServiceTag                = "openhashdb"
	TopicContentAnnounce      = "/openhashdb/announce/1.0.0"
	ProtocolContentExchange   = protocol.ID("/openhashdb/content/1.0.0")
	ProtocolChunkExchange     = protocol.ID("/openhashdb/chunk/1.0.0")
	MaxPeerEventLogs          = 100
	PeerDiscoveryInterval     = 5 * time.Second
	BootnodeDiscoveryInterval = 60 * time.Second
	MaxDiscoveryPeerReqCount  = 16
	MaxOutboundPeers          = 8
	MaxInboundPeers           = 32
	MinimumPeerConnections    = 1
	PeerOutboundBufferSize    = 1024
	ValidateBufferSize        = 1024
	SubscribeOutputBufferSize = 1024
	DefaultBucketSize         = 20
)

// PeerEvent represents a peer discovery, connection, or disconnection event
type PeerEvent struct {
	PeerID    peer.ID   `json:"peer_id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Addresses []string  `json:"addresses"`
}

// BootNode represents a libp2p boot node for peer discovery
type BootNode struct {
	host                  host.Host
	ctx                   context.Context
	cancel                context.CancelFunc
	mdns                  mdns.Service
	dht                   *dht.IpfsDHT
	routingTable          *kbucket.RoutingTable
	pubsub                *pubsub.PubSub
	contentAnnounceTopic  *pubsub.Topic
	dialQueue             *DialQueue
	connectionCounts      *ConnectionInfo
	bootnodes             *bootnodesWrapper
	temporaryDials        sync.Map
	peerEvents            []PeerEvent
	peerEventsMu          sync.RWMutex
	PeerConnectedCallback func(peer.ID)
}

// ConnectionInfo tracks connection limits and counts
type ConnectionInfo struct {
	inboundConnectionCount         int64
	outboundConnectionCount        int64
	pendingInboundConnectionCount  int64
	pendingOutboundConnectionCount int64
	maxInboundConnectionCount      int64
	maxOutboundConnectionCount     int64
}

// NewBlankConnectionInfo creates a new ConnectionInfo
func NewBlankConnectionInfo(maxInbound, maxOutbound int64) *ConnectionInfo {
	return &ConnectionInfo{
		maxInboundConnectionCount:  maxInbound,
		maxOutboundConnectionCount: maxOutbound,
	}
}

// HasFreeConnectionSlot checks if there are free slots
func (ci *ConnectionInfo) HasFreeConnectionSlot(direction network.Direction) bool {
	switch direction {
	case network.DirInbound:
		return atomic.LoadInt64(&ci.inboundConnectionCount)+atomic.LoadInt64(&ci.pendingInboundConnectionCount) < ci.maxInboundConnectionCount
	case network.DirOutbound:
		return atomic.LoadInt64(&ci.outboundConnectionCount)+atomic.LoadInt64(&ci.pendingOutboundConnectionCount) < ci.maxOutboundConnectionCount
	}
	return false
}

// UpdateConnCountByDirection updates connection counts
func (ci *ConnectionInfo) UpdateConnCountByDirection(delta int64, direction network.Direction) {
	switch direction {
	case network.DirInbound:
		atomic.AddInt64(&ci.inboundConnectionCount, delta)
	case network.DirOutbound:
		atomic.AddInt64(&ci.outboundConnectionCount, delta)
	}
}

// DialTask represents a dial task in the queue
type DialTask struct {
	index    int
	addrInfo *peer.AddrInfo
	priority uint64
}

// GetAddrInfo returns the peer information
func (dt *DialTask) GetAddrInfo() *peer.AddrInfo {
	return dt.addrInfo
}

// DialQueue is a priority queue for dial tasks
type DialQueue struct {
	sync.Mutex
	heap     dialQueueImpl
	tasks    map[peer.ID]*DialTask
	updateCh chan struct{}
	closeCh  chan struct{}
}

// dialQueueImpl is the heap implementation
type dialQueueImpl []*DialTask

func (t dialQueueImpl) Len() int           { return len(t) }
func (t dialQueueImpl) Less(i, j int) bool { return t[i].priority < t[j].priority }
func (t dialQueueImpl) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
	t[i].index = i
	t[j].index = j
}
func (t *dialQueueImpl) Push(x interface{}) {
	n := len(*t)
	item := x.(*DialTask)
	item.index = n
	*t = append(*t, item)
}
func (t *dialQueueImpl) Pop() interface{} {
	old := *t
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*t = old[0 : n-1]
	return item
}

// NewDialQueue creates a new DialQueue
func NewDialQueue() *DialQueue {
	return &DialQueue{
		heap:     dialQueueImpl{},
		tasks:    map[peer.ID]*DialTask{},
		updateCh: make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
	}
}

// Close closes the DialQueue
func (d *DialQueue) Close() {
	close(d.closeCh)
}

// Wait waits for updates or closure
func (d *DialQueue) Wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-d.updateCh:
		return false
	case <-d.closeCh:
		return true
	}
}

// PopTask removes and returns the highest priority task
func (d *DialQueue) PopTask() *DialTask {
	d.Lock()
	defer d.Unlock()
	if len(d.heap) != 0 {
		task := heap.Pop(&d.heap).(*DialTask)
		delete(d.tasks, task.addrInfo.ID)
		return task
	}
	return nil
}

// AddTask adds a new dial task
func (d *DialQueue) AddTask(addrInfo *peer.AddrInfo, priority uint64) {
	d.Lock()
	defer d.Unlock()
	if item, ok := d.tasks[addrInfo.ID]; ok {
		if item.priority > priority {
			item.addrInfo = addrInfo
			item.priority = priority
			heap.Fix(&d.heap, item.index)
		}
		return
	}
	task := &DialTask{
		addrInfo: addrInfo,
		priority: priority,
	}
	d.tasks[addrInfo.ID] = task
	heap.Push(&d.heap, task)
	select {
	case d.updateCh <- struct{}{}:
	default:
	}
}

// bootnodesWrapper manages bootnode information
type bootnodesWrapper struct {
	bootnodeArr  []*peer.AddrInfo
	bootnodesMap map[peer.ID]*peer.AddrInfo
}

// DefaultBootnodes for the VPS-hosted default node
var DefaultBootnodes = []string{
	"/ip4/148.251.35.204/tcp/9090/p2p/QmYQMdZkvC4R7DHqSkCKNh89Hs7gDPLjx9j9xnPsUkkS2P",
}

// loadOrCreateIdentity loads or creates a private key
func loadOrCreateIdentity(keyPath string) (crypto.PrivKey, error) {
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create key directory: %w", err)
	}
	if keyData, err := os.ReadFile(keyPath); err == nil {
		keyBytes, err := base64.StdEncoding.DecodeString(string(keyData))
		if err != nil {
			log.Printf("Warning: failed to decode key, creating new: %v", err)
		} else {
			privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
			if err == nil {
				log.Printf("Loaded identity from %s", keyPath)
				return privKey, nil
			}
			log.Printf("Warning: failed to unmarshal key, creating new: %v", err)
		}
	}
	log.Printf("Generating new identity at %s", keyPath)
	privKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}
	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}
	keyData := base64.StdEncoding.EncodeToString(keyBytes)
	if err := os.WriteFile(keyPath, []byte(keyData), 0600); err != nil {
		return nil, fmt.Errorf("failed to save private key: %w", err)
	}
	return privKey, nil
}

// convertBootnodesToAddrInfo converts multiaddresses to peer.AddrInfo
func convertBootnodesToAddrInfo(bootnodes []string) ([]peer.AddrInfo, error) {
	var addrInfos []peer.AddrInfo
	for _, addr := range bootnodes {
		if addr == "" {
			continue
		}
		addrInfo, err := peer.AddrInfoFromString(addr)
		if err != nil {
			log.Printf("Failed to parse bootnode address %s: %v", addr, err)
			continue
		}
		addrInfos = append(addrInfos, *addrInfo)
	}
	return addrInfos, nil
}

// NewBootNode creates a new libp2p boot node
func NewBootNode(ctx context.Context, keyPath string, bootnodes []string, p2pPort int) (*BootNode, error) {
	privKey, err := loadOrCreateIdentity(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load identity: %w", err)
	}

	listenAddrs := []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", p2pPort),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", p2pPort),
	}

	allBootnodes := append(DefaultBootnodes, bootnodes...)
	addrInfos, err := convertBootnodesToAddrInfo(allBootnodes)
	if err != nil {
		log.Printf("Warning: failed to parse some bootnode addresses: %v", err)
	}

	bootnodesMap := make(map[peer.ID]*peer.AddrInfo)
	bootnodeArr := make([]*peer.AddrInfo, len(addrInfos))
	for i, addrInfo := range addrInfos {
		bootnodesMap[addrInfo.ID] = &addrInfos[i]
		bootnodeArr[i] = &addrInfos[i]
	}

	var nodeDHT *dht.IpfsDHT
	var routingTable *kbucket.RoutingTable
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableRelay(),
		libp2p.EnableRelayService(),
		libp2p.EnableHolePunching(),
		libp2p.EnableAutoRelayWithPeerSource(func(ctx context.Context, numPeers int) <-chan peer.AddrInfo {
			peerChan := make(chan peer.AddrInfo, numPeers)
			go func() {
				defer close(peerChan)
				for _, pi := range addrInfos {
					select {
					case peerChan <- pi:
					case <-ctx.Done():
						return
					}
				}
			}()
			return peerChan
		}),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			nodeDHT, err = dht.New(ctx, h,
				dht.Mode(dht.ModeServer),
				dht.BootstrapPeers(addrInfos...),
				dht.BucketSize(DefaultBucketSize),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create DHT: %w", err)
			}
			routingTable, err = kbucket.NewRoutingTable(
				DefaultBucketSize,
				kbucket.ConvertPeerID(h.ID()),
				time.Minute,
				h.Peerstore(),
				10*time.Second,
				nil,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create routing table: %w", err)
			}
			return nodeDHT, nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	// Initialize PubSub
	ps, err := pubsub.NewGossipSub(
		ctx,
		h,
		pubsub.WithPeerOutboundQueueSize(PeerOutboundBufferSize),
		pubsub.WithValidateQueueSize(ValidateBufferSize),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pubsub: %w", err)
	}

	// Join content announcement topic
	contentAnnounceTopic, err := ps.Join(TopicContentAnnounce)
	if err != nil {
		return nil, fmt.Errorf("failed to join content announce topic: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	bn := &BootNode{
		host:                  h,
		ctx:                   nodeCtx,
		cancel:                cancel,
		dht:                   nodeDHT,
		routingTable:          routingTable,
		pubsub:                ps,
		contentAnnounceTopic:  contentAnnounceTopic,
		dialQueue:             NewDialQueue(),
		connectionCounts:      NewBlankConnectionInfo(MaxInboundPeers, MaxOutboundPeers),
		bootnodes:             &bootnodesWrapper{bootnodeArr: bootnodeArr, bootnodesMap: bootnodesMap},
		peerEvents:            make([]PeerEvent, 0, MaxPeerEventLogs),
		PeerConnectedCallback: func(p peer.ID) {},
	}

	// Set stream handlers to reject content and chunk requests
	h.SetStreamHandler(ProtocolContentExchange, func(s network.Stream) {
		log.Printf("Received content stream from %s, rejecting as bootnode does not store data", s.Conn().RemotePeer())
		mtr.NetworkErrorsTotal.WithLabelValues("content_rejected").Inc()
		s.Write([]byte("ERROR: bootnode does not store data"))
		s.Reset()
	})
	h.SetStreamHandler(ProtocolChunkExchange, func(s network.Stream) {
		log.Printf("Received chunk stream from %s, rejecting as bootnode does not store data", s.Conn().RemotePeer())
		mtr.NetworkErrorsTotal.WithLabelValues("chunk_rejected").Inc()
		s.Write([]byte("ERROR: bootnode does not store data"))
		s.Reset()
	})

	// Set up mDNS
	mdnsService := mdns.NewMdnsService(h, ServiceTag, &discoveryNotifiee{node: bn})
	if err := mdnsService.Start(); err != nil {
		log.Printf("Warning: failed to start mDNS: %v", err)
	} else {
		bn.mdns = mdnsService
	}

	// Set up network notifiee
	h.Network().Notify(&networkNotifiee{node: bn})

	// Subscribe to content announcement topic to forward announcements
	if err := bn.subscribeContentAnnounce(); err != nil {
		log.Printf("Warning: failed to subscribe to content announce topic: %v", err)
	}

	// Start discovery and dial loops
	go bn.startDiscovery()
	go bn.runDial()
	go bn.keepAliveMinimumPeerConnections()

	log.Printf("BootNode started with ID: %s", h.ID().String())
	log.Printf("Listening on addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  %s/p2p/%s", addr, h.ID().String())
	}

	// Initial bootstrap
	go func() {
		if err := bn.bootstrapDHT(); err != nil {
			log.Printf("Warning: failed to bootstrap DHT: %v", err)
		}
		if err := bn.connectToBootnodes(allBootnodes); err != nil {
			log.Printf("Warning: failed to connect to some bootnodes: %v", err)
		}
	}()

	return bn, nil
}

// Close shuts down the boot node
func (bn *BootNode) Close() error {
	var errs []error
	if bn.mdns != nil {
		if err := bn.mdns.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing mDNS: %w", err))
		}
	}
	if bn.contentAnnounceTopic != nil {
		if err := bn.contentAnnounceTopic.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing content announce topic: %w", err))
		}
	}
	if bn.dht != nil {
		if err := bn.dht.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing DHT: %w", err))
		}
	}
	bn.dialQueue.Close()
	bn.cancel()
	if err := bn.host.Close(); err != nil {
		errs = append(errs, fmt.Errorf("error closing host: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}
	return nil
}

// ID returns the node's peer ID
func (bn *BootNode) ID() peer.ID {
	return bn.host.ID()
}

// Addrs returns the node's addresses
func (bn *BootNode) Addrs() []string {
	var addrs []string
	for _, addr := range bn.host.Addrs() {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", addr, bn.host.ID().String()))
	}
	return addrs
}

// ConnectedPeers returns connected peers
func (bn *BootNode) ConnectedPeers() []peer.ID {
	return bn.routingTable.ListPeers()
}

// Connect connects to a peer
func (bn *BootNode) Connect(ctx context.Context, peerAddr string) error {
	addrInfo, err := peer.AddrInfoFromString(peerAddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer address %s: %w", peerAddr, err)
	}
	bn.dialQueue.AddTask(addrInfo, 1)
	return nil
}

// logPeerEvent logs a peer event
func (bn *BootNode) logPeerEvent(peerID peer.ID, eventType string, addrs []string) {
	bn.peerEventsMu.Lock()
	defer bn.peerEventsMu.Unlock()
	event := PeerEvent{
		PeerID:    peerID,
		Type:      eventType,
		Timestamp: time.Now(),
		Addresses: addrs,
	}
	bn.peerEvents = append(bn.peerEvents, event)
	if len(bn.peerEvents) > MaxPeerEventLogs {
		bn.peerEvents = bn.peerEvents[len(bn.peerEvents)-MaxPeerEventLogs:]
	}
	log.Printf("Peer %s event: %s at %s, Addresses: %v",
		peerID.String(), eventType, event.Timestamp.Format(time.RFC3339), addrs)
}

// GetLatestPeerEvents returns the latest peer events
func (bn *BootNode) GetLatestPeerEvents(limit int) []PeerEvent {
	bn.peerEventsMu.RLock()
	defer bn.peerEventsMu.RUnlock()
	if limit <= 0 || limit > len(bn.peerEvents) {
		limit = len(bn.peerEvents)
	}
	result := make([]PeerEvent, limit)
	for i := 0; i < limit; i++ {
		result[i] = bn.peerEvents[len(bn.peerEvents)-1-i]
	}
	return result
}

// networkNotifiee handles connection events
type networkNotifiee struct {
	node *BootNode
}

func (n *networkNotifiee) Connected(net network.Network, conn network.Conn) {
	mtr.PeerConnectionsTotal.Inc()
	peerID := conn.RemotePeer()
	addrs := make([]string, 0, len(n.node.host.Peerstore().Addrs(peerID)))
	for _, addr := range n.node.host.Peerstore().Addrs(peerID) {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(peerID, "connected", addrs)
	n.node.connectionCounts.UpdateConnCountByDirection(1, conn.Stat().Direction)
	n.node.routingTable.TryAddPeer(peerID, false, false)
	n.node.PeerConnectedCallback(peerID)
}

func (n *networkNotifiee) Disconnected(net network.Network, conn network.Conn) {
	mtr.PeerDisconnectionsTotal.Inc()
	peerID := conn.RemotePeer()
	addrs := make([]string, 0, len(n.node.host.Peerstore().Addrs(peerID)))
	for _, addr := range n.node.host.Peerstore().Addrs(peerID) {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(peerID, "disconnected", addrs)
	n.node.connectionCounts.UpdateConnCountByDirection(-1, conn.Stat().Direction)
	n.node.routingTable.RemovePeer(peerID)
}

func (n *networkNotifiee) Listen(net network.Network, addr multiaddr.Multiaddr)      {}
func (n *networkNotifiee) ListenClose(net network.Network, addr multiaddr.Multiaddr) {}

// discoveryNotifiee handles peer discovery
type discoveryNotifiee struct {
	node *BootNode
}

func (n *discoveryNotifiee) HandlePeerFound(pi peer.AddrInfo) {
	addrs := make([]string, 0, len(pi.Addrs))
	for _, addr := range pi.Addrs {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(pi.ID, "discovered", addrs)
	n.node.dialQueue.AddTask(&pi, 1) // Priority 1 for discovered peers
}

// subscribeContentAnnounce subscribes to content announcement topic to forward announcements
func (bn *BootNode) subscribeContentAnnounce() error {
	sub, err := bn.contentAnnounceTopic.Subscribe(pubsub.WithBufferSize(SubscribeOutputBufferSize))
	if err != nil {
		return fmt.Errorf("failed to subscribe to content announce topic: %w", err)
	}
	go bn.readContentAnnounceLoop(sub)
	return nil
}

// readContentAnnounceLoop forwards content announcement messages
func (bn *BootNode) readContentAnnounceLoop(sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(bn.ctx)
		if err != nil {
			if bn.ctx.Err() != nil {
				return
			}
			log.Printf("Failed to read content announce message: %v", err)
			mtr.NetworkErrorsTotal.WithLabelValues("read_content_announce").Inc()
			continue
		}
		log.Printf("Forwarding content announcement from %s", msg.GetFrom().String())
		addrs := bn.host.Peerstore().Addrs(msg.GetFrom())
		if len(addrs) > 0 {
			bn.host.Peerstore().AddAddr(msg.GetFrom(), addrs[0], peerstore.TempAddrTTL)
		}
		// Re-publish the announcement to ensure propagation
		if err := bn.contentAnnounceTopic.Publish(bn.ctx, msg.Data); err != nil {
			log.Printf("Failed to forward content announcement: %v", err)
			mtr.NetworkErrorsTotal.WithLabelValues("publish_content_announce").Inc()
		} else {
			mtr.NetworkMessagesTotal.WithLabelValues("content_announce").Inc()
		}
	}
}

// startDiscovery runs peer discovery loops
func (bn *BootNode) startDiscovery() {
	peerDiscoveryTicker := time.NewTicker(PeerDiscoveryInterval)
	bootnodeDiscoveryTicker := time.NewTicker(BootnodeDiscoveryInterval)
	defer func() {
		peerDiscoveryTicker.Stop()
		bootnodeDiscoveryTicker.Stop()
	}()
	for {
		select {
		case <-bn.ctx.Done():
			return
		case <-peerDiscoveryTicker.C:
			bn.regularPeerDiscovery()
		case <-bootnodeDiscoveryTicker.C:
			bn.bootnodePeerDiscovery()
		}
	}
}

// regularPeerDiscovery queries a random peer for new peers
func (bn *BootNode) regularPeerDiscovery() {
	if !bn.connectionCounts.HasFreeConnectionSlot(network.DirOutbound) {
		return
	}
	peerID := bn.getRandomPeer()
	if peerID == nil {
		return
	}
	log.Printf("Querying peer %s for new peers", peerID.String())
	peers := bn.routingTable.NearestPeers(kbucket.ConvertPeerID(*peerID), MaxDiscoveryPeerReqCount)
	for _, p := range peers {
		if p != *peerID && p != bn.host.ID() {
			info := bn.host.Peerstore().PeerInfo(p)
			bn.dialQueue.AddTask(&info, 1)
		}
	}
}

// bootnodePeerDiscovery queries a random bootnode
func (bn *BootNode) bootnodePeerDiscovery() {
	if !bn.connectionCounts.HasFreeConnectionSlot(network.DirOutbound) {
		return
	}
	bootnode := bn.getRandomBootnode()
	if bootnode == nil {
		return
	}
	isTemporaryDial := len(bn.bootnodes.bootnodeArr) > 0
	if isTemporaryDial {
		if _, loaded := bn.temporaryDials.LoadOrStore(bootnode.ID, true); loaded {
			return
		}
	}
	defer func() {
		if isTemporaryDial {
			bn.temporaryDials.Delete(bootnode.ID)
			if err := bn.host.Network().ClosePeer(bootnode.ID); err != nil {
				log.Printf("Failed to close temporary connection to %s: %v", bootnode.ID.String(), err)
			}
		}
	}()
	bn.dialQueue.AddTask(bootnode, 1)
	peers := bn.routingTable.NearestPeers(kbucket.ConvertPeerID(bootnode.ID), MaxDiscoveryPeerReqCount)
	for _, p := range peers {
		if p != bootnode.ID && p != bn.host.ID() {
			info := bn.host.Peerstore().PeerInfo(p)
			bn.dialQueue.AddTask(&info, 1)
		}
	}
}

// getRandomPeer returns a random connected peer
func (bn *BootNode) getRandomPeer() *peer.ID {
	peers := bn.routingTable.ListPeers()
	if len(peers) == 0 {
		return nil
	}
	randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(len(peers))))
	return &peers[randNum.Int64()]
}

// getRandomBootnode returns a random unconnected bootnode
func (bn *BootNode) getRandomBootnode() *peer.AddrInfo {
	nonConnectedNodes := make([]*peer.AddrInfo, 0, len(bn.bootnodes.bootnodeArr))
	for _, v := range bn.bootnodes.bootnodeArr {
		if bn.host.Network().Connectedness(v.ID) != network.Connected {
			nonConnectedNodes = append(nonConnectedNodes, v)
		}
	}
	if len(nonConnectedNodes) == 0 {
		return nil
	}
	randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(len(nonConnectedNodes))))
	return nonConnectedNodes[randNum.Int64()]
}

// runDial processes the dial queue
func (bn *BootNode) runDial() {
	slots := make(chan struct{}, MaxOutboundPeers)
	for i := int64(0); i < MaxOutboundPeers; i++ {
		slots <- struct{}{}
	}
	ctx, cancel := context.WithCancel(bn.ctx)
	defer cancel()
	for {
		if closed := bn.dialQueue.Wait(ctx); closed {
			return
		}
		for {
			tt := bn.dialQueue.PopTask()
			if tt == nil {
				break
			}
			peerInfo := tt.GetAddrInfo()
			if bn.host.Network().Connectedness(peerInfo.ID) == network.Connected {
				continue
			}
			log.Printf("Waiting for a dialing slot for %s", peerInfo.ID.String())
			select {
			case <-ctx.Done():
				return
			case <-slots:
				go func(pi *peer.AddrInfo) {
					defer func() { slots <- struct{}{} }()
					log.Printf("Dialing peer %s", pi.ID.String())
					ctxDial, cancelDial := context.WithTimeout(ctx, 30*time.Second)
					defer cancelDial()
					if err := bn.host.Connect(ctxDial, *pi); err != nil {
						log.Printf("Failed to dial %s: %v", pi.ID.String(), err)
						mtr.NetworkErrorsTotal.WithLabelValues("dial").Inc()
					}
				}(peerInfo)
			}
		}
	}
}

// keepAliveMinimumPeerConnections ensures minimum connections
func (bn *BootNode) keepAliveMinimumPeerConnections() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if len(bn.routingTable.ListPeers()) < MinimumPeerConnections {
				bootnode := bn.getRandomBootnode()
				if bootnode != nil {
					bn.dialQueue.AddTask(bootnode, 1)
				}
			}
		case <-bn.ctx.Done():
			return
		}
	}
}

// connectToBootnodes connects to bootnodes in parallel
func (bn *BootNode) connectToBootnodes(bootnodes []string) error {
	nodesToConnect := bootnodes
	if len(nodesToConnect) == 0 {
		nodesToConnect = DefaultBootnodes
	}
	if len(nodesToConnect) == 0 {
		log.Printf("No bootnodes specified")
		return nil
	}
	log.Printf("Connecting to %d bootnode(s)...", len(nodesToConnect))
	var wg sync.WaitGroup
	connectedCount := 0
	var lastErr error
	mu := sync.Mutex{}
	for _, bootnode := range nodesToConnect {
		if bootnode == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := bn.Connect(bn.ctx, addr); err != nil {
				mu.Lock()
				lastErr = err
				mu.Unlock()
				log.Printf("Failed to connect to bootnode %s: %v", addr, err)
			} else {
				mu.Lock()
				connectedCount++
				mu.Unlock()
			}
		}(bootnode)
	}
	wg.Wait()
	log.Printf("Connected to %d out of %d bootnodes", connectedCount, len(nodesToConnect))
	if connectedCount == 0 && len(nodesToConnect) > 0 {
		return fmt.Errorf("failed to connect to any bootnodes: %w", lastErr)
	}
	return nil
}

// bootstrapDHT bootstraps the DHT
func (bn *BootNode) bootstrapDHT() error {
	if bn.dht == nil {
		return fmt.Errorf("DHT not initialized")
	}
	log.Printf("Bootstrapping DHT...")
	ctx, cancel := context.WithTimeout(bn.ctx, 30*time.Second)
	defer cancel()
	return bn.dht.Bootstrap(ctx)
}

// GetNetworkStats returns network statistics
func (bn *BootNode) GetNetworkStats() map[string]interface{} {
	peers := bn.ConnectedPeers()
	bn.peerEventsMu.RLock()
	peerEvents := make([]map[string]interface{}, len(bn.peerEvents))
	for i, event := range bn.peerEvents {
		peerEvents[i] = map[string]interface{}{
			"peer_id":   event.PeerID.String(),
			"type":      event.Type,
			"timestamp": event.Timestamp.Format(time.RFC3339),
			"addresses": event.Addresses,
		}
	}
	bn.peerEventsMu.RUnlock()
	stats := map[string]interface{}{
		"peer_id":         bn.ID().String(),
		"connected_peers": len(peers),
		"peer_list":       make([]string, len(peers)),
		"addresses":       bn.Addrs(),
		"dht":             bn.GetDHTStats(),
		"peer_events":     peerEvents,
	}
	for i, peer := range peers {
		stats["peer_list"].([]string)[i] = peer.String()
	}
	return stats
}

// GetDHTStats returns DHT statistics
func (bn *BootNode) GetDHTStats() map[string]interface{} {
	if bn.dht == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}
	return map[string]interface{}{
		"enabled":     true,
		"peer_count":  bn.routingTable.Size(),
		"bucket_info": bn.routingTable.GetPeerInfos(),
	}
}
