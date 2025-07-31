package libp2p

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/mtr"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
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
	"github.com/multiformats/go-multihash"
	"github.com/patrickmn/go-cache"
)

const (
	ProtocolContentExchange   = protocol.ID("/openhashdb/content/1.0.0")
	ProtocolChunkExchange     = protocol.ID("/openhashdb/chunk/1.0.0")
	TopicContentAnnounce      = "/openhashdb/announce/1.0.0"
	ServiceTag                = "openhashdb"
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
	AnnouncementCacheTTL      = 5 * time.Minute
	AnnouncementCacheCleanup  = 10 * time.Minute
	AnnouncementRateLimit     = 1 * time.Second // New: Rate limit for processing announcements
)

// PeerEvent represents a peer discovery, connection, or disconnection event
type PeerEvent struct {
	PeerID    peer.ID   `json:"peer_id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Addresses []string  `json:"addresses"`
}

// Node represents a libp2p node with enhanced networking
type Node struct {
	host                  host.Host
	ctx                   context.Context
	cancel                context.CancelFunc
	mdns                  mdns.Service
	dht                   *dht.IpfsDHT
	routingTable          *kbucket.RoutingTable
	pubsub                *pubsub.PubSub
	contentAnnounceTopic  *pubsub.Topic
	storage               *storage.Storage
	dialQueue             *DialQueue
	connectionCounts      *ConnectionInfo
	bootnodes             *bootnodesWrapper
	temporaryDials        sync.Map
	peerEvents            []PeerEvent
	peerEventsMu          sync.RWMutex
	announcementCache     *cache.Cache
	ContentHandler        func(peer.ID, []byte) error
	ChunkHandler          func(peer.ID, []byte) error
	PeerConnectedCallback func(peer.ID)
	announceRateLimit     map[string]time.Time // New: Track last announcement time per peer
	announceRateLimitMu   sync.Mutex
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

// NewNodeWithKeyPath creates a new libp2p node
func NewNodeWithKeyPath(ctx context.Context, bootnodes []string, keyPath string) (*Node, error) {
	var privKey crypto.PrivKey
	var err error
	if keyPath != "" {
		privKey, err = loadOrCreateIdentity(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load identity: %w", err)
		}
	} else {
		log.Println("Warning: no keyPath, generating ephemeral identity")
		privKey, _, err = crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to generate ephemeral key: %w", err)
		}
	}

	// Combine bootnodes
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
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
		),
		libp2p.EnableRelay(),
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
				dht.Mode(dht.ModeAutoServer),
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

	// Initialize PubSub with enhanced message deduplication
	ps, err := pubsub.NewGossipSub(
		ctx,
		h,
		pubsub.WithPeerOutboundQueueSize(PeerOutboundBufferSize),
		pubsub.WithValidateQueueSize(ValidateBufferSize),
		pubsub.WithMessageIdFn(func(pmsg *pb.Message) string {
			hash := hasher.HashBytes(append(pmsg.Data, []byte(peer.ID(pmsg.GetFrom()).String())...))
			return base64.StdEncoding.EncodeToString(hash[:])
		}),
		pubsub.WithGossipSubParams(pubsub.GossipSubParams{
			D:                 6,
			Dlo:               4,
			Dhi:               8,
			Dlazy:             4,
			HeartbeatInterval: 700 * time.Millisecond, // Fix for non-positive ticker
			// Dscore:            0,
			// FanoutTTL:         60 * time.Second, // Fix for divide by zero
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pubsub: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pubsub: %w", err)
	}

	// Join content announcement topic
	contentAnnounceTopic, err := ps.Join(TopicContentAnnounce)
	if err != nil {
		return nil, fmt.Errorf("failed to join content announce topic: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	node := &Node{
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
		announcementCache:     cache.New(AnnouncementCacheTTL, AnnouncementCacheCleanup),
		announceRateLimit:     make(map[string]time.Time),
		PeerConnectedCallback: func(p peer.ID) {},
	}

	// Set up network notifiee
	h.Network().Notify(&networkNotifiee{node: node})

	// Set stream handlers
	h.SetStreamHandler(ProtocolContentExchange, node.handleContentStream)
	h.SetStreamHandler(ProtocolChunkExchange, node.handleChunkStream)

	// Subscribe to content announcement topic
	if err := node.subscribeContentAnnounce(); err != nil {
		log.Printf("Warning: failed to subscribe to content announce topic: %v", err)
	}

	// Setup mDNS
	if err := node.setupMDNS(); err != nil {
		log.Printf("Warning: failed to setup mDNS: %v", err)
	}

	// Start discovery and dial loops
	go node.startDiscovery()
	go node.runDial()
	go node.keepAliveMinimumPeerConnections()

	log.Printf("Node started with ID: %s", h.ID().String())
	log.Printf("Listening on addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  %s/p2p/%s", addr, h.ID().String())
	}

	// Initial bootstrap
	go func() {
		if err := node.bootstrapDHT(); err != nil {
			log.Printf("Warning: failed to bootstrap DHT: %v", err)
		}
		if err := node.connectToBootnodes(allBootnodes); err != nil {
			log.Printf("Warning: failed to connect to some bootnodes: %v", err)
		}
	}()

	return node, nil
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

// DefaultBootnodes for the VPS-hosted default node
var DefaultBootnodes = []string{
	"/ip4/148.251.35.204/tcp/9090/p2p/QmYQMdZkvC4R7DHqSkCKNh89Hs7gDPLjx9j9xnPsUkkS2P",
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

// logPeerEvent logs a peer event
func (n *Node) logPeerEvent(peerID peer.ID, eventType string, addrs []string) {
	n.peerEventsMu.Lock()
	defer n.peerEventsMu.Unlock()
	event := PeerEvent{
		PeerID:    peerID,
		Type:      eventType,
		Timestamp: time.Now(),
		Addresses: addrs,
	}
	n.peerEvents = append(n.peerEvents, event)
	if len(n.peerEvents) > MaxPeerEventLogs {
		n.peerEvents = n.peerEvents[len(n.peerEvents)-MaxPeerEventLogs:]
	}
	log.Printf("Peer %s event: %s at %s, Addresses: %v",
		peerID.String(), eventType, event.Timestamp.Format(time.RFC3339), addrs)
}

// GetLatestPeerEvents returns the latest peer events
func (n *Node) GetLatestPeerEvents(limit int) []PeerEvent {
	n.peerEventsMu.RLock()
	defer n.peerEventsMu.RUnlock()
	if limit <= 0 || limit > len(n.peerEvents) {
		limit = len(n.peerEvents)
	}
	result := make([]PeerEvent, limit)
	for i := 0; i < limit; i++ {
		result[i] = n.peerEvents[len(n.peerEvents)-1-i]
	}
	return result
}

// networkNotifiee handles connection events
type networkNotifiee struct {
	node *Node
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

// setupMDNS sets up mDNS discovery
func (n *Node) setupMDNS() error {
	mdnsService := mdns.NewMdnsService(n.host, ServiceTag, &discoveryNotifee{node: n})
	if err := mdnsService.Start(); err != nil {
		return fmt.Errorf("failed to start mDNS: %w", err)
	}
	n.mdns = mdnsService
	return nil
}

// discoveryNotifee handles peer discovery
type discoveryNotifee struct {
	node *Node
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	addrs := make([]string, 0, len(pi.Addrs))
	for _, addr := range pi.Addrs {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(pi.ID, "discovered", addrs)
	n.node.dialQueue.AddTask(&pi, 1) // Priority 1 for discovered peers
}

// startDiscovery runs peer discovery loops
func (n *Node) startDiscovery() {
	peerDiscoveryTicker := time.NewTicker(PeerDiscoveryInterval)
	bootnodeDiscoveryTicker := time.NewTicker(BootnodeDiscoveryInterval)
	defer func() {
		peerDiscoveryTicker.Stop()
		bootnodeDiscoveryTicker.Stop()
	}()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-peerDiscoveryTicker.C:
			n.regularPeerDiscovery()
		case <-bootnodeDiscoveryTicker.C:
			n.bootnodePeerDiscovery()
		}
	}
}

// regularPeerDiscovery queries a random peer for new peers
func (n *Node) regularPeerDiscovery() {
	if !n.connectionCounts.HasFreeConnectionSlot(network.DirOutbound) {
		return
	}
	peerID := n.getRandomPeer()
	if peerID == nil {
		return
	}
	log.Printf("Querying peer %s for new peers", peerID.String())
	peers := n.routingTable.NearestPeers(kbucket.ConvertPeerID(*peerID), MaxDiscoveryPeerReqCount)
	for _, p := range peers {
		if p != *peerID && p != n.host.ID() {
			info := n.host.Peerstore().PeerInfo(p)
			n.dialQueue.AddTask(&info, 1)
		}
	}
}

// bootnodePeerDiscovery queries a random bootnode
func (n *Node) bootnodePeerDiscovery() {
	if !n.connectionCounts.HasFreeConnectionSlot(network.DirOutbound) {
		return
	}
	bootnode := n.getRandomBootnode()
	if bootnode == nil {
		return
	}
	isTemporaryDial := len(n.bootnodes.bootnodeArr) > 0
	if isTemporaryDial {
		if _, loaded := n.temporaryDials.LoadOrStore(bootnode.ID, true); loaded {
			return
		}
	}
	defer func() {
		if isTemporaryDial {
			n.temporaryDials.Delete(bootnode.ID)
			if err := n.host.Network().ClosePeer(bootnode.ID); err != nil {
				log.Printf("Failed to close temporary connection to %s: %v", bootnode.ID.String(), err)
			}
		}
	}()
	n.dialQueue.AddTask(bootnode, 1)
	peers := n.routingTable.NearestPeers(kbucket.ConvertPeerID(bootnode.ID), MaxDiscoveryPeerReqCount)
	for _, p := range peers {
		if p != bootnode.ID && p != n.host.ID() {
			info := n.host.Peerstore().PeerInfo(p)
			n.dialQueue.AddTask(&info, 1)
		}
	}
}

// getRandomPeer returns a random connected peer
func (n *Node) getRandomPeer() *peer.ID {
	peers := n.routingTable.ListPeers()
	if len(peers) == 0 {
		return nil
	}
	randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(len(peers))))
	return &peers[randNum.Int64()]
}

// getRandomBootnode returns a random unconnected bootnode
func (n *Node) getRandomBootnode() *peer.AddrInfo {
	nonConnectedNodes := make([]*peer.AddrInfo, 0, len(n.bootnodes.bootnodeArr))
	for _, v := range n.bootnodes.bootnodeArr {
		if n.host.Network().Connectedness(v.ID) != network.Connected {
			nonConnectedNodes = append(nonConnectedNodes, v)
		}
	}
	if len(nonConnectedNodes) == 0 {
		return nil
	}
	randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(len(nonConnectedNodes))))
	return nonConnectedNodes[randNum.Int64()]
}

// runDial processes the dial queue with relay support
func (n *Node) runDial() {
	slots := make(chan struct{}, MaxOutboundPeers)
	for i := int64(0); i < MaxOutboundPeers; i++ {
		slots <- struct{}{}
	}
	ctx, cancel := context.WithCancel(n.ctx)
	defer cancel()
	for {
		if closed := n.dialQueue.Wait(ctx); closed {
			return
		}
		for {
			tt := n.dialQueue.PopTask()
			if tt == nil {
				break
			}
			peerInfo := tt.GetAddrInfo()
			if n.host.Network().Connectedness(peerInfo.ID) == network.Connected {
				continue
			}
			log.Printf("Waiting for a dialing slot for %s", peerInfo.ID.String())
			select {
			case <-ctx.Done():
				return
			case <-slots:
				go func(pi *peer.AddrInfo) {
					defer func() { slots <- struct{}{} }()
					log.Printf("Dialing peer %s (direct)", pi.ID.String())
					ctxDial, cancelDial := context.WithTimeout(ctx, 30*time.Second)
					defer cancelDial()
					if err := n.host.Connect(ctxDial, *pi); err != nil {
						log.Printf("Direct dial to %s failed: %v, attempting relay", pi.ID.String(), err)
						mtr.NetworkErrorsTotal.WithLabelValues("dial_direct").Inc()
						// Try relayed connection via a bootnode
						if bootnode := n.getRandomBootnode(); bootnode != nil {
							log.Printf("Attempting relayed connection to %s via %s", pi.ID.String(), bootnode.ID.String())
							ctxRelay, cancelRelay := context.WithTimeout(ctx, 30*time.Second)
							defer cancelRelay()
							relayAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/p2p/%s/p2p-circuit/p2p/%s", bootnode.ID.String(), pi.ID.String()))
							if err != nil {
								log.Printf("Failed to create relay address for %s: %v", pi.ID.String(), err)
								mtr.NetworkErrorsTotal.WithLabelValues("relay_addr").Inc()
								return
							}
							relayInfo := peer.AddrInfo{ID: pi.ID, Addrs: []multiaddr.Multiaddr{relayAddr}}
							if err := n.host.Connect(ctxRelay, relayInfo); err != nil {
								log.Printf("Relayed dial to %s via %s failed: %v", pi.ID.String(), bootnode.ID.String(), err)
								mtr.NetworkErrorsTotal.WithLabelValues("dial_relay").Inc()
							} else {
								log.Printf("Successfully connected to %s via relay %s", pi.ID.String(), bootnode.ID.String())
							}
						}
					} else {
						log.Printf("Successfully connected to %s (direct)", pi.ID.String())
					}
				}(peerInfo)
			}
		}
	}
}

// keepAliveMinimumPeerConnections ensures minimum connections
func (n *Node) keepAliveMinimumPeerConnections() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if len(n.routingTable.ListPeers()) < MinimumPeerConnections {
				bootnode := n.getRandomBootnode()
				if bootnode != nil {
					n.dialQueue.AddTask(bootnode, 1)
				}
			}
		case <-n.ctx.Done():
			return
		}
	}
}

// Close shuts down the node
func (n *Node) Close() error {
	if n.mdns != nil {
		if err := n.mdns.Close(); err != nil {
			log.Printf("Error closing mDNS: %v", err)
		}
	}
	if n.contentAnnounceTopic != nil {
		if err := n.contentAnnounceTopic.Close(); err != nil {
			log.Printf("Error closing content announce topic: %v", err)
		}
	}
	if n.dht != nil {
		if err := n.dht.Close(); err != nil {
			log.Printf("Error closing DHT: %v", err)
		}
	}
	n.dialQueue.Close()
	n.cancel()
	return n.host.Close()
}

// ID returns the node's peer ID
func (n *Node) ID() peer.ID {
	return n.host.ID()
}

// Addrs returns the node's addresses
func (n *Node) Addrs() []string {
	var addrs []string
	for _, addr := range n.host.Addrs() {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", addr, n.host.ID().String()))
	}
	return addrs
}

// ConnectedPeers returns connected peers
func (n *Node) ConnectedPeers() []peer.ID {
	return n.routingTable.ListPeers()
}

// Connect connects to a peer
func (n *Node) Connect(ctx context.Context, peerAddr string) error {
	addrInfo, err := peer.AddrInfoFromString(peerAddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer address %s: %w", peerAddr, err)
	}
	n.dialQueue.AddTask(addrInfo, 1)
	return nil
}

// SendContent sends content to a peer
func (n *Node) SendContent(ctx context.Context, peerID peer.ID, data []byte) error {
	mtr.NetworkMessagesTotal.WithLabelValues("content").Inc()
	return n.sendData(ctx, peerID, ProtocolContentExchange, data)
}

// SendChunk sends a chunk to a peer
func (n *Node) SendChunk(ctx context.Context, peerID peer.ID, data []byte) error {
	mtr.NetworkMessagesTotal.WithLabelValues("chunk").Inc()
	return n.sendData(ctx, peerID, ProtocolChunkExchange, data)
}

// sendData sends data to a peer with exponential backoff
func (n *Node) sendData(ctx context.Context, peerID peer.ID, protocolID protocol.ID, data []byte) error {
	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		stream, err := n.host.NewStream(ctx, peerID, protocolID)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to open stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("send_data").Inc()
			if attempt == maxRetries {
				return fmt.Errorf("failed to open stream to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		defer stream.Close()
		stream.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := stream.Write(data); err != nil {
			log.Printf("Attempt %d/%d: Failed to write data to %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("send_data").Inc()
			if attempt == maxRetries {
				return fmt.Errorf("failed to write data to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to send data to %s: max retries exceeded", peerID.String())
}

// handleContentStream handles content streams
func (n *Node) handleContentStream(stream network.Stream) {
	defer stream.Close()
	mtr.NetworkMessagesTotal.WithLabelValues("content_received").Inc()
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling content stream from %s", remotePeer.String())
	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	buf := make([]byte, 256)
	bytesRead, err := stream.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Failed to read content request from %s: %v", remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("read_content_request").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to read content request: %v", err)))
		return
	}
	contentHashStr := string(buf[:bytesRead])
	log.Printf("Received content request for %s from %s", contentHashStr, remotePeer.String())
	if n.storage == nil {
		log.Printf("Error: storage not configured for %s", remotePeer.String())
		mtr.NetworkErrorsTotal.WithLabelValues("storage_not_configured").Inc()
		_, _ = stream.Write([]byte("ERROR: storage not configured"))
		return
	}
	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		log.Printf("Invalid hash %s from %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("invalid_hash").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: invalid hash: %v", err)))
		return
	}
	if err := n.storage.ValidateContent(hash); err != nil {
		log.Printf("Content validation failed for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("content_validation").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: %v", err)))
		return
	}
	metadata, err := n.storage.GetContent(hash)
	if err != nil {
		log.Printf("Failed to get metadata for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("get_metadata").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to get metadata: %v", err)))
		return
	}
	dataStream, err := n.storage.GetDataStream(hash)
	if err != nil {
		log.Printf("Failed to get data stream for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("get_data").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to get data: %v", err)))
		return
	}
	defer dataStream.Close()
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		log.Printf("Failed to marshal metadata for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("marshal_metadata").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to marshal metadata: %v", err)))
		return
	}
	stream.SetWriteDeadline(time.Now().Add(60 * time.Second))
	if err := binary.Write(stream, binary.BigEndian, uint32(len(metaBytes))); err != nil {
		log.Printf("Failed to write metadata length for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("write_metadata_length").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write metadata length: %v", err)))
		return
	}
	if _, err := stream.Write(metaBytes); err != nil {
		log.Printf("Failed to write metadata for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("write_metadata").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write metadata: %v", err)))
		return
	}
	stream.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	bytesSent, err := io.Copy(stream, dataStream)
	if err != nil {
		log.Printf("Failed to write content for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("write_content").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write content: %v", err)))
		return
	}
	log.Printf("Sent %d bytes of content %s to %s", bytesSent, contentHashStr, remotePeer.String())
	if err := stream.CloseWrite(); err != nil {
		log.Printf("Failed to close write stream for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("close_write_stream").Inc()
	}
}

// handleChunkStream handles chunk streams
func (n *Node) handleChunkStream(stream network.Stream) {
	defer stream.Close()
	mtr.NetworkMessagesTotal.WithLabelValues("chunk_received").Inc()
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling chunk stream from %s", remotePeer.String())
	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	data := make([]byte, 1024*1024)
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read chunk stream from %s: %v", remotePeer.String(), err)
		mtr.NetworkErrorsTotal.WithLabelValues("read_chunk_stream").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to read chunk: %v", err)))
		return
	}
	if n.ChunkHandler != nil {
		if err := n.ChunkHandler(remotePeer, data[:bytesRead]); err != nil {
			log.Printf("Chunk handler error from %s: %v", remotePeer.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("chunk_handler").Inc()
			_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: chunk handler failed: %v", err)))
		}
	}
}

// subscribeContentAnnounce subscribes to content announcement topic
func (n *Node) subscribeContentAnnounce() error {
	sub, err := n.contentAnnounceTopic.Subscribe(pubsub.WithBufferSize(SubscribeOutputBufferSize))
	if err != nil {
		return fmt.Errorf("failed to subscribe to content announce topic: %w", err)
	}
	go n.readContentAnnounceLoop(sub)
	return nil
}

// readContentAnnounceLoop processes content announcement messages
func (n *Node) readContentAnnounceLoop(sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(n.ctx)
		if err != nil {
			if n.ctx.Err() != nil {
				return
			}
			log.Printf("Failed to read content announce message: %v", err)
			mtr.NetworkErrorsTotal.WithLabelValues("read_content_announce").Inc()
			continue
		}
		hashStr := string(msg.Data)
		peerID := msg.GetFrom()
		cacheKey := fmt.Sprintf("%s:%s", hashStr, peerID.String())

		// Rate limit announcements per peer
		n.announceRateLimitMu.Lock()
		if lastAnnounce, exists := n.announceRateLimit[cacheKey]; exists && time.Since(lastAnnounce) < AnnouncementRateLimit {
			n.announceRateLimitMu.Unlock()
			continue // Skip if rate limit exceeded
		}
		n.announceRateLimit[cacheKey] = time.Now()
		n.announceRateLimitMu.Unlock()

		// Check announcement cache
		if _, found := n.announcementCache.Get(cacheKey); found {
			continue // Skip duplicate announcement
		}
		n.announcementCache.Set(cacheKey, true, AnnouncementCacheTTL)
		log.Printf("Received content announcement for %s from %s", hashStr, peerID.String())

		// Update peerstore with addresses
		addrs := n.host.Peerstore().Addrs(peerID)
		if len(addrs) > 0 {
			n.host.Peerstore().AddAddr(peerID, addrs[0], peerstore.TempAddrTTL)
		}

		// Forward announcement only if not from self
		if peerID != n.host.ID() {
			if err := n.contentAnnounceTopic.Publish(n.ctx, []byte(hashStr)); err != nil {
				log.Printf("Failed to forward content announcement for %s: %v", hashStr, err)
				mtr.NetworkErrorsTotal.WithLabelValues("forward_content_announce").Inc()
			} else {
				log.Printf("Forwarded content announcement for %s from %s", hashStr, peerID.String())
			}
		}

		// Automatically attempt to fetch content if not already stored
		if n.storage != nil {
			hash, err := hasher.HashFromString(hashStr)
			if err == nil && n.storage.ValidateContent(hash) != nil {
				go func() {
					_, _, err := n.RequestContentFromPeer(peerID, hashStr)
					if err != nil {
						log.Printf("Failed to fetch content %s from %s: %v", hashStr, peerID.String(), err)
					}
				}()
			}
		}
	}
}

// AnnounceContent announces content availability
func (n *Node) AnnounceContent(contentHashStr string) error {
	if n.storage == nil {
		log.Printf("Storage not configured for content %s", contentHashStr)
		return fmt.Errorf("storage not configured")
	}
	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		return fmt.Errorf("invalid content hash: %w", err)
	}
	if err := n.storage.ValidateContent(hash); err != nil {
		log.Printf("Validation failed for content %s: %v", contentHashStr, err)
		return err
	}
	// Announce via DHT
	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
		defer cancel()
		mh, err := multihash.Sum([]byte(contentHashStr), multihash.SHA2_256, -1)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to create multihash for %s: %v", attempt, maxRetries, contentHashStr, err)
			if attempt == maxRetries {
				return fmt.Errorf("failed to create multihash: %w", err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		contentCID := cid.NewCidV1(cid.Raw, mh)
		log.Printf("Attempt %d/%d: Announcing content provider for hash: %s (CID: %s)", attempt, maxRetries, contentHashStr, contentCID.String())
		if err := n.dht.Provide(ctx, contentCID, true); err != nil {
			log.Printf("Attempt %d/%d: Failed to announce content %s: %v", attempt, maxRetries, contentHashStr, err)
			if attempt == maxRetries {
				return fmt.Errorf("failed to announce content after %d attempts: %w", maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		// Announce via PubSub with throttling
		cacheKey := fmt.Sprintf("%s:%s", contentHashStr, n.ID().String())
		if _, found := n.announcementCache.Get(cacheKey); found {
			log.Printf("Skipping duplicate announcement for %s", contentHashStr)
			return nil
		}
		n.announceRateLimitMu.Lock()
		if lastAnnounce, exists := n.announceRateLimit[cacheKey]; exists && time.Since(lastAnnounce) < AnnouncementRateLimit {
			n.announceRateLimitMu.Unlock()
			log.Printf("Rate limit hit for announcing %s", contentHashStr)
			return nil
		}
		n.announceRateLimit[cacheKey] = time.Now()
		n.announceRateLimitMu.Unlock()

		n.announcementCache.Set(cacheKey, true, AnnouncementCacheTTL)
		if err := n.contentAnnounceTopic.Publish(n.ctx, []byte(contentHashStr)); err != nil {
			log.Printf("Failed to publish content announcement for %s: %v", contentHashStr, err)
			mtr.NetworkErrorsTotal.WithLabelValues("publish_content_announce").Inc()
			continue
		}
		mtr.NetworkMessagesTotal.WithLabelValues("content_announce").Inc()
		return nil
	}
	return fmt.Errorf("failed to announce content %s: max retries exceeded", contentHashStr)
}

// FindContentProviders finds content providers
func (n *Node) FindContentProviders(contentHash string) ([]peer.AddrInfo, error) {
	if n.dht == nil {
		return nil, fmt.Errorf("DHT not initialized")
	}
	ctx, cancel := context.WithTimeout(n.ctx, 90*time.Second)
	defer cancel()
	hash, err := multihash.FromHexString(contentHash)
	if err != nil {
		mh, err := multihash.Sum([]byte(contentHash), multihash.SHA2_256, -1)
		if err != nil {
			log.Printf("Failed to create multihash for %s: %v", contentHash, err)
			return nil, fmt.Errorf("failed to create multihash: %w", err)
		}
		hash = mh
	}
	contentCID := cid.NewCidV1(cid.Raw, hash)
	log.Printf("Finding providers for content hash: %s (CID: %s)", contentHash, contentCID.String())
	providers := n.dht.FindProvidersAsync(ctx, contentCID, 20)
	var result []peer.AddrInfo
	for provider := range providers {
		if provider.ID != n.ID() {
			result = append(result, provider)
			log.Printf("Found provider: %s for hash %s", provider.ID.String(), contentHash)
			n.dialQueue.AddTask(&provider, 1)
		}
	}
	log.Printf("Found %d provider(s) for hash: %s", len(result), contentHash)
	return result, nil
}

// RequestContentFromPeer requests content from a peer with relay support
func (n *Node) RequestContentFromPeer(peerID peer.ID, contentHash string) ([]byte, *storage.ContentMetadata, error) {
	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
		defer cancel()
		log.Printf("Attempt %d/%d: Requesting content %s from %s", attempt, maxRetries, contentHash, peerID.String())

		// Ensure connectivity (direct or relayed)
		if n.host.Network().Connectedness(peerID) != network.Connected {
			pi := peer.AddrInfo{ID: peerID, Addrs: n.host.Peerstore().Addrs(peerID)}
			if err := n.host.Connect(ctx, pi); err != nil {
				log.Printf("Attempt %d/%d: Direct connect to %s failed: %v, trying relay", attempt, maxRetries, peerID.String(), err)
				// Try relayed connection
				bootnode := n.getRandomBootnode()
				if bootnode != nil {
					log.Printf("Attempting relayed connection to %s via %s", peerID.String(), bootnode.ID.String())
					relayAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/p2p/%s/p2p-circuit/p2p/%s", bootnode.ID.String(), peerID.String()))
					if err != nil {
						log.Printf("Failed to create relay address for %s: %v", peerID.String(), err)
						mtr.NetworkErrorsTotal.WithLabelValues("relay_addr").Inc()
						if attempt == maxRetries {
							return nil, nil, fmt.Errorf("failed to create relay address for %s: %w", peerID.String(), err)
						}
						mtr.NetworkRetriesTotal.Inc()
						time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
						continue
					}
					relayInfo := peer.AddrInfo{ID: peerID, Addrs: []multiaddr.Multiaddr{relayAddr}}
					if err := n.host.Connect(ctx, relayInfo); err != nil {
						log.Printf("Attempt %d/%d: Relayed connect to %s via %s failed: %v", attempt, maxRetries, peerID.String(), bootnode.ID.String(), err)
						if attempt == maxRetries {
							return nil, nil, fmt.Errorf("failed to connect to %s via relay after %d attempts: %w", peerID.String(), maxRetries, err)
						}
						mtr.NetworkRetriesTotal.Inc()
						time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
						continue
					}
					log.Printf("Successfully connected to %s via relay %s", peerID.String(), bootnode.ID.String())
				} else {
					log.Printf("No bootnode available for relay to %s", peerID.String())
					if attempt == maxRetries {
						return nil, nil, fmt.Errorf("failed to connect to %s after %d attempts: no relay available", peerID.String(), maxRetries)
					}
					mtr.NetworkRetriesTotal.Inc()
					time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
					continue
				}
			}
		}

		stream, err := n.host.NewStream(ctx, peerID, ProtocolContentExchange)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to open stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("open_stream").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to open stream to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		defer stream.Close()
		stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := stream.Write([]byte(contentHash)); err != nil {
			log.Printf("Attempt %d/%d: Failed to send content request to %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("write_content_request").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to send content request to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		if err := stream.CloseWrite(); err != nil {
			log.Printf("Attempt %d/%d: Failed to close write stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("close_write_stream").Inc()
			continue
		}
		stream.SetReadDeadline(time.Now().Add(60 * time.Second))
		buf := make([]byte, 256)
		nBytes, err := stream.Read(buf)
		if err == nil && nBytes > 0 && bytes.HasPrefix(buf[:nBytes], []byte("ERROR:")) {
			log.Printf("Received error from %s: %s", peerID.String(), string(buf[:nBytes]))
			mtr.NetworkErrorsTotal.WithLabelValues("remote_error").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("remote error from %s: %s", peerID.String(), string(buf[:nBytes]))
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		if err != nil && err != io.EOF {
			log.Printf("Attempt %d/%d: Failed to read initial response from %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("read_response").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read response from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		combinedReader := io.MultiReader(bytes.NewReader(buf[:nBytes]), stream)
		var metaLen uint32
		if err := binary.Read(combinedReader, binary.BigEndian, &metaLen); err != nil {
			log.Printf("Attempt %d/%d: Failed to read metadata length from %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("read_metadata_length").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read metadata length from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		metaBytes := make([]byte, metaLen)
		if _, err := io.ReadFull(combinedReader, metaBytes); err != nil {
			log.Printf("Attempt %d/%d: Failed to read metadata from %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("read_metadata").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read metadata from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		var metadata storage.ContentMetadata
		if err := json.Unmarshal(metaBytes, &metadata); err != nil {
			log.Printf("Attempt %d/%d: Failed to unmarshal metadata from %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("unmarshal_metadata").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to unmarshal metadata from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		data, err := io.ReadAll(combinedReader)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to read content from %s: %v", attempt, maxRetries, peerID.String(), err)
			mtr.NetworkErrorsTotal.WithLabelValues("read_content").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read content from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		if metadata.ContentHash == (hasher.Hash{}) {
			log.Printf("Warning: metadata from %s for %s has no ContentHash", peerID.String(), contentHash)
			metadata.ContentHash = hasher.HashBytes(data)
		}
		receivedContentHash := hasher.HashBytes(data)
		if !bytes.Equal(receivedContentHash[:], metadata.ContentHash[:]) {
			log.Printf("Attempt %d/%d: Content hash mismatch from %s: expected %s, got %s", attempt, maxRetries, peerID.String(), metadata.ContentHash.String(), receivedContentHash.String())
			mtr.NetworkErrorsTotal.WithLabelValues("hash_mismatch").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("content hash mismatch from %s: expected %s, got %s", peerID.String(), metadata.ContentHash.String(), receivedContentHash.String())
			}
			mtr.NetworkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		log.Printf("Successfully fetched %d bytes of content %s from %s", len(data), contentHash, peerID.String())
		return data, &metadata, nil
	}
	return nil, nil, fmt.Errorf("failed to fetch content %s from %s: max retries exceeded", contentHash, peerID.String())
}

// GetNetworkStats returns network statistics
func (n *Node) GetNetworkStats() map[string]interface{} {
	peers := n.ConnectedPeers()
	n.peerEventsMu.RLock()
	peerEvents := make([]map[string]interface{}, len(n.peerEvents))
	for i, event := range n.peerEvents {
		peerEvents[i] = map[string]interface{}{
			"peer_id":   event.PeerID.String(),
			"type":      event.Type,
			"timestamp": event.Timestamp.Format(time.RFC3339),
			"addresses": event.Addresses,
		}
	}
	n.peerEventsMu.RUnlock()
	stats := map[string]interface{}{
		"peer_id":         n.ID().String(),
		"connected_peers": len(peers),
		"peer_list":       make([]string, len(peers)),
		"addresses":       n.Addrs(),
		"dht":             n.GetDHTStats(),
		"peer_events":     peerEvents,
	}
	for i, peer := range peers {
		stats["peer_list"].([]string)[i] = peer.String()
	}
	return stats
}

// GetDHTStats returns DHT statistics
func (n *Node) GetDHTStats() map[string]interface{} {
	if n.dht == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}
	return map[string]interface{}{
		"enabled":     true,
		"peer_count":  n.routingTable.Size(),
		"bucket_info": n.routingTable.GetPeerInfos(),
	}
}

// connectToBootnodes connects to bootnodes
func (n *Node) connectToBootnodes(bootnodes []string) error {
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
			if err := n.Connect(n.ctx, addr); err != nil {
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
func (n *Node) bootstrapDHT() error {
	if n.dht == nil {
		return fmt.Errorf("DHT not initialized")
	}
	log.Printf("Bootstrapping DHT...")
	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()
	return n.dht.Bootstrap(ctx)
}

// SetStorage sets the storage backend
func (n *Node) SetStorage(s *storage.Storage) {
	n.storage = s
}
