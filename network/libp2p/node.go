package libp2p

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"openhashdb/core/blockstore"
	"openhashdb/network/bitswap"
	"openhashdb/protobuf/pb"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
    badcmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"

	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ProtocolGossip   = protocol.ID("/openhashdb/gossip/1.0.0")
	ServiceTag       = "openhashdb"
	MaxPeerEventLogs = 100
)

// Node represents a libp2p node
type Node struct {
	host             host.Host
	ctx              context.Context
	cancel           context.CancelFunc
	mdns             mdns.Service
	router           *Routing
	heartbeatService *HeartbeatService
	relayer          *Relayer
	blockstore       *blockstore.Blockstore
	bitswap          *bitswap.Engine
	GossipHandler    func(peer.ID, []byte) error
	peerEvents       []*pb.PeerEvent
	peerEventsMu     sync.RWMutex
}

// NewNodeWithKeyPath creates a new libp2p node
func NewNodeWithKeyPath(ctx context.Context, bootnodes []string, keyPath string, p2pPort int) (*Node, error) {
	var privKey crypto.PrivKey
	var err error

	if keyPath != "" {
		privKey, err = loadOrCreateIdentity(keyPath)
		if err != nil {
			return nil, fmt.Errorf("[libp2p] failed to load identity: %w", err)
		}
	} else {
		log.Println("[libp2p] Warning: no keyPath, generating ephemeral identity")
		privKey, _, err = crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to generate ephemeral key: %w", err)
		}
	}

	allBootnodes := append(DefaultBootnodes, bootnodes...)
	addrInfos, err := convertBootnodesToAddrInfo(allBootnodes)
	if err != nil {
		log.Printf("[libp2p] Warning: failed to parse some bootnode addresses: %v", err)
	}

	listenAddrs := []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", p2pPort),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", p2pPort),
	}

    var nodeDHT *dht.IpfsDHT
    var hostRef host.Host
	// Prepare candidate relays (use bootnodes by default). AutoRelay will
	// probe and maintain relay reservations and advertise relayed addresses
	// via Identify.
	relayCandidates := addrInfos

    // Build a connection manager first
    cm, err := badcmgr.NewConnManager(50, 120, badcmgr.WithGracePeriod(time.Minute))
    if err != nil {
        return nil, fmt.Errorf("failed to create conn manager: %w", err)
    }

    h, err := libp2p.New(
        libp2p.Identity(privKey),
        libp2p.ListenAddrStrings(listenAddrs...),
        libp2p.EnableRelay(),
        libp2p.NATPortMap(),
        libp2p.EnableNATService(),
        libp2p.ConnectionManager(cm),
        libp2p.EnableAutoRelayWithPeerSource(
            func(ctx context.Context, num int) <-chan peer.AddrInfo {
                ch := make(chan peer.AddrInfo, 32)
                go func() {
                    defer close(ch)
					count := 0
					// If we have a host reference, try dynamic relay-capable peers first
					if hostRef != nil {
						peers := hostRef.Peerstore().Peers()
						for _, pid := range peers {
							if pid == hostRef.ID() {
								continue
							}
							protos, err := hostRef.Peerstore().GetProtocols(pid)
							if err != nil {
								continue
							}
							hop := false
							for _, p := range protos {
								if p == RelayV2Hop {
									hop = true
									break
								}
							}
							if !hop {
								continue
							}
							info := hostRef.Peerstore().PeerInfo(pid)
							if len(info.Addrs) == 0 {
								continue
							}
							ch <- info
							count++
							if num > 0 && count >= num {
								return
							}
						}
					}
					// Then add configured candidates
					limit := len(relayCandidates)
					if num > 0 && num-count < limit {
						limit = num - count
					}
					for i := 0; i < limit; i++ {
						ch <- relayCandidates[i]
					}
				}()
				return ch
			},
			autorelay.WithNumRelays(2),
		),
		libp2p.EnableHolePunching(),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			nodeDHT, err = dht.New(ctx, h,
				dht.Mode(dht.ModeServer),
				dht.BootstrapPeers(addrInfos...),
				dht.BucketSize(20),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create DHT: %w", err)
			}
			return nodeDHT, nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	// Set hostRef after host creation so the AutoRelay PeerSource can use it.
	hostRef = h

	nodeCtx, cancel := context.WithCancel(ctx)
	node := &Node{
		host:       h,
		ctx:        nodeCtx,
		cancel:     cancel,
		peerEvents: make([]*pb.PeerEvent, 0, MaxPeerEventLogs),
	}
	node.router = NewRouting(node, nodeDHT)
	if err := node.router.Bootstrap(); err != nil {
		return nil, fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	node.relayer, err = NewRelayer(nodeCtx, h)
	if err != nil {
		return nil, err
	}

	node.heartbeatService = NewHeartbeatService(nodeCtx, node)

	n := &networkNotifiee{node: node}
	h.Network().Notify(n)

	h.SetStreamHandler(ProtocolGossip, node.handleGossipStream)

	if err := node.setupMDNS(); err != nil {
		log.Printf("[libp2p] Warning: failed to setup mDNS: %v", err)
	}

	log.Printf("[libp2p] Node started with ID: %s", h.ID().String())
	// log.Printf("[libp2p] Listening on addresses:")
	// for _, addr := range h.Addrs() {
	// 	log.Printf("  %s/p2p/%s", addr, h.ID().String())
	// }

	return node, nil
}

// Setters for components that are initialized after the node
func (n *Node) SetBlockstore(bs *blockstore.Blockstore) {
	n.blockstore = bs
}

func (n *Node) SetBitswap(b *bitswap.Engine) {
	n.bitswap = b
}

func (n *Node) GetBitswap() *bitswap.Engine {
	return n.bitswap
}

func (n *Node) Host() host.Host {
	return n.host
}

// networkNotifiee handles connection events
type networkNotifiee struct {
	node *Node
}

func (n *networkNotifiee) Connected(net network.Network, conn network.Conn) {
	addrs := make([]string, 0, len(n.node.host.Peerstore().Addrs(conn.RemotePeer())))
	for _, addr := range n.node.host.Peerstore().Addrs(conn.RemotePeer()) {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(conn.RemotePeer(), "connected", addrs)
	n.node.heartbeatService.MonitorConnection(conn.RemotePeer())
	if n.node.bitswap != nil {
		n.node.bitswap.HandleNewPeer(conn.RemotePeer())
	}

	// Add the newly connected peer to the DHT's routing table
	if n.node.router != nil && n.node.router.dht != nil {
		n.node.router.dht.RoutingTable().TryAddPeer(conn.RemotePeer(), true, true)
	}

	// Rely on AutoRelay/Identify to discover relays and manage reservations.
}

func (n *networkNotifiee) Disconnected(net network.Network, conn network.Conn) {
	addrs := make([]string, 0, len(n.node.host.Peerstore().Addrs(conn.RemotePeer())))
	for _, addr := range n.node.host.Peerstore().Addrs(conn.RemotePeer()) {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(conn.RemotePeer(), "disconnected", addrs)
	n.node.heartbeatService.StopMonitoring(conn.RemotePeer())
	if n.node.bitswap != nil {
		n.node.bitswap.HandlePeerDisconnect(conn.RemotePeer())
	}
}

func (n *networkNotifiee) Listen(net network.Network, addr multiaddr.Multiaddr)      {}
func (n *networkNotifiee) ListenClose(net network.Network, addr multiaddr.Multiaddr) {}

// AnnounceContent announces content availability
func (n *Node) AnnounceContent(contentHashStr string) error {
	return n.router.AnnounceContent(contentHashStr)
}

// FindContentProviders finds content providers
func (n *Node) FindContentProviders(contentHash string) ([]peer.AddrInfo, error) {
	return n.router.FindContentProviders(contentHash)
}

// setupMDNS sets up mDNS discovery
func (n *Node) setupMDNS() error {
	mdnsService := mdns.NewMdnsService(n.host, ServiceTag, &discoveryNotifee{node: n})
	if err := mdnsService.Start(); err != nil {
		return fmt.Errorf("[libp2p] failed to start mDNS: %w", err)
	}
	n.mdns = mdnsService
	return nil
}

// Close shuts down the node
func (n *Node) Close() error {
	if n.mdns != nil {
		if err := n.mdns.Close(); err != nil {
			log.Printf("[libp2p] Error closing mDNS: %v", err)
		}
	}
	if err := n.router.Close(); err != nil {
		log.Printf("[libp2p] Error closing DHT: %v", err)
	}
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
	return n.host.Network().Peers()
}

// Connect connects to a peer with exponential backoff
func (n *Node) Connect(ctx context.Context, peerAddr string) error {
	maddr, err := peer.AddrInfoFromString(peerAddr)
	if err != nil {
		return fmt.Errorf("[libp2p] failed to parse peer address %s: %w", peerAddr, err)
	}

	if swarm, ok := n.host.Network().(*swarm.Swarm); ok {
		swarm.Backoff().Clear(maddr.ID)
	}

	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(10+5*attempt)*time.Second)
		defer cancel()

		err = n.host.Connect(ctx, *maddr)
		if err == nil {
			log.Printf("[libp2p] Connected to peer: %s", maddr.ID.String())
			return nil
		}
		log.Printf("[libp2p] Attempt %d/%d: Failed to connect to peer %s: %v", attempt, maxRetries, maddr.ID.String(), err)
		if attempt < maxRetries {
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond) // Exponential backoff
		}
	}
	return fmt.Errorf("[libp2p] failed to connect to peer %s after %d attempts: %w", maddr.ID.String(), maxRetries, err)
}

// BroadcastGossip broadcasts gossip message
func (n *Node) BroadcastGossip(ctx context.Context, data []byte) error {
	peers := n.ConnectedPeers()
	for _, peerID := range peers {
		go func(pid peer.ID) {
			if err := n.sendData(ctx, pid, ProtocolGossip, data); err != nil {
				log.Printf("[libp2p] Failed to send gossip to %s: %v", pid.String(), err)
			}
		}(peerID)
	}
	return nil
}

// sendData sends data to a peer with exponential backoff
func (n *Node) sendData(ctx context.Context, peerID peer.ID, protocolID protocol.ID, data []byte) error {
	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		stream, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "send-data"), peerID, protocolID)
		if err != nil {
			log.Printf("[libp2p] Attempt %d/%d: Failed to open stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			if attempt == maxRetries {
				return fmt.Errorf("[libp2p] failed to open stream to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		defer stream.Close()
		if _, err := stream.Write(data); err != nil {
			log.Printf("[libp2p] Attempt %d/%d: Failed to write data to %s: %v", attempt, maxRetries, peerID.String(), err)
			if attempt == maxRetries {
				return fmt.Errorf("[libp2p] failed to write data to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		return nil
	}
	return fmt.Errorf("[libp2p] failed to send data to %s: max retries exceeded", peerID.String())
}

// handleGossipStream handles gossip streams
func (n *Node) handleGossipStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("[libp2p] Handling gossip stream from %s", remotePeer.String())

	data := make([]byte, 64*1024)
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("[libp2p] Failed to read gossip stream from %s: %v", remotePeer.String(), err)
		return
	}

	if n.GossipHandler != nil {
		if err := n.GossipHandler(remotePeer, data[:bytesRead]); err != nil {
			log.Printf("[libp2p] Gossip handler error from %s: %v", remotePeer.String(), err)
		}
	}
}

// discoveryNotifee handles peer discovery
type discoveryNotifee struct {
	node *Node
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if n.node.IsSelf(pi) {
		return
	}
	addrs := make([]string, 0, len(pi.Addrs))
	for _, addr := range pi.Addrs {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(pi.ID, "discovered", addrs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.node.host.Connect(ctx, pi); err != nil {
		log.Printf("[libp2p] Failed to connect to discovered peer %s: %v", pi.ID.String(), err)
	} else {
		log.Printf("[libp2p] Successfully connected to discovered peer %s", pi.ID.String())
	}
}

// GetNetworkStats returns network statistics
func (n *Node) GetNetworkStats() *pb.NetworkStatsResponse {
	peers := n.ConnectedPeers()
	n.peerEventsMu.RLock()
	peerEvents := make([]*pb.PeerEvent, len(n.peerEvents))
	copy(peerEvents, n.peerEvents)
	n.peerEventsMu.RUnlock()

	peerList := make([]string, len(peers))
	for i, p := range peers {
		peerList[i] = p.String()
	}

	return &pb.NetworkStatsResponse{
		PeerId:         n.ID().String(),
		ConnectedPeers: int32(len(peers)),
		PeerList:       peerList,
		Addresses:      n.Addrs(),
		Dht:            n.router.GetStats(),
		PeerEvents:     peerEvents,
	}
}

// IsSelf checks if a given AddrInfo belongs to the current node.
func (n *Node) IsSelf(pi peer.AddrInfo) bool {
	if pi.ID == n.host.ID() {
		return true
	}
	myAddrs := n.host.Addrs()
	for _, a := range pi.Addrs {
		for _, myA := range myAddrs {
			if a.Equal(myA) {
				return true
			}
		}
	}
	return false
}

// connectToBootnodes connects to bootnodes in parallel
func (n *Node) connectToBootnodes(bootnodes []string) error {
	nodesToConnect := bootnodes
	if len(nodesToConnect) == 0 {
		nodesToConnect = DefaultBootnodes
	}
	if len(nodesToConnect) == 0 {
		log.Printf("[libp2p] No bootnodes specified")
		return nil
	}

	log.Printf("[libp2p] Connecting to %d bootnode(s)...", len(nodesToConnect))
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
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := n.Connect(ctx, addr); err != nil {
				mu.Lock()
				lastErr = err
				mu.Unlock()
				log.Printf("[libp2p] Failed to connect to bootnode %s: %v", addr, err)
			} else {
				mu.Lock()
				connectedCount++
				mu.Unlock()
				// No manual reservation; AutoRelay manages this.
			}
		}(bootnode)
	}
	wg.Wait()

	log.Printf("[libp2p] Connected to %d out of %d bootnodes", connectedCount, len(nodesToConnect))
	if connectedCount == 0 && len(nodesToConnect) > 0 {
		return fmt.Errorf("[libp2p] failed to connect to any bootnodes: %w", lastErr)
	}
	return nil
}

// Helper functions that were removed
func (n *Node) logPeerEvent(peerID peer.ID, eventType string, addrs []string) {
	n.peerEventsMu.Lock()
	defer n.peerEventsMu.Unlock()

	event := &pb.PeerEvent{
		PeerId:    peerID.String(),
		Type:      eventType,
		Timestamp: timestamppb.Now(),
		Addresses: addrs,
	}

	n.peerEvents = append(n.peerEvents, event)
	if len(n.peerEvents) > MaxPeerEventLogs {
		n.peerEvents = n.peerEvents[len(n.peerEvents)-MaxPeerEventLogs:]
	}

	log.Printf("Peer %s event: %s at %s",
		peerID.String(), eventType, event.Timestamp.AsTime().Format(time.RFC3339))
}

func loadOrCreateIdentity(keyPath string) (crypto.PrivKey, error) {
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return nil, fmt.Errorf("[libp2p] failed to create key directory: %w", err)
	}

	if keyData, err := os.ReadFile(keyPath); err == nil {
		keyBytes, err := base64.StdEncoding.DecodeString(string(keyData))
		if err != nil {
			log.Printf("[libp2p] Warning: failed to decode key, creating new: %v", err)
		} else {
			privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
			if err == nil {
				log.Printf("[libp2p] Loaded identity from %s", keyPath)
				return privKey, nil
			}
			log.Printf("[libp2p] Warning: failed to unmarshal key, creating new: %v", err)
		}
	}

	log.Printf("[libp2p] Generating new identity at %s", keyPath)
	privKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("[libp2p] failed to generate key pair: %w", err)
	}

	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("[libp2p] failed to marshal private key: %w", err)
	}

	keyData := base64.StdEncoding.EncodeToString(keyBytes)
	if err := os.WriteFile(keyPath, []byte(keyData), 0600); err != nil {
		return nil, fmt.Errorf("[libp2p] failed to save private key: %w", err)
	}

	return privKey, nil
}

func convertBootnodesToAddrInfo(bootnodes []string) ([]peer.AddrInfo, error) {
	var addrInfos []peer.AddrInfo
	for _, addr := range bootnodes {
		if addr == "" {
			continue
		}
		addrInfo, err := peer.AddrInfoFromString(addr)
		if err != nil {
			log.Printf("[libp2p] Failed to parse bootnode address %s: %v", addr, err)
			continue
		}
		addrInfos = append(addrInfos, *addrInfo)
	}
	return addrInfos, nil
}
