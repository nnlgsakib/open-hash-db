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
	"openhashdb/core/hasher"
	"openhashdb/network/bitswap"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	relayv2client "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	circuit "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

const (
	ProtocolGossip   = protocol.ID("/openhashdb/gossip/1.0.0")
	ServiceTag       = "openhashdb"
	MaxPeerEventLogs = 100
)

// PeerEvent represents a peer discovery, connection, or disconnection event
type PeerEvent struct {
	PeerID    peer.ID   `json:"peer_id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Addresses []string  `json:"addresses"`
}

// Node represents a libp2p node
type Node struct {
	host                  host.Host
	ctx                   context.Context
	cancel                context.CancelFunc
	mdns                  mdns.Service
	dht                   *dht.IpfsDHT
	heartbeatService      *HeartbeatService
	blockstore            *blockstore.Blockstore
	bitswap               *bitswap.Engine
	GossipHandler         func(peer.ID, []byte) error
	peerEvents            []PeerEvent
	peerEventsMu          sync.RWMutex
}

// NewNodeWithKeyPath creates a new libp2p node
func NewNodeWithKeyPath(ctx context.Context, bootnodes []string, keyPath string, p2pPort int) (*Node, error) {
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

	allBootnodes := append(DefaultBootnodes, bootnodes...)
	addrInfos, err := convertBootnodesToAddrInfo(allBootnodes)
	if err != nil {
		log.Printf("Warning: failed to parse some bootnode addresses: %v", err)
	}

	listenAddrs := []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", p2pPort),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", p2pPort),
	}

	var nodeDHT *dht.IpfsDHT
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			nodeDHT, err = dht.New(ctx, h,
				dht.Mode(dht.ModeAutoServer),
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

	if _, err := circuit.New(h); err != nil {
		return nil, fmt.Errorf("failed to create circuit relay: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	node := &Node{
		host:                  h,
		ctx:                   nodeCtx,
		cancel:                cancel,
		dht:                   nodeDHT,
		peerEvents:            make([]PeerEvent, 0, MaxPeerEventLogs),
	}
	node.heartbeatService = NewHeartbeatService(nodeCtx, node)

	n := &networkNotifiee{node: node}
	h.Network().Notify(n)

	h.SetStreamHandler(ProtocolGossip, node.handleGossipStream)

	if err := node.setupMDNS(); err != nil {
		log.Printf("Warning: failed to setup mDNS: %v", err)
	}

	log.Printf("Node started with ID: %s", h.ID().String())
	log.Printf("Listening on addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  %s/p2p/%s", addr, h.ID().String())
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-nodeCtx.Done():
				return
			case <-ticker.C:
				if err := node.bootstrapDHT(); err != nil {
					log.Printf("Failed to bootstrap DHT: %v", err)
				}
			}
		}
	}()

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
}

func (n *networkNotifiee) Disconnected(net network.Network, conn network.Conn) {
	addrs := make([]string, 0, len(n.node.host.Peerstore().Addrs(conn.RemotePeer())))
	for _, addr := range n.node.host.Peerstore().Addrs(conn.RemotePeer()) {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(conn.RemotePeer(), "disconnected", addrs)
	n.node.heartbeatService.StopMonitoring(conn.RemotePeer())
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

// Close shuts down the node
func (n *Node) Close() error {
	if n.mdns != nil {
		if err := n.mdns.Close(); err != nil {
			log.Printf("Error closing mDNS: %v", err)
		}
	}
	if n.dht != nil {
		if err := n.dht.Close(); err != nil {
			log.Printf("Error closing DHT: %v", err)
		}
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
		return fmt.Errorf("failed to parse peer address %s: %w", peerAddr, err)
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
			log.Printf("Connected to peer: %s", maddr.ID.String())
			return nil
		}
		log.Printf("Attempt %d/%d: Failed to connect to peer %s: %v", attempt, maxRetries, maddr.ID.String(), err)
		if attempt < maxRetries {
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond) // Exponential backoff
		}
	}
	return fmt.Errorf("failed to connect to peer %s after %d attempts: %w", maddr.ID.String(), maxRetries, err)
}

// BroadcastGossip broadcasts gossip message
func (n *Node) BroadcastGossip(ctx context.Context, data []byte) error {
	peers := n.ConnectedPeers()
	for _, peerID := range peers {
		go func(pid peer.ID) {
			if err := n.sendData(ctx, pid, ProtocolGossip, data); err != nil {
				log.Printf("Failed to send gossip to %s: %v", pid.String(), err)
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
			log.Printf("Attempt %d/%d: Failed to open stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			if attempt == maxRetries {
				return fmt.Errorf("failed to open stream to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		defer stream.Close()
		if _, err := stream.Write(data); err != nil {
			log.Printf("Attempt %d/%d: Failed to write data to %s: %v", attempt, maxRetries, peerID.String(), err)
			if attempt == maxRetries {
				return fmt.Errorf("failed to write data to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to send data to %s: max retries exceeded", peerID.String())
}

// handleGossipStream handles gossip streams
func (n *Node) handleGossipStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling gossip stream from %s", remotePeer.String())

	data := make([]byte, 64*1024)
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read gossip stream from %s: %v", remotePeer.String(), err)
		return
	}

	if n.GossipHandler != nil {
		if err := n.GossipHandler(remotePeer, data[:bytesRead]); err != nil {
			log.Printf("Gossip handler error from %s: %v", remotePeer.String(), err)
		}
	}
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.node.host.Connect(ctx, pi); err != nil {
		log.Printf("Failed to connect to discovered peer %s: %v", pi.ID.String(), err)
	} else {
		log.Printf("Successfully connected to discovered peer %s", pi.ID.String())
	}
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

// connectToBootnodes connects to bootnodes in parallel
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
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := n.Connect(ctx, addr); err != nil {
				mu.Lock()
				lastErr = err
				mu.Unlock()
				log.Printf("Failed to connect to bootnode %s: %v", addr, err)
			} else {
				mu.Lock()
				connectedCount++
				mu.Unlock()
				pinfo, err := peer.AddrInfoFromString(addr)
				if err != nil {
					log.Printf("Could not parse bootnode address %s for reservation: %v", addr, err)
					return
				}
				log.Printf("Attempting to reserve slot with bootnode %s", pinfo.ID)
				reserveCtx, reserveCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer reserveCancel()
				_, err = relayv2client.Reserve(reserveCtx, n.host, *pinfo)
				if err != nil {
					log.Printf("Failed to reserve slot with %s: %v", pinfo.ID, err)
				} else {
					log.Printf("Successfully reserved slot with %s.", pinfo.ID)
				}
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

// AnnounceContent announces content availability
func (n *Node) AnnounceContent(contentHashStr string) error {
	if n.dht == nil {
		return fmt.Errorf("DHT not initialized")
	}

	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		return fmt.Errorf("invalid content hash: %w", err)
	}

	if n.blockstore == nil {
		log.Printf("Blockstore not configured for content %s", contentHashStr)
		return fmt.Errorf("blockstore not configured")
	}
	if !n.blockstore.HasContent(hash) {
		log.Printf("Validation failed for content %s: content not in blockstore", contentHashStr)
		return fmt.Errorf("content not in blockstore")
	}

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
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
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
		}
	}

	log.Printf("Found %d provider(s) for hash: %s", len(result), contentHash)
	return result, nil
}

// GetDHTStats returns DHT statistics
func (n *Node) GetDHTStats() map[string]interface{} {
	if n.dht == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}
	routingTable := n.dht.RoutingTable()
	return map[string]interface{}{
		"enabled":     true,
		"peer_count":  routingTable.Size(),
		"bucket_info": routingTable.GetPeerInfos(),
	}
}

// Helper functions that were removed
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
