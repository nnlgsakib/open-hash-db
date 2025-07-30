package libp2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"openhashdb/core/hasher"
	"openhashdb/core/storage"

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
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics
var (
	networkMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openhashdb_network_messages_total",
			Help: "Total number of network messages",
		},
		[]string{"type"},
	)
	networkErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openhashdb_network_errors_total",
			Help: "Total number of network errors",
		},
		[]string{"type"},
	)
	networkRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_network_retries_total",
			Help: "Total number of network operation retries",
		},
	)
	peerConnectionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_peer_connections_total",
			Help: "Total number of peer connections",
		},
	)
	peerDisconnectionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_peer_disconnections_total",
			Help: "Total number of peer disconnections",
		},
	)
)

const (
	ProtocolContentExchange = protocol.ID("/openhashdb/content/1.0.0")
	ProtocolChunkExchange   = protocol.ID("/openhashdb/chunk/1.0.0")
	ProtocolGossip          = protocol.ID("/openhashdb/gossip/1.0.0")
	ServiceTag              = "openhashdb"
	MaxPeerEventLogs        = 100
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
	storage               *storage.Storage
	ContentHandler        func(peer.ID, []byte) error
	ChunkHandler          func(peer.ID, []byte) error
	GossipHandler         func(peer.ID, []byte) error
	PeerConnectedCallback func(peer.ID)
	peerEvents            []PeerEvent
	peerEventsMu          sync.RWMutex
}

// SetStorage sets the storage backend
func (n *Node) SetStorage(s *storage.Storage) {
	n.storage = s
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
	"/ip4/148.251.35.204/tcp/35949/p2p/QmNwQH2JdRrh9bGGEprm6QA7D2ErNJ3a8WRWZTCVYDS1NS",
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

	var nodeDHT *dht.IpfsDHT
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
		),
		libp2p.EnableNATService(),
		libp2p.EnableRelay(),
		libp2p.EnableRelayService(),
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

	// Enable relay service
	_, err = relayv2.New(h)
	if err != nil {
		log.Printf("Warning: failed to enable relay service: %v", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	node := &Node{
		host:                  h,
		ctx:                   nodeCtx,
		cancel:                cancel,
		dht:                   nodeDHT,
		peerEvents:            make([]PeerEvent, 0, MaxPeerEventLogs),
		PeerConnectedCallback: func(p peer.ID) {},
	}

	// Set up network notifiee
	n := &networkNotifiee{node: node}
	h.Network().Notify(n)

	// Set stream handlers
	h.SetStreamHandler(ProtocolContentExchange, node.handleContentStream)
	h.SetStreamHandler(ProtocolChunkExchange, node.handleChunkStream)
	h.SetStreamHandler(ProtocolGossip, node.handleGossipStream)

	// Setup mDNS
	if err := node.setupMDNS(); err != nil {
		log.Printf("Warning: failed to setup mDNS: %v", err)
	}

	log.Printf("Node started with ID: %s", h.ID().String())
	log.Printf("Listening on addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  %s/p2p/%s", addr, h.ID().String())
	}

	// Periodic DHT bootstrap
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

// networkNotifiee handles connection events
type networkNotifiee struct {
	node *Node
}

func (n *networkNotifiee) Connected(net network.Network, conn network.Conn) {
	peerConnectionsTotal.Inc()
	addrs := make([]string, 0, len(n.node.host.Peerstore().Addrs(conn.RemotePeer())))
	for _, addr := range n.node.host.Peerstore().Addrs(conn.RemotePeer()) {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(conn.RemotePeer(), "connected", addrs)
	n.node.PeerConnectedCallback(conn.RemotePeer())
}

func (n *networkNotifiee) Disconnected(net network.Network, conn network.Conn) {
	peerDisconnectionsTotal.Inc()
	addrs := make([]string, 0, len(n.node.host.Peerstore().Addrs(conn.RemotePeer())))
	for _, addr := range n.node.host.Peerstore().Addrs(conn.RemotePeer()) {
		addrs = append(addrs, addr.String())
	}
	n.node.logPeerEvent(conn.RemotePeer(), "disconnected", addrs)
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

	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = n.host.Connect(ctx, *maddr)
		if err == nil {
			log.Printf("Connected to peer: %s", maddr.ID.String())
			n.PeerConnectedCallback(maddr.ID)
			return nil
		}
		log.Printf("Attempt %d/%d: Failed to connect to peer %s: %v", attempt, maxRetries, maddr.ID.String(), err)
		networkErrorsTotal.WithLabelValues("connect").Inc()
		if attempt < maxRetries {
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond) // Exponential backoff
		}
	}
	return fmt.Errorf("failed to connect to peer %s after %d attempts: %w", maddr.ID.String(), maxRetries, err)
}

// SendContent sends content to a peer
func (n *Node) SendContent(ctx context.Context, peerID peer.ID, data []byte) error {
	networkMessagesTotal.WithLabelValues("content").Inc()
	return n.sendData(ctx, peerID, ProtocolContentExchange, data)
}

// SendChunk sends a chunk to a peer
func (n *Node) SendChunk(ctx context.Context, peerID peer.ID, data []byte) error {
	networkMessagesTotal.WithLabelValues("chunk").Inc()
	return n.sendData(ctx, peerID, ProtocolChunkExchange, data)
}

// BroadcastGossip broadcasts gossip message
func (n *Node) BroadcastGossip(ctx context.Context, data []byte) error {
	networkMessagesTotal.WithLabelValues("gossip").Inc()
	peers := n.ConnectedPeers()
	for _, peerID := range peers {
		go func(pid peer.ID) {
			if err := n.sendData(ctx, pid, ProtocolGossip, data); err != nil {
				log.Printf("Failed to send gossip to %s: %v", pid.String(), err)
				networkErrorsTotal.WithLabelValues("gossip").Inc()
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

		stream, err := n.host.NewStream(ctx, peerID, protocolID)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to open stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("send_data").Inc()
			if attempt == maxRetries {
				return fmt.Errorf("failed to open stream to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		defer stream.Close()

		stream.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := stream.Write(data); err != nil {
			log.Printf("Attempt %d/%d: Failed to write data to %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("send_data").Inc()
			if attempt == maxRetries {
				return fmt.Errorf("failed to write data to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
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
	networkMessagesTotal.WithLabelValues("content_received").Inc()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling content stream from %s", remotePeer.String())

	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	buf := make([]byte, 256)
	bytesRead, err := stream.Read(buf)
	if err != nil && err != io.EOF && err != context.Canceled {
		log.Printf("Failed to read content request from %s: %v", remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("read_content_request").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to read content request: %v", err)))
		return
	}
	contentHashStr := string(buf[:bytesRead])
	log.Printf("Received content request for %s from %s", contentHashStr, remotePeer.String())

	if n.storage == nil {
		log.Printf("Error: storage not configured for %s", remotePeer.String())
		networkErrorsTotal.WithLabelValues("storage_not_configured").Inc()
		_, _ = stream.Write([]byte("ERROR: storage not configured"))
		return
	}

	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		log.Printf("Invalid hash %s from %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("invalid_hash").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: invalid hash: %v", err)))
		return
	}

	if err := n.storage.ValidateContent(hash); err != nil {
		log.Printf("Content validation failed for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("content_validation").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: %v", err)))
		return
	}

	metadata, err := n.storage.GetContent(hash)
	if err != nil {
		log.Printf("Failed to get metadata for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("get_metadata").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to get metadata: %v", err)))
		return
	}

	dataStream, err := n.storage.GetDataStream(hash)
	if err != nil {
		log.Printf("Failed to get data stream for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("get_data").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to get data: %v", err)))
		return
	}
	defer dataStream.Close()

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		log.Printf("Failed to marshal metadata for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("marshal_metadata").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to marshal metadata: %v", err)))
		return
	}

	stream.SetWriteDeadline(time.Now().Add(60 * time.Second))
	if err := binary.Write(stream, binary.BigEndian, uint32(len(metaBytes))); err != nil {
		log.Printf("Failed to write metadata length for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("write_metadata_length").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write metadata length: %v", err)))
		return
	}

	if _, err := stream.Write(metaBytes); err != nil {
		log.Printf("Failed to write metadata for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("write_metadata").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write metadata: %v", err)))
		return
	}

	stream.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	bytesSent, err := io.Copy(stream, dataStream)
	if err != nil {
		log.Printf("Failed to write content for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("write_content").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write content: %v", err)))
		return
	}
	log.Printf("Sent %d bytes of content %s to %s", bytesSent, contentHashStr, remotePeer.String())

	if err := stream.CloseWrite(); err != nil {
		log.Printf("Failed to close write stream for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("close_write_stream").Inc()
	}
}

// handleChunkStream handles chunk streams
func (n *Node) handleChunkStream(stream network.Stream) {
	defer stream.Close()
	networkMessagesTotal.WithLabelValues("chunk_received").Inc()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling chunk stream from %s", remotePeer.String())

	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	data := make([]byte, 1024*1024)
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read chunk stream from %s: %v", remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("read_chunk_stream").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to read chunk: %v", err)))
		return
	}

	if n.ChunkHandler != nil {
		if err := n.ChunkHandler(remotePeer, data[:bytesRead]); err != nil {
			log.Printf("Chunk handler error from %s: %v", remotePeer.String(), err)
			networkErrorsTotal.WithLabelValues("chunk_handler").Inc()
			_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: chunk handler failed: %v", err)))
		}
	}
}

// handleGossipStream handles gossip streams
func (n *Node) handleGossipStream(stream network.Stream) {
	defer stream.Close()
	networkMessagesTotal.WithLabelValues("gossip_received").Inc()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling gossip stream from %s", remotePeer.String())

	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	data := make([]byte, 64*1024)
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read gossip stream from %s: %v", remotePeer.String(), err)
		networkErrorsTotal.WithLabelValues("read_gossip_stream").Inc()
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to read gossip: %v", err)))
		return
	}

	if n.GossipHandler != nil {
		if err := n.GossipHandler(remotePeer, data[:bytesRead]); err != nil {
			log.Printf("Gossip handler error from %s: %v", remotePeer.String(), err)
			networkErrorsTotal.WithLabelValues("gossip_handler").Inc()
			_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: gossip handler failed: %v", err)))
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
	if err := n.node.Connect(ctx, pi.String()); err != nil {
		log.Printf("Failed to connect to discovered peer %s: %v", pi.ID.String(), err)
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

	if n.storage == nil {
		log.Printf("Storage not configured for content %s", contentHashStr)
		return fmt.Errorf("storage not configured")
	}
	if err := n.storage.ValidateContent(hash); err != nil {
		log.Printf("Validation failed for content %s: %v", contentHashStr, err)
		return err
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
			networkRetriesTotal.Inc()
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
			networkRetriesTotal.Inc()
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

// RequestContentFromPeer requests content from a peer
func (n *Node) RequestContentFromPeer(peerID peer.ID, contentHash string) ([]byte, *storage.ContentMetadata, error) {
	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
		defer cancel()

		log.Printf("Attempt %d/%d: Requesting content %s from %s", attempt, maxRetries, contentHash, peerID.String())
		stream, err := n.host.NewStream(ctx, peerID, ProtocolContentExchange)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to open stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("open_stream").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to open stream to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}

		stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := stream.Write([]byte(contentHash)); err != nil {
			log.Printf("Attempt %d/%d: Failed to send content request to %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("write_content_request").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to send content request to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		stream.CloseWrite()

		stream.SetReadDeadline(time.Now().Add(60 * time.Second))
		buf := make([]byte, 256)
		nBytes, err := stream.Read(buf)
		if err == nil && nBytes > 0 && bytes.HasPrefix(buf[:nBytes], []byte("ERROR:")) {
			log.Printf("Received error from %s: %s", peerID.String(), string(buf[:nBytes]))
			networkErrorsTotal.WithLabelValues("remote_error").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("remote error from %s: %s", peerID.String(), string(buf[:nBytes]))
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}

		if err != nil && err != io.EOF {
			log.Printf("Attempt %d/%d: Failed to read initial response from %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("read_response").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read response from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}

		combinedReader := io.MultiReader(bytes.NewReader(buf[:nBytes]), stream)
		var metaLen uint32
		if err := binary.Read(combinedReader, binary.BigEndian, &metaLen); err != nil {
			log.Printf("Attempt %d/%d: Failed to read metadata length from %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("read_metadata_length").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read metadata length from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}

		metaBytes := make([]byte, metaLen)
		if _, err := io.ReadFull(combinedReader, metaBytes); err != nil {
			log.Printf("Attempt %d/%d: Failed to read metadata from %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("read_metadata").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read metadata from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}

		var metadata storage.ContentMetadata
		if err := json.Unmarshal(metaBytes, &metadata); err != nil {
			log.Printf("Attempt %d/%d: Failed to unmarshal metadata from %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("unmarshal_metadata").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to unmarshal metadata from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}

		data, err := io.ReadAll(stream)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to read content from %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("read_content").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read content from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
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
			networkErrorsTotal.WithLabelValues("hash_mismatch").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("content hash mismatch from %s: expected %s, got %s", peerID.String(), metadata.ContentHash.String(), receivedContentHash.String())
			}
			networkRetriesTotal.Inc()
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}

		log.Printf("Successfully fetched %d bytes of content %s from %s", len(data), contentHash, peerID.String())
		return data, &metadata, nil
	}

	return nil, nil, fmt.Errorf("failed to fetch content %s from %s: max retries exceeded", contentHash, peerID.String())
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
