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
	"github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for monitoring
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
)

const (
	ProtocolContentExchange = protocol.ID("/openhashdb/content/1.0.0")
	ProtocolChunkExchange   = protocol.ID("/openhashdb/chunk/1.0.0")
	ProtocolGossip          = protocol.ID("/openhashdb/gossip/1.0.0")
	ServiceTag              = "openhashdb"
)

// Node represents a libp2p node for OpenHashDB
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
}

// SetStorage sets the storage backend
func (n *Node) SetStorage(s *storage.Storage) {
	n.storage = s
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

	var nodeDHT *dht.IpfsDHT
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			nodeDHT, err = dht.New(ctx, h, dht.Mode(dht.ModeServer))
			if err != nil {
				return nil, fmt.Errorf("failed to create DHT: %w", err)
			}
			return nodeDHT, nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	node := &Node{
		host:                  h,
		ctx:                   nodeCtx,
		cancel:                cancel,
		dht:                   nodeDHT,
		PeerConnectedCallback: func(p peer.ID) {},
	}

	h.SetStreamHandler(ProtocolContentExchange, node.handleContentStream)
	h.SetStreamHandler(ProtocolChunkExchange, node.handleChunkStream)
	h.SetStreamHandler(ProtocolGossip, node.handleGossipStream)

	if err := node.setupMDNS(); err != nil {
		log.Printf("Warning: failed to setup mDNS: %v", err)
	}

	log.Printf("Node started with ID: %s", h.ID().String())
	log.Printf("Listening on addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  %s/p2p/%s", addr, h.ID().String())
	}

	if err := node.bootstrapDHT(); err != nil {
		log.Printf("Warning: failed to bootstrap DHT: %v", err)
	}

	if err := node.connectToBootnodes(bootnodes); err != nil {
		log.Printf("Warning: failed to connect to some bootnodes: %v", err)
	}

	return node, nil
}

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

// Connect connects to a peer
func (n *Node) Connect(ctx context.Context, peerAddr string) error {
	maddr, err := peer.AddrInfoFromString(peerAddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer address: %w", err)
	}
	if err := n.host.Connect(ctx, *maddr); err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}
	log.Printf("Connected to peer: %s", maddr.ID.String())
	n.PeerConnectedCallback(maddr.ID)
	return nil
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

// sendData sends data to a peer
func (n *Node) sendData(ctx context.Context, peerID peer.ID, protocolID protocol.ID, data []byte) error {
	stream, err := n.host.NewStream(ctx, peerID, protocolID)
	if err != nil {
		networkErrorsTotal.WithLabelValues("send_data").Inc()
		return fmt.Errorf("failed to open stream to %s: %w", peerID.String(), err)
	}
	defer stream.Close()

	stream.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := stream.Write(data); err != nil {
		networkErrorsTotal.WithLabelValues("send_data").Inc()
		return fmt.Errorf("failed to write data to %s: %w", peerID.String(), err)
	}
	return nil
}

// handleContentStream handles content streams
func (n *Node) handleContentStream(stream network.Stream) {
	defer stream.Close()
	networkMessagesTotal.WithLabelValues("content_received").Inc()

	buf := make([]byte, 256)
	bytesRead, err := stream.Read(buf)
	if err != nil && err != io.EOF && err != context.Canceled {
		log.Printf("Failed to read content request: %v", err)
		networkErrorsTotal.WithLabelValues("read_content_request").Inc()
		return
	}
	contentHashStr := string(buf[:bytesRead])
	log.Printf("Received content request for %s from %s", contentHashStr, stream.Conn().RemotePeer().String())

	if n.storage == nil {
		log.Printf("Error: storage not configured")
		networkErrorsTotal.WithLabelValues("storage_not_configured").Inc()
		stream.Reset()
		return
	}

	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		log.Printf("Invalid hash: %s", contentHashStr)
		networkErrorsTotal.WithLabelValues("invalid_hash").Inc()
		stream.Reset()
		return
	}

	// Retry up to 3 times with 100ms delay to account for storage write delays
	const maxRetries = 3
	const retryDelay = 100 * time.Millisecond
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if n.storage.HasContent(hash) {
			break
		}
		if attempt == maxRetries {
			log.Printf("Content %s not found locally after %d attempts", contentHashStr, maxRetries)
			networkErrorsTotal.WithLabelValues("content_not_found").Inc()
			stream.Reset()
			return
		}
		log.Printf("Content %s not found, retrying (%d/%d)", contentHashStr, attempt, maxRetries)
		networkRetriesTotal.Inc()
		time.Sleep(retryDelay)
	}

	metadata, err := n.storage.GetContent(hash)
	if err != nil {
		log.Printf("Failed to get metadata for %s: %v", contentHashStr, err)
		networkErrorsTotal.WithLabelValues("get_metadata").Inc()
		stream.Reset()
		return
	}

	data, err := n.storage.GetData(hash)
	if err != nil {
		log.Printf("Failed to get data for %s: %v", contentHashStr, err)
		networkErrorsTotal.WithLabelValues("get_data").Inc()
		stream.Reset()
		return
	}

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		log.Printf("Failed to marshal metadata for %s: %v", contentHashStr, err)
		networkErrorsTotal.WithLabelValues("marshal_metadata").Inc()
		stream.Reset()
		return
	}

	stream.SetWriteDeadline(time.Now().Add(60 * time.Second))
	if err := binary.Write(stream, binary.BigEndian, uint32(len(metaBytes))); err != nil {
		log.Printf("Failed to write metadata length: %v", err)
		networkErrorsTotal.WithLabelValues("write_metadata_length").Inc()
		return
	}

	if _, err := stream.Write(metaBytes); err != nil {
		log.Printf("Failed to write metadata: %v", err)
		networkErrorsTotal.WithLabelValues("write_metadata").Inc()
		return
	}

	if _, err := stream.Write(data); err != nil {
		log.Printf("Failed to write content: %v", err)
		networkErrorsTotal.WithLabelValues("write_content").Inc()
	} else {
		log.Printf("Sent content %s to %s", contentHashStr, stream.Conn().RemotePeer().String())
	}
}

// handleChunkStream handles chunk streams
func (n *Node) handleChunkStream(stream network.Stream) {
	defer stream.Close()
	networkMessagesTotal.WithLabelValues("chunk_received").Inc()

	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	data := make([]byte, 1024*1024)
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read chunk stream: %v", err)
		networkErrorsTotal.WithLabelValues("read_chunk_stream").Inc()
		return
	}

	if n.ChunkHandler != nil {
		if err := n.ChunkHandler(stream.Conn().RemotePeer(), data[:bytesRead]); err != nil {
			log.Printf("Chunk handler error: %v", err)
			networkErrorsTotal.WithLabelValues("chunk_handler").Inc()
		}
	}
}

// handleGossipStream handles gossip streams
func (n *Node) handleGossipStream(stream network.Stream) {
	defer stream.Close()
	networkMessagesTotal.WithLabelValues("gossip_received").Inc()

	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	data := make([]byte, 64*1024)
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read gossip stream: %v", err)
		networkErrorsTotal.WithLabelValues("read_gossip_stream").Inc()
		return
	}

	if n.GossipHandler != nil {
		if err := n.GossipHandler(stream.Conn().RemotePeer(), data[:bytesRead]); err != nil {
			log.Printf("Gossip handler error: %v", err)
			networkErrorsTotal.WithLabelValues("gossip_handler").Inc()
		}
	}
}

// discoveryNotifee handles peer discovery
type discoveryNotifee struct {
	node *Node
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	log.Printf("Discovered peer: %s", pi.ID.String())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.node.host.Connect(ctx, pi); err != nil {
		log.Printf("Failed to connect to peer %s: %v", pi.ID.String(), err)
	} else {
		log.Printf("Connected to peer: %s", pi.ID.String())
		n.node.PeerConnectedCallback(pi.ID)
	}
}

// GetNetworkStats returns network statistics
func (n *Node) GetNetworkStats() map[string]interface{} {
	peers := n.ConnectedPeers()
	stats := map[string]interface{}{
		"peer_id":         n.ID().String(),
		"connected_peers": len(peers),
		"peer_list":       make([]string, len(peers)),
		"addresses":       n.Addrs(),
		"dht":             n.GetDHTStats(),
	}
	for i, peer := range peers {
		stats["peer_list"].([]string)[i] = peer.String()
	}
	return stats
}

var DefaultBootnodes = []string{
	"/ip4/148.251.35.204/tcp/44185/p2p/QmQ9riUo5SQpHy2mAmgon73DVwWwGgpDEMv5CoEPXEuyjD",
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
	connectedCount := 0
	var lastErr error

	for _, bootnode := range nodesToConnect {
		if bootnode == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := n.Connect(ctx, bootnode); err != nil {
			log.Printf("Failed to connect to bootnode %s: %v", bootnode, err)
			lastErr = err
		} else {
			connectedCount++
		}
		cancel()
	}

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
	return n.dht.Bootstrap(n.ctx)
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

	// Ensure content exists locally before announcing
	if n.storage != nil && !n.storage.HasContent(hash) {
		log.Printf("Content %s not found locally, skipping announcement", contentHashStr)
		return fmt.Errorf("content %s not found locally", contentHashStr)
	}

	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()

	mh, err := multihash.Sum([]byte(contentHashStr), multihash.SHA2_256, -1)
	if err != nil {
		return fmt.Errorf("failed to create multihash: %w", err)
	}
	contentCID := cid.NewCidV1(cid.Raw, mh)

	log.Printf("Announcing content provider for hash: %s", contentHashStr)
	return n.dht.Provide(ctx, contentCID, true)
}

// FindContentProviders finds content providers
func (n *Node) FindContentProviders(contentHash string) ([]peer.AddrInfo, error) {
	if n.dht == nil {
		return nil, fmt.Errorf("DHT not initialized")
	}

	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()

	hash, err := multihash.FromHexString("1220" + contentHash)
	if err != nil {
		mh, err := multihash.Sum([]byte(contentHash), multihash.SHA2_256, -1)
		if err != nil {
			return nil, fmt.Errorf("failed to create multihash: %w", err)
		}
		hash = mh
	}

	contentCID := cid.NewCidV1(cid.Raw, hash)
	log.Printf("Finding providers for content hash: %s", contentHash)

	providers := n.dht.FindProvidersAsync(ctx, contentCID, 10)
	var result []peer.AddrInfo
	for provider := range providers {
		result = append(result, provider)
		log.Printf("Found provider: %s", provider.ID.String())
	}

	log.Printf("Found %d provider(s) for hash: %s", len(result), contentHash)
	return result, nil
}

// RequestContentFromPeer requests content from a peer
func (n *Node) RequestContentFromPeer(peerID peer.ID, contentHash string) ([]byte, *storage.ContentMetadata, error) {
	const maxRetries = 3
	const retryDelay = 100 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
		defer cancel()

		stream, err := n.host.NewStream(ctx, peerID, ProtocolContentExchange)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to open stream to %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("open_stream").Inc()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to open stream to %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(retryDelay)
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
			time.Sleep(retryDelay)
			continue
		}
		stream.CloseWrite()

		stream.SetReadDeadline(time.Now().Add(60 * time.Second))
		var metaLen uint32
		if err := binary.Read(stream, binary.BigEndian, &metaLen); err != nil {
			log.Printf("Attempt %d/%d: Failed to read metadata length from %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("read_metadata_length").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read metadata length from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(retryDelay)
			continue
		}

		metaBytes := make([]byte, metaLen)
		if _, err := io.ReadFull(stream, metaBytes); err != nil {
			log.Printf("Attempt %d/%d: Failed to read metadata from %s: %v", attempt, maxRetries, peerID.String(), err)
			networkErrorsTotal.WithLabelValues("read_metadata").Inc()
			stream.Close()
			if attempt == maxRetries {
				return nil, nil, fmt.Errorf("failed to read metadata from %s after %d attempts: %w", peerID.String(), maxRetries, err)
			}
			networkRetriesTotal.Inc()
			time.Sleep(retryDelay)
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
			time.Sleep(retryDelay)
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
			time.Sleep(retryDelay)
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
			time.Sleep(retryDelay)
			continue
		}

		return data, &metadata, nil
	}

	return nil, nil, fmt.Errorf("failed to fetch content from %s: max retries exceeded", peerID.String())
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
