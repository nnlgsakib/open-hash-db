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
	"io/ioutil"
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
)

const (
	// Protocol IDs for different OpenHashDB operations
	ProtocolContentExchange = protocol.ID("/openhashdb/content/1.0.0")
	ProtocolChunkExchange   = protocol.ID("/openhashdb/chunk/1.0.0")
	ProtocolGossip          = protocol.ID("/openhashdb/gossip/1.0.0")

	// Service tag for mDNS discovery
	ServiceTag = "openhashdb"
)

// Node represents a libp2p node for OpenHashDB
type Node struct {
	host     host.Host
	ctx      context.Context
	cancel   context.CancelFunc
	mdns     mdns.Service
	dht      *dht.IpfsDHT
	storage  *storage.Storage

	// Callbacks for handling different types of messages
	ContentHandler        func(peer.ID, []byte) error
	ChunkHandler          func(peer.ID, []byte) error
	GossipHandler         func(peer.ID, []byte) error
	PeerConnectedCallback func(peer.ID)
}

// SetStorage sets the storage backend for the node to use for content requests.
func (n *Node) SetStorage(s *storage.Storage) {
	n.storage = s
}

// loadOrCreateIdentity loads an existing identity or creates a new one
func loadOrCreateIdentity(keyPath string) (crypto.PrivKey, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create key directory: %w", err)
	}

	// Try to load existing key
	if keyData, err := ioutil.ReadFile(keyPath); err == nil {
		// Decode base64 key
		keyBytes, err := base64.StdEncoding.DecodeString(string(keyData))
		if err != nil {
			log.Printf("Warning: failed to decode existing key, creating new one: %v", err)
		} else {
			// Unmarshal private key
			privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
			if err != nil {
				log.Printf("Warning: failed to unmarshal existing key, creating new one: %v", err)
			} else {
				log.Printf("Loaded existing identity from %s", keyPath)
				return privKey, nil
			}
		}
	}

	// Generate new key
	log.Printf("Generating new identity and saving to %s", keyPath)
	privKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Marshal and save the key
	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	// Encode to base64 and save
	keyData := base64.StdEncoding.EncodeToString(keyBytes)
	if err := ioutil.WriteFile(keyPath, []byte(keyData), 0600); err != nil {
		return nil, fmt.Errorf("failed to save private key: %w", err)
	}

	return privKey, nil
}

// NewNode creates a new libp2p node
// NewNodeWithKeyPath creates a new libp2p node with a specific key path
func NewNodeWithKeyPath(ctx context.Context, bootnodes []string, keyPath string) (*Node, error) {
	var privKey crypto.PrivKey
	var err error

	if keyPath != "" {
		// Load or create persistent identity
		privKey, err = loadOrCreateIdentity(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load identity: %w", err)
		}
	} else {
		// Generate new ephemeral identity
		log.Println("Warning: keyPath not provided, generating ephemeral identity. Node ID will change on restart.")
		privKey, _, err = crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to generate ephemeral key pair: %w", err)
		}
	}

	// Create a new libp2p host with persistent identity
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			// Create DHT in server mode
			kadDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
			if err != nil {
				return nil, err
			}
			return kadDHT, nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)

	// Get the DHT from the routing
	kadDHT, ok := h.Peerstore().(interface{ DHT() *dht.IpfsDHT })
	var nodeDHT *dht.IpfsDHT
	if ok {
		nodeDHT = kadDHT.DHT()
	} else {
		// Create DHT manually if not available from routing
		nodeDHT, err = dht.New(nodeCtx, h, dht.Mode(dht.ModeServer))
		if err != nil {
			h.Close()
			cancel()
			return nil, fmt.Errorf("failed to create DHT: %w", err)
		}
	}

	node := &Node{
		host:                  h,
		ctx:                   nodeCtx,
		cancel:                cancel,
		dht:                   nodeDHT,
		PeerConnectedCallback: func(p peer.ID) {}, // Default no-op
	}

	// Set up protocol handlers
	h.SetStreamHandler(ProtocolContentExchange, node.handleContentStream)
	h.SetStreamHandler(ProtocolChunkExchange, node.handleChunkStream)
	h.SetStreamHandler(ProtocolGossip, node.handleGossipStream)

	// Set up mDNS discovery
	if err := node.setupMDNS(); err != nil {
		log.Printf("Warning: failed to setup mDNS: %v", err)
	}

	log.Printf("Node started with ID: %s", h.ID().String())
	log.Printf("Listening on addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  %s/p2p/%s", addr, h.ID().String())
	}

	// Bootstrap DHT
	if err := node.bootstrapDHT(); err != nil {
		log.Printf("Warning: failed to bootstrap DHT: %v", err)
	}

	// Connect to bootnodes
	if err := node.connectToBootnodes(bootnodes); err != nil {
		log.Printf("Warning: failed to connect to some bootnodes: %v", err)
	}

	return node, nil
}

// setupMDNS sets up mDNS discovery
func (n *Node) setupMDNS() error {
	mdnsService := mdns.NewMdnsService(n.host, ServiceTag, &discoveryNotifee{node: n})
	if err := mdnsService.Start(); err != nil {
		return fmt.Errorf("failed to start mDNS service: %w", err)
	}

	n.mdns = mdnsService
	return nil
}

// Close shuts down the node
func (n *Node) Close() error {
	if n.mdns != nil {
		if err := n.mdns.Close(); err != nil {
			log.Printf("Error closing mDNS service: %v", err)
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

// ConnectedPeers returns the list of connected peers
func (n *Node) ConnectedPeers() []peer.ID {
	return n.host.Network().Peers()
}

// Connect connects to a peer
func (n *Node) Connect(ctx context.Context, peerAddr string) error {
	// Parse the peer address
	maddr, err := peer.AddrInfoFromString(peerAddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer address: %w", err)
	}

	// Connect to the peer
	if err := n.host.Connect(ctx, *maddr); err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	log.Printf("Connected to peer: %s", maddr.ID.String())
	return nil
}

// SendContent sends content to a peer
func (n *Node) SendContent(ctx context.Context, peerID peer.ID, data []byte) error {
	return n.sendData(ctx, peerID, ProtocolContentExchange, data)
}

// SendChunk sends a chunk to a peer
func (n *Node) SendChunk(ctx context.Context, peerID peer.ID, data []byte) error {
	return n.sendData(ctx, peerID, ProtocolChunkExchange, data)
}

// BroadcastGossip broadcasts gossip message to all connected peers
func (n *Node) BroadcastGossip(ctx context.Context, data []byte) error {
	peers := n.ConnectedPeers()

	for _, peerID := range peers {
		go func(pid peer.ID) {
			if err := n.sendData(ctx, pid, ProtocolGossip, data); err != nil {
				log.Printf("Failed to send gossip to peer %s: %v", pid.String(), err)
			}
		}(peerID)
	}

	return nil
}

// sendData sends data to a peer using the specified protocol
func (n *Node) sendData(ctx context.Context, peerID peer.ID, protocolID protocol.ID, data []byte) error {
	// Open a stream to the peer
	stream, err := n.host.NewStream(ctx, peerID, protocolID)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	// Set write deadline
	stream.SetWriteDeadline(time.Now().Add(30 * time.Second))

	// Send the data
	if _, err := stream.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// handleContentStream handles incoming content streams by reading a hash and sending back metadata and content.
func (n *Node) handleContentStream(stream network.Stream) {
	defer stream.Close()

	// Read the requested content hash
	buf := make([]byte, 256) // Hashes are short
	bytesRead, err := stream.Read(buf)
	if err != nil {
		if err != io.EOF && err != context.Canceled {
			log.Printf("Failed to read content request hash: %v", err)
		}
		return
	}
	contentHashStr := string(buf[:bytesRead])
	log.Printf("Received content request for hash: %s from %s", contentHashStr, stream.Conn().RemotePeer().String())

	// Check if storage is available
	if n.storage == nil {
		log.Printf("Error: storage not configured for libp2p node, cannot handle content request")
		stream.Reset()
		return
	}

	// Get hash object
	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		log.Printf("Invalid hash received in content request: %s", contentHashStr)
		stream.Reset()
		return
	}

	// Get metadata from storage
	metadata, err := n.storage.GetContent(hash)
	if err != nil {
		log.Printf("Failed to get metadata for hash %s: %v", contentHashStr, err)
		stream.Reset()
		return
	}

	// Get data from storage
	data, err := n.storage.GetData(hash)
	if err != nil {
		log.Printf("Failed to get data for hash %s: %v", contentHashStr, err)
		stream.Reset()
		return
	}

	// Serialize metadata to JSON
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		log.Printf("Failed to marshal metadata for hash %s: %v", contentHashStr, err)
		stream.Reset()
		return
	}

	// Write the metadata length, then metadata, then content
	stream.SetWriteDeadline(time.Now().Add(60 * time.Second))

	// Write metadata length (as 4-byte big-endian integer)
	metaLen := uint32(len(metaBytes))
	if err := binary.Write(stream, binary.BigEndian, metaLen); err != nil {
		log.Printf("Failed to write metadata length: %v", err)
		return
	}

	// Write metadata
	if _, err := stream.Write(metaBytes); err != nil {
		log.Printf("Failed to write metadata: %v", err)
		return
	}

	// Write content data
	if _, err := stream.Write(data); err != nil {
		log.Printf("Failed to write content response to stream: %v", err)
	} else {
		log.Printf("Successfully sent metadata and content %s to %s", contentHashStr, stream.Conn().RemotePeer().String())
	}
}

// handleChunkStream handles incoming chunk streams
func (n *Node) handleChunkStream(stream network.Stream) {
	defer stream.Close()

	// Set read deadline
	stream.SetReadDeadline(time.Now().Add(30 * time.Second))

	// Read the data
	data := make([]byte, 1024*1024) // 1MB buffer
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read chunk stream: %v", err)
		return
	}

	// Call the chunk handler if set
	if n.ChunkHandler != nil {
		if err := n.ChunkHandler(stream.Conn().RemotePeer(), data[:bytesRead]); err != nil {
			log.Printf("Chunk handler error: %v", err)
		}
	}
}

// handleGossipStream handles incoming gossip streams
func (n *Node) handleGossipStream(stream network.Stream) {
	defer stream.Close()

	// Set read deadline
	stream.SetReadDeadline(time.Now().Add(30 * time.Second))

	// Read the data
	data := make([]byte, 64*1024) // 64KB buffer for gossip messages
	bytesRead, err := stream.Read(data)
	if err != nil {
		log.Printf("Failed to read gossip stream: %v", err)
		return
	}

	// Call the gossip handler if set
	if n.GossipHandler != nil {
		if err := n.GossipHandler(stream.Conn().RemotePeer(), data[:bytesRead]); err != nil {
			log.Printf("Gossip handler error: %v", err)
		}
	}
}

// discoveryNotifee handles peer discovery notifications
type discoveryNotifee struct {
	node *Node
}

// HandlePeerFound is called when a new peer is discovered
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	log.Printf("Discovered peer: %s", pi.ID.String())

	// Try to connect to the discovered peer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := n.node.host.Connect(ctx, pi); err != nil {
		log.Printf("Failed to connect to discovered peer %s: %v", pi.ID.String(), err)
	} else {
		log.Printf("Successfully connected to discovered peer: %s", pi.ID.String())
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

// Default bootnodes for initial network connectivity
var DefaultBootnodes = []string{
	// Add hardcoded bootnode addresses here
	// These should be stable, publicly accessible nodes
	// Example: "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
	// For now, we'll use empty list - in production, add real bootnode addresses
}

// connectToBootnodes connects to the provided bootnodes or default ones
func (n *Node) connectToBootnodes(bootnodes []string) error {
	// Use provided bootnodes or fall back to defaults
	nodesToConnect := bootnodes
	if len(nodesToConnect) == 0 {
		nodesToConnect = DefaultBootnodes
	}

	if len(nodesToConnect) == 0 {
		log.Printf("No bootnodes specified and no default bootnodes available")
		return nil
	}

	log.Printf("Connecting to %d bootnode(s)...", len(nodesToConnect))

	var lastErr error
	connectedCount := 0

	for _, bootnode := range nodesToConnect {
		if bootnode == "" {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := n.Connect(ctx, bootnode); err != nil {
			log.Printf("Failed to connect to bootnode %s: %v", bootnode, err)
			lastErr = err
		} else {
			log.Printf("Successfully connected to bootnode: %s", bootnode)
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

// AnnounceContent announces that this node provides the given content hash
func (n *Node) AnnounceContent(contentHash string) error {
	if n.dht == nil {
		return fmt.Errorf("DHT not initialized")
	}

	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()

	// Create CID from content hash
	hash, err := multihash.FromHexString("1220" + contentHash) // SHA256 prefix + hex hash
	if err != nil {
		// Try direct conversion from string
		mh, err := multihash.Sum([]byte(contentHash), multihash.SHA2_256, -1)
		if err != nil {
			return fmt.Errorf("failed to create multihash: %w", err)
		}
		hash = mh
	}

	contentCID := cid.NewCidV1(cid.Raw, hash)

	log.Printf("Announcing content provider for hash: %s", contentHash)
	return n.dht.Provide(ctx, contentCID, true)
}

// FindContentProviders finds peers that provide the given content hash
func (n *Node) FindContentProviders(contentHash string) ([]peer.AddrInfo, error) {
	if n.dht == nil {
		return nil, fmt.Errorf("DHT not initialized")
	}

	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()

	// Create CID from content hash
	hash, err := multihash.FromHexString("1220" + contentHash) // SHA256 prefix + hex hash
	if err != nil {
		// Try direct conversion from string
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

// RequestContentFromPeer requests content from a specific peer. It returns the fetched data and its metadata.
func (n *Node) RequestContentFromPeer(peerID peer.ID, contentHash string) ([]byte, *storage.ContentMetadata, error) {
	ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
	defer cancel()

	// Open a stream to the peer
	stream, err := n.host.NewStream(ctx, peerID, ProtocolContentExchange)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open stream to peer %s: %w", peerID.String(), err)
	}
	defer stream.Close()

	// Send content request (hash)
	stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := stream.Write([]byte(contentHash)); err != nil {
		return nil, nil, fmt.Errorf("failed to send content request: %w", err)
	}

	// We need to close the write side of the stream to signal to the other peer
	// that we're done sending our request.
	stream.CloseWrite()

	// Read response
	stream.SetReadDeadline(time.Now().Add(60 * time.Second))

	// Read metadata length
	var metaLen uint32
	if err := binary.Read(stream, binary.BigEndian, &metaLen); err != nil {
		return nil, nil, fmt.Errorf("failed to read metadata length: %w", err)
	}

	// Read metadata
	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(stream, metaBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	// Unmarshal metadata
	var metadata storage.ContentMetadata
	if err := json.Unmarshal(metaBytes, &metadata); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Read content data
	data, err := ioutil.ReadAll(stream)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read content response: %w", err)
	}

	// Verify content hash
	if metadata.ContentHash == (hasher.Hash{}) {
		// If the metadata from the peer doesn't have a ContentHash, compute it from the received data
		// and log a warning. This might happen with older content or misconfigured peers.
		log.Printf("Warning: Received metadata from peer %s for hash %s has no ContentHash. Computing from data.", peerID.String(), contentHash)
		metadata.ContentHash = hasher.HashBytes(data)
	}

	receivedContentHash := hasher.HashBytes(data)
	if !bytes.Equal(receivedContentHash[:], metadata.ContentHash[:]) {
		return nil, nil, fmt.Errorf("content hash mismatch: expected %s, got %s", metadata.ContentHash.String(), receivedContentHash.String())
	}

	return data, &metadata, nil
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
