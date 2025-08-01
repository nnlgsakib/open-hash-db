package bootnode

import (
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

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

const (
	ServiceTag              = "openhashdb"
	ProtocolGossip          = protocol.ID("/openhashdb/gossip/1.0.0")
	ProtocolContentExchange = protocol.ID("/openhashdb/content/1.0.0")
)

// BootNode represents a libp2p boot node.
type BootNode struct {
	host    host.Host
	ctx     context.Context
	cancel  context.CancelFunc
	dht     *dht.IpfsDHT
	mdns    mdns.Service
	storage *storage.Storage
}

// SetStorage sets the storage backend for the boot node.
func (bn *BootNode) SetStorage(s *storage.Storage) {
	bn.storage = s
}

// loadOrCreateIdentity loads or creates a private key.
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

	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(keyBytes)), 0600); err != nil {
		return nil, fmt.Errorf("failed to save private key: %w", err)
	}

	return privKey, nil
}

// DefaultBootnodes for the VPS-hosted default node.
var DefaultBootnodes = []string{
	// "/ip4/148.251.35.204/tcp/35949/p2p/QmNwQH2JdRrh9bGGEprm6QA7D2ErNJ3a8WRWZTCVYDS1NS",
}

// convertBootnodesToAddrInfo converts multiaddresses to peer.AddrInfo.
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

// discoveryHandler implements the mdns.DiscoveryHandler interface.
type discoveryHandler struct {
	host host.Host
}

func (dh *discoveryHandler) HandlePeerFound(pi peer.AddrInfo) {
	log.Printf("Discovered peer via mDNS: %s", pi.ID)
	if err := dh.host.Connect(context.Background(), pi); err != nil {
		log.Printf("Failed to connect to mDNS peer %s: %v", pi.ID, err)
	} else {
		log.Printf("Connected to mDNS peer %s", pi.ID)
	}
}

// NewBootNode creates a new libp2p boot node.
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

	var nodeDHT *dht.IpfsDHT
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableRelay(),
		libp2p.EnableRelayService(),
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

	nodeCtx, cancel := context.WithCancel(ctx)
	bootNode := &BootNode{
		host:   h,
		ctx:    nodeCtx,
		cancel: cancel,
		dht:    nodeDHT,
	}

	// Accept gossip streams but do nothing with them
	h.SetStreamHandler(ProtocolGossip, func(s network.Stream) {
		log.Printf("Received gossip stream from %s, closing.", s.Conn().RemotePeer())
		s.Reset()
	})

	h.SetStreamHandler(ProtocolContentExchange, bootNode.handleContentStream)

	// Set up mDNS with a proper handler
	mdnsService := mdns.NewMdnsService(h, ServiceTag, &discoveryHandler{host: h})
	if err := mdnsService.Start(); err != nil {
		log.Printf("Warning: failed to start mDNS: %v", err)
	} else {
		bootNode.mdns = mdnsService
	}

	log.Printf("BootNode started with ID: %s", h.ID().String())
	log.Printf("Listening on addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  %s/p2p/%s", addr, h.ID().String())
	}

	// Start periodic DHT bootstrapping
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-nodeCtx.Done():
				return
			case <-ticker.C:
				if err := bootNode.bootstrapDHT(); err != nil {
					log.Printf("Failed to bootstrap DHT: %v", err)
				}
			}
		}
	}()

	// Perform initial DHT bootstrap and bootnode connections
	go func() {
		if err := bootNode.bootstrapDHT(); err != nil {
			log.Printf("Warning: failed to bootstrap DHT: %v", err)
		}
		if err := bootNode.connectToBootnodes(allBootnodes); err != nil {
			log.Printf("Warning: failed to connect to some bootnodes: %v", err)
		}
	}()

	return bootNode, nil
}

// Close shuts down the boot node.
func (bn *BootNode) Close() error {
	var errs []error
	if bn.mdns != nil {
		if err := bn.mdns.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing mDNS: %w", err))
		}
	}
	if bn.dht != nil {
		if err := bn.dht.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing DHT: %w", err))
		}
	}
	if err := bn.host.Close(); err != nil {
		errs = append(errs, fmt.Errorf("error closing host: %w", err))
	}
	bn.cancel()

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}
	return nil
}

// Addrs returns the node's addresses.
func (bn *BootNode) Addrs() []string {
	var addrs []string
	for _, addr := range bn.host.Addrs() {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", addr, bn.host.ID().String()))
	}
	return addrs
}

// ID returns the node's peer ID.
func (bn *BootNode) ID() peer.ID {
	return bn.host.ID()
}

// connectToBootnodes connects to bootnodes in parallel.
func (bn *BootNode) connectToBootnodes(bootnodes []string) error {
	if len(bootnodes) == 0 {
		log.Println("No bootnodes specified")
		return nil
	}

	log.Printf("Connecting to %d bootnode(s)...", len(bootnodes))
	var wg sync.WaitGroup
	var mu sync.Mutex
	connectedCount := 0
	var lastErr error

	for _, bootnode := range bootnodes {
		if bootnode == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(bn.ctx, 30*time.Second)
			defer cancel()

			addrInfo, err := peer.AddrInfoFromString(addr)
			if err != nil {
				log.Printf("Failed to parse peer address %s: %v", addr, err)
				mu.Lock()
				lastErr = err
				mu.Unlock()
				return
			}

			if err := bn.host.Connect(ctx, *addrInfo); err != nil {
				log.Printf("Failed to connect to bootnode %s: %v", addr, err)
				mu.Lock()
				lastErr = err
				mu.Unlock()
			} else {
				mu.Lock()
				connectedCount++
				mu.Unlock()
			}
		}(bootnode)
	}
	wg.Wait()

	log.Printf("Connected to %d out of %d bootnodes", connectedCount, len(bootnodes))
	if connectedCount == 0 && len(bootnodes) > 0 {
		return fmt.Errorf("failed to connect to any bootnodes: %w", lastErr)
	}
	return nil
}

// bootstrapDHT bootstraps the DHT.
func (bn *BootNode) bootstrapDHT() error {
	if bn.dht == nil {
		return fmt.Errorf("DHT not initialized")
	}
	log.Println("Bootstrapping DHT...")
	ctx, cancel := context.WithTimeout(bn.ctx, 30*time.Second)
	defer cancel()
	if err := bn.dht.Bootstrap(ctx); err != nil {
		return fmt.Errorf("failed to bootstrap DHT: %w", err)
	}
	return nil
}

// handleContentStream handles content streams
func (bn *BootNode) handleContentStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling content stream from %s", remotePeer.String())

	stream.SetReadDeadline(time.Now().Add(30 * time.Second))
	buf := make([]byte, 256)
	bytesRead, err := stream.Read(buf)
	if err != nil && err != io.EOF && err != context.Canceled {
		log.Printf("Failed to read content request from %s: %v", remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to read content request: %v", err)))
		return
	}
	contentHashStr := string(buf[:bytesRead])
	log.Printf("Received content request for %s from %s", contentHashStr, remotePeer.String())

	if bn.storage == nil {
		log.Printf("Error: storage not configured for %s", remotePeer.String())
		_, _ = stream.Write([]byte("ERROR: storage not configured"))
		return
	}

	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		log.Printf("Invalid hash %s from %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: invalid hash: %v", err)))
		return
	}

	if err := bn.storage.ValidateContent(hash); err != nil {
		log.Printf("Content validation failed for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: %v", err)))
		return
	}

	metadata, err := bn.storage.GetContent(hash)
	if err != nil {
		log.Printf("Failed to get metadata for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to get metadata: %v", err)))
		return
	}

	dataStream, err := bn.storage.GetDataStream(hash)
	if err != nil {
		log.Printf("Failed to get data stream for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to get data: %v", err)))
		return
	}
	defer dataStream.Close()

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		log.Printf("Failed to marshal metadata for %s from %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to marshal metadata: %v", err)))
		return
	}

	stream.SetWriteDeadline(time.Now().Add(60 * time.Second))
	if err := binary.Write(stream, binary.BigEndian, uint32(len(metaBytes))); err != nil {
		log.Printf("Failed to write metadata length for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write metadata length: %v", err)))
		return
	}

	if _, err := stream.Write(metaBytes); err != nil {
		log.Printf("Failed to write metadata for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write metadata: %v", err)))
		return
	}

	stream.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	bytesSent, err := io.Copy(stream, dataStream)
	if err != nil {
		log.Printf("Failed to write content for %s to %s: %v", contentHashStr, remotePeer.String(), err)
		_, _ = stream.Write([]byte(fmt.Sprintf("ERROR: failed to write content: %v", err)))
		return
	}
	log.Printf("Sent %d bytes of content %s to %s", bytesSent, contentHashStr, remotePeer.String())

	if err := stream.CloseWrite(); err != nil {
		log.Printf("Failed to close write stream for %s to %s: %v", contentHashStr, remotePeer.String(), err)
	}
}
