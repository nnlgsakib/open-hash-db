package libp2p

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	// "github.com/libp2p/go-libp2p/core/protocol"
)

const (
	// PeerExchangeRequest is the message type for requesting peers.
	PeerExchangeRequest = "PEER_EXCHANGE_REQUEST"
	// PeerExchangeResponse is the message type for responding with peers.
	PeerExchangeResponse = "PEER_EXCHANGE_RESPONSE"
	// MaxPeersPerResponse is the maximum number of peers to send in a single response.
	MaxPeersPerResponse = 10
)

// DiscoveryMessage represents a message for the peer discovery protocol.
type DiscoveryMessage struct {
	Type    string   `json:"type"`
	Payload []string `json:"payload"`
}

// DiscoveryService handles peer discovery.
type DiscoveryService struct {
	host   host.Host
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

// NewDiscoveryService creates a new DiscoveryService.
func NewDiscoveryService(ctx context.Context, h host.Host) *DiscoveryService {
	ctx, cancel := context.WithCancel(ctx)
	ds := &DiscoveryService{
		host:   h,
		ctx:    ctx,
		cancel: cancel,
	}
	h.SetStreamHandler(DiscoveryProtocol, ds.handleStream)
	return ds
}

// Start begins the discovery service.
func (ds *DiscoveryService) Start() {
	go ds.periodicPeerRequest()
}

// Stop stops the discovery service.
func (ds *DiscoveryService) Stop() {
	ds.cancel()
}

// handleStream handles incoming discovery streams.
func (ds *DiscoveryService) handleStream(s network.Stream) {
	defer s.Close()

	var msg DiscoveryMessage
	if err := json.NewDecoder(s).Decode(&msg); err != nil {
		log.Printf("Failed to decode discovery message from %s: %v", s.Conn().RemotePeer(), err)
		return
	}

	switch msg.Type {
	case PeerExchangeRequest:
		ds.handlePeerExchangeRequest(s.Conn().RemotePeer())
	case PeerExchangeResponse:
		ds.handlePeerExchangeResponse(s.Conn().RemotePeer(), msg.Payload)
	default:
		log.Printf("Unknown discovery message type from %s: %s", s.Conn().RemotePeer(), msg.Type)
	}
}

// handlePeerExchangeRequest handles a request for peers.
func (ds *DiscoveryService) handlePeerExchangeRequest(p peer.ID) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	peers := ds.host.Network().Peers()
	var peerAddrs []string
	for _, peerID := range peers {
		if peerID == p {
			continue
		}
		for _, addr := range ds.host.Peerstore().Addrs(peerID) {
			peerAddrs = append(peerAddrs, addr.String()+"/p2p/"+peerID.String())
		}
	}

	if len(peerAddrs) == 0 {
		return
	}

	// Limit the number of peers sent in a single response
	if len(peerAddrs) > MaxPeersPerResponse {
		peerAddrs = peerAddrs[:MaxPeersPerResponse]
	}

	msg := DiscoveryMessage{
		Type:    PeerExchangeResponse,
		Payload: peerAddrs,
	}

	if err := ds.sendMessage(p, msg); err != nil {
		log.Printf("Failed to send peer exchange response to %s: %v", p, err)
	}
}

// handlePeerExchangeResponse handles a response with peers.
func (ds *DiscoveryService) handlePeerExchangeResponse(p peer.ID, peerAddrs []string) {
	for _, addrStr := range peerAddrs {
		addr, err := peer.AddrInfoFromString(addrStr)
		if err != nil {
			log.Printf("Failed to parse peer address from %s: %s", p, addrStr)
			continue
		}

		if addr.ID == ds.host.ID() {
			continue
		}

		if ds.host.Network().Connectedness(addr.ID) != network.Connected {
			go func(addrInfo peer.AddrInfo) {
				ctx, cancel := context.WithTimeout(ds.ctx, 15*time.Second)
				defer cancel()
				if err := ds.host.Connect(ctx, addrInfo); err != nil {
					log.Printf("Failed to connect to new peer %s: %v", addrInfo.ID, err)
				} else {
					log.Printf("Connected to new peer %s", addrInfo.ID)
				}
			}(*addr)
		}
	}
}

// sendMessage sends a discovery message to a peer.
func (ds *DiscoveryService) sendMessage(p peer.ID, msg DiscoveryMessage) error {
	s, err := ds.host.NewStream(ds.ctx, p, DiscoveryProtocol)
	if err != nil {
		return err
	}
	defer s.Close()

	return json.NewEncoder(s).Encode(msg)
}

// periodicPeerRequest periodically requests peers from connected peers.
func (ds *DiscoveryService) periodicPeerRequest() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ds.ctx.Done():
			return
		case <-ticker.C:
			ds.requestPeersFromAll()
		}
	}
}

// requestPeersFromAll requests peers from all connected peers.
func (ds *DiscoveryService) requestPeersFromAll() {
	peers := ds.host.Network().Peers()
	if len(peers) == 0 {
		return
	}

	msg := DiscoveryMessage{
		Type: PeerExchangeRequest,
	}

	for _, p := range peers {
		go func(peerID peer.ID) {
			if err := ds.sendMessage(peerID, msg); err != nil {
				log.Printf("Failed to send peer exchange request to %s: %v", peerID, err)
			}
		}(p)
	}
}
