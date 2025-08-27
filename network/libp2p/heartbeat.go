package libp2p

import (
    "context"
    "log"
    "math/rand"
    "strings"
    "sync"
    "time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	ProtocolHeartbeat = protocol.ID("/openhashdb/heartbeat/1.0.0")
	HeartbeatInterval = 15 * time.Second
	HeartbeatTimeout  = 60 * time.Second
)

// Metrics
var (
	heartbeatSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_heartbeat_success_total",
			Help: "Total number of successful heartbeats",
		},
	)
	heartbeatFailureTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_heartbeat_failure_total",
			Help: "Total number of failed heartbeats",
		},
	)
	connectionResetTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_connection_reset_total",
			Help: "Total number of connection resets due to heartbeat failure",
		},
	)
)

// HeartbeatService manages peer connection liveness and peer exchange.
type HeartbeatService struct {
    node          *Node
    ctx           context.Context
    monitored     map[peer.ID]context.CancelFunc // Tracks monitored peers
    monitoredMu   sync.Mutex                     // Protects monitored map
    peerExchanger *PeerExchanger
    failures      map[peer.ID]int // consecutive failures
    stateMu       sync.Mutex
}

// NewHeartbeatService creates a new HeartbeatService
func NewHeartbeatService(ctx context.Context, node *Node) *HeartbeatService {
    hs := &HeartbeatService{
        node:          node,
        ctx:           ctx,
        monitored:     make(map[peer.ID]context.CancelFunc),
        peerExchanger: NewPeerExchanger(ctx, node),
        failures:      make(map[peer.ID]int),
    }
    node.host.SetStreamHandler(ProtocolHeartbeat, hs.handleHeartbeatStream)
    return hs
}

// handleHeartbeatStream handles incoming heartbeat requests and peer exchange
func (hs *HeartbeatService) handleHeartbeatStream(stream network.Stream) {
	log.Printf("[libp2p] Received heartbeat from %s, starting peer exchange.", stream.Conn().RemotePeer().String())
	hs.peerExchanger.handleExchange(stream)
}

// MonitorConnection starts monitoring a peer’s connection health
func (hs *HeartbeatService) MonitorConnection(peerID peer.ID) {
	hs.monitoredMu.Lock()
	if _, exists := hs.monitored[peerID]; exists {
		hs.monitoredMu.Unlock()
		return // Already monitoring
	}

	ctx, cancel := context.WithCancel(hs.ctx)
	hs.monitored[peerID] = cancel
	hs.monitoredMu.Unlock()

	go hs.monitor(ctx, peerID)
}

// StopMonitoring stops monitoring a peer’s connection
func (hs *HeartbeatService) StopMonitoring(peerID peer.ID) {
	hs.monitoredMu.Lock()
	if cancel, exists := hs.monitored[peerID]; exists {
		cancel()
		delete(hs.monitored, peerID)
	}
	hs.monitoredMu.Unlock()
}

// monitor periodically sends heartbeats to a peer
func (hs *HeartbeatService) monitor(ctx context.Context, peerID peer.ID) {
	defer hs.StopMonitoring(peerID)

	// Add a random initial delay to de-synchronize heartbeats
	initialDelay := time.Duration(rand.Intn(1000)) * time.Millisecond
	select {
	case <-time.After(initialDelay):
	case <-ctx.Done():
		log.Printf("[libp2p] Stopping heartbeat monitor for peer %s before initial heartbeat", peerID.String())
		return
	}

	log.Printf("[libp2p] Starting heartbeat monitor for peer %s", peerID.String())
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

    const failureThreshold = 3
    for {
        if err := hs.sendHeartbeat(peerID); err != nil {
            log.Printf("[libp2p] Heartbeat to %s failed: %v", peerID.String(), err)
            heartbeatFailureTotal.Inc()
            hs.stateMu.Lock()
            hs.failures[peerID]++
            fails := hs.failures[peerID]
            hs.stateMu.Unlock()
            if isProtocolUnsupported(err) {
                // If the peer doesn't support the protocol, don't close the connection.
                // Stop monitoring after a few attempts to avoid log spam.
                if fails >= failureThreshold {
                    log.Printf("[libp2p] Peer %s appears not to support heartbeat after %d attempts; stopping monitor (connection remains).", peerID.String(), fails)
                    return
                }
            } else if fails >= failureThreshold {
                // For other errors, reset the peer after repeated failures.
                hs.node.host.Network().ClosePeer(peerID)
                connectionResetTotal.Inc()
                log.Printf("[libp2p] Connection to %s reset after %d heartbeat failures", peerID.String(), fails)
                return
            }
            // brief backoff before next attempt
            select {
            case <-time.After(HeartbeatInterval):
            case <-ctx.Done():
                return
            }
            continue
        }
        heartbeatSuccessTotal.Inc()
        log.Printf("[libp2p] Successful heartbeat to %s", peerID.String())
        hs.stateMu.Lock()
        delete(hs.failures, peerID)
        hs.stateMu.Unlock()

		select {
		case <-ctx.Done():
			log.Printf("[libp2p] Stopping heartbeat monitor for peer %s", peerID.String())
			return
		case <-ticker.C:
		}
	}
}

// sendHeartbeat sends a single heartbeat to a peer and initiates peer exchange
func (hs *HeartbeatService) sendHeartbeat(peerID peer.ID) error {
    ctx, cancel := context.WithTimeout(hs.ctx, HeartbeatTimeout)
    defer cancel()

    // Try to open a heartbeat stream; if the peer doesn't support it, we'll get
    // a negotiation failure/reset which we classify and handle in the caller.
    stream, err := hs.node.host.NewStream(network.WithAllowLimitedConn(ctx, "heartbeat"), peerID, ProtocolHeartbeat)
    if err != nil {
        log.Printf("[libp2p] Failed to open heartbeat stream to %s: %v", peerID.String(), err)
        return err
    }

	// The actual exchange logic is handled by the PeerExchanger
	// It will close the stream
	if err := hs.peerExchanger.initiateExchange(stream); err != nil {
		log.Printf("Peer exchange with %s failed: %v", peerID.String(), err)
		return err
	}

    return nil
}

func isProtocolUnsupported(err error) bool {
    if err == nil {
        return false
    }
    s := err.Error()
    if strings.Contains(s, "protocol not supported") ||
        strings.Contains(s, "protocol negotiation failed") ||
        strings.Contains(s, "no handler for protocol") ||
        // Treat common relay/remote drops as non-fatal for heartbeat
        strings.Contains(s, "unexpected EOF") ||
        strings.Contains(s, "stream reset") ||
        strings.Contains(s, "connection closed") {
        return true
    }
    return false
}
