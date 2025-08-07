package libp2p

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	ProtocolHeartbeat = protocol.ID("/openhashdb/heartbeat/1.0.0")
	HeartbeatInterval = 15 * time.Second
	HeartbeatTimeout  = 30 * time.Second
	MaxRetries        = 3
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

// HeartbeatService manages peer connection liveness
type HeartbeatService struct {
	host        host.Host
	ctx         context.Context
	monitored   map[peer.ID]context.CancelFunc // Tracks monitored peers
	monitoredMu sync.Mutex                     // Protects monitored map
}

// NewHeartbeatService creates a new HeartbeatService
func NewHeartbeatService(ctx context.Context, host host.Host) *HeartbeatService {
	hs := &HeartbeatService{
		host:      host,
		ctx:       ctx,
		monitored: make(map[peer.ID]context.CancelFunc),
	}
	host.SetStreamHandler(ProtocolHeartbeat, hs.handleHeartbeatStream)
	return hs
}

// handleHeartbeatStream handles incoming heartbeat requests
func (hs *HeartbeatService) handleHeartbeatStream(stream network.Stream) {
	defer stream.Close()
	log.Printf("Received heartbeat from %s", stream.Conn().RemotePeer().String())
	// Stream is closed immediately to signal liveness
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
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	defer hs.StopMonitoring(peerID)

	log.Printf("Starting heartbeat monitor for peer %s", peerID.String())

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping heartbeat monitor for peer %s", peerID.String())
			return
		case <-ticker.C:
			if err := hs.sendHeartbeat(peerID); err != nil {
				log.Printf("Heartbeat to %s failed: %v", peerID.String(), err)
				heartbeatFailureTotal.Inc()
				// Reset all connections to the peer on failure
				hs.host.Network().ClosePeer(peerID)
				connectionResetTotal.Inc()
				log.Printf("Connection to %s reset due to heartbeat failure", peerID.String())
				return
			}
			heartbeatSuccessTotal.Inc()
			log.Printf("Successful heartbeat to %s", peerID.String())
		}
	}
}

// sendHeartbeat sends a single heartbeat to a peer with retries
func (hs *HeartbeatService) sendHeartbeat(peerID peer.ID) error {
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(hs.ctx, HeartbeatTimeout)
		defer cancel()

		stream, err := hs.host.NewStream(ctx, peerID, ProtocolHeartbeat)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to open heartbeat stream to %s: %v", attempt, MaxRetries, peerID.String(), err)
			if attempt == MaxRetries {
				return err
			}
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond) // Exponential backoff
			continue
		}
		defer stream.Close()

		// Wait for the remote peer to close the stream, indicating liveness
		buf := make([]byte, 1)
		stream.SetReadDeadline(time.Now().Add(HeartbeatTimeout))
		_, err = stream.Read(buf)
		if err == io.EOF {
			return nil // Expected EOF from graceful close
		}
		log.Printf("Attempt %d/%d: Unexpected response from %s: %v", attempt, MaxRetries, peerID.String(), err)
		stream.Reset()
		if attempt == MaxRetries {
			return err
		}
		time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
	}
	return fmt.Errorf("failed to send heartbeat to %s after %d attempts", peerID.String(), MaxRetries)
}
