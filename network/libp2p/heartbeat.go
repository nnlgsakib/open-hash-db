package libp2p

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	ProtocolHeartbeat = protocol.ID("/openhashdb/heartbeat/1.0.0")
	HeartbeatInterval = 15 * time.Second
	HeartbeatTimeout  = 30 * time.Second
)

// HeartbeatService manages connection liveness.
type HeartbeatService struct {
	host host.Host
	ctx  context.Context
}

// NewHeartbeatService creates a new HeartbeatService.
func NewHeartbeatService(ctx context.Context, host host.Host) *HeartbeatService {
	hs := &HeartbeatService{
		host: host,
		ctx:  ctx,
	}
	host.SetStreamHandler(ProtocolHeartbeat, hs.handleHeartbeatStream)
	return hs
}

// handleHeartbeatStream handles incoming heartbeat requests. It does nothing, just proves liveness.
func (hs *HeartbeatService) handleHeartbeatStream(stream network.Stream) {
	// The stream is immediately closed to signal that the peer is alive.
	stream.Close()
}

// MonitorStream starts a monitoring goroutine for a given data stream.
// It returns a function that should be called to stop the monitor when the stream is closed.
func (hs *HeartbeatService) MonitorStream(dataStream network.Stream) func() {
	peerID := dataStream.Conn().RemotePeer()
	ctx, cancel := context.WithCancel(hs.ctx)

	go hs.monitor(ctx, peerID, dataStream)

	return cancel
}

func (hs *HeartbeatService) monitor(ctx context.Context, peerID peer.ID, dataStream network.Stream) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	log.Printf("Starting heartbeat monitor for stream with %s", peerID)
	defer log.Printf("Stopping heartbeat monitor for stream with %s", peerID)

	for {
		select {
		case <-ticker.C:
			if err := hs.sendHeartbeat(peerID); err != nil {
				log.Printf("Heartbeat to %s failed, resetting data stream: %v", peerID, err)
				dataStream.Reset()
				return
			}
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			// A non-blocking read to check if the stream is closed.
			// This is a bit of a hack, but it's the most reliable way to check for stream closure in this version of libp2p.
			buf := make([]byte, 1)
			dataStream.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
			_, err := dataStream.Read(buf)
			if err == io.EOF {
				log.Printf("Data stream with %s closed, stopping heartbeat monitor.", peerID)
				return
			}
		}
	}
}

// sendHeartbeat sends a single heartbeat to a peer and waits for it to complete.
func (hs *HeartbeatService) sendHeartbeat(peerID peer.ID) error {
	ctx, cancel := context.WithTimeout(hs.ctx, HeartbeatTimeout)
	defer cancel()

	stream, err := hs.host.NewStream(ctx, peerID, ProtocolHeartbeat)
	if err != nil {
		return err
	}
	defer stream.Close()

	// We expect the other side to close it immediately.
	// The read will block until the stream is closed or the timeout is hit.
	buf := make([]byte, 1)
	_, err = stream.Read(buf)
	if err != io.EOF {
		// We expect an EOF from a graceful close. Any other error is a problem.
		stream.Reset()
		return err
	}
	return nil
}
