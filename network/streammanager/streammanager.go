package streammanager

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"openhashdb/core/hasher"
	"openhashdb/network/libp2p"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	MaxConcurrentStreams = 10
	StreamTimeout        = 5 * time.Minute
	QueueTimeout         = 10 * time.Minute
)

// TransferRequest represents a request to transfer a large file.
type TransferRequest struct {
	Hash      hasher.Hash
	PeerID    peer.ID
	Response  chan io.ReadCloser
	Error     chan error
	Ctx       context.Context
}

// StreamManager handles the queuing and processing of large file transfers.
type StreamManager struct {
	node          *libp2p.Node
	requests      chan *TransferRequest
	activeStreams map[string]context.CancelFunc
	streamSlots   chan struct{}
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager(node *libp2p.Node) *StreamManager {
	ctx, cancel := context.WithCancel(context.Background())
	sm := &StreamManager{
		node:          node,
		requests:      make(chan *TransferRequest, 100),
		activeStreams: make(map[string]context.CancelFunc),
		streamSlots:   make(chan struct{}, MaxConcurrentStreams),
		ctx:           ctx,
		cancel:        cancel,
	}
	go sm.processRequests()
	return sm
}

// Close shuts down the StreamManager.
func (sm *StreamManager) Close() {
	sm.cancel()
	close(sm.requests)
}

// RequestTransfer queues a new transfer request.
func (sm *StreamManager) RequestTransfer(ctx context.Context, hash hasher.Hash, peerID peer.ID) (io.ReadCloser, error) {
	req := &TransferRequest{
		Hash:     hash,
		PeerID:   peerID,
		Response: make(chan io.ReadCloser, 1),
		Error:    make(chan error, 1),
		Ctx:      ctx,
	}

	select {
	case sm.requests <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case stream := <-req.Response:
		return stream, nil
	case err := <-req.Error:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// processRequests processes queued transfer requests.
func (sm *StreamManager) processRequests() {
	for {
		select {
		case req := <-sm.requests:
			sm.streamSlots <- struct{}{} // Acquire a slot for a concurrent stream
			go sm.handleTransfer(req)
		case <-sm.ctx.Done():
			return
		}
	}
}

// handleTransfer manages a single file transfer, including resuming.
func (sm *StreamManager) handleTransfer(req *TransferRequest) {
	defer func() {
		<-sm.streamSlots // Release the stream slot
	}()

	streamID := fmt.Sprintf("%s-%s", req.PeerID.String(), req.Hash.String())
	sm.mu.Lock()
	if _, exists := sm.activeStreams[streamID]; exists {
		sm.mu.Unlock()
		req.Error <- fmt.Errorf("transfer for %s from %s already in progress", req.Hash.String(), req.PeerID.String())
		return
	}

	_, cancel := context.WithCancel(req.Ctx)
	sm.activeStreams[streamID] = cancel
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.activeStreams, streamID)
		sm.mu.Unlock()
		cancel()
	}()

	log.Printf("Starting large file stream for %s from %s", req.Hash.String(), req.PeerID.String())

	req.Error <- fmt.Errorf("resumable streams not yet implemented with bitswap")
}