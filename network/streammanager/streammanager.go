package streammanager

import (
	"context"
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
	"openhashdb/network/libp2p"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	MaxConcurrentStreams = 2
	CheckpointSize       = 10 << 20 // 10MB per checkpoint
	CheckpointDir        = "checkpoints"
	HeartbeatInterval    = 15 * time.Second // Matches libp2p.HeartbeatInterval
	MaxCheckpointRetries = 3
)

// Metrics
var (
	transferProgressGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "openhashdb_transfer_progress_bytes",
			Help: "Current progress of file transfers in bytes",
		},
		[]string{"hash", "peer_id"},
	)
	transferTotalGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "openhashdb_transfer_total_bytes",
			Help: "Total size of file transfers in bytes",
		},
		[]string{"hash", "peer_id"},
	)
	checkpointSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_checkpoint_success_total",
			Help: "Total number of successful checkpoints saved",
		},
	)
	checkpointFailureTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_checkpoint_failure_total",
			Help: "Total number of failed checkpoint attempts",
		},
	)
)

// TransferRequest represents a request to transfer a large file.
type TransferRequest struct {
	Hash     hasher.Hash
	PeerID   peer.ID
	Response chan io.ReadCloser
	Error    chan error
	Ctx      context.Context
	Priority bool // True for foreground (new) requests, false for background (resumed)
}

// CheckpointMetadata stores information about a transfer checkpoint.
type CheckpointMetadata struct {
	Hash         hasher.Hash `json:"hash"`
	PeerID       peer.ID     `json:"peer_id"`
	Offset       int64       `json:"offset"`
	TotalSize    int64       `json:"total_size"`
	LastModified time.Time   `json:"last_modified"`
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
	storage       *storage.Storage // For checkpoint persistence
	checkpointMu  sync.RWMutex
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager(node *libp2p.Node, storage *storage.Storage) *StreamManager {
	ctx, cancel := context.WithCancel(context.Background())
	sm := &StreamManager{
		node:          node,
		requests:      make(chan *TransferRequest, 100),
		activeStreams: make(map[string]context.CancelFunc),
		streamSlots:   make(chan struct{}, MaxConcurrentStreams),
		ctx:           ctx,
		cancel:        cancel,
		storage:       storage,
	}
	go sm.processRequests()
	return sm
}

// Close shuts down the StreamManager.
func (sm *StreamManager) Close() {
	sm.cancel()
	close(sm.requests)
}

// saveCheckpoint saves the checkpoint metadata to disk.
func (sm *StreamManager) saveCheckpoint(hash hasher.Hash, peerID peer.ID, offset, totalSize int64) error {
	sm.checkpointMu.Lock()
	defer sm.checkpointMu.Unlock()

	checkpoint := CheckpointMetadata{
		Hash:         hash,
		PeerID:       peerID,
		Offset:       offset,
		TotalSize:    totalSize,
		LastModified: time.Now(),
	}

	metaBytes, err := json.Marshal(checkpoint)
	if err != nil {
		checkpointFailureTotal.Inc()
		return fmt.Errorf("failed to marshal checkpoint metadata: %w", err)
	}

	checkpointPath := filepath.Join(CheckpointDir, fmt.Sprintf("%s-%s.meta", hash.String(), peerID.String()))
	if err := os.MkdirAll(CheckpointDir, 0755); err != nil {
		checkpointFailureTotal.Inc()
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	if err := os.WriteFile(checkpointPath, metaBytes, 0600); err != nil {
		checkpointFailureTotal.Inc()
		return fmt.Errorf("failed to write checkpoint metadata: %w", err)
	}

	checkpointSuccessTotal.Inc()
	log.Printf("Saved checkpoint for %s from %s at offset %d/%d", hash.String(), peerID.String(), offset, totalSize)
	return nil
}

// loadCheckpoint loads the checkpoint metadata from disk.
func (sm *StreamManager) loadCheckpoint(hash hasher.Hash, peerID peer.ID) (*CheckpointMetadata, error) {
	sm.checkpointMu.RLock()
	defer sm.checkpointMu.RUnlock()

	checkpointPath := filepath.Join(CheckpointDir, fmt.Sprintf("%s-%s.meta", hash.String(), peerID.String()))
	metaBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No checkpoint exists
		}
		return nil, fmt.Errorf("failed to read checkpoint metadata: %w", err)
	}

	var checkpoint CheckpointMetadata
	if err := json.Unmarshal(metaBytes, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint metadata: %w", err)
	}

	return &checkpoint, nil
}

// getPartialDataPath returns the path for partial data storage.
func (sm *StreamManager) getPartialDataPath(hash hasher.Hash, peerID peer.ID) string {
	return filepath.Join(CheckpointDir, fmt.Sprintf("%s-%s.partial", hash.String(), peerID.String()))
}

// RequestTransfer queues a new transfer request, resuming from checkpoint if available.
func (sm *StreamManager) RequestTransfer(ctx context.Context, hash hasher.Hash, peerID peer.ID) (io.ReadCloser, error) {
	checkpoint, err := sm.loadCheckpoint(hash, peerID)
	if err != nil {
		log.Printf("Failed to load checkpoint for %s from %s: %v", hash.String(), peerID.String(), err)
	}

	req := &TransferRequest{
		Hash:     hash,
		PeerID:   peerID,
		Response: make(chan io.ReadCloser, 1),
		Error:    make(chan error, 1),
		Ctx:      ctx,
		Priority: true, // New requests are foreground by default
	}

	if checkpoint != nil {
		req.Priority = false // Resumed transfers are background
		log.Printf("Resuming transfer for %s from %s at offset %d/%d", hash.String(), peerID.String(), checkpoint.Offset, checkpoint.TotalSize)
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

// processRequests processes queued transfer requests, prioritizing foreground tasks.
func (sm *StreamManager) processRequests() {
	foreground := make(chan *TransferRequest, 100)
	background := make(chan *TransferRequest, 100)

	// Split requests into foreground and background queues
	go func() {
		for req := range sm.requests {
			if req.Priority {
				select {
				case foreground <- req:
				case <-sm.ctx.Done():
					return
				}
			} else {
				select {
				case background <- req:
				case <-sm.ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case req := <-foreground:
			sm.streamSlots <- struct{}{} // Acquire a slot for a concurrent stream
			go sm.handleTransfer(req)
		case req := <-background:
			select {
			case sm.streamSlots <- struct{}{}: // Try to acquire a slot
				go sm.handleTransfer(req)
			default:
				// If no slots are available, requeue for later
				select {
				case background <- req:
				case <-sm.ctx.Done():
					return
				}
			}
		case <-sm.ctx.Done():
			return
		}
	}
}

// handleTransfer manages a single file transfer with checkpointing and progress reporting.
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

	ctx, cancel := context.WithCancel(req.Ctx)
	sm.activeStreams[streamID] = cancel
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.activeStreams, streamID)
		sm.mu.Unlock()
		cancel()
	}()

	log.Printf("Starting transfer for %s from %s (priority: %v)", req.Hash.String(), req.PeerID.String(), req.Priority)

	// Load checkpoint if available
	checkpoint, err := sm.loadCheckpoint(req.Hash, req.PeerID)
	var offset int64
	var totalSize int64
	partialFilePath := sm.getPartialDataPath(req.Hash, req.PeerID)
	var partialFile *os.File

	if checkpoint != nil {
		offset = checkpoint.Offset
		totalSize = checkpoint.TotalSize
		// Open partial file for appending
		partialFile, err = os.OpenFile(partialFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			req.Error <- fmt.Errorf("failed to open partial file: %w", err)
			return
		}
		defer partialFile.Close()
	} else {
		// Create new partial file
		partialFile, err = os.OpenFile(partialFilePath, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			req.Error <- fmt.Errorf("failed to create partial file: %w", err)
			return
		}
		defer partialFile.Close()
	}

	// Request stream from libp2p
	stream, metadata, err := sm.node.RequestContentStreamFromPeer(ctx, req.PeerID, req.Hash.String())
	if err != nil {
		req.Error <- err
		return
	}

	if totalSize == 0 && metadata.Size > 0 {
		totalSize = metadata.Size
	}

	// Set up progress reporting
	transferTotalGauge.WithLabelValues(req.Hash.String(), req.PeerID.String()).Set(float64(totalSize))
	transferProgressGauge.WithLabelValues(req.Hash.String(), req.PeerID.String()).Set(float64(offset))

	// Create a checkpointing stream
	checkpointStream := &checkpointStream{
		Stream:      stream,
		partialFile: partialFile,
		hash:        req.Hash,
		peerID:      req.PeerID,
		offset:      offset,
		totalSize:   totalSize,
		sm:          sm,
		ctx:         ctx,
	}

	// Start progress reporting in the background
	go sm.reportProgress(checkpointStream)

	req.Response <- checkpointStream
}

// reportProgress reports transfer progress after every heartbeat interval.
func (sm *StreamManager) reportProgress(cs *checkpointStream) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-ticker.C:
			cs.mu.RLock()
			remaining := cs.totalSize - cs.offset
			log.Printf("Transfer progress for %s from %s: %d/%d bytes (%.2f%% complete, %d bytes remaining)",
				cs.hash.String(), cs.peerID.String(), cs.offset, cs.totalSize,
				float64(cs.offset)/float64(cs.totalSize)*100, remaining)
			transferProgressGauge.WithLabelValues(cs.hash.String(), cs.peerID.String()).Set(float64(cs.offset))
			cs.mu.RUnlock()
		}
	}
}

// checkpointStream wraps a stream to handle checkpointing and progress tracking.
type checkpointStream struct {
	Stream      io.ReadCloser
	partialFile *os.File
	hash        hasher.Hash
	peerID      peer.ID
	offset      int64
	totalSize   int64
	sm          *StreamManager
	ctx         context.Context
	mu          sync.RWMutex
}

func (cs *checkpointStream) Read(p []byte) (int, error) {
	n, err := cs.Stream.Read(p)
	if err != nil && err != io.EOF {
		return n, err
	}

	cs.mu.Lock()
	cs.offset += int64(n)
	cs.mu.Unlock()

	// Save checkpoint every CheckpointSize bytes
	if cs.offset/CheckpointSize > (cs.offset-int64(n))/CheckpointSize {
		for attempt := 1; attempt <= MaxCheckpointRetries; attempt++ {
			if err := cs.sm.saveCheckpoint(cs.hash, cs.peerID, cs.offset, cs.totalSize); err != nil {
				log.Printf("Attempt %d/%d: Failed to save checkpoint for %s from %s at offset %d: %v",
					attempt, MaxCheckpointRetries, cs.hash.String(), cs.peerID.String(), cs.offset, err)
				if attempt == MaxCheckpointRetries {
					return n, fmt.Errorf("failed to save checkpoint after %d attempts: %w", MaxCheckpointRetries, err)
				}
				time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
				continue
			}
			// Write data to partial file
			if _, err := cs.partialFile.Write(p[:n]); err != nil {
				log.Printf("Failed to write to partial file for %s from %s: %v", cs.hash.String(), cs.peerID.String(), err)
				return n, fmt.Errorf("failed to write to partial file: %w", err)
			}
			break
		}
	}

	return n, err
}

func (cs *checkpointStream) Close() error {
	// Save final checkpoint
	cs.mu.RLock()
	offset := cs.offset
	totalSize := cs.totalSize
	cs.mu.RUnlock()

	if err := cs.sm.saveCheckpoint(cs.hash, cs.peerID, offset, totalSize); err != nil {
		log.Printf("Failed to save final checkpoint for %s from %s: %v", cs.hash.String(), cs.peerID.String(), err)
	}

	// Write any remaining data to partial file
	if _, err := cs.partialFile.Write(nil); err != nil {
		log.Printf("Failed to flush partial file for %s from %s: %v", cs.hash.String(), cs.peerID.String(), err)
	}

	if offset >= totalSize && totalSize > 0 {
		// Transfer complete, clean up checkpoint and partial file
		checkpointPath := filepath.Join(CheckpointDir, fmt.Sprintf("%s-%s.meta", cs.hash.String(), cs.peerID.String()))
		partialPath := cs.sm.getPartialDataPath(cs.hash, cs.peerID)
		if err := os.Remove(checkpointPath); err != nil && !os.IsNotExist(err) {
			log.Printf("Failed to remove checkpoint file for %s from %s: %v", cs.hash.String(), cs.peerID.String(), err)
		}
		if err := os.Remove(partialPath); err != nil && !os.IsNotExist(err) {
			log.Printf("Failed to remove partial file for %s from %s: %v", cs.hash.String(), cs.peerID.String(), err)
		}
		transferProgressGauge.WithLabelValues(cs.hash.String(), cs.peerID.String()).Set(0)
		transferTotalGauge.WithLabelValues(cs.hash.String(), cs.peerID.String()).Set(0)
	}

	return cs.Stream.Close()
}