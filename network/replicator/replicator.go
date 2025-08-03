package replicator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/libp2p"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for monitoring
var (
	replicationRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openhashdb_replication_requests_total",
			Help: "Total number of replication requests",
		},
		[]string{"type"},
	)
	replicationSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_replication_success_total",
			Help: "Total number of successful replications",
		},
	)
	replicationFailuresTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_replication_failures_total",
			Help: "Total number of failed replications",
		},
	)
)

// ReplicationFactor represents the desired number of replicas
type ReplicationFactor int

const (
	DefaultReplicationFactor ReplicationFactor = 3
	MinReplicationFactor     ReplicationFactor = 1
	MaxReplicationFactor     ReplicationFactor = 10
	RelayThreshold                             = 5
	RequestTimeout                             = 30 * time.Second
	CleanupInterval                            = 1 * time.Minute
)

// ContentAnnouncement represents an announcement of new content
type ContentAnnouncement struct {
	Hash      hasher.Hash `json:"hash"`
	Size      int64       `json:"size"`
	Timestamp time.Time   `json:"timestamp"`
	PeerID    string      `json:"peer_id"`
	IsLarge   bool        `json:"is_large"`
}

// ChunkRequest represents a request for a specific chunk
type ChunkRequest struct {
	Hash      hasher.Hash `json:"hash"`
	RequestID string      `json:"request_id"`
}

// ChunkResponse represents a response to a chunk request
type ChunkResponse struct {
	Hash      hasher.Hash `json:"hash"`
	Data      []byte      `json:"data"`
	RequestID string      `json:"request_id"`
	Success   bool        `json:"success"`
}

// PinRequest represents a request to pin content
type PinRequest struct {
	Hash     hasher.Hash `json:"hash"`
	Priority int         `json:"priority"`
}

// RequestTracker tracks content request frequency
type RequestTracker struct {
	Count     int
	FirstSeen time.Time
}

// Replicator handles content replication and availability
type Replicator struct {
	storage           *storage.Storage
	node              *libp2p.Node
	replicationFactor ReplicationFactor
	pinnedContent     map[string]struct{}
	pendingRequests   map[string]chan ChunkResponse
	requestTracker    map[string]*RequestTracker
	relayCache        map[string][]byte
	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
}

// NewReplicator creates a new replicator
func NewReplicator(storage *storage.Storage, node *libp2p.Node, replicationFactor ReplicationFactor) *Replicator {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Replicator{
		storage:           storage,
		node:              node,
		replicationFactor: replicationFactor,
		pinnedContent:     make(map[string]struct{}),
		pendingRequests:   make(map[string]chan ChunkResponse),
		requestTracker:    make(map[string]*RequestTracker),
		relayCache:        make(map[string][]byte),
		ctx:               ctx,
		cancel:            cancel,
	}

	node.GossipHandler = r.handleGossipMessage
	node.ChunkHandler = r.handleChunkMessage
	go r.announceContentPeriodically()
	go r.cleanupPendingRequests()

	return r
}

// Close shuts down the replicator
func (r *Replicator) Close() error {
	r.cancel()
	return nil
}

// RequestChunk requests a chunk from the network.
func (r *Replicator) RequestChunk(hash hasher.Hash) ([]byte, error) {
	// Check local storage first
	if r.storage.HasData(hash) {
		return r.storage.GetData(hash)
	}

	// Find providers for the chunk
	providers, err := r.node.FindContentProviders(hash.String())
	if err != nil {
		return nil, fmt.Errorf("failed to find providers for chunk %s: %w", hash.String(), err)
	}

	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers found for chunk %s", hash.String())
	}

	// Request the chunk from the first provider
	// A more robust implementation would try multiple providers
	data, _, err := r.node.RequestContentFromPeer(providers[0].ID, hash.String())
	if err != nil {
		return nil, fmt.Errorf("failed to request chunk %s from peer %s: %w", hash.String(), providers[0].ID, err)
	}

	// Store the chunk locally
	if err := r.storage.StoreData(hash, data); err != nil {
		log.Printf("Failed to store chunk %s: %v", hash.String(), err)
	}

	return data, nil
}

// AnnounceContent announces new content
func (r *Replicator) AnnounceContent(hash hasher.Hash, size int64, isLargeFile bool) error {
	// Verify that content metadata exists before announcing
	if !r.storage.HasContent(hash) {
		log.Printf("Content %s not found locally, skipping announcement", hash.String())
		return fmt.Errorf("content %s not found locally", hash.String())
	}

	if err := r.node.AnnounceContent(hash.String()); err != nil {
		log.Printf("Warning: failed to announce content to DHT: %v", err)
	}

	announcement := ContentAnnouncement{
		Hash:      hash,
		Size:      size,
		Timestamp: time.Now(),
		PeerID:    r.node.ID().String(),
		IsLarge:   isLargeFile,
	}

	data, err := json.Marshal(announcement)
	if err != nil {
		return fmt.Errorf("failed to marshal announcement: %w", err)
	}

	return r.node.BroadcastGossip(r.ctx, data)
}





// PinContent pins content
func (r *Replicator) PinContent(hash hasher.Hash) error {
	r.mu.Lock()
	r.pinnedContent[hash.String()] = struct{}{}
	r.mu.Unlock()

	if r.storage.HasContent(hash) {
		return r.storage.IncrementRefCount(hash)
	}

	// If content is not local, fetch it in the background
	go func() {
		log.Printf("Content %s is pinned but not local, fetching...", hash.String())
		providers, err := r.node.FindContentProviders(hash.String())
		if err != nil {
			log.Printf("Failed to find providers for pinned content %s: %v", hash.String(), err)
			return
		}
		if len(providers) == 0 {
			log.Printf("No providers found for pinned content %s", hash.String())
			return
		}

		var data []byte
		var metadata *storage.ContentMetadata
		var fetchErr error

		for _, p := range providers {
			if p.ID == r.node.ID() {
				continue // Skip self
			}
			data, metadata, fetchErr = r.node.RequestContentFromPeer(p.ID, hash.String())
			if fetchErr == nil {
				break // Success
			}
			log.Printf("Failed to fetch pinned content %s from peer %s: %v", hash.String(), p.ID, fetchErr)
		}

		if fetchErr != nil {
			log.Printf("Failed to fetch pinned content %s from all providers", hash.String())
			return
		}

		if err := r.storage.StoreContent(metadata); err != nil {
			log.Printf("Failed to store metadata for pinned content %s: %v", hash.String(), err)
		}
		if err := r.storage.StoreData(hash, data); err != nil {
			log.Printf("Failed to store data for pinned content %s: %v", hash.String(), err)
		}
		r.storage.IncrementRefCount(hash)
		log.Printf("Successfully fetched and stored pinned content %s", hash.String())
	}()

	return nil
}

// UnpinContent unpins content
func (r *Replicator) UnpinContent(hash hasher.Hash) error {
	r.mu.Lock()
	delete(r.pinnedContent, hash.String())
	r.mu.Unlock()

	if r.storage.HasContent(hash) {
		return r.storage.DecrementRefCount(hash)
	}

	return nil
}

// IsPinned checks if content is pinned
func (r *Replicator) IsPinned(hash hasher.Hash) bool {
	r.mu.RLock()
	_, pinned := r.pinnedContent[hash.String()]
	r.mu.RUnlock()
	return pinned
}

// GetPinnedContent returns all pinned content hashes
func (r *Replicator) GetPinnedContent() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, 0, len(r.pinnedContent))
	for hash := range r.pinnedContent {
		result = append(result, hash)
	}
	return result
}

// handleGossipMessage handles incoming gossip messages
func (r *Replicator) handleGossipMessage(peerID peer.ID, data []byte) error {
	var announcement ContentAnnouncement
	if err := json.Unmarshal(data, &announcement); err == nil {
		return r.handleContentAnnouncement(peerID, &announcement)
	}

	var request ChunkRequest
	if err := json.Unmarshal(data, &request); err == nil {
		return r.handleChunkRequest(peerID, &request)
	}

	log.Printf("Unknown gossip message from peer %s", peerID.String())
	return nil
}

// handleContentAnnouncement handles content announcements
func (r *Replicator) handleContentAnnouncement(peerID peer.ID, announcement *ContentAnnouncement) error {
	// log.Printf("Received content announcement from %s: %s", peerID.String(), announcement.Hash.String())

	if r.storage.HasContent(announcement.Hash) {
		// log.Printf("Content %s already exists locally, skipping replication and announcement", announcement.Hash.String())
		return nil
	}

	// Do not auto-replicate large files
	if announcement.IsLarge {
		log.Printf("Received announcement for large file %s. Will not auto-replicate.", announcement.Hash.String())
		return nil
	}

	availableSpace, err := r.storage.GetAvailableSpace()
	if err != nil {
		log.Printf("Failed to check storage capacity: %v", err)
		return nil
	}

	providers, err := r.node.FindContentProviders(announcement.Hash.String())
	if err != nil {
		log.Printf("Failed to find providers for %s: %v", announcement.Hash.String(), err)
		return nil
	}

	if len(providers) >= int(r.replicationFactor) {
		log.Printf("Replication factor met for %s, skipping replication", announcement.Hash.String())
		return nil
	}

	if availableSpace < announcement.Size {
		log.Printf("Insufficient storage space for %s (need %d, have %d)", announcement.Hash.String(), announcement.Size, availableSpace)
		return nil
	}

	data, metadata, err := r.node.RequestContentFromPeer(peerID, announcement.Hash.String())
	if err != nil {
		replicationFailuresTotal.Inc()
		log.Printf("Failed to fetch content %s from %s: %v", announcement.Hash.String(), peerID.String(), err)
		return nil
	}

	if err := r.storage.StoreData(announcement.Hash, data); err != nil {
		replicationFailuresTotal.Inc()
		log.Printf("Failed to store replicated content %s: %v", announcement.Hash.String(), err)
		return nil
	}

	if err := r.storage.StoreContent(metadata); err != nil {
		replicationFailuresTotal.Inc()
		log.Printf("Failed to store replicated metadata %s: %v", announcement.Hash.String(), err)
		return nil
	}

	replicationSuccessTotal.Inc()
	log.Printf("Successfully replicated content %s from %s", announcement.Hash.String(), peerID.String())
	if err := r.AnnounceContent(announcement.Hash, metadata.Size, false); err != nil {
		log.Printf("Failed to announce replicated content %s: %v", announcement.Hash.String(), err)
	}

	return nil
}

// handleChunkRequest handles chunk requests
func (r *Replicator) handleChunkRequest(peerID peer.ID, request *ChunkRequest) error {
	log.Printf("Received chunk request from %s: %s", peerID.String(), request.Hash.String())

	var responseData []byte
	var err error

	// Check local storage first
	if r.storage.HasData(request.Hash) {
		responseData, err = r.storage.GetData(request.Hash)
		if err != nil {
			log.Printf("Failed to get local chunk data for %s: %v", request.Hash.String(), err)
			return nil
		}
	} else {
		// Check relay cache
		r.mu.RLock()
		data, exists := r.relayCache[request.Hash.String()]
		r.mu.RUnlock()
		if !exists {
			log.Printf("Chunk %s not found in local storage or relay cache", request.Hash.String())
			return nil
		}
		responseData = data
	}

	response := ChunkResponse{
		Hash:      request.Hash,
		Data:      responseData,
		RequestID: request.RequestID,
		Success:   true,
	}

	data, err := json.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal chunk response for %s: %v", request.Hash.String(), err)
		return nil
	}

	return r.node.SendChunk(r.ctx, peerID, data)
}

// handleChunkMessage handles incoming chunk messages
func (r *Replicator) handleChunkMessage(peerID peer.ID, data []byte) error {
	var response ChunkResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return fmt.Errorf("failed to unmarshal chunk response: %w", err)
	}

	log.Printf("Received chunk response from %s: %s", peerID.String(), response.Hash.String())

	r.mu.RLock()
	responseChan, exists := r.pendingRequests[response.RequestID]
	r.mu.RUnlock()

	if !exists {
		log.Printf("No pending request found for ID: %s", response.RequestID)
		return nil
	}

	select {
	case responseChan <- response:
	default:
		log.Printf("Response channel full or closed for %s", response.RequestID)
	}

	return nil
}

// announceContentPeriodically periodically announces pinned content
func (r *Replicator) announceContentPeriodically() {
	log.Println("Starting periodic announcement of pinned content...")
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.announcePinnedContent()
		case <-r.ctx.Done():
			return
		}
	}
}

// announcePinnedContent announces all pinned local content
func (r *Replicator) announcePinnedContent() {
	r.mu.RLock()
	hashes := make([]string, 0, len(r.pinnedContent))
	for hash := range r.pinnedContent {
		hashes = append(hashes, hash)
	}
	r.mu.RUnlock()

	log.Printf("Announcing %d pinned content items...", len(hashes))

	for _, hashStr := range hashes {
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			log.Printf("Invalid pinned hash %s: %v", hashStr, err)
			continue
		}

		metadata, err := r.storage.GetContent(hash)
		if err != nil {
			log.Printf("Could not get metadata for pinned content %s: %v", hash.String(), err)
			continue
		}

		if err := r.AnnounceContent(hash, metadata.Size, false); err != nil {
			log.Printf("Failed to announce pinned content %s: %v", hash.String(), err)
		}
	}
}

// cleanupPendingRequests cleans up old pending requests
func (r *Replicator) cleanupPendingRequests() {
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.mu.Lock()
			for requestID, responseChan := range r.pendingRequests {
				select {
				case <-responseChan:
				default:
					close(responseChan)
				}
				delete(r.pendingRequests, requestID)
			}

			for hash, tracker := range r.requestTracker {
				if time.Since(tracker.FirstSeen) > 24*time.Hour {
					delete(r.requestTracker, hash)
					delete(r.relayCache, hash)
				}
			}
			r.mu.Unlock()
		case <-r.ctx.Done():
			return
		}
	}
}

// GetStats returns replication statistics
func (r *Replicator) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]interface{}{
		"replication_factor": int(r.replicationFactor),
		"pinned_content":     len(r.pinnedContent),
		"pending_requests":   len(r.pendingRequests),
		"relay_cache_size":   len(r.relayCache),
		"tracked_requests":   len(r.requestTracker),
	}
}
