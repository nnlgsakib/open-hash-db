package replicator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/libp2p"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ReplicationFactor represents the desired number of replicas
type ReplicationFactor int

const (
	DefaultReplicationFactor ReplicationFactor = 3
	MinReplicationFactor     ReplicationFactor = 1
	MaxReplicationFactor     ReplicationFactor = 10
)

// ContentAnnouncement represents an announcement of new content
type ContentAnnouncement struct {
	Hash      hasher.Hash `json:"hash"`
	Size      int64       `json:"size"`
	Timestamp time.Time   `json:"timestamp"`
	PeerID    string      `json:"peer_id"`
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
	Priority int         `json:"priority"` // Higher priority = more important to keep
}

// Replicator handles content replication and availability
type Replicator struct {
	storage  *storage.Storage
	node     *libp2p.Node
	
	// Configuration
	replicationFactor ReplicationFactor
	
	// State tracking
	pinnedContent map[string]int // hash -> priority
	pendingRequests map[string]chan ChunkResponse // requestID -> response channel
	
	// Synchronization
	mu sync.RWMutex
	
	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc
}

// NewReplicator creates a new replicator
func NewReplicator(storage *storage.Storage, node *libp2p.Node, replicationFactor ReplicationFactor) *Replicator {
	ctx, cancel := context.WithCancel(context.Background())
	
	r := &Replicator{
		storage:           storage,
		node:              node,
		replicationFactor: replicationFactor,
		pinnedContent:     make(map[string]int),
		pendingRequests:   make(map[string]chan ChunkResponse),
		ctx:               ctx,
		cancel:            cancel,
	}
	
	// Set up message handlers
	node.GossipHandler = r.handleGossipMessage
	node.ChunkHandler = r.handleChunkMessage
	
	// Start background tasks
	go r.announceContentPeriodically()
	go r.cleanupPendingRequests()
	
	return r
}

// Close shuts down the replicator
func (r *Replicator) Close() error {
	r.cancel()
	return nil
}

// AnnounceContent announces new content to the network
func (r *Replicator) AnnounceContent(hash hasher.Hash, size int64) error {
	// Announce to the DHT that we are a provider for this content
	if err := r.node.AnnounceContent(hash.String()); err != nil {
		log.Printf("Warning: failed to announce content to DHT: %v", err)
	}

	// Also send a gossip message for faster propagation to connected peers
	announcement := ContentAnnouncement{
		Hash:      hash,
		Size:      size,
		Timestamp: time.Now(),
		PeerID:    r.node.ID().String(),
	}

	data, err := json.Marshal(announcement)
	if err != nil {
		return fmt.Errorf("failed to marshal announcement: %w", err)
	}

	return r.node.BroadcastGossip(r.ctx, data)
}

// ReannounceAll tells the network about all content stored locally.
func (r *Replicator) ReannounceAll() {
	log.Println("Re-announcing all local content to the network...")
	hashes, err := r.storage.ListContent()
	if err != nil {
		log.Printf("Error listing content for re-announcement: %v", err)
		return
	}

	for _, hash := range hashes {
		// This can be slow if there are many hashes, so run in background
		go func(h hasher.Hash) {
			metadata, err := r.storage.GetContent(h)
			if err != nil {
				log.Printf("Could not get metadata for content %s: %v", h.String(), err)
				return
			}
			if err := r.AnnounceContent(h, metadata.Size); err != nil {
				log.Printf("Failed to re-announce content %s: %v", h.String(), err)
			}
		}(hash)
	}
	log.Printf("Finished re-announcing %d content hashes.", len(hashes))
}

// RequestChunk requests a chunk from the network
func (r *Replicator) RequestChunk(hash hasher.Hash) ([]byte, error) {
	// Check if we already have the chunk locally
	if r.storage.HasData(hash) {
		return r.storage.GetData(hash)
	}
	
	// Generate a unique request ID
	requestID := fmt.Sprintf("%s-%d", hash.String()[:8], time.Now().UnixNano())
	
	// Create response channel
	responseChan := make(chan ChunkResponse, 1)
	
	r.mu.Lock()
	r.pendingRequests[requestID] = responseChan
	r.mu.Unlock()
	
	// Clean up when done
	defer func() {
		r.mu.Lock()
		delete(r.pendingRequests, requestID)
		r.mu.Unlock()
		close(responseChan)
	}()
	
	// Create chunk request
	request := ChunkRequest{
		Hash:      hash,
		RequestID: requestID,
	}
	
	data, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal chunk request: %w", err)
	}
	
	// Broadcast the request
	if err := r.node.BroadcastGossip(r.ctx, data); err != nil {
		return nil, fmt.Errorf("failed to broadcast chunk request: %w", err)
	}
	
	// Wait for response with timeout
	select {
	case response := <-responseChan:
		if response.Success {
			// Verify the chunk
			if !hasher.Verify(response.Data, hash) {
				return nil, fmt.Errorf("chunk verification failed")
			}
			
			// Store the chunk locally
			if err := r.storage.StoreData(hash, response.Data); err != nil {
				log.Printf("Failed to store received chunk: %v", err)
			}
			
			return response.Data, nil
		}
		return nil, fmt.Errorf("chunk request failed")
		
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("chunk request timeout")
		
	case <-r.ctx.Done():
		return nil, fmt.Errorf("replicator shutting down")
	}
}

// PinContent pins content with a given priority
func (r *Replicator) PinContent(hash hasher.Hash, priority int) error {
	r.mu.Lock()
	r.pinnedContent[hash.String()] = priority
	r.mu.Unlock()
	
	// Increment reference count
	if r.storage.HasContent(hash) {
		return r.storage.IncrementRefCount(hash)
	}
	
	return fmt.Errorf("content not found: %s", hash.String())
}

// UnpinContent unpins content
func (r *Replicator) UnpinContent(hash hasher.Hash) error {
	r.mu.Lock()
	delete(r.pinnedContent, hash.String())
	r.mu.Unlock()
	
	// Decrement reference count
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

// GetPinnedContent returns all pinned content
func (r *Replicator) GetPinnedContent() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	result := make(map[string]int)
	for hash, priority := range r.pinnedContent {
		result[hash] = priority
	}
	return result
}

// handleGossipMessage handles incoming gossip messages
func (r *Replicator) handleGossipMessage(peerID peer.ID, data []byte) error {
	// Try to parse as content announcement
	var announcement ContentAnnouncement
	if err := json.Unmarshal(data, &announcement); err == nil {
		return r.handleContentAnnouncement(peerID, &announcement)
	}
	
	// Try to parse as chunk request
	var request ChunkRequest
	if err := json.Unmarshal(data, &request); err == nil {
		return r.handleChunkRequest(peerID, &request)
	}
	
	// Unknown message type
	log.Printf("Unknown gossip message from peer %s", peerID.String())
	return nil
}

// handleContentAnnouncement handles content announcements
func (r *Replicator) handleContentAnnouncement(peerID peer.ID, announcement *ContentAnnouncement) error {
	log.Printf("Received content announcement from %s: %s", peerID.String(), announcement.Hash.String())
	
	// Check if we already have this content
	if r.storage.HasContent(announcement.Hash) {
		return nil
	}
	
	// TODO: Implement intelligent replication decisions based on:
	// - Available storage space
	// - Content popularity
	// - Network topology
	// - Replication factor requirements
	
	return nil
}

// handleChunkRequest handles chunk requests
func (r *Replicator) handleChunkRequest(peerID peer.ID, request *ChunkRequest) error {
	log.Printf("Received chunk request from %s: %s", peerID.String(), request.Hash.String())
	
	// Check if we have the requested chunk
	if !r.storage.HasData(request.Hash) {
		return nil // We don't have it, ignore the request
	}
	
	// Get the chunk data
	data, err := r.storage.GetData(request.Hash)
	if err != nil {
		log.Printf("Failed to get chunk data: %v", err)
		return nil
	}
	
	// Create response
	response := ChunkResponse{
		Hash:      request.Hash,
		Data:      data,
		RequestID: request.RequestID,
		Success:   true,
	}
	
	responseData, err := json.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal chunk response: %v", err)
		return nil
	}
	
	// Send response directly to the requesting peer
	return r.node.SendChunk(r.ctx, peerID, responseData)
}

// handleChunkMessage handles incoming chunk messages (responses)
func (r *Replicator) handleChunkMessage(peerID peer.ID, data []byte) error {
	var response ChunkResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return fmt.Errorf("failed to unmarshal chunk response: %w", err)
	}
	
	log.Printf("Received chunk response from %s: %s", peerID.String(), response.Hash.String())
	
	// Find the pending request
	r.mu.RLock()
	responseChan, exists := r.pendingRequests[response.RequestID]
	r.mu.RUnlock()
	
	if !exists {
		log.Printf("No pending request found for ID: %s", response.RequestID)
		return nil
	}
	
	// Send response to waiting goroutine
	select {
	case responseChan <- response:
	default:
		// Channel is full or closed, ignore
	}
	
	return nil
}

// announceContentPeriodically periodically announces our content
func (r *Replicator) announceContentPeriodically() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			r.announceAllContent()
		case <-r.ctx.Done():
			return
		}
	}
}

// announceAllContent announces all our content to the network
func (r *Replicator) announceAllContent() {
	hashes, err := r.storage.ListContent()
	if err != nil {
		log.Printf("Failed to list content for announcement: %v", err)
		return
	}
	
	for _, hash := range hashes {
		metadata, err := r.storage.GetContent(hash)
		if err != nil {
			continue
		}
		
		if err := r.AnnounceContent(hash, metadata.Size); err != nil {
			log.Printf("Failed to announce content %s: %v", hash.String(), err)
		}
	}
}

// cleanupPendingRequests cleans up old pending requests
func (r *Replicator) cleanupPendingRequests() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// TODO: Implement cleanup of old pending requests
			// This would require tracking request timestamps
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
	}
}

