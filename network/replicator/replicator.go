package replicator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/libp2p"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
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
	replicationSuccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openhashdb_replication_success_total",
			Help: "Total number of successful replications",
		},
		[]string{"type"}, // "manifest" or "chunk"
	)
	replicationFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openhashdb_replication_failures_total",
			Help: "Total number of failed replications",
		},
		[]string{"type"}, // "manifest" or "chunk"
	)
)

// ReplicationFactor represents the desired number of replicas
type ReplicationFactor int

const (
	MinReplicationFactor     ReplicationFactor = 1
	MaxReplicationFactor     ReplicationFactor = 10
	MinReplicationPercentage                   = 0.55 // 55% of chunks required
	MaxParallelDownloads                       = 10
	RequestTimeout                             = 30 * time.Second
	CleanupInterval                            = 1 * time.Minute
	LargeFileSizeThreshold                     = 200 * 1024 * 1024 // 200MB
	DefaultReplicationFactor                   = 3                 // Default replication factor if not specified
)

// ManifestAnnouncement represents an announcement of a new manifest
type ManifestAnnouncement struct {
	MerkleRoot hasher.Hash `json:"merkle_root"`
	TotalSize  int64       `json:"total_size"`
	ChunkCount int         `json:"chunk_count"`
	Timestamp  time.Time   `json:"timestamp"`
	PeerID     string      `json:"peer_id"`
}

// ChunkRequest represents a request for a specific chunk
type ChunkRequest struct {
	MerkleRoot hasher.Hash `json:"merkle_root"`
	ChunkHash  hasher.Hash `json:"chunk_hash"`
	RequestID  string      `json:"request_id"`
}

// ChunkResponse represents a response to a chunk request
type ChunkResponse struct {
	MerkleRoot hasher.Hash `json:"merkle_root"`
	ChunkHash  hasher.Hash `json:"chunk_hash"`
	Data       []byte      `json:"data"`
	RequestID  string      `json:"request_id"`
	Success    bool        `json:"success"`
}

// PinRequest represents a request to pin a manifest
type PinRequest struct {
	MerkleRoot hasher.Hash `json:"merkle_root"`
	Priority   int         `json:"priority"`
}

// RequestTracker tracks chunk request frequency
type RequestTracker struct {
	Count     int
	FirstSeen time.Time
}

// Replicator handles manifest and chunk replication
type Replicator struct {
	storage           *storage.Storage
	node              *libp2p.Node
	replicationFactor ReplicationFactor
	pinnedManifests   map[string]struct{}
	pendingRequests   map[string]chan ChunkResponse
	requestTracker    map[string]*RequestTracker
	relayCache        map[string][]byte // Key: chunkHash.String()
	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
}

// NewReplicator creates a new replicator
func NewReplicator(storage *storage.Storage, node *libp2p.Node, replicationFactor ReplicationFactor) *Replicator {
	if replicationFactor < MinReplicationFactor {
		replicationFactor = MinReplicationFactor
	}
	if replicationFactor > MaxReplicationFactor {
		replicationFactor = MaxReplicationFactor
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Replicator{
		storage:           storage,
		node:              node,
		replicationFactor: replicationFactor,
		pinnedManifests:   make(map[string]struct{}),
		pendingRequests:   make(map[string]chan ChunkResponse),
		requestTracker:    make(map[string]*RequestTracker),
		relayCache:        make(map[string][]byte),
		ctx:               ctx,
		cancel:            cancel,
	}

	node.GossipHandler = r.handleGossipMessage
	node.ChunkHandler = r.handleChunkMessage
	go r.announceManifestsPeriodically()
	go r.cleanupPendingRequests()

	return r
}

// Close shuts down the replicator
func (r *Replicator) Close() error {
	r.cancel()
	return nil
}

// AnnounceManifest announces a new manifest
func (r *Replicator) AnnounceManifest(merkleRoot hasher.Hash, totalSize int64, chunkCount int) error {
	// Verify that manifest exists before announcing
	manifest, err := r.storage.GetManifest(merkleRoot)
	if err != nil {
		log.Printf("Manifest %s not found locally, skipping announcement: %v", merkleRoot.String(), err)
		return fmt.Errorf("manifest %s not found locally: %w", merkleRoot.String(), err)
	}

	if err := r.node.AnnounceContent(merkleRoot.String()); err != nil {
		log.Printf("Warning: failed to announce manifest to DHT: %v", err)
	}

	announcement := ManifestAnnouncement{
		MerkleRoot: merkleRoot,
		TotalSize:  totalSize,
		ChunkCount: len(manifest.Chunks),
		Timestamp:  time.Now(),
		PeerID:     r.node.ID().String(),
	}

	data, err := json.Marshal(announcement)
	if err != nil {
		return fmt.Errorf("failed to marshal announcement: %w", err)
	}

	return r.node.BroadcastGossip(r.ctx, data)
}

// RequestManifest requests a manifest and its chunks from the network
func (r *Replicator) RequestManifest(merkleRoot hasher.Hash) (*chunker.Manifest, error) {
	// Check local storage for manifest
	manifest, err := r.storage.GetManifest(merkleRoot)
	if err == nil {
		// Verify all chunks are present
		missingChunks := r.GetMissingChunks(manifest)
		if len(missingChunks) == 0 {
			return manifest, nil
		}
		// Fetch missing chunks
		if err := r.FetchChunks(manifest, missingChunks); err != nil {
			return nil, fmt.Errorf("failed to fetch missing chunks for manifest %s: %w", merkleRoot.String(), err)
		}
		return manifest, nil
	}

	// Find providers
	providers, err := r.node.FindContentProviders(merkleRoot.String())
	if err != nil {
		return nil, fmt.Errorf("failed to find providers for manifest %s: %w", merkleRoot.String(), err)
	}
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers found for manifest %s", merkleRoot.String())
	}

	// Request manifest from first provider
	manifestData, err := r.node.RequestContentFromPeer(providers[0].ID, merkleRoot.String())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch manifest %s from %s: %w", merkleRoot.String(), providers[0].ID.String(), err)
	}

	var fetchedManifest chunker.Manifest
	if err := json.Unmarshal(manifestData, &fetchedManifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest %s: %w", merkleRoot.String(), err)
	}

	// Store manifest
	if err := r.storage.StoreManifest(&fetchedManifest); err != nil {
		return nil, fmt.Errorf("failed to store manifest %s: %w", merkleRoot.String(), err)
	}

	// Fetch chunks
	if err := r.FetchChunks(&fetchedManifest, fetchedManifest.Chunks); err != nil {
		return nil, fmt.Errorf("failed to fetch chunks for manifest %s: %w", merkleRoot.String(), err)
	}

	return &fetchedManifest, nil
}

// RequestChunk requests a specific chunk from the network or relays it
func (r *Replicator) RequestChunk(merkleRoot, chunkHash hasher.Hash) ([]byte, error) {
	// Check local storage
	if r.storage.HasChunkData(merkleRoot, chunkHash) {
		data, err := r.storage.GetChunkData(merkleRoot, chunkHash)
		if err != nil {
			return nil, fmt.Errorf("failed to get local chunk data for %s: %w", chunkHash.String(), err)
		}
		return data, nil
	}

	// Check relay cache
	r.mu.RLock()
	cacheKey := chunkHash.String()
	if data, exists := r.relayCache[cacheKey]; exists {
		r.mu.RUnlock()
		return data, nil
	}
	r.mu.RUnlock()

	// Track request frequency
	r.mu.Lock()
	tracker, exists := r.requestTracker[cacheKey]
	if !exists {
		tracker = &RequestTracker{
			Count:     1,
			FirstSeen: time.Now(),
		}
		r.requestTracker[cacheKey] = tracker
	} else {
		tracker.Count++
	}
	r.mu.Unlock()

	// Find providers
	providers, err := r.node.FindContentProviders(chunkHash.String())
	if err != nil {
		return nil, fmt.Errorf("failed to find providers for chunk %s: %w", chunkHash.String(), err)
	}
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers found for chunk %s", chunkHash.String())
	}

	// Request chunk from first provider
	data, err := r.node.RequestContentFromPeer(providers[0].ID, chunkHash.String())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chunk %s from %s: %w", chunkHash.String(), providers[0].ID.String(), err)
	}

	// Verify chunk integrity
	if !hasher.Verify(data, chunkHash) {
		return nil, fmt.Errorf("chunk %s failed integrity check", chunkHash.String())
	}

	// Store in relay cache
	r.mu.Lock()
	r.relayCache[cacheKey] = data
	r.mu.Unlock()

	return data, nil
}

// PinManifest pins a manifest and its chunks
func (r *Replicator) PinManifest(merkleRoot hasher.Hash) error {
	r.mu.Lock()
	r.pinnedManifests[merkleRoot.String()] = struct{}{}
	r.mu.Unlock()

	if _, err := r.storage.GetManifest(merkleRoot); err == nil {
		return r.storage.IncrementRefCount(merkleRoot)
	}

	// If manifest is not local, fetch it in the background
	go func() {
		log.Printf("Manifest %s is pinned but not local, fetching...", merkleRoot.String())
		manifest, err := r.RequestManifest(merkleRoot)
		if err != nil {
			log.Printf("Failed to fetch pinned manifest %s: %v", merkleRoot.String(), err)
			return
		}
		if err := r.storage.IncrementRefCount(merkleRoot); err != nil {
			log.Printf("Failed to increment ref count for pinned manifest %s: %v", merkleRoot.String(), err)
		}
		log.Printf("Successfully fetched and pinned manifest %s with %d chunks", merkleRoot.String(), len(manifest.Chunks))
	}()

	return nil
}

// UnpinManifest unpins a manifest
func (r *Replicator) UnpinManifest(merkleRoot hasher.Hash) error {
	r.mu.Lock()
	delete(r.pinnedManifests, merkleRoot.String())
	r.mu.Unlock()

	if r.storage.HasChunk(merkleRoot) {
		return r.storage.DecrementRefCount(merkleRoot)
	}

	return nil
}

// IsPinned checks if a manifest is pinned
func (r *Replicator) IsPinned(merkleRoot hasher.Hash) bool {
	r.mu.RLock()
	_, pinned := r.pinnedManifests[merkleRoot.String()]
	r.mu.RUnlock()
	return pinned
}

// GetPinnedManifests returns all pinned manifest hashes
func (r *Replicator) GetPinnedManifests() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, 0, len(r.pinnedManifests))
	for hash := range r.pinnedManifests {
		result = append(result, hash)
	}
	return result
}

// handleGossipMessage handles incoming gossip messages
func (r *Replicator) handleGossipMessage(peerID peer.ID, data []byte) error {
	var announcement ManifestAnnouncement
	if err := json.Unmarshal(data, &announcement); err == nil {
		return r.handleManifestAnnouncement(peerID, &announcement)
	}

	var request ChunkRequest
	if err := json.Unmarshal(data, &request); err == nil {
		return r.handleChunkRequest(peerID, &request)
	}

	log.Printf("Unknown gossip message from peer %s", peerID.String())
	return nil
}

// handleManifestAnnouncement handles manifest announcements
func (r *Replicator) handleManifestAnnouncement(peerID peer.ID, announcement *ManifestAnnouncement) error {
	log.Printf("Received manifest announcement from %s: %s", peerID.String(), announcement.MerkleRoot.String())

	if _, err := r.storage.GetManifest(announcement.MerkleRoot); err == nil {
		log.Printf("Manifest %s already exists locally, skipping replication", announcement.MerkleRoot.String())
		return nil
	}

	// Skip automatic replication for large files
	if announcement.TotalSize > LargeFileSizeThreshold {
		log.Printf("Manifest %s is larger than %d bytes, skipping automatic replication", announcement.MerkleRoot.String(), LargeFileSizeThreshold)
		return nil
	}

	availableSpace, err := r.storage.GetAvailableSpace()
	if err != nil {
		log.Printf("Failed to check storage capacity: %v", err)
		return nil
	}

	// Ensure TotalSize is non-negative and compare as int64
	if announcement.TotalSize < 0 {
		log.Printf("Invalid TotalSize %d for manifest %s, skipping replication", announcement.TotalSize, announcement.MerkleRoot.String())
		return nil
	}
	if availableSpace < announcement.TotalSize {
		log.Printf("Insufficient storage space for %s (need %d, have %d)", announcement.MerkleRoot.String(), announcement.TotalSize, availableSpace)
		return nil
	}

	// Fetch manifest
	manifestData, err := r.node.RequestContentFromPeer(peerID, announcement.MerkleRoot.String())
	if err != nil {
		replicationFailuresTotal.WithLabelValues("manifest").Inc()
		log.Printf("Failed to fetch manifest %s from %s: %v", announcement.MerkleRoot.String(), peerID.String(), err)
		return nil
	}

	var manifest chunker.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		replicationFailuresTotal.WithLabelValues("manifest").Inc()
		log.Printf("Failed to unmarshal manifest %s: %v", announcement.MerkleRoot.String(), err)
		return nil
	}

	// Store manifest
	if err := r.storage.StoreManifest(&manifest); err != nil {
		replicationFailuresTotal.WithLabelValues("manifest").Inc()
		log.Printf("Failed to store manifest %s: %v", announcement.MerkleRoot.String(), err)
		return nil
	}

	// Replicate chunks (only 55% needed)
	if err := r.replicateChunks(&manifest); err != nil {
		replicationFailuresTotal.WithLabelValues("chunk").Inc()
		log.Printf("Failed to replicate chunks for manifest %s: %v", announcement.MerkleRoot.String(), err)
		return nil
	}

	replicationSuccessTotal.WithLabelValues("manifest").Inc()
	log.Printf("Successfully replicated manifest %s from %s", announcement.MerkleRoot.String(), peerID.String())
	if err := r.AnnounceManifest(announcement.MerkleRoot, manifest.TotalSize, len(manifest.Chunks)); err != nil {
		log.Printf("Failed to announce replicated manifest %s: %v", announcement.MerkleRoot.String(), err)
	}

	return nil
}

// handleChunkRequest handles chunk requests
func (r *Replicator) handleChunkRequest(peerID peer.ID, request *ChunkRequest) error {
	log.Printf("Received chunk request from %s: %s", peerID.String(), request.ChunkHash.String())

	var responseData []byte
	var err error

	// Check local storage first
	if r.storage.HasChunkData(request.MerkleRoot, request.ChunkHash) {
		responseData, err = r.storage.GetChunkData(request.MerkleRoot, request.ChunkHash)
		if err != nil {
			log.Printf("Failed to get local chunk data for %s: %v", request.ChunkHash.String(), err)
			return nil
		}
	} else {
		// Check relay cache
		r.mu.RLock()
		data, exists := r.relayCache[request.ChunkHash.String()]
		r.mu.RUnlock()
		if !exists {
			log.Printf("Chunk %s not found in local storage or relay cache", request.ChunkHash.String())
			return nil
		}
		responseData = data
	}

	response := ChunkResponse{
		MerkleRoot: request.MerkleRoot,
		ChunkHash:  request.ChunkHash,
		Data:       responseData,
		RequestID:  request.RequestID,
		Success:    true,
	}

	data, err := json.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal chunk response for %s: %v", request.ChunkHash.String(), err)
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

	log.Printf("Received chunk response from %s: %s", peerID.String(), response.ChunkHash.String())

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

// announceManifestsPeriodically periodically announces pinned manifests
func (r *Replicator) announceManifestsPeriodically() {
	log.Println("Starting periodic announcement of pinned manifests...")
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.announcePinnedManifests()
		case <-r.ctx.Done():
			return
		}
	}
}

// announcePinnedManifests announces all pinned local manifests
func (r *Replicator) announcePinnedManifests() {
	r.mu.RLock()
	hashes := make([]string, 0, len(r.pinnedManifests))
	for hash := range r.pinnedManifests {
		hashes = append(hashes, hash)
	}
	r.mu.RUnlock()

	log.Printf("Announcing %d pinned manifests...", len(hashes))

	for _, hashStr := range hashes {
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			log.Printf("Invalid pinned hash %s: %v", hashStr, err)
			continue
		}

		manifest, err := r.storage.GetManifest(hash)
		if err != nil {
			log.Printf("Could not get manifest for pinned content %s: %v", hash.String(), err)
			continue
		}

		if err := r.AnnounceManifest(hash, manifest.TotalSize, len(manifest.Chunks)); err != nil {
			log.Printf("Failed to announce pinned manifest %s: %v", hash.String(), err)
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
		"pinned_manifests":   len(r.pinnedManifests),
		"pending_requests":   len(r.pendingRequests),
		"relay_cache_size":   len(r.relayCache),
		"tracked_requests":   len(r.requestTracker),
	}
}

// getMissingChunks returns chunks missing locally for a manifest
func (r *Replicator) GetMissingChunks(manifest *chunker.Manifest) []chunker.ChunkInfo {
	var missing []chunker.ChunkInfo
	for _, chunkInfo := range manifest.Chunks {
		if !r.storage.HasChunkData(manifest.MerkleRoot, chunkInfo.Hash) {
			missing = append(missing, chunkInfo)
		}
	}
	return missing
}

// fetchChunks fetches the specified chunks for a manifest
func (r *Replicator) FetchChunks(manifest *chunker.Manifest, chunks []chunker.ChunkInfo) error {
	eg, ctx := errgroup.WithContext(r.ctx)
	eg.SetLimit(MaxParallelDownloads)

	for _, chunkInfo := range chunks {
		chunkInfo := chunkInfo // Capture for closure
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				data, err := r.RequestChunk(manifest.MerkleRoot, chunkInfo.Hash)
				if err != nil {
					return fmt.Errorf("failed to fetch chunk %s: %w", chunkInfo.Hash.String(), err)
				}
				if err := r.storage.StoreChunkData(manifest.MerkleRoot, chunkInfo.Hash, data); err != nil {
					return fmt.Errorf("failed to store chunk %s: %w", chunkInfo.Hash.String(), err)
				}
				if err := r.storage.IncrementChunkRefCount(chunkInfo.Hash); err != nil {
					log.Printf("Failed to increment ref count for chunk %s: %v", chunkInfo.Hash.String(), err)
				}
				replicationSuccessTotal.WithLabelValues("chunk").Inc()
				log.Printf("Successfully fetched and stored chunk %s for manifest %s", chunkInfo.Hash.String(), manifest.MerkleRoot.String())
				return nil
			}
		})
	}

	return eg.Wait()
}

// replicateChunks replicates at least 55% of a manifest's chunks
func (r *Replicator) replicateChunks(manifest *chunker.Manifest) error {
	// Calculate minimum number of chunks to replicate (55%)
	minChunks := int(float64(len(manifest.Chunks)) * MinReplicationPercentage)
	if minChunks < 1 {
		minChunks = 1
	}

	// Filter chunks not already replicated locally
	var chunksToReplicate []chunker.ChunkInfo
	for _, chunkInfo := range manifest.Chunks {
		if !r.storage.HasChunkData(manifest.MerkleRoot, chunkInfo.Hash) {
			chunksToReplicate = append(chunksToReplicate, chunkInfo)
		}
	}

	// If not enough chunks need replication, we're done
	if len(chunksToReplicate) <= minChunks {
		return r.FetchChunks(manifest, chunksToReplicate)
	}

	// Select up to minChunks for replication
	if len(chunksToReplicate) > minChunks {
		chunksToReplicate = chunksToReplicate[:minChunks]
	}

	return r.FetchChunks(manifest, chunksToReplicate)
}
