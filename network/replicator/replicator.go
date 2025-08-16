package replicator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"openhashdb/core/blockstore"
	"openhashdb/core/hasher"
	"openhashdb/network/bitswap"
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
	LargeFileSizeThreshold                     = 200 * 1024 * 1024 // 200MB
)

// ContentAnnouncement represents an announcement of new content
type ContentAnnouncement struct {
	Hash      hasher.Hash `json:"hash"`
	Size      int64       `json:"size"`
	Timestamp time.Time   `json:"timestamp"`
	PeerID    string      `json:"peer_id"`
}

// Replicator handles content replication and availability
type Replicator struct {
	blockstore        *blockstore.Blockstore
	node              *libp2p.Node
	bitswap           *bitswap.Engine
	replicationFactor ReplicationFactor
	pinnedContent     map[string]struct{}
	replicatingNow    map[string]struct{}
	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
}

// NewReplicator creates a new replicator
func NewReplicator(bs *blockstore.Blockstore, node *libp2p.Node, bitswap *bitswap.Engine, replicationFactor ReplicationFactor) *Replicator {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Replicator{
		blockstore:        bs,
		node:              node,
		bitswap:           bitswap,
		replicationFactor: replicationFactor,
		pinnedContent:     make(map[string]struct{}),
		replicatingNow:    make(map[string]struct{}),
		ctx:               ctx,
		cancel:            cancel,
	}

	node.GossipHandler = r.handleGossipMessage
	go r.announceContentPeriodically()

	return r
}

// Close shuts down the replicator
func (r *Replicator) Close() {
	r.cancel()
}

// AnnounceContent announces new content
func (r *Replicator) AnnounceContent(hash hasher.Hash, size int64) error {
	if !r.blockstore.HasContent(hash) {
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

	if r.blockstore.HasContent(hash) {
		// TODO: Implement ref counting on blockstore content metadata
		return nil
	}

	// If content is not local, fetch it in the background
	go r.FetchAndStore(hash)

	return nil
}

// UnpinContent unpins content
func (r *Replicator) UnpinContent(hash hasher.Hash) error {
	r.mu.Lock()
	delete(r.pinnedContent, hash.String())
	r.mu.Unlock()

	if r.blockstore.HasContent(hash) {
		// TODO: Implement ref counting on blockstore content metadata
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

	log.Printf("Unknown gossip message from peer %s", peerID.String())
	return nil
}

// handleContentAnnouncement handles content announcements by replicating content.
func (r *Replicator) handleContentAnnouncement(peerID peer.ID, announcement *ContentAnnouncement) error {
	if r.blockstore.HasContent(announcement.Hash) {
		return nil
	}

	r.mu.Lock()
	if _, ongoing := r.replicatingNow[announcement.Hash.String()]; ongoing {
		r.mu.Unlock()
		return nil
	}
	r.replicatingNow[announcement.Hash.String()] = struct{}{}
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.replicatingNow, announcement.Hash.String())
		r.mu.Unlock()
	}()

	if announcement.Size > LargeFileSizeThreshold {
		log.Printf("Content %s is larger than %d bytes, skipping automatic replication", announcement.Hash.String(), LargeFileSizeThreshold)
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

	log.Printf("Replication factor not met for %s, starting replication...", announcement.Hash.String())
	go r.FetchAndStore(announcement.Hash)

	return nil
}

func (r *Replicator) FetchAndStore(hash hasher.Hash) error {
	log.Printf("Fetching content %s...", hash.String())

	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Minute)
	defer cancel()

	// Fetch the root block, which should be the metadata
	rootBlock, err := r.bitswap.GetBlock(ctx, hash)
	if err != nil {
		log.Printf("Failed to fetch root block %s: %v", hash.String(), err)
		replicationFailuresTotal.Inc()
		return err
	}

	// The root block for a file/directory is its metadata
	var metadata blockstore.ContentMetadata
	if err := json.Unmarshal(rootBlock.RawData(), &metadata); err != nil {
		log.Printf("Fetched root block %s is not valid metadata: %v", hash.String(), err)
		replicationFailuresTotal.Inc()
		return err
	}

	// Store the metadata
	if err := r.blockstore.StoreContent(&metadata); err != nil {
		log.Printf("Failed to store metadata for %s: %v", hash.String(), err)
		return err
	}

	// If it's a file, fetch all its chunks
	if !metadata.IsDirectory && len(metadata.Chunks) > 0 {
		blockHashes := make([]hasher.Hash, len(metadata.Chunks))
		for i, chunk := range metadata.Chunks {
			blockHashes[i] = chunk.Hash
		}

		blockChannel, err := r.bitswap.GetBlocks(ctx, blockHashes)
		if err != nil {
			log.Printf("Failed to start bitswap session for chunks of %s: %v", hash.String(), err)
			replicationFailuresTotal.Inc()
			return err
		}

		for blk := range blockChannel {
			log.Printf("Replicated block %s for content %s", blk.Hash().String(), hash.String())
		}
	}

	log.Printf("Successfully replicated content %s", hash.String())
	replicationSuccessTotal.Inc()

	if err := r.AnnounceContent(hash, metadata.Size); err != nil {
		log.Printf("Failed to announce newly replicated content %s: %v", hash.String(), err)
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

		metadata, err := r.blockstore.GetContent(hash)
		if err != nil {
			log.Printf("Could not get metadata for pinned content %s: %v", hash.String(), err)
			continue
		}

		if err := r.AnnounceContent(hash, metadata.Size); err != nil {
			log.Printf("Failed to announce pinned content %s: %v", hash.String(), err)
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
		"replicating_now":    len(r.replicatingNow),
	}
}
