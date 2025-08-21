package replicator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"openhashdb/core/blockstore"
	"openhashdb/core/hasher"
	"openhashdb/network/bitswap"
	"openhashdb/network/libp2p"
	"openhashdb/protobuf/pb"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	DefaultGCRunInterval                       = 6 * time.Hour
)

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
	go r.runGC()

	return r
}

// Close shuts down the replicator
func (r *Replicator) Close() {
	r.cancel()
}

// AnnounceContent announces new content
func (r *Replicator) AnnounceContent(hash hasher.Hash, size int64) error {
	if !r.blockstore.HasContent(hash) {
		log.Printf("[Replicator] Content %s not found locally, skipping announcement", hash.String())
		return fmt.Errorf("content %s not found locally", hash.String())
	}

	if err := r.node.AnnounceContent(hash.String()); err != nil {
		log.Printf("[Replicator] Warning: failed to announce content to DHT: %v", err)
	}

	announcement := &pb.ContentAnnouncement{
		Hash:      hash[:],
		Size:      size,
		Timestamp: timestamppb.Now(),
		PeerId:    r.node.ID().String(),
	}

	data, err := proto.Marshal(announcement)
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
		return nil
	}

	go r.FetchAndStore(hash)
	return nil
}

// UnpinContent unpins content
func (r *Replicator) UnpinContent(hash hasher.Hash) error {
	r.mu.Lock()
	delete(r.pinnedContent, hash.String())
	r.mu.Unlock()
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
	var announcement pb.ContentAnnouncement
	if err := proto.Unmarshal(data, &announcement); err == nil {
		return r.handleContentAnnouncement(peerID, &announcement)
	}
	return nil
}

// handleContentAnnouncement handles content announcements by replicating content.
func (r *Replicator) handleContentAnnouncement(peerID peer.ID, announcement *pb.ContentAnnouncement) error {
	h, err := hasher.HashFromBytes(announcement.Hash)
	if err != nil {
		return err
	}
	if r.blockstore.HasContent(h) {
		return nil
	}

	r.mu.Lock()
	if _, ongoing := r.replicatingNow[h.String()]; ongoing {
		r.mu.Unlock()
		return nil
	}
	r.replicatingNow[h.String()] = struct{}{}
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.replicatingNow, h.String())
		r.mu.Unlock()
	}()

	if announcement.Size > LargeFileSizeThreshold {
		return nil
	}

	providers, err := r.node.FindContentProviders(h.String())
	if err != nil {
		return nil
	}

	if len(providers) >= int(r.replicationFactor) {
		return nil
	}

	log.Printf("[Replicator] Replication factor not met for %s, starting replication...", h.String())
	go r.FetchAndStore(h)

	return nil
}

func (r *Replicator) FetchAndStore(hash hasher.Hash) error {
	log.Printf("[Replicator] Fetching content %s...", hash.String())

	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Minute)
	defer cancel()

	rootBlock, err := r.bitswap.GetBlock(ctx, hash)
	if err != nil {
		log.Printf("[Replicator] Failed to fetch root block %s: %v", hash.String(), err)
		replicationFailuresTotal.Inc()
		return err
	}

	var metadata pb.ContentMetadata
	if err := proto.Unmarshal(rootBlock.RawData(), &metadata); err != nil {
		log.Printf("[Replicator] Fetched root block %s is not valid metadata, treating as raw file.", hash.String())
		// Create metadata for a single-block file
		rootBlockH := rootBlock.Hash()
		metadata = pb.ContentMetadata{
			Filename:    hash.String(),
			Size:        int64(len(rootBlock.RawData())),
			IsDirectory: false,
			Chunks:      []*pb.ChunkInfo{{Hash: rootBlockH[:], Size: int64(len(rootBlock.RawData()))}},
			CreatedAt:   timestamppb.Now(),
			ModTime:     timestamppb.Now(),
			RefCount:    1,
		}
		metadataBytes, _ := proto.Marshal(&metadata)
		metadataHash := hasher.HashBytes(metadataBytes)
		metadata.Hash = metadataHash[:]
	}

	if err := r.blockstore.StoreContent(&metadata); err != nil {
		log.Printf("[Replicator] Failed to store metadata for %s: %v", hash.String(), err)
		return err
	}

	if !metadata.IsDirectory && len(metadata.Chunks) > 0 {
		blockHashes := make([]hasher.Hash, 0)
		for _, chunk := range metadata.Chunks {
			chunkHash, err := hasher.HashFromBytes(chunk.Hash)
			if err != nil {
				continue
			}
			if chunkHash != rootBlock.Hash() {
				blockHashes = append(blockHashes, chunkHash)
			}
		}

		if len(blockHashes) > 0 {
			blockChannel, err := r.bitswap.GetBlocks(ctx, blockHashes)
			if err != nil {
				log.Printf("[Replicator] Failed to start bitswap session for chunks of %s: %v", hash.String(), err)
				replicationFailuresTotal.Inc()
				return err
			}
			replicatedCount := 0
			for range blockChannel {
				replicatedCount++
			}

			if replicatedCount < len(blockHashes) {
				err := fmt.Errorf("replication failed for %s: received %d of %d blocks", hash.String(), replicatedCount, len(blockHashes))
				log.Print(err)
				replicationFailuresTotal.Inc()
				return err
			}
			log.Printf("[Replicator] Fetched %d/%d blocks for content %s", replicatedCount, len(blockHashes), hash.String())
		}
	}

	log.Printf("[Replicator] Successfully replicated content %s", hash.String())
	replicationSuccessTotal.Inc()

	metadataHash, err := hasher.HashFromBytes(metadata.Hash)
	if err != nil {
		return err
	}
	if err := r.AnnounceContent(metadataHash, metadata.Size); err != nil {
		log.Printf("[Replicator] Failed to announce newly replicated content %s: %v", metadataHash.String(), err)
	}
	return nil
}

// announceContentPeriodically periodically announces pinned content
func (r *Replicator) announceContentPeriodically() {
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

	if len(hashes) > 0 {
		log.Printf("[Replicator] Announcing %d pinned content items...", len(hashes))
	}

	for _, hashStr := range hashes {
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			continue
		}

		metadata, err := r.blockstore.GetContent(hash)
		if err != nil {
			continue
		}

		if err := r.AnnounceContent(hash, metadata.Size); err != nil {
			log.Printf("[Replicator] Failed to announce pinned content %s: %v", hash.String(), err)
		}
	}
}

// runGC periodically runs the garbage collector.
func (r *Replicator) runGC() {
	// Run once shortly after startup
	initialDelay := time.After(5 * time.Minute)
	ticker := time.NewTicker(DefaultGCRunInterval)
	defer ticker.Stop()

	for {
		select {
		case <-initialDelay:
			log.Println("[Replicator] Kicking off initial garbage collection run.")
			r.triggerGC()
		case <-ticker.C:
			log.Println("[Replicator] Kicking off scheduled garbage collection run.")
			r.triggerGC()
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Replicator) triggerGC() {
	r.mu.RLock()
	pinned := make([]string, 0, len(r.pinnedContent))
	for hashStr := range r.pinnedContent {
		pinned = append(pinned, hashStr)
	}
	r.mu.RUnlock()

	if len(pinned) == 0 {
		log.Println("[Replicator] No pinned content, skipping GC run.")
		return
	}

	rootHashes := make(chan hasher.Hash, len(pinned))
	for _, hashStr := range pinned {
		h, err := hasher.HashFromString(hashStr)
		if err == nil {
			rootHashes <- h
		}
	}
	close(rootHashes)

	_, err := r.blockstore.GC(r.ctx, rootHashes)
	if err != nil {
		log.Printf("[Replicator] Garbage collection run failed: %v", err)
	}
}

// GetStats returns replication statistics
func (r *Replicator) GetStats() *pb.ReplicationStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &pb.ReplicationStats{
		ReplicationFactor: int32(r.replicationFactor),
		PinnedContent:     int32(len(r.pinnedContent)),
		ReplicatingNow:    int32(len(r.replicatingNow)),
	}
}
