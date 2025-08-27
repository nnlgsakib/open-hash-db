package blockstore

import (
    "context"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "sync"

    "openhashdb/core/block"
    "openhashdb/core/hasher"
    "openhashdb/protobuf/pb"
    "openhashdb/core/cidutil"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
    "github.com/syndtr/goleveldb/leveldb/util"
    "google.golang.org/protobuf/proto"
    cid "github.com/ipfs/go-cid"
)

// Metrics for blockstore operations
var (
	blockstoreOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openhashdb_blockstore_operations_total",
			Help: "Total number of blockstore operations",
		},
		[]string{"operation", "status"},
	)
	blockstoreSpaceAvailable = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "openhashdb_blockstore_space_available_bytes",
			Help: "Available blockstore space in bytes",
		},
	)
	blockstoreGcTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_blockstore_gc_total",
			Help: "Total number of garbage collection runs.",
		},
	)
	blockstoreGcBlocksDeleted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openhashdb_blockstore_gc_blocks_deleted_total",
			Help: "Total number of blocks deleted by garbage collection.",
		},
	)
)

const (
    contentPrefix = "content:"
    // blockPrefix is no longer used for leveldb but const is kept for reference
    blockPrefix = "block:"
    // cidContentPrefix stores content metadata keyed by CID string
    cidContentPrefix = "cid:"
)

// Blockstore handles persistent storage of blocks.
type Blockstore struct {
	db         *leveldb.DB
	rootPath   string
	shardsPath string
}

// NewBlockstore creates a new blockstore instance at the given root path.
func NewBlockstore(rootPath string) (*Blockstore, error) {
	// Ensure the root directory exists
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database root directory %s: %w", rootPath, err)
	}

	shardsPath := filepath.Join(rootPath, "shards")
	if err := os.MkdirAll(shardsPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shards directory %s: %w", shardsPath, err)
	}

	leveldbPath := filepath.Join(rootPath, "leveldb")
	db, err := leveldb.OpenFile(leveldbPath, &opt.Options{
		WriteBuffer:            64 * 1024 * 1024,
		CompactionTableSize:    8 * 1024 * 1024,
		CompactionTotalSize:    64 * 1024 * 1024,
		OpenFilesCacheCapacity: 500,
	})
	if err != nil {
		blockstoreOperationsTotal.WithLabelValues("open_db", "error").Inc()
		return nil, fmt.Errorf("failed to open database at %s: %w", leveldbPath, err)
	}

	bs := &Blockstore{
		db:         db,
		rootPath:   rootPath,
		shardsPath: shardsPath,
	}

	if space, err := bs.GetAvailableSpace(); err == nil {
		blockstoreSpaceAvailable.Set(float64(space))
	}

	blockstoreOperationsTotal.WithLabelValues("open_db", "success").Inc()
	return bs, nil
}

// getShardPath returns the directory and full file path for a given block hash.
// It creates a 2-level directory structure to avoid too many files in one directory.
// e.g., <root>/shards/ab/cd/abcdef123...
func (bs *Blockstore) getShardPath(h hasher.Hash) (string, string) {
	hashStr := h.String()
	if len(hashStr) < 4 {
		// Should not happen with sha256, but handle defensively
		dir := bs.shardsPath
		return dir, filepath.Join(dir, hashStr)
	}
	// Use first 2 chars for the first level, next 2 for the second.
	dir := filepath.Join(bs.shardsPath, hashStr[:2], hashStr[2:4])
	return dir, filepath.Join(dir, hashStr)
}

// Close closes the blockstore.
func (bs *Blockstore) Close() error {
	err := bs.db.Close()
	if err != nil {
		blockstoreOperationsTotal.WithLabelValues("close_db", "error").Inc()
		return fmt.Errorf("failed to close database: %w", err)
	}
	blockstoreOperationsTotal.WithLabelValues("close_db", "success").Inc()
	return nil
}

// Get retrieves a block from the blockstore.
func (bs *Blockstore) Get(h hasher.Hash) (block.Block, error) {
	_, filePath := bs.getShardPath(h)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			blockstoreOperationsTotal.WithLabelValues("get", "not_found").Inc()
			return nil, fmt.Errorf("block not found: %s", h.String())
		}
		blockstoreOperationsTotal.WithLabelValues("get", "error").Inc()
		return nil, fmt.Errorf("failed to get block %s from disk: %w", h.String(), err)
	}

	blockstoreOperationsTotal.WithLabelValues("get", "success").Inc()
	return block.NewBlockWithHash(h, data), nil
}

// Put stores a block in the blockstore.
func (bs *Blockstore) Put(b block.Block) error {
	hash := b.Hash()
	dir, filePath := bs.getShardPath(hash)

	// Check if block already exists on disk to avoid unnecessary writes
	if _, err := os.Stat(filePath); err == nil {
		blockstoreOperationsTotal.WithLabelValues("put", "exists").Inc()
		return nil // Block already exists
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		blockstoreOperationsTotal.WithLabelValues("put", "error").Inc()
		return fmt.Errorf("failed to create shard directory for block %s: %w", hash.String(), err)
	}

	if err := os.WriteFile(filePath, b.RawData(), 0644); err != nil {
		blockstoreOperationsTotal.WithLabelValues("put", "error").Inc()
		return fmt.Errorf("failed to store block %s to disk: %w", hash.String(), err)
	}

	if space, err := bs.GetAvailableSpace(); err == nil {
		blockstoreSpaceAvailable.Set(float64(space))
	}

	blockstoreOperationsTotal.WithLabelValues("put", "success").Inc()
	return nil
}

// Has checks if a block exists in the blockstore.
func (bs *Blockstore) Has(h hasher.Hash) (bool, error) {
	_, filePath := bs.getShardPath(h)
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// GetSize returns the size of a block.
func (bs *Blockstore) GetSize(h hasher.Hash) (int, error) {
	_, filePath := bs.getShardPath(h)
	stat, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, fmt.Errorf("block not found: %s", h.String())
		}
		return -1, err
	}
	return int(stat.Size()), nil
}

// AllKeysChan returns a channel that streams all block keys.
func (bs *Blockstore) AllKeysChan(ctx context.Context) (<-chan hasher.Hash, error) {
	ch := make(chan hasher.Hash)
	go func() {
		defer close(ch)

		err := filepath.Walk(bs.shardsPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			// The filename is the hash
			hashStr := info.Name()
			h, err := hasher.HashFromString(hashStr)
			if err != nil {
				log.Printf("Skipping invalid block file in shards: %s", path)
				return nil // Continue walking
			}

			select {
			case ch <- h:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})

		if err != nil {
			log.Printf("Error walking through shards directory: %v", err)
		}
	}()
	return ch, nil
}

// --- Content (DAG) Metadata Management ---

// StoreContent stores content metadata.
func (bs *Blockstore) StoreContent(metadata *pb.ContentMetadata) error {
    // Store by CID as primary key
    h, err := hasher.HashFromBytes(metadata.Hash)
    if err != nil {
        return err
    }
    c, err := cidutil.FromHash(h, cidutil.Raw)
    if err != nil {
        return fmt.Errorf("failed to build CID: %w", err)
    }
    key := cidContentPrefix + c.String()
    data, err := proto.Marshal(metadata)
    if err != nil {
        return fmt.Errorf("failed to marshal metadata: %w", err)
    }
    if err := bs.db.Put([]byte(key), data, &opt.WriteOptions{Sync: true}); err != nil {
        return err
    }
    // Deprecated: also write legacy hash key for backward compatibility during transition
    legacyKey := contentPrefix + h.String()
    _ = bs.db.Put([]byte(legacyKey), data, &opt.WriteOptions{Sync: false})
    return nil
}

// GetContent retrieves content metadata.
func (bs *Blockstore) GetContent(hash hasher.Hash) (*pb.ContentMetadata, error) {
    // Primary: CID key
    c, err := cidutil.FromHash(hash, cidutil.Raw)
    if err == nil {
        key := cidContentPrefix + c.String()
        if data, err := bs.db.Get([]byte(key), nil); err == nil {
            var metadata pb.ContentMetadata
            if uErr := proto.Unmarshal(data, &metadata); uErr == nil {
                return &metadata, nil
            }
        }
    }
    // Fallback: legacy hash key
    legacyKey := contentPrefix + hash.String()
    data, err := bs.db.Get([]byte(legacyKey), nil)
    if err != nil {
        if err == leveldb.ErrNotFound {
            return nil, fmt.Errorf("content metadata not found for hash %s: %w", hash.String(), err)
        }
        return nil, fmt.Errorf("failed to get content metadata: %w", err)
    }
    var metadata pb.ContentMetadata
    if err := proto.Unmarshal(data, &metadata); err != nil {
        return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
    }
    return &metadata, nil
}

// HasContent checks if content metadata exists.
func (bs *Blockstore) HasContent(hash hasher.Hash) bool {
    // Check CID first
    if c, err := cidutil.FromHash(hash, cidutil.Raw); err == nil {
        key := cidContentPrefix + c.String()
        if val, err := bs.db.Has([]byte(key), nil); err == nil && val {
            return true
        }
    }
    // Fallback legacy
    key := contentPrefix + hash.String()
    val, err := bs.db.Has([]byte(key), nil)
    return err == nil && val
}

// ListContent returns all content hashes.
func (bs *Blockstore) ListContent() ([]hasher.Hash, error) {
    var hashes []hasher.Hash
    // Prefer CID-indexed entries
    cidPrefix := []byte(cidContentPrefix)
    iter := bs.db.NewIterator(util.BytesPrefix(cidPrefix), nil)
    defer iter.Release()

    for iter.Next() {
        key := string(iter.Key())
        cidStr := key[len(cidContentPrefix):]
        if c, err := cid.Parse(cidStr); err == nil {
            if h, err := cidutil.ToHash(c); err == nil {
                hashes = append(hashes, h)
            }
        }
    }

    if err := iter.Error(); err != nil {
        return nil, fmt.Errorf("iterator error while listing content: %w", err)
    }
    // If none found under CID index, fallback to legacy keys
    if len(hashes) == 0 {
        prefix := []byte(contentPrefix)
        iter2 := bs.db.NewIterator(util.BytesPrefix(prefix), nil)
        for iter2.Next() {
            key := string(iter2.Key())
            hashStr := key[len(contentPrefix):]
            if h, err := hasher.HashFromString(hashStr); err == nil {
                hashes = append(hashes, h)
            }
        }
        iter2.Release()
    }
    return hashes, nil
}

// --- Garbage Collection ---

// Delete removes a block from the blockstore.
func (bs *Blockstore) Delete(h hasher.Hash) error {
	_, filePath := bs.getShardPath(h)
	err := os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// If it doesn't exist, we can consider it a success.
			blockstoreOperationsTotal.WithLabelValues("delete", "success").Inc()
			return nil
		}
		blockstoreOperationsTotal.WithLabelValues("delete", "error").Inc()
		return fmt.Errorf("failed to delete block %s from disk: %w", h.String(), err)
	}
	blockstoreOperationsTotal.WithLabelValues("delete", "success").Inc()
	return nil
}

// GC performs garbage collection on the blockstore, deleting unpinned blocks.
func (bs *Blockstore) GC(ctx context.Context, rootHashes <-chan hasher.Hash) (int, error) {
	log.Println("Starting blockstore garbage collection...")
	blockstoreGcTotal.Inc()

	liveBlocks := &sync.Map{} // Use a sync.Map for concurrent access

	// Mark phase
	log.Println("GC: Mark phase started.")
	var markWg sync.WaitGroup
	for rootHash := range rootHashes {
		markWg.Add(1)
		go func(h hasher.Hash) {
			defer markWg.Done()
			if err := bs.markLive(ctx, h, liveBlocks); err != nil {
				log.Printf("GC: Error marking blocks for root %s: %v", h.String(), err)
			}
		}(rootHash)
	}
	markWg.Wait()

	liveBlockCount := 0
	liveBlocks.Range(func(_, _ interface{}) bool {
		liveBlockCount++
		return true
	})
	log.Printf("GC: Mark phase complete. Found %d live blocks.", liveBlockCount)

	// Sweep phase
	log.Println("GC: Sweep phase started.")
	allBlocks, err := bs.AllKeysChan(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get all block keys for GC: %w", err)
	}

	deletedCount := 0
	for blockHash := range allBlocks {
		select {
		case <-ctx.Done():
			return deletedCount, ctx.Err()
		default:
			if _, isLive := liveBlocks.Load(blockHash); !isLive {
				if err := bs.Delete(blockHash); err != nil {
					log.Printf("GC: Failed to delete block %s: %v", blockHash.String(), err)
				} else {
				deletedCount++
				}
			}
		}
	}
	log.Printf("GC: Sweep phase complete. Deleted %d blocks.", deletedCount)
	blockstoreGcBlocksDeleted.Add(float64(deletedCount))

	if space, err := bs.GetAvailableSpace(); err == nil {
		blockstoreSpaceAvailable.Set(float64(space))
	}

	log.Printf("Blockstore garbage collection finished. Deleted %d blocks.", deletedCount)
	return deletedCount, nil
}

// markLive recursively traverses a DAG from a root content hash, adding all encountered blocks to the live set.
func (bs *Blockstore) markLive(ctx context.Context, rootHash hasher.Hash, liveBlocks *sync.Map) error {
	// Check if this content hash has already been processed.
	if _, exists := liveBlocks.LoadOrStore(rootHash, struct{}{}); exists {
		return nil
	}

	// Get the metadata for the content hash.
	metadata, err := bs.GetContent(rootHash)
	if err != nil {
		// This root hash doesn't correspond to any known content.
		// It might be a raw block that's pinned. We already marked the hash itself, so it won't be deleted.
		return nil
	}

	// Mark all the data chunks of this content as live.
	for _, chunk := range metadata.Chunks {
		h, err := hasher.HashFromBytes(chunk.Hash)
		if err != nil {
			log.Printf("GC: Error converting chunk hash: %v", err)
			continue
		}
		liveBlocks.Store(h, struct{}{})
	}

	// Recursively mark all linked content.
	var linkWg sync.WaitGroup
	for _, link := range metadata.Links {
		linkWg.Add(1)
		go func(linkHash []byte) {
			defer linkWg.Done()
			h, err := hasher.HashFromBytes(linkHash)
			if err != nil {
				log.Printf("GC: Error converting link hash: %v", err)
				return
			}
			bs.markLive(ctx, h, liveBlocks)
		}(link.Hash)
	}
	linkWg.Wait()

	return nil
}
