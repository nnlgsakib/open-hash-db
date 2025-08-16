package blockstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"openhashdb/core/block"
	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"openhashdb/core/merkle"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
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
)

const (
	contentPrefix = "content:"
	blockPrefix   = "block:"
)

// ContentMetadata represents metadata for stored content (DAGs).
type ContentMetadata struct {
	Hash        hasher.Hash         `json:"hash"`
	Filename    string              `json:"filename"`
	MimeType    string              `json:"mime_type"`
	Size        int64               `json:"size"`
	ModTime     time.Time           `json:"mod_time"`
	IsDirectory bool                `json:"is_directory"`
	CreatedAt   time.Time           `json:"created_at"`
	RefCount    int                 `json:"ref_count"`
	Chunks      []chunker.ChunkInfo `json:"chunks,omitempty"`
	Links       []merkle.Link       `json:"links,omitempty"`
}

// Blockstore handles persistent storage of blocks.
type Blockstore struct {
	db       *leveldb.DB
	rootPath string
}

// NewBlockstore creates a new blockstore instance at the given root path.
func NewBlockstore(rootPath string) (*Blockstore, error) {
	// Ensure the root directory exists
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database root directory %s: %w", rootPath, err)
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
		db:       db,
		rootPath: rootPath,
	}

	if space, err := bs.GetAvailableSpace(); err == nil {
		blockstoreSpaceAvailable.Set(float64(space))
	}

	blockstoreOperationsTotal.WithLabelValues("open_db", "success").Inc()
	return bs, nil
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
	key := []byte(blockPrefix + h.String())
	data, err := bs.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			blockstoreOperationsTotal.WithLabelValues("get", "not_found").Inc()
			return nil, fmt.Errorf("block not found: %s", h.String())
		}
		blockstoreOperationsTotal.WithLabelValues("get", "error").Inc()
		return nil, fmt.Errorf("failed to get block %s: %w", h.String(), err)
	}

	blockstoreOperationsTotal.WithLabelValues("get", "success").Inc()
	return block.NewBlockWithHash(h, data), nil
}

// Put stores a block in the blockstore.
func (bs *Blockstore) Put(b block.Block) error {
	hash := b.Hash()
	key := []byte(blockPrefix + hash.String())

	exists, err := bs.db.Has(key, nil)
	if err != nil {
		blockstoreOperationsTotal.WithLabelValues("put", "error").Inc()
		return fmt.Errorf("failed to check for block %s: %w", hash.String(), err)
	}
	if exists {
		blockstoreOperationsTotal.WithLabelValues("put", "exists").Inc()
		return nil // Block already exists
	}

	if err := bs.db.Put(key, b.RawData(), nil); err != nil {
		blockstoreOperationsTotal.WithLabelValues("put", "error").Inc()
		return fmt.Errorf("failed to store block %s: %w", hash.String(), err)
	}

	if space, err := bs.GetAvailableSpace(); err == nil {
		blockstoreSpaceAvailable.Set(float64(space))
	}

	blockstoreOperationsTotal.WithLabelValues("put", "success").Inc()
	return nil
}

// Has checks if a block exists in the blockstore.
func (bs *Blockstore) Has(h hasher.Hash) (bool, error) {
	key := []byte(blockPrefix + h.String())
	return bs.db.Has(key, nil)
}

// GetSize returns the size of a block.
func (bs *Blockstore) GetSize(h hasher.Hash) (int, error) {
	key := []byte(blockPrefix + h.String())
	data, err := bs.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return -1, fmt.Errorf("block not found: %s", h.String())
		}
		return -1, err
	}
	return len(data), nil
}

// AllKeysChan returns a channel that streams all block keys.
func (bs *Blockstore) AllKeysChan(ctx context.Context) (<-chan hasher.Hash, error) {
	ch := make(chan hasher.Hash)
	go func() {
		defer close(ch)

		prefix := []byte(blockPrefix)
		iter := bs.db.NewIterator(util.BytesPrefix(prefix), nil)
		defer iter.Release()

		for iter.Next() {
			key := iter.Key()
			// Trim prefix to get the hash string
			hashStr := string(key[len(prefix):])
			h, err := hasher.HashFromString(hashStr)
			if err != nil {
				log.Printf("Skipping invalid block key in database: %s", key)
				continue
			}
			select {
			case ch <- h:
			case <-ctx.Done():
				return
			}
		}
		if err := iter.Error(); err != nil {
			log.Printf("Error iterating through blockstore: %v", err)
		}
	}()
	return ch, nil
}

// --- Content (DAG) Metadata Management ---

// StoreContent stores content metadata.
func (bs *Blockstore) StoreContent(metadata *ContentMetadata) error {
	key := contentPrefix + metadata.Hash.String()
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return bs.db.Put([]byte(key), data, &opt.WriteOptions{Sync: true})
}

// GetContent retrieves content metadata.
func (bs *Blockstore) GetContent(hash hasher.Hash) (*ContentMetadata, error) {
	key := contentPrefix + hash.String()
	data, err := bs.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, fmt.Errorf("content metadata not found for hash %s: %w", hash.String(), err)
		}
		return nil, fmt.Errorf("failed to get content metadata: %w", err)
	}

	var metadata ContentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return &metadata, nil
}

// HasContent checks if content metadata exists.
func (bs *Blockstore) HasContent(hash hasher.Hash) bool {
	key := contentPrefix + hash.String()
	val, err := bs.db.Has([]byte(key), nil)
	return err == nil && val
}

// ListContent returns all content hashes.
func (bs *Blockstore) ListContent() ([]hasher.Hash, error) {
	var hashes []hasher.Hash
	prefix := []byte(contentPrefix)
	iter := bs.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		key := string(iter.Key())
		hashStr := key[len(contentPrefix):]
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			continue
		}
		hashes = append(hashes, hash)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error while listing content: %w", err)
	}
	return hashes, nil
}
