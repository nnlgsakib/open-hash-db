package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"openhashdb/core/hasher"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Metrics for storage operations
var (
	storageOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openhashdb_storage_operations_total",
			Help: "Total number of storage operations",
		},
		[]string{"operation", "status"},
	)
	storageSpaceAvailable = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "openhashdb_storage_space_available_bytes",
			Help: "Available storage space in bytes",
		},
	)
)

const (
	contentPrefix = "content:"
	chunkPrefix   = "chunk:"
	dataPrefix    = "data:"
)

// ContentMetadata represents metadata for stored content
type ContentMetadata struct {
	Hash        hasher.Hash   `json:"hash"`         // Random hash used as the primary identifier
	ContentHash hasher.Hash   `json:"content_hash"` // Content-based hash for verification
	Filename    string        `json:"filename"`
	MimeType    string        `json:"mime_type"`
	Size        int64         `json:"size"`
	ModTime     time.Time     `json:"mod_time"`
	ChunkHashes []hasher.Hash `json:"chunk_hashes,omitempty"` // Hashes of the file's chunks
	ChunkCount  int           `json:"chunk_count,omitempty"`
	IsDirectory bool          `json:"is_directory"`
	CreatedAt   time.Time     `json:"created_at"`
	RefCount    int           `json:"ref_count"`
}

// ChunkMetadata represents metadata for a chunk
type ChunkMetadata struct {
	Hash      hasher.Hash `json:"hash"`
	Size      int         `json:"size"`
	RefCount  int         `json:"ref_count"`
	CreatedAt time.Time   `json:"created_at"`
}

// Storage handles persistent storage operations
type Storage struct {
	db       *leveldb.DB
	dataPath string
	mu       sync.RWMutex // Mutex for thread-safe database and filesystem access
}

// NewStorage creates a new storage instance
func NewStorage(dbPath string) (*Storage, error) {
	db, err := leveldb.OpenFile(dbPath, &opt.Options{
		WriteBuffer:            64 * 1024 * 1024, // 64MB write buffer
		CompactionTableSize:    8 * 1024 * 1024,  // 8MB table size for compaction
		CompactionTotalSize:    64 * 1024 * 1024, // 64MB total size for compaction
		OpenFilesCacheCapacity: 500,              // Cache for open files
	})
	if err != nil {
		storageOperationsTotal.WithLabelValues("open_db", "error").Inc()
		return nil, fmt.Errorf("failed to open database at %s: %w", dbPath, err)
	}

	dataPath := filepath.Join(filepath.Dir(dbPath), "data")
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		storageOperationsTotal.WithLabelValues("mkdir", "error").Inc()
		return nil, fmt.Errorf("failed to create data directory %s: %w", dataPath, err)
	}

	s := &Storage{
		db:       db,
		dataPath: dataPath,
		mu:       sync.RWMutex{},
	}

	// Update available space metric
	if space, err := s.GetAvailableSpace(); err == nil {
		storageSpaceAvailable.Set(float64(space))
	}

	storageOperationsTotal.WithLabelValues("open_db", "success").Inc()
	return s, nil
}

// Close closes the storage
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Close()
	if err != nil {
		storageOperationsTotal.WithLabelValues("close_db", "error").Inc()
		return fmt.Errorf("failed to close database: %w", err)
	}
	storageOperationsTotal.WithLabelValues("close_db", "success").Inc()
	return nil
}

// ValidateContent checks if both metadata and data exist for a hash
func (s *Storage) ValidateContent(hash hasher.Hash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.HasContent(hash) {
		return fmt.Errorf("content metadata not found for %s", hash.String())
	}
	if !s.HasData(hash) {
		return fmt.Errorf("content data not found for %s", hash.String())
	}
	return nil
}

// StoreContent stores content metadata
func (s *Storage) StoreContent(metadata *ContentMetadata) error {
	if metadata == nil {
		storageOperationsTotal.WithLabelValues("store_content", "error").Inc()
		return fmt.Errorf("nil metadata provided")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := contentPrefix + metadata.Hash.String()
	data, err := json.Marshal(metadata)
	if err != nil {
		storageOperationsTotal.WithLabelValues("store_content", "error").Inc()
		return fmt.Errorf("failed to marshal metadata for %s: %w", metadata.Hash.String(), err)
	}

	if err := s.db.Put([]byte(key), data, &opt.WriteOptions{Sync: true}); err != nil {
		storageOperationsTotal.WithLabelValues("store_content", "error").Inc()
		return fmt.Errorf("failed to store content metadata for %s: %w", metadata.Hash.String(), err)
	}

	log.Printf("Stored content metadata for random hash %s with content hash %s", metadata.Hash.String(), metadata.ContentHash.String())
	storageOperationsTotal.WithLabelValues("store_content", "success").Inc()
	return nil
}

// HasContentByHash checks if content with a specific content hash exists
func (s *Storage) HasContentByHash(contentHash hasher.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	prefix := []byte(contentPrefix)
	for iter.Seek(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		var metadata ContentMetadata
		if err := json.Unmarshal(iter.Value(), &metadata); err == nil {
			if bytes.Equal(metadata.ContentHash[:], contentHash[:]) {
				log.Printf("Found content metadata for content hash %s (random hash %s)", contentHash.String(), metadata.Hash.String())
				storageOperationsTotal.WithLabelValues("has_content_by_hash", "success").Inc()
				return true
			}
		}
	}

	if err := iter.Error(); err != nil {
		log.Printf("Iterator error in HasContentByHash: %v", err)
		storageOperationsTotal.WithLabelValues("has_content_by_hash", "error").Inc()
	}
	log.Printf("No content metadata found for content hash %s", contentHash.String())
	storageOperationsTotal.WithLabelValues("has_content_by_hash", "not_found").Inc()
	return false
}

// GetContent retrieves content metadata
func (s *Storage) GetContent(hash hasher.Hash) (*ContentMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := contentPrefix + hash.String()
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Printf("Content metadata not found for %s", hash.String())
			storageOperationsTotal.WithLabelValues("get_content", "not_found").Inc()
			return nil, fmt.Errorf("content not found: %s", hash.String())
		}
		log.Printf("Failed to get content metadata for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("get_content", "error").Inc()
		return nil, fmt.Errorf("failed to get content metadata for %s: %w", hash.String(), err)
	}

	var metadata ContentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		log.Printf("Failed to unmarshal metadata for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("get_content", "error").Inc()
		return nil, fmt.Errorf("failed to unmarshal metadata for %s: %w", hash.String(), err)
	}

	log.Printf("Retrieved content metadata for %s (content hash %s)", hash.String(), metadata.ContentHash.String())
	storageOperationsTotal.WithLabelValues("get_content", "success").Inc()
	return &metadata, nil
}

// StoreChunk stores chunk metadata
func (s *Storage) StoreChunk(metadata *ChunkMetadata) error {
	if metadata == nil {
		storageOperationsTotal.WithLabelValues("store_chunk", "error").Inc()
		return fmt.Errorf("nil chunk metadata provided")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := chunkPrefix + metadata.Hash.String()
	data, err := json.Marshal(metadata)
	if err != nil {
		storageOperationsTotal.WithLabelValues("store_chunk", "error").Inc()
		return fmt.Errorf("failed to marshal chunk metadata for %s: %w", metadata.Hash.String(), err)
	}

	if err := s.db.Put([]byte(key), data, &opt.WriteOptions{Sync: true}); err != nil {
		storageOperationsTotal.WithLabelValues("store_chunk", "error").Inc()
		return fmt.Errorf("failed to store chunk metadata for %s: %w", metadata.Hash.String(), err)
	}

	log.Printf("Stored chunk metadata for %s", metadata.Hash.String())
	storageOperationsTotal.WithLabelValues("store_chunk", "success").Inc()
	return nil
}

// GetChunk retrieves chunk metadata
func (s *Storage) GetChunk(hash hasher.Hash) (*ChunkMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := chunkPrefix + hash.String()
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Printf("Chunk metadata not found for %s", hash.String())
			storageOperationsTotal.WithLabelValues("get_chunk", "not_found").Inc()
			return nil, fmt.Errorf("chunk not found: %s", hash.String())
		}
		log.Printf("Failed to get chunk metadata for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("get_chunk", "error").Inc()
		return nil, fmt.Errorf("failed to get chunk metadata for %s: %w", hash.String(), err)
	}

	var metadata ChunkMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		log.Printf("Failed to unmarshal chunk metadata for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("get_chunk", "error").Inc()
		return nil, fmt.Errorf("failed to unmarshal chunk metadata for %s: %w", hash.String(), err)
	}

	log.Printf("Retrieved chunk metadata for %s", hash.String())
	storageOperationsTotal.WithLabelValues("get_chunk", "success").Inc()
	return &metadata, nil
}

// StoreData stores raw data to the filesystem
func (s *Storage) StoreData(hash hasher.Hash, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := filepath.Join(s.dataPath, hash.String())
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		log.Printf("Failed to store data for %s at %s: %v", hash.String(), filePath, err)
		storageOperationsTotal.WithLabelValues("store_data", "error").Inc()
		return fmt.Errorf("failed to store data for %s: %w", hash.String(), err)
	}

	log.Printf("Stored data for %s at %s (%d bytes)", hash.String(), filePath, len(data))
	// Update available space metric
	if space, err := s.GetAvailableSpace(); err == nil {
		storageSpaceAvailable.Set(float64(space))
	}

	storageOperationsTotal.WithLabelValues("store_data", "success").Inc()
	return nil
}

// StoreDataFromFile moves a file from srcPath to the storage data path.
func (s *Storage) StoreDataFromFile(hash hasher.Hash, srcPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dstPath := filepath.Join(s.dataPath, hash.String())
	if err := os.Rename(srcPath, dstPath); err != nil {
		// If rename fails (e.g., across different devices), fall back to copy
		log.Printf("Failed to move file, falling back to copy: %v", err)
		src, err := os.Open(srcPath)
		if err != nil {
			return fmt.Errorf("failed to open source file for copy: %w", err)
		}
		defer src.Close()

		dst, err := os.Create(dstPath)
		if err != nil {
			return fmt.Errorf("failed to create destination file for copy: %w", err)
		}
		defer dst.Close()

		if _, err := io.Copy(dst, src); err != nil {
			return fmt.Errorf("failed to copy file to storage: %w", err)
		}
	}

	log.Printf("Stored data for %s from file %s", hash.String(), srcPath)
	// Update available space metric
	if space, err := s.GetAvailableSpace(); err == nil {
		storageSpaceAvailable.Set(float64(space))
	}

	storageOperationsTotal.WithLabelValues("store_data_from_file", "success").Inc()
	return nil
}

// GetData retrieves raw data from the filesystem
func (s *Storage) GetData(hash hasher.Hash) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.dataPath, hash.String())
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Data not found for %s at %s", hash.String(), filePath)
			storageOperationsTotal.WithLabelValues("get_data", "not_found").Inc()
			return nil, fmt.Errorf("data not found: %s", hash.String())
		}
		log.Printf("Failed to get data for %s at %s: %v", hash.String(), filePath, err)
		storageOperationsTotal.WithLabelValues("get_data", "error").Inc()
		return nil, fmt.Errorf("failed to get data for %s: %w", hash.String(), err)
	}

	log.Printf("Retrieved data for %s from %s (%d bytes)", hash.String(), filePath, len(data))
	storageOperationsTotal.WithLabelValues("get_data", "success").Inc()
	return data, nil
}

// GetDataStream returns a reader for the raw data from the filesystem
func (s *Storage) GetDataStream(hash hasher.Hash) (*os.File, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.dataPath, hash.String())
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Data not found for %s at %s", hash.String(), filePath)
			storageOperationsTotal.WithLabelValues("get_data_stream", "not_found").Inc()
			return nil, fmt.Errorf("data not found: %s", hash.String())
		}
		log.Printf("Failed to open data stream for %s at %s: %v", hash.String(), filePath, err)
		storageOperationsTotal.WithLabelValues("get_data_stream", "error").Inc()
		return nil, fmt.Errorf("failed to open data stream for %s: %w", hash.String(), err)
	}

	log.Printf("Opened data stream for %s at %s", hash.String(), filePath)
	storageOperationsTotal.WithLabelValues("get_data_stream", "success").Inc()
	return file, nil
}

// HasContent checks if content metadata exists
func (s *Storage) HasContent(hash hasher.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := contentPrefix + hash.String()
	_, err := s.db.Get([]byte(key), nil)
	if err == nil {
		log.Printf("Content metadata found for %s", hash.String())
		storageOperationsTotal.WithLabelValues("has_content", "success").Inc()
		return true
	}
	log.Printf("Content metadata not found for %s: %v", hash.String(), err)
	storageOperationsTotal.WithLabelValues("has_content", "not_found").Inc()
	return false
}

// HasChunk checks if chunk metadata exists
func (s *Storage) HasChunk(hash hasher.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := chunkPrefix + hash.String()
	_, err := s.db.Get([]byte(key), nil)
	if err == nil {
		log.Printf("Chunk metadata found for %s", hash.String())
		storageOperationsTotal.WithLabelValues("has_chunk", "success").Inc()
		return true
	}
	log.Printf("Chunk metadata not found for %s: %v", hash.String(), err)
	storageOperationsTotal.WithLabelValues("has_chunk", "not_found").Inc()
	return false
}

// HasData checks if data exists on the filesystem
func (s *Storage) HasData(hash hasher.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.dataPath, hash.String())
	_, err := os.Stat(filePath)
	if err == nil {
		log.Printf("Data found for %s at %s", hash.String(), filePath)
		storageOperationsTotal.WithLabelValues("has_data", "success").Inc()
		return true
	}
	log.Printf("Data not found for %s at %s: %v", hash.String(), filePath, err)
	storageOperationsTotal.WithLabelValues("has_data", "not_found").Inc()
	return false
}

// IncrementRefCount increments reference count for content
func (s *Storage) IncrementRefCount(hash hasher.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	metadata, err := s.GetContent(hash)
	if err != nil {
		log.Printf("Failed to increment ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("increment_ref_count", "error").Inc()
		return err
	}

	metadata.RefCount++
	if err := s.StoreContent(metadata); err != nil {
		log.Printf("Failed to store updated ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("increment_ref_count", "error").Inc()
		return err
	}

	log.Printf("Incremented ref count for %s to %d", hash.String(), metadata.RefCount)
	storageOperationsTotal.WithLabelValues("increment_ref_count", "success").Inc()
	return nil
}

// DecrementRefCount decrements reference count for content
func (s *Storage) DecrementRefCount(hash hasher.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	metadata, err := s.GetContent(hash)
	if err != nil {
		log.Printf("Failed to decrement ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("decrement_ref_count", "error").Inc()
		return err
	}

	if metadata.RefCount > 0 {
		metadata.RefCount--
	}
	if err := s.StoreContent(metadata); err != nil {
		log.Printf("Failed to store updated ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("decrement_ref_count", "error").Inc()
		return err
	}

	log.Printf("Decremented ref count for %s to %d", hash.String(), metadata.RefCount)
	storageOperationsTotal.WithLabelValues("decrement_ref_count", "success").Inc()
	return nil
}

// IncrementChunkRefCount increments reference count for chunk
func (s *Storage) IncrementChunkRefCount(hash hasher.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	metadata, err := s.GetChunk(hash)
	if err != nil {
		log.Printf("Failed to increment chunk ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("increment_chunk_ref_count", "error").Inc()
		return err
	}

	metadata.RefCount++
	if err := s.StoreChunk(metadata); err != nil {
		log.Printf("Failed to store updated chunk ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("increment_chunk_ref_count", "error").Inc()
		return err
	}

	log.Printf("Incremented chunk ref count for %s to %d", hash.String(), metadata.RefCount)
	storageOperationsTotal.WithLabelValues("increment_chunk_ref_count", "success").Inc()
	return nil
}

// DecrementChunkRefCount decrements reference count for chunk
func (s *Storage) DecrementChunkRefCount(hash hasher.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	metadata, err := s.GetChunk(hash)
	if err != nil {
		log.Printf("Failed to decrement chunk ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("decrement_chunk_ref_count", "error").Inc()
		return err
	}

	if metadata.RefCount > 0 {
		metadata.RefCount--
	}
	if err := s.StoreChunk(metadata); err != nil {
		log.Printf("Failed to store updated chunk ref count for %s: %v", hash.String(), err)
		storageOperationsTotal.WithLabelValues("decrement_chunk_ref_count", "error").Inc()
		return err
	}

	log.Printf("Decremented chunk ref count for %s to %d", hash.String(), metadata.RefCount)
	storageOperationsTotal.WithLabelValues("decrement_chunk_ref_count", "success").Inc()
	return nil
}

// ListContent returns all content hashes
func (s *Storage) ListContent() ([]hasher.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var hashes []hasher.Hash
	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	prefix := []byte(contentPrefix)
	for iter.Seek(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		key := string(iter.Key())
		hashStr := key[len(contentPrefix):]
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			log.Printf("Invalid hash in database: %s, error: %v", hashStr, err)
			continue
		}
		hashes = append(hashes, hash)
	}

	if err := iter.Error(); err != nil {
		log.Printf("Iterator error while listing content: %v", err)
		storageOperationsTotal.WithLabelValues("list_content", "error").Inc()
		return nil, fmt.Errorf("iterator error while listing content: %w", err)
	}

	log.Printf("Listed %d content hashes", len(hashes))
	storageOperationsTotal.WithLabelValues("list_content", "success").Inc()
	return hashes, nil
}

// ListChunks returns all chunk hashes
func (s *Storage) ListChunks() ([]hasher.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var hashes []hasher.Hash
	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	prefix := []byte(chunkPrefix)
	for iter.Seek(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		key := string(iter.Key())
		hashStr := key[len(chunkPrefix):]
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			log.Printf("Invalid chunk hash in database: %s, error: %v", hashStr, err)
			continue
		}
		hashes = append(hashes, hash)
	}

	if err := iter.Error(); err != nil {
		log.Printf("Iterator error while listing chunks: %v", err)
		storageOperationsTotal.WithLabelValues("list_chunks", "error").Inc()
		return nil, fmt.Errorf("iterator error while listing chunks: %w", err)
	}

	log.Printf("Listed %d chunk hashes", len(hashes))
	storageOperationsTotal.WithLabelValues("list_chunks", "success").Inc()
	return hashes, nil
}

// GarbageCollect removes content and chunks with zero reference count
func (s *Storage) GarbageCollect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	contentHashes, err := s.ListContent()
	if err != nil {
		log.Printf("Failed to list content for garbage collection: %v", err)
		storageOperationsTotal.WithLabelValues("garbage_collect", "error").Inc()
		return fmt.Errorf("failed to list content: %w", err)
	}

	for _, hash := range contentHashes {
		metadata, err := s.GetContent(hash)
		if err != nil {
			log.Printf("Failed to get content metadata for %s: %v", hash.String(), err)
			continue
		}

		if metadata.RefCount == 0 {
			contentKey := contentPrefix + hash.String()
			if err := s.db.Delete([]byte(contentKey), nil); err != nil {
				log.Printf("Failed to delete content metadata %s: %v", hash.String(), err)
			}
			filePath := filepath.Join(s.dataPath, hash.String())
			if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
				log.Printf("Failed to delete content data file %s: %v", filePath, err)
			}
			log.Printf("Garbage collected content %s", hash.String())
			storageOperationsTotal.WithLabelValues("garbage_collect_content", "success").Inc()
		}
	}

	chunkHashes, err := s.ListChunks()
	if err != nil {
		log.Printf("Failed to list chunks for garbage collection: %v", err)
		storageOperationsTotal.WithLabelValues("garbage_collect", "error").Inc()
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	for _, hash := range chunkHashes {
		metadata, err := s.GetChunk(hash)
		if err != nil {
			log.Printf("Failed to get chunk metadata for %s: %v", hash.String(), err)
			continue
		}

		if metadata.RefCount == 0 {
			chunkKey := chunkPrefix + hash.String()
			if err := s.db.Delete([]byte(chunkKey), nil); err != nil {
				log.Printf("Failed to delete chunk metadata %s: %v", hash.String(), err)
			}
			filePath := filepath.Join(s.dataPath, hash.String())
			if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
				log.Printf("Failed to delete chunk data file %s: %v", filePath, err)
			}
			log.Printf("Garbage collected chunk %s", hash.String())
			storageOperationsTotal.WithLabelValues("garbage_collect_chunk", "success").Inc()
		}
	}

	// Update available space metric
	if space, err := s.GetAvailableSpace(); err == nil {
		storageSpaceAvailable.Set(float64(space))
	}

	log.Printf("Completed garbage collection")
	storageOperationsTotal.WithLabelValues("garbage_collect", "success").Inc()
	return nil
}

// GetStats returns storage statistics
func (s *Storage) GetStats() (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	contentHashes, err := s.ListContent()
	if err != nil {
		log.Printf("Failed to list content for stats: %v", err)
		storageOperationsTotal.WithLabelValues("get_stats", "error").Inc()
		return nil, fmt.Errorf("failed to list content: %w", err)
	}

	chunkHashes, err := s.ListChunks()
	if err != nil {
		log.Printf("Failed to list chunks for stats: %v", err)
		storageOperationsTotal.WithLabelValues("get_stats", "error").Inc()
		return nil, fmt.Errorf("failed to list chunks: %w", err)
	}

	availableSpace, err := s.GetAvailableSpace()
	if err != nil {
		log.Printf("Failed to get available space for stats: %v", err)
		storageOperationsTotal.WithLabelValues("get_stats", "error").Inc()
		return nil, fmt.Errorf("failed to get available space: %w", err)
	}

	stats := map[string]interface{}{
		"content_count":   len(contentHashes),
		"chunk_count":     len(chunkHashes),
		"available_space": availableSpace,
	}

	log.Printf("Retrieved storage stats: %+v", stats)
	storageOperationsTotal.WithLabelValues("get_stats", "success").Inc()
	return stats, nil
}
