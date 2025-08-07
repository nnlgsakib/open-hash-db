package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"openhashdb/core/chunker"
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
	manifestPrefix = "manifest:"
	chunkPrefix    = "chunk:"
)

// ContentMetadata represents metadata for stored content
type ContentMetadata struct {
	MerkleRoot  hasher.Hash `json:"merkle_root"` // Merkle root of the manifest
	Filename    string      `json:"filename"`
	MimeType    string      `json:"mime_type"`
	Size        int64       `json:"size"`
	ModTime     time.Time   `json:"mod_time"`
	ChunkCount  int         `json:"chunk_count"`
	CreatedAt   time.Time   `json:"created_at"`
	RefCount    int         `json:"ref_count"`
	IsDirectory bool        `json:"is_directory"` // Added field
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
	mu       sync.RWMutex
}

// NewStorage creates a new storage instance
func NewStorage(dbPath string) (*Storage, error) {
	db, err := leveldb.OpenFile(dbPath, &opt.Options{
		WriteBuffer:            64 * 1024 * 1024,
		CompactionTableSize:    8 * 1024 * 1024,
		CompactionTotalSize:    64 * 1024 * 1024,
		OpenFilesCacheCapacity: 500,
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

	if space, err := s.GetAvailableSpace(); err == nil {
		storageSpaceAvailable.Set(float64(space))
	}

	storageOperationsTotal.WithLabelValues("open_db", "success").Inc()
	return s, nil
}

// GetDataPath returns the data path
func (s *Storage) GetDataPath() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dataPath
}

// HasContent checks if content metadata exists for a given hash
func (s *Storage) HasContent(merkleRoot hasher.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := manifestPrefix + merkleRoot.String()
	_, err := s.db.Get([]byte(key), nil)
	if err == nil {
		log.Printf("Content metadata found for %s", merkleRoot.String())
		storageOperationsTotal.WithLabelValues("has_content", "success").Inc()
		return true
	}
	log.Printf("Content metadata not found for %s: %v", merkleRoot.String(), err)
	storageOperationsTotal.WithLabelValues("has_content", "not_found").Inc()
	return false
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

// StoreManifest stores the manifest metadata
func (s *Storage) StoreManifest(manifest *chunker.Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := manifestPrefix + manifest.MerkleRoot.String()
	data, err := json.Marshal(manifest)
	if err != nil {
		storageOperationsTotal.WithLabelValues("store_manifest", "error").Inc()
		return fmt.Errorf("failed to marshal manifest for %s: %w", manifest.MerkleRoot.String(), err)
	}

	if err := s.db.Put([]byte(key), data, &opt.WriteOptions{Sync: true}); err != nil {
		storageOperationsTotal.WithLabelValues("store_manifest", "error").Inc()
		return fmt.Errorf("failed to store manifest for %s: %w", manifest.MerkleRoot.String(), err)
	}

	log.Printf("Stored manifest for %s", manifest.MerkleRoot.String())
	storageOperationsTotal.WithLabelValues("store_manifest", "success").Inc()
	return nil
}

// GetManifest retrieves the manifest metadata
func (s *Storage) GetManifest(merkleRoot hasher.Hash) (*chunker.Manifest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := manifestPrefix + merkleRoot.String()
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Printf("Manifest not found for %s", merkleRoot.String())
			storageOperationsTotal.WithLabelValues("get_manifest", "not_found").Inc()
			return nil, fmt.Errorf("manifest not found: %s", merkleRoot.String())
		}
		log.Printf("Failed to get manifest for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("get_manifest", "error").Inc()
		return nil, fmt.Errorf("failed to get manifest for %s: %w", merkleRoot.String(), err)
	}

	var manifest chunker.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		log.Printf("Failed to unmarshal manifest for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("get_manifest", "error").Inc()
		return nil, fmt.Errorf("failed to unmarshal manifest for %s: %w", merkleRoot.String(), err)
	}

	log.Printf("Retrieved manifest for %s", merkleRoot.String())
	storageOperationsTotal.WithLabelValues("get_manifest", "success").Inc()
	return &manifest, nil
}

// StoreContent stores content metadata
func (s *Storage) StoreContent(metadata *ContentMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := manifestPrefix + metadata.MerkleRoot.String()
	data, err := json.Marshal(metadata)
	if err != nil {
		storageOperationsTotal.WithLabelValues("store_content", "error").Inc()
		return fmt.Errorf("failed to marshal content metadata for %s: %w", metadata.MerkleRoot.String(), err)
	}

	if err := s.db.Put([]byte(key), data, &opt.WriteOptions{Sync: true}); err != nil {
		storageOperationsTotal.WithLabelValues("store_content", "error").Inc()
		return fmt.Errorf("failed to store content metadata for %s: %w", metadata.MerkleRoot.String(), err)
	}

	log.Printf("Stored content metadata for %s", metadata.MerkleRoot.String())
	storageOperationsTotal.WithLabelValues("store_content", "success").Inc()
	return nil
}

// GetContent retrieves content metadata
func (s *Storage) GetContent(merkleRoot hasher.Hash) (*ContentMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := manifestPrefix + merkleRoot.String()
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Printf("Content metadata not found for %s", merkleRoot.String())
			storageOperationsTotal.WithLabelValues("get_content", "not_found").Inc()
			return nil, fmt.Errorf("content not found: %s", merkleRoot.String())
		}
		log.Printf("Failed to get content metadata for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("get_content", "error").Inc()
		return nil, fmt.Errorf("failed to get content metadata for %s: %w", merkleRoot.String(), err)
	}

	var metadata ContentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		log.Printf("Failed to unmarshal content metadata for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("get_content", "error").Inc()
		return nil, fmt.Errorf("failed to unmarshal content metadata for %s: %w", merkleRoot.String(), err)
	}

	log.Printf("Retrieved content metadata for %s", merkleRoot.String())
	storageOperationsTotal.WithLabelValues("get_content", "success").Inc()
	return &metadata, nil
}

// StoreChunk stores chunk metadata
func (s *Storage) StoreChunk(metadata *ChunkMetadata) error {
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

// StoreChunkData stores chunk data to the filesystem
func (s *Storage) StoreChunkData(merkleRoot, chunkHash hasher.Hash, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirPath := filepath.Join(s.dataPath, merkleRoot.String())
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		storageOperationsTotal.WithLabelValues("store_chunk_data", "error").Inc()
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	filePath := filepath.Join(dirPath, chunkHash.String())
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		log.Printf("Failed to store chunk data for %s at %s: %v", chunkHash.String(), filePath, err)
		storageOperationsTotal.WithLabelValues("store_chunk_data", "error").Inc()
		return fmt.Errorf("failed to store chunk data for %s: %w", chunkHash.String(), err)
	}

	log.Printf("Stored chunk data for %s at %s", chunkHash.String(), filePath)
	if space, err := s.GetAvailableSpace(); err == nil {
		storageSpaceAvailable.Set(float64(space))
	}

	storageOperationsTotal.WithLabelValues("store_chunk_data", "success").Inc()
	return nil
}

// GetChunkData retrieves chunk data from the filesystem
func (s *Storage) GetChunkData(merkleRoot, chunkHash hasher.Hash) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.dataPath, merkleRoot.String(), chunkHash.String())
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Chunk data not found for %s at %s", chunkHash.String(), filePath)
			storageOperationsTotal.WithLabelValues("get_chunk_data", "not_found").Inc()
			return nil, fmt.Errorf("chunk data not found: %s", chunkHash.String())
		}
		log.Printf("Failed to get chunk data for %s at %s: %v", chunkHash.String(), filePath, err)
		storageOperationsTotal.WithLabelValues("get_chunk_data", "error").Inc()
		return nil, fmt.Errorf("failed to get chunk data for %s: %w", chunkHash.String(), err)
	}

	log.Printf("Retrieved chunk data for %s from %s", chunkHash.String(), filePath)
	storageOperationsTotal.WithLabelValues("get_chunk_data", "success").Inc()
	return data, nil
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

// HasChunkData checks if chunk data exists on the filesystem
func (s *Storage) HasChunkData(merkleRoot, chunkHash hasher.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.dataPath, merkleRoot.String(), chunkHash.String())
	_, err := os.Stat(filePath)
	if err == nil {
		log.Printf("Chunk data found for %s at %s", chunkHash.String(), filePath)
		storageOperationsTotal.WithLabelValues("has_chunk_data", "success").Inc()
		return true
	}
	log.Printf("Chunk data not found for %s at %s: %v", chunkHash.String(), filePath, err)
	storageOperationsTotal.WithLabelValues("has_chunk_data", "not_found").Inc()
	return false
}

// IncrementRefCount increments reference count for content
func (s *Storage) IncrementRefCount(merkleRoot hasher.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := manifestPrefix + merkleRoot.String()
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Printf("Content metadata not found for %s", merkleRoot.String())
			storageOperationsTotal.WithLabelValues("increment_ref_count", "not_found").Inc()
			return fmt.Errorf("content not found: %s", merkleRoot.String())
		}
		log.Printf("Failed to get content metadata for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("increment_ref_count", "error").Inc()
		return fmt.Errorf("failed to get content metadata for %s: %w", merkleRoot.String(), err)
	}

	var metadata ContentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		log.Printf("Failed to unmarshal metadata for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("increment_ref_count", "error").Inc()
		return fmt.Errorf("failed to unmarshal metadata for %s: %w", merkleRoot.String(), err)
	}

	metadata.RefCount++

	newData, err := json.Marshal(&metadata)
	if err != nil {
		storageOperationsTotal.WithLabelValues("increment_ref_count", "error").Inc()
		return fmt.Errorf("failed to marshal metadata for %s: %w", metadata.MerkleRoot.String(), err)
	}

	if err := s.db.Put([]byte(key), newData, &opt.WriteOptions{Sync: true}); err != nil {
		storageOperationsTotal.WithLabelValues("increment_ref_count", "error").Inc()
		return fmt.Errorf("failed to store content metadata for %s: %w", metadata.MerkleRoot.String(), err)
	}

	log.Printf("Incremented ref count for %s to %d", merkleRoot.String(), metadata.RefCount)
	storageOperationsTotal.WithLabelValues("increment_ref_count", "success").Inc()
	return nil
}

// DecrementRefCount decrements reference count for content
func (s *Storage) DecrementRefCount(merkleRoot hasher.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := manifestPrefix + merkleRoot.String()
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Printf("Content metadata not found for %s", merkleRoot.String())
			storageOperationsTotal.WithLabelValues("decrement_ref_count", "not_found").Inc()
			return fmt.Errorf("content not found: %s", merkleRoot.String())
		}
		log.Printf("Failed to get content metadata for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("decrement_ref_count", "error").Inc()
		return fmt.Errorf("failed to get content metadata for %s: %w", merkleRoot.String(), err)
	}

	var metadata ContentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		log.Printf("Failed to unmarshal metadata for %s: %v", merkleRoot.String(), err)
		storageOperationsTotal.WithLabelValues("decrement_ref_count", "error").Inc()
		return fmt.Errorf("failed to unmarshal metadata for %s: %w", merkleRoot.String(), err)
	}

	if metadata.RefCount > 0 {
		metadata.RefCount--
	}

	newData, err := json.Marshal(&metadata)
	if err != nil {
		storageOperationsTotal.WithLabelValues("decrement_ref_count", "error").Inc()
		return fmt.Errorf("failed to marshal metadata for %s: %w", metadata.MerkleRoot.String(), err)
	}

	if err := s.db.Put([]byte(key), newData, &opt.WriteOptions{Sync: true}); err != nil {
		storageOperationsTotal.WithLabelValues("decrement_ref_count", "error").Inc()
		return fmt.Errorf("failed to store content metadata for %s: %w", metadata.MerkleRoot.String(), err)
	}

	log.Printf("Decremented ref count for %s to %d", merkleRoot.String(), metadata.RefCount)
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

// ListManifests returns all manifest hashes
func (s *Storage) ListManifests() ([]hasher.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var hashes []hasher.Hash
	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	prefix := []byte(manifestPrefix)
	for iter.Seek(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		key := string(iter.Key())
		hashStr := key[len(manifestPrefix):]
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			log.Printf("Invalid manifest hash in database: %s, error: %v", hashStr, err)
			continue
		}
		hashes = append(hashes, hash)
	}

	if err := iter.Error(); err != nil {
		log.Printf("Iterator error while listing manifests: %v", err)
		storageOperationsTotal.WithLabelValues("list_manifests", "error").Inc()
		return nil, fmt.Errorf("iterator error while listing manifests: %w", err)
	}

	log.Printf("Listed %d manifest hashes", len(hashes))
	storageOperationsTotal.WithLabelValues("list_manifests", "success").Inc()
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

// GarbageCollect removes manifests and chunks with zero reference count
func (s *Storage) GarbageCollect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	manifestHashes, err := s.ListManifests()
	if err != nil {
		log.Printf("Failed to list manifests for garbage collection: %v", err)
		storageOperationsTotal.WithLabelValues("garbage_collect", "error").Inc()
		return fmt.Errorf("failed to list manifests: %w", err)
	}

	for _, hash := range manifestHashes {
		metadata, err := s.GetContent(hash)
		if err != nil {
			log.Printf("Failed to get content metadata for %s: %v", hash.String(), err)
			continue
		}

		if metadata.RefCount == 0 {
			manifestKey := manifestPrefix + hash.String()
			if err := s.db.Delete([]byte(manifestKey), nil); err != nil {
				log.Printf("Failed to delete manifest metadata %s: %v", hash.String(), err)
			}
			dirPath := filepath.Join(s.dataPath, hash.String())
			if err := os.RemoveAll(dirPath); err != nil && !os.IsNotExist(err) {
				log.Printf("Failed to delete manifest directory %s: %v", dirPath, err)
			}
			log.Printf("Garbage collected manifest %s", hash.String())
			storageOperationsTotal.WithLabelValues("garbage_collect_manifest", "success").Inc()
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
			// Chunks are stored under manifest directories, so no direct file deletion
			log.Printf("Garbage collected chunk metadata %s", hash.String())
			storageOperationsTotal.WithLabelValues("garbage_collect_chunk", "success").Inc()
		}
	}

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

	manifestHashes, err := s.ListManifests()
	if err != nil {
		log.Printf("Failed to list manifests for stats: %v", err)
		storageOperationsTotal.WithLabelValues("get_stats", "error").Inc()
		return nil, fmt.Errorf("failed to list manifests: %w", err)
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
		"manifest_count":  len(manifestHashes),
		"chunk_count":     len(chunkHashes),
		"available_space": availableSpace,
	}

	log.Printf("Retrieved storage stats: %+v", stats)
	storageOperationsTotal.WithLabelValues("get_stats", "success").Inc()
	return stats, nil
}
