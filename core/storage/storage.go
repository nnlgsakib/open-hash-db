package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"openhashdb/core/hasher"
	"path/filepath"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// ContentMetadata represents metadata for stored content
// type ContentMetadata struct {
// 	Hash        hasher.Hash `json:"hash"`
// 	Filename    string      `json:"filename"`
// 	MimeType    string      `json:"mime_type"`
// 	Size        int64       `json:"size"`
// 	ModTime     time.Time   `json:"mod_time"`
// 	ChunkCount  int         `json:"chunk_count,omitempty"`
// 	IsDirectory bool        `json:"is_directory"`
// 	CreatedAt   time.Time   `json:"created_at"`
// 	RefCount    int         `json:"ref_count"`
// }

// ContentMetadata represents metadata for stored content
type ContentMetadata struct {
	Hash        hasher.Hash `json:"hash"`         // Random hash used as the primary identifier
	ContentHash hasher.Hash `json:"content_hash"` // Content-based hash for verification
	Filename    string      `json:"filename"`
	MimeType    string      `json:"mime_type"`
	Size        int64       `json:"size"`
	ModTime     time.Time   `json:"mod_time"`
	ChunkCount  int         `json:"chunk_count,omitempty"`
	IsDirectory bool        `json:"is_directory"`
	CreatedAt   time.Time   `json:"created_at"`
	RefCount    int         `json:"ref_count"`
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
}

// NewStorage creates a new storage instance
func NewStorage(dbPath string) (*Storage, error) {
	db, err := leveldb.OpenFile(dbPath, &opt.Options{
		WriteBuffer: 64 * 1024 * 1024, // 64MB write buffer
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &Storage{
		db:       db,
		dataPath: filepath.Join(filepath.Dir(dbPath), "data"),
	}, nil
}

// Close closes the storage
func (s *Storage) Close() error {
	return s.db.Close()
}

// Key prefixes for different data types
const (
	contentPrefix = "content:"
	chunkPrefix   = "chunk:"
	dataPrefix    = "data:"
)

// StoreContent stores content metadata
func (s *Storage) StoreContent(metadata *ContentMetadata) error {
	// Check for duplicates based on ContentHash
	if s.HasContentByHash(metadata.ContentHash) {
		log.Printf("Content with hash %s already exists, skipping store.", metadata.ContentHash.String())
		return nil // Or return a specific error/status
	}

	key := contentPrefix + metadata.Hash.String()

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	return s.db.Put([]byte(key), data, nil)
}

// HasContentByHash checks if content with a specific content hash already exists.
func (s *Storage) HasContentByHash(contentHash hasher.Hash) bool {
	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	prefix := []byte(contentPrefix)
	for iter.Seek(prefix); iter.Valid() && len(iter.Key()) > len(prefix); iter.Next() {
		var metadata ContentMetadata
		if err := json.Unmarshal(iter.Value(), &metadata); err == nil {
			if metadata.ContentHash == contentHash {
				return true
			}
		}
	}
	return false
}

// GetContent retrieves content metadata
func (s *Storage) GetContent(hash hasher.Hash) (*ContentMetadata, error) {
	key := contentPrefix + hash.String()

	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, fmt.Errorf("content not found: %s", hash.String())
		}
		return nil, fmt.Errorf("failed to get content: %w", err)
	}

	var metadata ContentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// StoreChunk stores chunk metadata
func (s *Storage) StoreChunk(metadata *ChunkMetadata) error {
	key := chunkPrefix + metadata.Hash.String()

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk metadata: %w", err)
	}

	return s.db.Put([]byte(key), data, nil)
}

// GetChunk retrieves chunk metadata
func (s *Storage) GetChunk(hash hasher.Hash) (*ChunkMetadata, error) {
	key := chunkPrefix + hash.String()

	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, fmt.Errorf("chunk not found: %s", hash.String())
		}
		return nil, fmt.Errorf("failed to get chunk: %w", err)
	}

	var metadata ChunkMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal chunk metadata: %w", err)
	}

	return &metadata, nil
}

// StoreData stores raw data (chunk content)
func (s *Storage) StoreData(hash hasher.Hash, data []byte) error {
	// Verify content-based hash if metadata is available
	metadata, err := s.GetContent(hash)
	if err == nil && metadata.ContentHash != (hasher.Hash{}) {
		// actualContentHash := hasher.HashBytes(data)
		// log.Printf("Storing data for hash %s: expected ContentHash=%s, actual ContentHash=%s",
		// 	hash.String(), metadata.ContentHash.String(), actualContentHash.String())
		if !hasher.Verify(data, metadata.ContentHash) {
			return fmt.Errorf("content integrity check failed for hash %s", hash.String())
		}
	} else {
		log.Printf("No ContentHash verification for hash %s during storage", hash.String())
	}

	key := dataPrefix + hash.String()
	return s.db.Put([]byte(key), data, nil)
}

// GetData retrieves raw data
func (s *Storage) GetData(hash hasher.Hash) ([]byte, error) {
	key := dataPrefix + hash.String()

	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, fmt.Errorf("data not found: %s", hash.String())
		}
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	// Verify content-based hash if metadata is available
	metadata, err := s.GetContent(hash)
	if err == nil && metadata.ContentHash != (hasher.Hash{}) {
		actualContentHash := hasher.HashBytes(data)
		log.Printf("Retrieving data for hash %s: expected ContentHash=%s, actual ContentHash=%s",
			hash.String(), metadata.ContentHash.String(), actualContentHash.String())
		if !hasher.Verify(data, metadata.ContentHash) {
			return nil, fmt.Errorf("content integrity check failed for hash %s", hash.String())
		}
	} else {
		log.Printf("No ContentHash verification for hash %s during retrieval", hash.String())
	}

	return data, nil
}

// HasContent checks if content exists
func (s *Storage) HasContent(hash hasher.Hash) bool {
	key := contentPrefix + hash.String()
	_, err := s.db.Get([]byte(key), nil)
	return err == nil
}

// HasChunk checks if chunk exists
func (s *Storage) HasChunk(hash hasher.Hash) bool {
	key := chunkPrefix + hash.String()
	_, err := s.db.Get([]byte(key), nil)
	return err == nil
}

// HasData checks if data exists
func (s *Storage) HasData(hash hasher.Hash) bool {
	key := dataPrefix + hash.String()
	_, err := s.db.Get([]byte(key), nil)
	return err == nil
}

// IncrementRefCount increments reference count for content
func (s *Storage) IncrementRefCount(hash hasher.Hash) error {
	metadata, err := s.GetContent(hash)
	if err != nil {
		return err
	}

	metadata.RefCount++
	return s.StoreContent(metadata)
}

// DecrementRefCount decrements reference count for content
func (s *Storage) DecrementRefCount(hash hasher.Hash) error {
	metadata, err := s.GetContent(hash)
	if err != nil {
		return err
	}

	if metadata.RefCount > 0 {
		metadata.RefCount--
	}
	return s.StoreContent(metadata)
}

// IncrementChunkRefCount increments reference count for chunk
func (s *Storage) IncrementChunkRefCount(hash hasher.Hash) error {
	metadata, err := s.GetChunk(hash)
	if err != nil {
		return err
	}

	metadata.RefCount++
	return s.StoreChunk(metadata)
}

// DecrementChunkRefCount decrements reference count for chunk
func (s *Storage) DecrementChunkRefCount(hash hasher.Hash) error {
	metadata, err := s.GetChunk(hash)
	if err != nil {
		return err
	}

	if metadata.RefCount > 0 {
		metadata.RefCount--
	}
	return s.StoreChunk(metadata)
}

// ListContent returns all content hashes
func (s *Storage) ListContent() ([]hasher.Hash, error) {
	var hashes []hasher.Hash

	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	prefix := []byte(contentPrefix)
	for iter.Seek(prefix); iter.Valid() && len(iter.Key()) > len(prefix); iter.Next() {
		key := string(iter.Key())
		if len(key) <= len(contentPrefix) {
			continue
		}

		hashStr := key[len(contentPrefix):]
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			continue // Skip invalid hashes
		}

		hashes = append(hashes, hash)
	}

	return hashes, iter.Error()
}

// ListChunks returns all chunk hashes
func (s *Storage) ListChunks() ([]hasher.Hash, error) {
	var hashes []hasher.Hash

	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	prefix := []byte(chunkPrefix)
	for iter.Seek(prefix); iter.Valid() && len(iter.Key()) > len(prefix); iter.Next() {
		key := string(iter.Key())
		if len(key) <= len(chunkPrefix) {
			continue
		}

		hashStr := key[len(chunkPrefix):]
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			continue // Skip invalid hashes
		}

		hashes = append(hashes, hash)
	}

	return hashes, iter.Error()
}

// GarbageCollect removes content and chunks with zero reference count
func (s *Storage) GarbageCollect() error {
	// Collect content with zero ref count
	contentHashes, err := s.ListContent()
	if err != nil {
		return fmt.Errorf("failed to list content: %w", err)
	}

	for _, hash := range contentHashes {
		metadata, err := s.GetContent(hash)
		if err != nil {
			continue
		}

		if metadata.RefCount == 0 {
			// Delete content metadata
			contentKey := contentPrefix + hash.String()
			s.db.Delete([]byte(contentKey), nil)

			// Delete associated data
			dataKey := dataPrefix + hash.String()
			s.db.Delete([]byte(dataKey), nil)
		}
	}

	// Collect chunks with zero ref count
	chunkHashes, err := s.ListChunks()
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	for _, hash := range chunkHashes {
		metadata, err := s.GetChunk(hash)
		if err != nil {
			continue
		}

		if metadata.RefCount == 0 {
			// Delete chunk metadata
			chunkKey := chunkPrefix + hash.String()
			s.db.Delete([]byte(chunkKey), nil)

			// Delete associated data
			dataKey := dataPrefix + hash.String()
			s.db.Delete([]byte(dataKey), nil)
		}
	}

	return nil
}

// GetStats returns storage statistics
func (s *Storage) GetStats() (map[string]interface{}, error) {
	contentHashes, err := s.ListContent()
	if err != nil {
		return nil, err
	}

	chunkHashes, err := s.ListChunks()
	if err != nil {
		return nil, err
	}

	stats := map[string]interface{}{
		"content_count": len(contentHashes),
		"chunk_count":   len(chunkHashes),
	}

	return stats, nil
}
