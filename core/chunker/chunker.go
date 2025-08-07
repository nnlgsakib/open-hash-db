package chunker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"openhashdb/core/hasher"
	"openhashdb/core/merkle"
)

// ChunkSize represents different chunk size configurations
type ChunkSize int

const (
	ChunkSize64KB  ChunkSize = 64 * 1024   // 64KB for small files
	ChunkSize256KB ChunkSize = 256 * 1024  // 256KB for medium files
	ChunkSize1MB   ChunkSize = 1024 * 1024 // 1MB for large files
)

// Chunk represents a single chunk of data
type Chunk struct {
	Hash     hasher.Hash `json:"hash"`
	Data     []byte      `json:"data"`
	Size     int         `json:"size"`
	Sequence int         `json:"sequence"` // Added for ordering
}

// ChunkInfo represents metadata about a chunk without the actual data
type ChunkInfo struct {
	Hash     hasher.Hash `json:"hash"`
	Size     int         `json:"size"`
	Sequence int         `json:"sequence"`
}

// Manifest represents the file's metadata and Merkle Tree
type Manifest struct {
	MerkleRoot hasher.Hash `json:"merkle_root"`
	Chunks     []ChunkInfo `json:"chunks"`
	TotalSize  int64       `json:"total_size"`
}

// Chunker handles file chunking operations
type Chunker struct {
	chunkSize int
	dataPath  string // Base path for storing chunks
}

// NewChunker creates a new chunker with dynamic chunk size based on data size
func NewChunker(dataSize int64, dataPath string) *Chunker {
	var chunkSize ChunkSize
	switch {
	case dataSize < 1*1024*1024: // <1MB
		chunkSize = ChunkSize64KB
	case dataSize < 100*1024*1024: // 1MB–100MB
		chunkSize = ChunkSize256KB
	default: // >100MB
		chunkSize = ChunkSize1MB
	}
	return &Chunker{
		chunkSize: int(chunkSize),
		dataPath:  dataPath,
	}
}

// ChunkBytes splits byte slice into chunks and stores them
func (c *Chunker) ChunkBytes(data []byte) (*Manifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot chunk empty data")
	}

	var chunks []Chunk
	offset := 0
	sequence := 0

	for offset < len(data) {
		end := offset + c.chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunkData := data[offset:end]
		chunk := Chunk{
			Hash:     hasher.HashBytes(chunkData),
			Data:     make([]byte, len(chunkData)),
			Size:     len(chunkData),
			Sequence: sequence,
		}
		copy(chunk.Data, chunkData)
		chunks = append(chunks, chunk)
		offset = end
		sequence++
	}

	return c.createManifest(chunks)
}

// ChunkReader splits data from reader into chunks and stores them
func (c *Chunker) ChunkReader(r io.Reader) (*Manifest, error) {
	var chunks []Chunk
	buffer := make([]byte, c.chunkSize)
	sequence := 0

	for {
		n, err := r.Read(buffer)
		if n == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read chunk: %w", err)
			}
		}

		chunkData := make([]byte, n)
		copy(chunkData, buffer[:n])

		chunk := Chunk{
			Hash:     hasher.HashBytes(chunkData),
			Data:     chunkData,
			Size:     n,
			Sequence: sequence,
		}
		chunks = append(chunks, chunk)
		sequence++

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}
	}

	return c.createManifest(chunks)
}

// createManifest creates a manifest and stores chunks
func (c *Chunker) createManifest(chunks []Chunk) (*Manifest, error) {
	var chunkInfos []ChunkInfo
	var totalSize int64
	var chunkHashes []hasher.Hash

	for _, chunk := range chunks {
		chunkInfos = append(chunkInfos, ChunkInfo{
			Hash:     chunk.Hash,
			Size:     chunk.Size,
			Sequence: chunk.Sequence,
		})
		totalSize += int64(chunk.Size)
		chunkHashes = append(chunkHashes, chunk.Hash)
	}

	// Create Merkle Tree
	tree, err := merkle.NewMerkleTree(chunkHashes)
	if err != nil {
		return nil, fmt.Errorf("failed to create Merkle Tree: %w", err)
	}

	// Create manifest
	manifest := &Manifest{
		MerkleRoot: tree.RootHash,
		Chunks:     chunkInfos,
		TotalSize:  totalSize,
	}

	// Create directory for storage
	dirPath := filepath.Join(c.dataPath, manifest.MerkleRoot.String())
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	// Store manifest
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	manifestPath := filepath.Join(dirPath, manifest.MerkleRoot.String()+".json")
	if err := os.WriteFile(manifestPath, manifestBytes, 0644); err != nil {
		return nil, fmt.Errorf("failed to store manifest %s: %w", manifestPath, err)
	}

	// Store chunks
	for _, chunk := range chunks {
		chunkPath := filepath.Join(dirPath, chunk.Hash.String())
		if err := os.WriteFile(chunkPath, chunk.Data, 0644); err != nil {
			return nil, fmt.Errorf("failed to store chunk %s: %w", chunk.Hash.String(), err)
		}
	}

	return manifest, nil
}

// ReassembleChunks reconstructs original data from chunks
func ReassembleChunks(chunks []Chunk) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks provided for reassembly")
	}

	// Sort chunks by sequence
	sortedChunks := make([]Chunk, len(chunks))
	sequenceMap := make(map[int]Chunk)
	for _, chunk := range chunks {
		if _, exists := sequenceMap[chunk.Sequence]; exists {
			return nil, fmt.Errorf("duplicate sequence number %d", chunk.Sequence)
		}
		sequenceMap[chunk.Sequence] = chunk
	}

	// Verify sequence numbers are contiguous
	for i := 0; i < len(chunks); i++ {
		chunk, exists := sequenceMap[i]
		if !exists {
			return nil, fmt.Errorf("missing chunk with sequence %d", i)
		}
		sortedChunks[i] = chunk
	}

	var buffer bytes.Buffer
	for _, chunk := range sortedChunks {
		if chunk.Size != len(chunk.Data) {
			return nil, fmt.Errorf("chunk at sequence %d size mismatch: expected %d, got %d", chunk.Sequence, chunk.Size, len(chunk.Data))
		}
		if !hasher.Verify(chunk.Data, chunk.Hash) {
			return nil, fmt.Errorf("chunk at sequence %d failed integrity check", chunk.Sequence)
		}
		buffer.Write(chunk.Data)
	}

	return buffer.Bytes(), nil
}

// VerifyManifest verifies the integrity of a manifest and its chunks
func VerifyManifest(manifest *Manifest, chunks []Chunk) error {
	if len(chunks) != len(manifest.Chunks) {
		return fmt.Errorf("chunk count mismatch: expected %d, got %d", len(manifest.Chunks), len(chunks))
	}

	var totalSize int64
	var chunkHashes []hasher.Hash

	// Sort chunks by sequence
	sortedChunks := make([]Chunk, len(chunks))
	sequenceMap := make(map[int]Chunk)
	for _, chunk := range chunks {
		if _, exists := sequenceMap[chunk.Sequence]; exists {
			return fmt.Errorf("duplicate sequence number %d", chunk.Sequence)
		}
		sequenceMap[chunk.Sequence] = chunk
	}

	// Verify sequence numbers are contiguous and match manifest
	for i := 0; i < len(manifest.Chunks); i++ {
		chunk, exists := sequenceMap[i]
		if !exists {
			return fmt.Errorf("missing chunk with sequence %d", i)
		}
		sortedChunks[i] = chunk
	}

	for _, chunk := range sortedChunks {
		found := false
		for _, expectedInfo := range manifest.Chunks {
			if expectedInfo.Sequence == chunk.Sequence {
				if !chunk.Hash.Equals(expectedInfo.Hash) {
					return fmt.Errorf("chunk at sequence %d hash mismatch", chunk.Sequence)
				}
				if chunk.Size != expectedInfo.Size {
					return fmt.Errorf("chunk at sequence %d size mismatch", chunk.Sequence)
				}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("chunk at sequence %d not found in manifest", chunk.Sequence)
		}
		if !hasher.Verify(chunk.Data, chunk.Hash) {
			return fmt.Errorf("chunk at sequence %d failed integrity check", chunk.Sequence)
		}
		totalSize += int64(chunk.Size)
		chunkHashes = append(chunkHashes, chunk.Hash)
	}

	if totalSize != manifest.TotalSize {
		return fmt.Errorf("total size mismatch: expected %d, got %d", manifest.TotalSize, totalSize)
	}

	// Verify Merkle root
	tree, err := merkle.NewMerkleTree(chunkHashes)
	if err != nil {
		return fmt.Errorf("failed to create Merkle Tree for verification: %w", err)
	}
	if !tree.RootHash.Equals(manifest.MerkleRoot) {
		return fmt.Errorf("Merkle root hash mismatch")
	}

	return nil
}
