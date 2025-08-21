package chunker

import (
	"bytes"
	"fmt"
	"io"

	"openhashdb/core/hasher"

	"github.com/jotfs/fastcdc-go"
)

const (
	MinChunkSize = 64 * 1024
	AvgChunkSize = 256 * 1024
	MaxChunkSize = 1 * 1024 * 1024
)

// Chunk represents a single chunk of data
type Chunk struct {
	Hash hasher.Hash
	Data []byte
	Size int
}

// ChunkInfo represents metadata about a chunk without the actual data
type ChunkInfo struct {
	Hash hasher.Hash
	Size int
}

// ChunkedFile represents a file split into chunks
type ChunkedFile struct {
	Chunks    []ChunkInfo
	TotalSize int64
	RootHash  hasher.Hash
}

// Chunker handles file chunking operations
type Chunker struct{}

// NewChunker creates a new chunker
func NewChunker() *Chunker {
	return &Chunker{}
}

// ChunkBytes splits byte slice into chunks using FastCDC.
func (c *Chunker) ChunkBytes(data []byte) ([]Chunk, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot chunk empty data")
	}
	return c.ChunkReader(bytes.NewReader(data))
}

// ChunkReader splits data from a reader into chunks using FastCDC.
func (c *Chunker) ChunkReader(r io.Reader) ([]Chunk, error) {
	var chunks []Chunk

	opts := fastcdc.Options{
		MinSize:     MinChunkSize,
		AverageSize: AvgChunkSize,
		MaxSize:     MaxChunkSize,
	}

	chunker, err := fastcdc.NewChunker(r, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create fastcdc chunker: %w", err)
	}

	for {
		cdcChunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read next chunk: %w", err)
		}

		// The data from fastcdc.Chunk is only valid until the next call to Next().
		// We must copy it.
		chunkData := make([]byte, cdcChunk.Length)
		copy(chunkData, cdcChunk.Data)

		chunk := Chunk{
			Hash: hasher.HashBytes(chunkData),
			Data: chunkData,
			Size: cdcChunk.Length,
		}

		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// CreateChunkedFile creates a ChunkedFile from chunks
func (c *Chunker) CreateChunkedFile(chunks []Chunk) *ChunkedFile {
	var chunkInfos []ChunkInfo
	var totalSize int64
	var hashes []hasher.Hash

	for _, chunk := range chunks {
		chunkInfos = append(chunkInfos, ChunkInfo{
			Hash: chunk.Hash,
			Size: chunk.Size,
		})
		totalSize += int64(chunk.Size)
		hashes = append(hashes, chunk.Hash)
	}

	// Create Merkle tree root hash
	rootHash := c.computeMerkleRoot(hashes)

	return &ChunkedFile{
		Chunks:    chunkInfos,
		TotalSize: totalSize,
		RootHash:  rootHash,
	}
}

// computeMerkleRoot computes the Merkle tree root hash from chunk hashes
func (c *Chunker) computeMerkleRoot(hashes []hasher.Hash) hasher.Hash {
	if len(hashes) == 0 {
		return hasher.Hash{}
	}
	if len(hashes) == 1 {
		return hashes[0]
	}

	// Build Merkle tree bottom-up
	currentLevel := make([]hasher.Hash, len(hashes))
	copy(currentLevel, hashes)

	for len(currentLevel) > 1 {
		var nextLevel []hasher.Hash

		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				// Pair exists, hash both
				combined := hasher.HashMultiple(currentLevel[i], currentLevel[i+1])
				nextLevel = append(nextLevel, combined)
			} else {
				// Odd number, promote single hash
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}

		currentLevel = nextLevel
	}

	return currentLevel[0]
}

// ReassembleChunks reconstructs original data from chunks
func ReassembleChunks(chunks []Chunk) ([]byte, error) {
	var buffer bytes.Buffer

	for i, chunk := range chunks {
		if chunk.Size != len(chunk.Data) {
			return nil, fmt.Errorf("chunk %d size mismatch: expected %d, got %d",
				i, chunk.Size, len(chunk.Data))
		}

		// Verify chunk integrity
		if !hasher.Verify(chunk.Data, chunk.Hash) {
			return nil, fmt.Errorf("chunk %d failed integrity check", i)
		}

		buffer.Write(chunk.Data)
	}

	return buffer.Bytes(), nil
}

// VerifyChunkedFile verifies the integrity of a chunked file
func VerifyChunkedFile(chunkedFile *ChunkedFile, chunks []Chunk) error {
	if len(chunks) != len(chunkedFile.Chunks) {
		return fmt.Errorf("chunk count mismatch: expected %d, got %d",
			len(chunkedFile.Chunks), len(chunks))
	}

	var totalSize int64
	var hashes []hasher.Hash

	for i, chunk := range chunks {
		expectedInfo := chunkedFile.Chunks[i]

		if chunk.Hash != expectedInfo.Hash {
			return fmt.Errorf("chunk %d hash mismatch", i)
		}

		if chunk.Size != expectedInfo.Size {
			return fmt.Errorf("chunk %d size mismatch", i)
		}

		if !hasher.Verify(chunk.Data, chunk.Hash) {
			return fmt.Errorf("chunk %d failed integrity check", i)
		}

		totalSize += int64(chunk.Size)
		hashes = append(hashes, chunk.Hash)
	}

	if totalSize != chunkedFile.TotalSize {
		return fmt.Errorf("total size mismatch: expected %d, got %d",
			chunkedFile.TotalSize, totalSize)
	}

	// Verify Merkle root
	chunker := NewChunker() // Size doesn't matter for verification
	rootHash := chunker.computeMerkleRoot(hashes)
	if rootHash != chunkedFile.RootHash {
		return fmt.Errorf("Merkle root hash mismatch")
	}

	return nil
}
