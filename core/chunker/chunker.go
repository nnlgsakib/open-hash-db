package chunker

import (
	"bytes"
	"fmt"
	"io"
	"openhashdb/core/hasher"
)

// ChunkSize represents different chunk size configurations
type ChunkSize int

const (
	ChunkSize256KB ChunkSize = 256 * 1024  // 256KB for general content
	ChunkSize1MB   ChunkSize = 1024 * 1024 // 1MB for large media files
)

// Chunk represents a single chunk of data
type Chunk struct {
	Hash hasher.Hash `json:"hash"`
	Data []byte      `json:"data"`
	Size int         `json:"size"`
}

// ChunkInfo represents metadata about a chunk without the actual data
type ChunkInfo struct {
	Hash hasher.Hash `json:"hash"`
	Size int         `json:"size"`
}

// ChunkedFile represents a file split into chunks
type ChunkedFile struct {
	Chunks    []ChunkInfo   `json:"chunks"`
	TotalSize int64         `json:"total_size"`
	RootHash  hasher.Hash   `json:"root_hash"`
}

// Chunker handles file chunking operations
type Chunker struct {
	chunkSize int
}

// NewChunker creates a new chunker with specified chunk size
func NewChunker(size ChunkSize) *Chunker {
	return &Chunker{
		chunkSize: int(size),
	}
}

// ChunkBytes splits byte slice into chunks
func (c *Chunker) ChunkBytes(data []byte) ([]Chunk, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot chunk empty data")
	}

	var chunks []Chunk
	offset := 0
	
	for offset < len(data) {
		end := offset + c.chunkSize
		if end > len(data) {
			end = len(data)
		}
		
		chunkData := data[offset:end]
		chunk := Chunk{
			Hash: hasher.HashBytes(chunkData),
			Data: make([]byte, len(chunkData)),
			Size: len(chunkData),
		}
		copy(chunk.Data, chunkData)
		
		chunks = append(chunks, chunk)
		offset = end
	}
	
	return chunks, nil
}

// ChunkReader splits data from reader into chunks
func (c *Chunker) ChunkReader(r io.Reader) ([]Chunk, error) {
	var chunks []Chunk
	buffer := make([]byte, c.chunkSize)
	
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
			Hash: hasher.HashBytes(chunkData),
			Data: chunkData,
			Size: n,
		}
		
		chunks = append(chunks, chunk)
		
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}
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
	chunker := NewChunker(ChunkSize256KB) // Size doesn't matter for verification
	rootHash := chunker.computeMerkleRoot(hashes)
	if rootHash != chunkedFile.RootHash {
		return fmt.Errorf("Merkle root hash mismatch")
	}
	
	return nil
}

