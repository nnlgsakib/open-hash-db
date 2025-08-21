package merkle

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"openhashdb/core/block"
	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"openhashdb/core/sharder"
)

// Link represents a link to another Merkle tree (a file or a directory).
type Link struct {
	Name string      `json:"name"`
	Hash hasher.Hash `json:"hash"` // Merkle Root of the linked content
	Size int64       `json:"size"`
	Type string      `json:"type"` // "file" or "directory"
}

// File represents a file as a Merkle tree of its chunks.
type File struct {
	Root      hasher.Hash         `json:"root"`
	Chunks    []chunker.ChunkInfo `json:"chunks"`
	TotalSize int64               `json:"total_size"`
}

// Directory represents a directory as a list of links to its contents.
// The Merkle root of a directory is the hash of the serialized, sorted list of links.
type Directory struct {
	Links []Link `json:"links"`
}

// BuildFileTree chunks a file and builds a Merkle tree representation.
// It returns the created File object and the individual chunks.
func BuildFileTree(r io.Reader, c *chunker.Chunker) (*File, []chunker.Chunk, error) {
	chunks, err := c.ChunkReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to chunk reader: %w", err)
	}
	if len(chunks) == 0 {
		// Handle empty file
		emptyHash := hasher.HashBytes([]byte{})
		file := &File{
			Root:      emptyHash,
			Chunks:    []chunker.ChunkInfo{},
			TotalSize: 0,
		}
		return file, []chunker.Chunk{}, nil
	}

	chunkedFile := c.CreateChunkedFile(chunks)

	file := &File{
		Root:      chunkedFile.RootHash,
		Chunks:    chunkedFile.Chunks,
		TotalSize: chunkedFile.TotalSize,
	}

	return file, chunks, nil
}

// BuildErasureCodedFileTree erasure codes a file and builds a Merkle tree representation.
// It returns the created File object and the individual shards as block.Block objects.
func BuildErasureCodedFileTree(r io.Reader, s sharder.ErasureCoder) (*File, []block.Block, error) {
	data, err := sharder.ReadAll(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read data for sharding: %w", err)
	}

	if len(data) == 0 {
		// Handle empty file
		emptyHash := hasher.HashBytes([]byte{})
		file := &File{
			Root:      emptyHash,
			Chunks:    []chunker.ChunkInfo{},
			TotalSize: 0,
		}
		return file, []block.Block{}, nil
	}

	shards, err := s.Encode(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to erasure code data: %w", err)
	}

	shardBlocks := make([]block.Block, s.ShardCount())
	shardInfos := make([]chunker.ChunkInfo, s.ShardCount())
	shardHashes := make([]hasher.Hash, s.ShardCount())

	for i, shardData := range shards {
		shardBlock := block.NewBlock(shardData)
		shardBlocks[i] = shardBlock
		shardHashes[i] = shardBlock.Hash()
		shardInfos[i] = chunker.ChunkInfo{
			Hash: shardBlock.Hash(),
			Size: len(shardData),
		}
	}

	// The root of an erasure-coded file is the Merkle root of its shard hashes.
	rootHash := hasher.HashMultiple(shardHashes...)

	file := &File{
		Root:      rootHash,
		Chunks:    shardInfos, // Chunks field now holds shard info
		TotalSize: int64(len(data)),
	}

	return file, shardBlocks, nil
}

// BuildDirectoryTree creates a hash for a directory from its links.
func BuildDirectoryTree(links []Link) (hasher.Hash, error) {
	// Sort links by name for deterministic hashing
	sort.Slice(links, func(i, j int) bool {
		return links[i].Name < links[j].Name
	})

	dir := Directory{Links: links}
	data, err := json.Marshal(dir)
	if err != nil {
		return hasher.Hash{}, fmt.Errorf("failed to marshal directory links: %w", err)
	}

	return hasher.HashBytes(data), nil
}