package merkle

import (
	"encoding/json"
	"fmt"
	"io"
	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"sort"
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
