package tree

import (
	"fmt"
	"io"
	"sort"

	"openhashdb/core/block"
	"openhashdb/core/chunker"
	"openhashdb/core/sharder"
	"openhashdb/core/tmt"
	"openhashdb/protobuf/pb"

	"google.golang.org/protobuf/proto"
)

// Link represents a link to another Merkle (TMT) tree (a file or a directory).
type Link struct {
	Name string
	Hash tmt.Hash // Root of linked content
	Size int64
	Type string // "file" or "directory"
}

// File represents a file as a TMT tree of its chunks.
type File struct {
	Root      tmt.Hash
	Chunks    []chunker.ChunkInfo
	TotalSize int64
	tree      *tmt.TernaryMeshTree
}

// Directory represents a directory as a list of links to its contents.
type Directory struct {
	Links []Link
}

// ------------------ File Trees ------------------

// BuildFileTree chunks a file and builds a TMT-based tree representation.
func BuildFileTree(r io.Reader, c *chunker.Chunker) (*File, []chunker.Chunk, error) {
	chunks, err := c.ChunkReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to chunk reader: %w", err)
	}
	if len(chunks) == 0 {
		// Handle empty file
		emptyHash := tmt.ComputeHash(nil)
		file := &File{Root: emptyHash, Chunks: []chunker.ChunkInfo{}, TotalSize: 0}
		return file, []chunker.Chunk{}, nil
	}

	chunkedFile := c.CreateChunkedFile(chunks)

	// Build TMT root from chunk hashes
	dataBlocks := make([][]byte, len(chunkedFile.Chunks))
	for i, info := range chunkedFile.Chunks {
		dataBlocks[i] = info.Hash[:] // use raw hash bytes as leaves
	}
	tree := tmt.NewDefault()
	if err := tree.Build(dataBlocks); err != nil {
		return nil, nil, fmt.Errorf("tmt build error: %w", err)
	}
	root, _ := tree.RootHash()

	file := &File{
		Root:      root,
		Chunks:    chunkedFile.Chunks,
		TotalSize: chunkedFile.TotalSize,
		tree:      tree,
	}
	return file, chunks, nil
}

// BuildErasureCodedFileTree erasure codes a file and builds a TMT tree representation.
func BuildErasureCodedFileTree(r io.Reader, s sharder.ErasureCoder) (*File, []block.Block, error) {
	data, err := sharder.ReadAll(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read data for sharding: %w", err)
	}
	if len(data) == 0 {
		emptyHash := tmt.ComputeHash(nil)
		file := &File{Root: emptyHash, Chunks: []chunker.ChunkInfo{}, TotalSize: 0}
		return file, []block.Block{}, nil
	}

	shards, err := s.Encode(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to erasure code data: %w", err)
	}

	shardBlocks := make([]block.Block, s.ShardCount())
	shardInfos := make([]chunker.ChunkInfo, s.ShardCount())
	leafHashes := make([][]byte, s.ShardCount())

	for i, shardData := range shards {
		shardBlock := block.NewBlock(shardData)
		shardBlocks[i] = shardBlock
		h := shardBlock.Hash()
		shardInfos[i] = chunker.ChunkInfo{Hash: h, Size: len(shardData)}
		leafHashes[i] = h[:]
	}

	tree := tmt.NewDefault()
	if err := tree.Build(leafHashes); err != nil {
		return nil, nil, fmt.Errorf("tmt build error: %w", err)
	}
	root, _ := tree.RootHash()

	file := &File{
		Root:      root,
		Chunks:    shardInfos,
		TotalSize: int64(len(data)),
		tree:      tree,
	}
	return file, shardBlocks, nil
}

// ------------------ Directory Trees ------------------

// BuildDirectoryTree creates a root hash for a directory from its links.
func BuildDirectoryTree(links []Link) (tmt.Hash, error) {
	// Sort links by name for deterministic order
	sort.Slice(links, func(i, j int) bool { return links[i].Name < links[j].Name })

	pbLinks := make([]*pb.Link, len(links))
	for i, link := range links {
		pbLinks[i] = &pb.Link{
			Name: link.Name,
			Hash: link.Hash[:],
			Size: link.Size,
			Type: link.Type,
		}
	}

	dir := &pb.Directory{Links: pbLinks}
	data, err := proto.Marshal(dir)
	if err != nil {
		return tmt.Hash{}, fmt.Errorf("failed to marshal directory links: %w", err)
	}

	// Root = TMT over the serialized directory blob
	tree := tmt.NewDefault()
	if err := tree.Build([][]byte{data}); err != nil {
		return tmt.Hash{}, err
	}
	root, _ := tree.RootHash()
	return root, nil
}

// ------------------ Verification ------------------

// GenerateProof generates a verification proof for a leaf in a file tree.
func (f *File) GenerateProof(leafIndex int) (tmt.VerificationProof, error) {
	if f.tree == nil {
		return tmt.VerificationProof{}, fmt.Errorf("tree is not available")
	}
	return f.tree.GenerateProof(leafIndex)
}

// VerifyProof verifies a proof for a given leaf data.
func VerifyProof(proof tmt.VerificationProof, leafData []byte, rootHash tmt.Hash) (bool, error) {
	return tmt.VerifyProofWithRoot(proof, leafData, rootHash)
}
