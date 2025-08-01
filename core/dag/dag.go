package dag

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"mime"
	"openhashdb/core/hasher"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// NodeType represents the type of DAG node
type NodeType string

const (
	NodeTypeFile      NodeType = "file"
	NodeTypeDirectory NodeType = "directory"
)

// Link represents a link to another node in the DAG
type Link struct {
	Name string      `json:"name"`
	Hash hasher.Hash `json:"hash"`
	Size int64       `json:"size"`
	Type NodeType    `json:"type"`
}

// DAGNode represents a node in the Merkle DAG
type DAGNode struct {
	Type      NodeType    `json:"type"`
	Links     []Link      `json:"links,omitempty"`
	Data      []byte      `json:"data,omitempty"`
	Hash      hasher.Hash `json:"hash"`
	Size      int64       `json:"size"`
	CreatedAt time.Time   `json:"created_at"`
}

// FileInfo represents file metadata
type FileInfo struct {
	Name     string      `json:"name"`
	Size     int64       `json:"size"`
	Mode     fs.FileMode `json:"mode"`
	ModTime  time.Time   `json:"mod_time"`
	MimeType string      `json:"mime_type"`
}

// DAGBuilder builds Merkle DAGs from file system structures
type DAGBuilder struct {
	// Optional callback for progress reporting
	ProgressCallback func(path string, processed, total int)
}

// NewDAGBuilder creates a new DAG builder
func NewDAGBuilder() *DAGBuilder {
	return &DAGBuilder{}
}

// BuildFromPath builds a DAG from a file system path
func (b *DAGBuilder) BuildFromPath(rootPath string) (*DAGNode, error) {
	return b.buildNode(rootPath, "")
}

// buildNode recursively builds DAG nodes
func (b *DAGBuilder) buildNode(path, name string) (*DAGNode, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat %s: %w", path, err)
	}

	if info.IsDir() {
		return b.buildDirectoryNode(path, name, info)
	} else {
		return b.buildFileNode(path, name, info)
	}
}

// buildFileNode builds a DAG node for a file
func (b *DAGBuilder) buildFileNode(path, name string, info fs.FileInfo) (*DAGNode, error) {
	// Read file content
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	// Determine MIME type
	mimeType := mime.TypeByExtension(filepath.Ext(path))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// Create file metadata
	fileInfo := FileInfo{
		Name:     name,
		Size:     info.Size(),
		Mode:     info.Mode(),
		ModTime:  info.ModTime(),
		MimeType: mimeType,
	}

	// Serialize file metadata
	metadataBytes, err := json.Marshal(fileInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal file metadata: %w", err)
	}

	// Create DAG node
	node := &DAGNode{
		Type:      NodeTypeFile,
		Data:      content,
		Size:      info.Size(),
		CreatedAt: time.Now(),
	}

	// Compute hash of metadata + content
	combinedData := append(metadataBytes, content...)
	node.Hash = hasher.HashBytes(combinedData)

	return node, nil
}

// buildDirectoryNode builds a DAG node for a directory
func (b *DAGBuilder) buildDirectoryNode(path, name string, info fs.FileInfo) (*DAGNode, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", path, err)
	}

	var links []Link
	var totalSize int64
	var childHashes []hasher.Hash

	// Process each entry in the directory
	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())

		// Skip hidden files and directories starting with .
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		// Recursively build child node
		childNode, err := b.buildNode(entryPath, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to build child node %s: %w", entryPath, err)
		}

		// Create link to child
		link := Link{
			Name: entry.Name(),
			Hash: childNode.Hash,
			Size: childNode.Size,
			Type: childNode.Type,
		}

		links = append(links, link)
		totalSize += childNode.Size
		childHashes = append(childHashes, childNode.Hash)

		// Report progress if callback is set
		if b.ProgressCallback != nil {
			b.ProgressCallback(entryPath, len(links), len(entries))
		}
	}

	// Create directory metadata
	dirInfo := FileInfo{
		Name:    name,
		Size:    totalSize,
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
	}

	// Serialize directory metadata
	metadataBytes, err := json.Marshal(dirInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal directory metadata: %w", err)
	}

	// Create DAG node
	node := &DAGNode{
		Type:      NodeTypeDirectory,
		Links:     links,
		Size:      totalSize,
		CreatedAt: time.Now(),
	}

	// Compute hash of metadata + child hashes
	hashData := append(metadataBytes, []byte("links:")...)
	for _, hash := range childHashes {
		hashData = append(hashData, hash[:]...)
	}
	node.Hash = hasher.HashBytes(hashData)

	return node, nil
}

// Serialize serializes a DAG node to JSON
func (node *DAGNode) Serialize() ([]byte, error) {
	return json.Marshal(node)
}

// Deserialize deserializes a DAG node from JSON
func Deserialize(data []byte) (*DAGNode, error) {
	var node DAGNode
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("failed to deserialize DAG node: %w", err)
	}
	return &node, nil
}

// FindLink finds a link by name in the DAG node
func (node *DAGNode) FindLink(name string) (*Link, error) {
	if node.Type != NodeTypeDirectory {
		return nil, fmt.Errorf("cannot find link in non-directory node")
	}

	for _, link := range node.Links {
		if link.Name == name {
			return &link, nil
		}
	}

	return nil, fmt.Errorf("link not found: %s", name)
}

// ListLinks returns all links in the DAG node
func (node *DAGNode) ListLinks() []Link {
	if node.Type != NodeTypeDirectory {
		return nil
	}
	return node.Links
}

// GetPath resolves a path within the DAG structure
func (node *DAGNode) GetPath(path string) (*Link, error) {
	if path == "" || path == "/" {
		// Return self as a link
		return &Link{
			Name: "",
			Hash: node.Hash,
			Size: node.Size,
			Type: node.Type,
		}, nil
	}

	if node.Type != NodeTypeDirectory {
		return nil, fmt.Errorf("cannot traverse path in non-directory node")
	}

	// Split path into components
	components := strings.Split(strings.Trim(path, "/"), "/")
	if len(components) == 0 {
		return nil, fmt.Errorf("invalid path: %s", path)
	}

	// Find the first component
	link, err := node.FindLink(components[0])
	if err != nil {
		return nil, err
	}

	// If this is the final component, return the link
	if len(components) == 1 {
		return link, nil
	}

	// For deeper paths, we would need to load the child node and continue
	// This would require access to storage, so we return an error for now
	return nil, fmt.Errorf("deep path resolution requires storage access")
}

// Verify verifies the integrity of a DAG node
func (node *DAGNode) Verify() error {
	// For file nodes, verify that hash matches content
	if node.Type == NodeTypeFile {
		expectedHash := hasher.HashBytes(node.Data)
		if expectedHash != node.Hash {
			return fmt.Errorf("file node hash mismatch")
		}
		return nil
	}

	// For directory nodes, we can't fully verify without child nodes
	// But we can check basic consistency
	if node.Type == NodeTypeDirectory {
		if node.Data != nil && len(node.Data) > 0 {
			return fmt.Errorf("directory node should not have data")
		}

		var totalSize int64
		for _, link := range node.Links {
			totalSize += link.Size
		}

		if totalSize != node.Size {
			return fmt.Errorf("directory size mismatch: expected %d, got %d",
				node.Size, totalSize)
		}
	}

	return nil
}

// Clone creates a deep copy of the DAG node
func (node *DAGNode) Clone() *DAGNode {
	clone := &DAGNode{
		Type:      node.Type,
		Hash:      node.Hash,
		Size:      node.Size,
		CreatedAt: node.CreatedAt,
	}

	if node.Data != nil {
		clone.Data = make([]byte, len(node.Data))
		copy(clone.Data, node.Data)
	}

	if node.Links != nil {
		clone.Links = make([]Link, len(node.Links))
		copy(clone.Links, node.Links)
	}

	return clone
}
