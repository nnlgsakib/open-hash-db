package merkle

import (
	"encoding/json"
	"fmt"
	"openhashdb/core/hasher"
)

// MerkleNode represents a node in the Merkle Tree
type MerkleNode struct {
	Hash   hasher.Hash `json:"hash"`
	Left   *MerkleNode `json:"left,omitempty"`
	Right  *MerkleNode `json:"right,omitempty"`
	IsLeaf bool        `json:"is_leaf"`
}

// MerkleTree represents the entire Merkle Tree
type MerkleTree struct {
	RootHash hasher.Hash   `json:"root_hash"`
	Nodes    []MerkleNode  `json:"nodes"`
	Leaves   []hasher.Hash `json:"leaves"`
}

// NewMerkleTree creates a Merkle Tree from chunk hashes
func NewMerkleTree(chunkHashes []hasher.Hash) (*MerkleTree, error) {
	if len(chunkHashes) == 0 {
		return nil, fmt.Errorf("cannot create Merkle Tree with no chunk hashes")
	}

	// Create leaf nodes
	var nodes []MerkleNode
	for _, hash := range chunkHashes {
		nodes = append(nodes, MerkleNode{
			Hash:   hash,
			IsLeaf: true,
		})
	}

	// Pad with duplicate last hash if odd number of leaves
	if len(nodes)%2 != 0 {
		nodes = append(nodes, nodes[len(nodes)-1])
	}

	// Build tree bottom-up
	for len(nodes) > 1 {
		var nextLevel []MerkleNode
		for i := 0; i < len(nodes); i += 2 {
			left := nodes[i]
			right := nodes[i+1]
			parentHash := hasher.HashMultiple(left.Hash, right.Hash)
			parent := MerkleNode{
				Hash:  parentHash,
				Left:  &left,
				Right: &right,
			}
			nextLevel = append(nextLevel, parent)
		}
		nodes = nextLevel
	}

	tree := &MerkleTree{
		RootHash: nodes[0].Hash,
		Nodes:    nodes,
		Leaves:   chunkHashes,
	}

	return tree, nil
}

// GetProof generates a Merkle proof for a chunk hash
func (t *MerkleTree) GetProof(chunkHash hasher.Hash) ([]hasher.Hash, error) {
	leafIndex := -1
	for i, leaf := range t.Leaves {
		if leaf == chunkHash {
			leafIndex = i
			break
		}
	}
	if leafIndex == -1 {
		return nil, fmt.Errorf("chunk hash %s not found in Merkle Tree", chunkHash.String())
	}

	var proof []hasher.Hash
	currentNodes := t.Nodes
	currentIndex := leafIndex

	for len(currentNodes) > 1 {
		// Find sibling
		isLeft := currentIndex%2 == 0
		siblingIndex := currentIndex + 1
		if !isLeft {
			siblingIndex = currentIndex - 1
		}

		if siblingIndex < len(currentNodes) {
			proof = append(proof, currentNodes[siblingIndex].Hash)
		}

		// Move to parent level
		currentIndex /= 2
		var nextLevel []MerkleNode
		for i := 0; i < len(currentNodes); i += 2 {
			if i+1 < len(currentNodes) {
				parentHash := hasher.HashMultiple(currentNodes[i].Hash, currentNodes[i+1].Hash)
				nextLevel = append(nextLevel, MerkleNode{Hash: parentHash})
			} else {
				nextLevel = append(nextLevel, currentNodes[i])
			}
		}
		currentNodes = nextLevel
	}

	return proof, nil
}

// VerifyProof verifies a chunk's inclusion in the Merkle Tree
func (t *MerkleTree) VerifyProof(chunkHash hasher.Hash, proof []hasher.Hash) bool {
	currentHash := chunkHash
	for _, sibling := range proof {
		currentHash = hasher.HashMultiple(currentHash, sibling)
	}
	return currentHash == t.RootHash
}

// Serialize serializes the Merkle Tree to JSON
func (t *MerkleTree) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

// Deserialize deserializes a Merkle Tree from JSON
func Deserialize(data []byte) (*MerkleTree, error) {
	var tree MerkleTree
	if err := json.Unmarshal(data, &tree); err != nil {
		return nil, fmt.Errorf("failed to deserialize Merkle Tree: %w", err)
	}
	return &tree, nil
}
