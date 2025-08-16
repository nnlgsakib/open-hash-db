package block

import "openhashdb/core/hasher"

// Block represents a unit of data in the system.
type Block interface {
	Hash() hasher.Hash
	RawData() []byte
}

// basicBlock is a basic implementation of the Block interface.
type basicBlock struct {
	BlockHash hasher.Hash `json:"hash"`
	RawBlockData []byte      `json:"data"`
}

// NewBlock creates a new block from data.
func NewBlock(data []byte) Block {
	return &basicBlock{
		BlockHash: hasher.HashBytes(data),
		RawBlockData: data,
	}
}

// NewBlockWithHash creates a new block from a hash and data.
func NewBlockWithHash(h hasher.Hash, data []byte) Block {
	return &basicBlock{
		BlockHash: h,
		RawBlockData: data,
	}
}

func (b *basicBlock) Hash() hasher.Hash {
	return b.BlockHash
}

func (b *basicBlock) RawData() []byte {
	return b.RawBlockData
}
