package block

import (
	"openhashdb/core/hasher"
	"openhashdb/protobuf/pb"
)

// Block represents a unit of data in the system.
type Block interface {
	Hash() hasher.Hash
	RawData() []byte
	ToProto() *pb.Block
}

// block is a basic implementation of the Block interface.
type block struct {
	p *pb.Block
	h hasher.Hash
}

// NewBlock creates a new block from data.
func NewBlock(data []byte) Block {
	h := hasher.HashBytes(data)
	return &block{
		p: &pb.Block{
			Hash: h[:],
			Data: data,
		},
		h: h,
	}
}

// NewBlockWithHash creates a new block from a hash and data.
func NewBlockWithHash(h hasher.Hash, data []byte) Block {
	return &block{
		p: &pb.Block{
			Hash: h[:],
			Data: data,
		},
		h: h,
	}
}

func FromProto(p *pb.Block) (Block, error) {
	h, err := hasher.HashFromBytes(p.Hash)
	if err != nil {
		return nil, err
	}
	return &block{
		p: p,
		h: h,
	}, nil
}

func (b *block) Hash() hasher.Hash {
	return b.h
}

func (b *block) RawData() []byte {
	return b.p.Data
}

func (b *block) ToProto() *pb.Block {
	return b.p
}