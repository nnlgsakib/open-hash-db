package block

import (
    "errors"

    cidlib "github.com/ipfs/go-cid"
    "openhashdb/core/cidutil"
    "openhashdb/core/hasher"
    "openhashdb/protobuf/pb"
)

// Block represents a unit of immutable, content-addressed data.
// Methods align with production expectations (immutability, verification, CID access)
// while preserving compatibility with existing code.
type Block interface {
    // Hash returns the content hash (sha2-256) of the block's data.
    Hash() hasher.Hash
    // RawData returns a copy of the block's bytes (immutable to callers).
    RawData() []byte
    // ToProto returns a fresh protobuf message containing block hash and data.
    ToProto() *pb.Block
    // Size returns the size in bytes of the block's data.
    Size() int
    // CID returns the CIDv1 (codec=raw) derived from the content hash.
    CID() cidlib.Cid
    // Verify recomputes the hash of the data and checks it matches the header.
    Verify() bool
}

// block is the concrete implementation of Block.
// Data is immutable after construction.
type block struct {
    // h is the content hash of data.
    h hasher.Hash
    // data holds the immutable payload.
    data []byte
}

var (
    // ErrHashMismatch is returned when provided hash does not match data.
    ErrHashMismatch = errors.New("block hash does not match data")
)

// NewBlock creates a new block from data, copying the bytes to ensure immutability.
func NewBlock(data []byte) Block {
    // Defensive copy to prevent external mutation.
    buf := make([]byte, len(data))
    copy(buf, data)
    h := hasher.HashBytes(buf)
    return &block{h: h, data: buf}
}

// NewBlockWithHash creates a new block from a hash and data, validating integrity.
// The data is copied to ensure immutability. Returns a Block even if the hash
// mismatches for backward compatibility; callers should use Verify() and handle errors
// returned by NewValidatedBlock when strictness is required.
func NewBlockWithHash(h hasher.Hash, data []byte) Block {
    buf := make([]byte, len(data))
    copy(buf, data)
    return &block{h: h, data: buf}
}

// NewValidatedBlock returns an error if the provided hash does not match the data.
func NewValidatedBlock(h hasher.Hash, data []byte) (Block, error) {
    b := NewBlockWithHash(h, data).(*block)
    if !b.Verify() {
        return nil, ErrHashMismatch
    }
    return b, nil
}

// FromProto constructs a block from protobuf and validates hash format.
// The data is copied. Does not fail on hash mismatch to preserve compatibility
// with existing flows; use FromProtoValidated when strictness is required.
func FromProto(p *pb.Block) (Block, error) {
    h, err := hasher.HashFromBytes(p.Hash)
    if err != nil {
        return nil, err
    }
    return NewBlockWithHash(h, p.Data), nil
}

// FromProtoValidated constructs a block and validates content integrity.
func FromProtoValidated(p *pb.Block) (Block, error) {
    h, err := hasher.HashFromBytes(p.Hash)
    if err != nil {
        return nil, err
    }
    return NewValidatedBlock(h, p.Data)
}

func (b *block) Hash() hasher.Hash { return b.h }

// RawData returns a copy to ensure immutability outside the block.
func (b *block) RawData() []byte {
    if len(b.data) == 0 {
        return nil
    }
    out := make([]byte, len(b.data))
    copy(out, b.data)
    return out
}

// ToProto returns a fresh protobuf Block with copied fields to avoid external mutation.
func (b *block) ToProto() *pb.Block {
    ph := make([]byte, len(b.h))
    copy(ph, b.h[:])
    pd := make([]byte, len(b.data))
    copy(pd, b.data)
    // build and return cloned message
    blk := &pb.Block{Hash: ph, Data: pd}
    // proto.Clone is not required since we create a fresh message already
    return blk
}

// Size returns the length of the data in bytes.
func (b *block) Size() int { return len(b.data) }

// CID returns the CIDv1 (raw) computed from the sha2-256 content hash.
func (b *block) CID() cidlib.Cid {
    c, _ := cidutil.FromHash(b.h, cidutil.Raw)
    return c
}

// Verify recomputes the hash and compares it to the header hash.
func (b *block) Verify() bool { return hasher.HashBytes(b.data) == b.h }
