package cidutil

import (
    "fmt"

    gocid "github.com/ipfs/go-cid"
    mh "github.com/multiformats/go-multihash"

    "openhashdb/core/hasher"
)

// Common multicodecs
const (
    // Raw is the multicodec code for raw binary blocks (leaf chunks)
    Raw uint64 = 0x55
    // DagPB is the multicodec code for dag-pb
    DagPB uint64 = 0x70
    // DagCBOR is the multicodec code for dag-cbor
    DagCBOR uint64 = 0x71
)

// FromHash builds a CIDv1 from a hasher.Hash and codec.
// By default we wrap the 32-byte sha2-256 hash as multihash code 0x12, length 32.
func FromHash(h hasher.Hash, codec uint64) (gocid.Cid, error) {
    m, err := mh.Encode(h[:], mh.SHA2_256)
    if err != nil {
        return gocid.Cid{}, fmt.Errorf("multihash encode: %w", err)
    }
    return gocid.NewCidV1(codec, m), nil
}

// ToHash extracts a hasher.Hash from a CID that uses sha2-256 multihash.
func ToHash(c gocid.Cid) (hasher.Hash, error) {
    var out hasher.Hash
    mhb := c.Hash()
    dec, err := mh.Decode(mhb)
    if err != nil {
        return out, fmt.Errorf("decode multihash: %w", err)
    }
    if dec.Code != mh.SHA2_256 {
        return out, fmt.Errorf("unsupported multihash code: %d", dec.Code)
    }
    if len(dec.Digest) != len(out) {
        return out, fmt.Errorf("unexpected digest length: %d", len(dec.Digest))
    }
    copy(out[:], dec.Digest)
    return out, nil
}

// Parse parses a CID string.
func Parse(s string) (gocid.Cid, error) { return gocid.Parse(s) }

// String returns a base32 string for CIDv1.
func String(c gocid.Cid) string { return c.String() }
