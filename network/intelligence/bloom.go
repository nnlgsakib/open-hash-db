package intelligence

import (
    "hash/fnv"
)

// Bloom is a simple bloom filter implementation with k hash functions
type Bloom struct {
    bits []uint64
    m    uint64
    k    uint64
}

func NewBloom(bitCount, k uint64) *Bloom {
    // round bitCount to multiple of 64
    if bitCount%64 != 0 {
        bitCount += 64 - (bitCount % 64)
    }
    return &Bloom{
        bits: make([]uint64, bitCount/64),
        m:    bitCount,
        k:    k,
    }
}

func (b *Bloom) Add(data []byte) {
    for i := uint64(0); i < b.k; i++ {
        idx := b.hashN(data, i) % b.m
        b.bits[idx/64] |= 1 << (idx % 64)
    }
}

func (b *Bloom) Test(data []byte) bool {
    for i := uint64(0); i < b.k; i++ {
        idx := b.hashN(data, i) % b.m
        if (b.bits[idx/64]&(1<<(idx%64))) == 0 {
            return false
        }
    }
    return true
}

func (b *Bloom) hashN(data []byte, n uint64) uint64 {
    h := fnv.New64a()
    // seed: write n as 8 bytes
    var seed [8]byte
    for i := 0; i < 8; i++ {
        seed[i] = byte((n >> (8 * i)) & 0xff)
    }
    _, _ = h.Write(seed[:])
    _, _ = h.Write(data)
    return h.Sum64()
}

