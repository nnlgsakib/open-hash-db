package sharder

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
)

const (
	// DefaultDataShards is the default number of data shards.
	DefaultDataShards = 10
	// DefaultParityShards is the default number of parity shards.
	DefaultParityShards = 4
)

// ErasureCoder defines the interface for erasure coding.
type ErasureCoder interface {
	Encode(data []byte) ([][]byte, error)
	Reconstruct(shards [][]byte) ([]byte, error)
	ShardCount() int
	DataShardCount() int
	ParityShardCount() int
}

// ReedSolomon implements ErasureCoder using Reed-Solomon codes.
type ReedSolomon struct {
	enc              reedsolomon.Encoder
	dataShards       int
	parityShards     int
	totalShards      int
}

// NewReedSolomon creates a new Reed-Solomon encoder/decoder.
func NewReedSolomon(dataShards, parityShards int) (ErasureCoder, error) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon encoder: %w", err)
	}
	return &ReedSolomon{
		enc:          enc,
		dataShards:   dataShards,
		parityShards: parityShards,
		totalShards:  dataShards + parityShards,
	}, nil
}

// Encode splits data into shards and generates parity shards.
func (rs *ReedSolomon) Encode(data []byte) ([][]byte, error) {
	shards, err := rs.enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("failed to split data into shards: %w", err)
	}

	if err := rs.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("failed to encode parity shards: %w", err)
	}

	return shards, nil
}

// Reconstruct reconstructs the original data from the provided shards.
// Some shards can be nil to indicate they are missing.
func (rs *ReedSolomon) Reconstruct(shards [][]byte) ([]byte, error) {
	ok, err := rs.enc.Verify(shards)
	if !ok {
		if err := rs.enc.Reconstruct(shards); err != nil {
			return nil, fmt.Errorf("failed to reconstruct data: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to verify shards: %w", err)
	}

	var buf bytes.Buffer
	// Join the shards and write the reconstructed data to the buffer.
	// We need to specify the size of the original data.
	// Since the sharder doesn't store the original size, the caller must handle it.
	// A common approach is to store the original size in the metadata.
	// For now, we assume the caller will handle trimming any padding.
	if err := rs.enc.Join(&buf, shards, len(shards[0])*rs.dataShards); err != nil {
		return nil, fmt.Errorf("failed to join shards: %w", err)
	}

	return buf.Bytes(), nil
}

func (rs *ReedSolomon) ShardCount() int {
	return rs.totalShards
}

func (rs *ReedSolomon) DataShardCount() int {
	return rs.dataShards
}

func (rs *ReedSolomon) ParityShardCount() int {
	return rs.parityShards
}

// ReadAll reads from the reader until EOF and returns the data.
// This is a helper function needed because the reedsolomon library works on byte slices.
func ReadAll(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
