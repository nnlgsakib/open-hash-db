package hasher

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// Hash represents a SHA-256 hash
type Hash [32]byte

// String returns the hex representation of the hash
func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

// HashFromString creates a Hash from a hex string
func HashFromString(s string) (Hash, error) {
	var h Hash
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return h, fmt.Errorf("invalid hash string: %w", err)
	}
	if len(bytes) != 32 {
		return h, fmt.Errorf("hash must be 32 bytes, got %d", len(bytes))
	}
	copy(h[:], bytes)
	return h, nil
}

// HashFromBytes creates a Hash from a byte slice
func HashFromBytes(b []byte) (Hash, error) {
	var h Hash
	if len(b) != 32 {
		return h, fmt.Errorf("hash must be 32 bytes, got %d", len(b))
	}
	copy(h[:], b)
	return h, nil
}

// HashBytes computes SHA-256 hash of byte slice
func HashBytes(data []byte) Hash {
	return sha256.Sum256(data)
}

// HashString computes SHA-256 hash of string
func HashString(s string) Hash {
	return HashBytes([]byte(s))
}

// HashReader computes SHA-256 hash of data from io.Reader
func HashReader(r io.Reader) (Hash, error) {
	hasher := sha256.New()
	_, err := io.Copy(hasher, r)
	if err != nil {
		return Hash{}, fmt.Errorf("failed to hash reader: %w", err)
	}
	
	var result Hash
	copy(result[:], hasher.Sum(nil))
	return result, nil
}

// HashFile computes SHA-256 hash of file
func HashFile(filepath string) (Hash, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return Hash{}, fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()
	
	return HashReader(file)
}

// HashMultiple computes hash of concatenated hashes (for Merkle trees)
func HashMultiple(hashes ...Hash) Hash {
	hasher := sha256.New()
	for _, h := range hashes {
		hasher.Write(h[:])
	}
	
	var result Hash
	copy(result[:], hasher.Sum(nil))
	return result
}

// Verify checks if data matches the expected hash
func Verify(data []byte, expectedHash Hash) bool {
	actualHash := HashBytes(data)
	return actualHash == expectedHash
}

// VerifyReader checks if reader data matches the expected hash
func VerifyReader(r io.Reader, expectedHash Hash) (bool, error) {
	actualHash, err := HashReader(r)
	if err != nil {
		return false, err
	}
	return actualHash == expectedHash, nil
}

// VerifyFile checks if file matches the expected hash
func VerifyFile(filepath string, expectedHash Hash) (bool, error) {
	actualHash, err := HashFile(filepath)
	if err != nil {
		return false, err
	}
	return actualHash == expectedHash, nil
}

