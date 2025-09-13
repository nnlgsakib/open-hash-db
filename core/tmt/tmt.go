// Package tmt provides a thread-safe Ternary Mesh Tree with BLAKE3 hashing,
// compact proofs, optional hash caching, metrics, and gob-based serialization.
//
// Usage:
//
//	tree := tmt.NewDefault()
//	_ = tree.Build([][]byte{[]byte("block1"), []byte("block2"), []byte("block3")})
//	ok, _ := tree.Verify(0, []byte("block1"))
package tmt

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"lukechampine.com/blake3"
)

type Hash = [32]byte
type NodeID = uint64

// ---------------------- Hash utilities ----------------------

func ComputeHash(data []byte) Hash {
	return blake3.Sum256(data)
}

func CombineHashes(hashes []Hash) Hash {
	h := blake3.New(32, nil)
	for i := range hashes {
		h.Write(hashes[i][:])
	}
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

func HashToHex(h Hash) string { return hex.EncodeToString(h[:]) }

// ---------------------- Serializable & internal nodes ----------------------

type SerializableNode struct {
	ID       NodeID
	Hash     Hash
	Children []NodeID // empty for leaves, up to 3 for internals
	IsLeaf   bool
	Parent   *NodeID
}

type internalNode struct {
	hash     Hash
	children []NodeID
	isLeaf   bool
	parent   *NodeID
}

// ---------------------- Verification proof ----------------------

type SiblingHash struct {
	Pos  int
	Hash Hash
}

type VerificationProof struct {
	LeafIndex     int
	SiblingHashes []SiblingHash // (position, hash) pairs
	PathLength    int
	NodeArities   []int
	PathIndices   []int
}

// ---------------------- Metrics ----------------------

type Metrics struct {
	BuildTimeMS            uint64
	LastVerificationTimeNS uint64
	LastUpdateTimeNS       uint64
	TotalVerifications     uint64
	TotalUpdates           uint64
	MemoryUsageBytes       uint64
}

func (m *Metrics) clone() Metrics { return *m }

// ---------------------- Config ----------------------

type Config struct {
	EnableCaching     bool
	MaxCacheSize      int
	EnableMetrics     bool
	ParallelThreshold int // chunked parallel pre-hash when leaves >= this
}

func DefaultConfig() Config {
	return Config{
		EnableCaching:     true,
		MaxCacheSize:      10_000,
		EnableMetrics:     true,
		ParallelThreshold: 1000,
	}
}

// ---------------------- Errors ----------------------

var (
	ErrEmptyData     = errors.New("cannot build tree from empty data")
	ErrInvalidIndex  = errors.New("invalid index")
	ErrUninitialized = errors.New("tree is not initialized")
	ErrSerialization = errors.New("serialization error")
	ErrInvalidProof  = errors.New("invalid proof")
	ErrMissingParent = errors.New("missing parent while walking upward")
)

// ---------------------- TernaryMeshTree ----------------------

type TernaryMeshTree struct {
	mu        sync.RWMutex
	nodes     []internalNode
	leafData  [][]byte
	rootID    *NodeID
	leafCount int
	cfg       Config

	metrics   Metrics
	cacheMu   sync.RWMutex
	hashCache map[string]Hash // key = data as string; small + simple cache
}

// New creates a tree with the provided config.
func New(cfg Config) *TernaryMeshTree {
	return &TernaryMeshTree{
		cfg:       cfg,
		hashCache: make(map[string]Hash, min(256, cfg.MaxCacheSize)),
	}
}

// NewDefault creates a tree with the default config.
func NewDefault() *TernaryMeshTree { return New(DefaultConfig()) }

// ---------------------- Build ----------------------

func (t *TernaryMeshTree) Build(dataBlocks [][]byte) error {
	start := time.Now()

	if len(dataBlocks) == 0 {
		return ErrEmptyData
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// reset old state
	t.nodes = t.nodes[:0]
	t.leafData = t.leafData[:0]
	t.leafCount = len(dataBlocks)
	t.rootID = nil

    // leaves
    current := make([]NodeID, 0, nextMultipleOf(len(dataBlocks), 3))
    for i, d := range dataBlocks {
        // Optimization: if leaf length is 32 bytes, treat it as an already-computed hash
        var h Hash
        if len(d) == 32 {
            copy(h[:], d)
        } else {
            h = t.getCachedHashLocked(d)
        }
        t.nodes = append(t.nodes, internalNode{
            hash:     h,
            children: nil,
            isLeaf:   true,
            parent:   nil,
        })
        // Avoid storing large leaf data when not needed
        if len(d) == 0 {
            t.leafData = append(t.leafData, nil)
        } else if len(d) == 32 {
            // do not retain a copy for hash-leaves; store nil to keep indices aligned
            t.leafData = append(t.leafData, nil)
        } else {
            t.leafData = append(t.leafData, append([]byte(nil), d...))
        }
        current = append(current, NodeID(i))
    }

	// pad to divisible by 3
	for len(current)%3 != 0 {
		h := ComputeHash(nil)
		t.nodes = append(t.nodes, internalNode{hash: h, isLeaf: true})
		t.leafData = append(t.leafData, nil)
		current = append(current, NodeID(len(t.nodes)-1))
	}

	// helper to append a parent
	appendParent := func(chunk []NodeID, parentHash Hash) NodeID {
		p := internalNode{
			hash:     parentHash,
			children: append([]NodeID(nil), chunk...),
			isLeaf:   false,
		}
		pid := NodeID(len(t.nodes))
		t.nodes = append(t.nodes, p)
		for _, cid := range chunk {
			t.nodes[cid].parent = &pid
		}
		return pid
	}

	// bottom-up
	for len(current) > 1 {
		next := make([]NodeID, 0, (len(current)+2)/3)

		// optional parallel pre-hash of child groups
		if t.cfg.ParallelThreshold > 0 && len(current) >= t.cfg.ParallelThreshold {
			chunks := chunkBy(current, 3)

			type pre struct {
				i     int
				chunk []NodeID
				hash  Hash
			}
			precomp := make([]pre, len(chunks))
			var wg sync.WaitGroup
			wg.Add(len(chunks))
			for i := range chunks {
				i := i
				go func() {
					defer wg.Done()
					childHashes := make([]Hash, 0, len(chunks[i]))
					for _, id := range chunks[i] {
						childHashes = append(childHashes, t.nodes[id].hash)
					}
					precomp[i] = pre{
						i:     i,
						chunk: chunks[i],
						hash:  CombineHashes(childHashes),
					}
				}()
			}
			wg.Wait()
			sort.Slice(precomp, func(i, j int) bool { return precomp[i].i < precomp[j].i })
			for _, p := range precomp {
				next = append(next, appendParent(p.chunk, p.hash))
			}
		} else {
			// serial
			for i := 0; i < len(current); i += 3 {
				chunk := current[i:min(i+3, len(current))]
				childHashes := make([]Hash, 0, len(chunk))
				for _, id := range chunk {
					childHashes = append(childHashes, t.nodes[id].hash)
				}
				next = append(next, appendParent(chunk, CombineHashes(childHashes)))
			}
		}

		current = next
	}

	root := current[0]
	t.rootID = &root

	if t.cfg.EnableMetrics {
		t.metrics.BuildTimeMS = uint64(time.Since(start).Milliseconds())
		t.metrics.MemoryUsageBytes = uint64(t.estimateMemoryUsageLocked())
	}
	return nil
}

// ---------------------- Verify ----------------------

func (t *TernaryMeshTree) Verify(leafIndex int, data []byte) (bool, error) {
	start := time.Now()

	t.mu.RLock()
	defer t.mu.RUnlock()

	if leafIndex < 0 || leafIndex >= t.leafCount {
		return false, fmt.Errorf("%w: %d", ErrInvalidIndex, leafIndex)
	}
	if t.rootID == nil {
		return false, ErrUninitialized
	}

	exp := ComputeHash(data)
	if t.nodes[leafIndex].hash != exp {
		if t.cfg.EnableMetrics {
			t.metrics.LastVerificationTimeNS = uint64(time.Since(start).Nanoseconds())
			t.metrics.TotalVerifications++
		}
		return false, nil
	}

	proof, err := t.generateProofInternalLocked(leafIndex)
	if err != nil {
		return false, err
	}
	ok := t.verifyProofInternalLocked(proof, exp, *t.rootID)

	if t.cfg.EnableMetrics {
		t.metrics.LastVerificationTimeNS = uint64(time.Since(start).Nanoseconds())
		t.metrics.TotalVerifications++
	}
	return ok, nil
}

// ---------------------- Update & BatchUpdate ----------------------

func (t *TernaryMeshTree) Update(leafIndex int, newData []byte) error {
	start := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	if leafIndex < 0 || leafIndex >= t.leafCount {
		return fmt.Errorf("%w: %d", ErrInvalidIndex, leafIndex)
	}

	t.leafData[leafIndex] = append([]byte(nil), newData...)
	t.nodes[leafIndex].hash = ComputeHash(newData)

	if err := t.updateAncestorsLocked(NodeID(leafIndex)); err != nil {
		return err
	}

	if t.cfg.EnableMetrics {
		t.metrics.LastUpdateTimeNS = uint64(time.Since(start).Nanoseconds())
		t.metrics.TotalUpdates++
	}
	return nil
}

func (t *TernaryMeshTree) BatchUpdate(updates map[int][]byte) error {
	start := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	for idx := range updates {
		if idx < 0 || idx >= t.leafCount {
			return fmt.Errorf("%w: %d", ErrInvalidIndex, idx)
		}
	}

	for idx, data := range updates {
		t.leafData[idx] = append([]byte(nil), data...)
		t.nodes[idx].hash = ComputeHash(data)
	}

	affected := make(map[NodeID]struct{})
	for idx := range updates {
		if err := t.collectAncestorsLocked(NodeID(idx), affected); err != nil {
			return err
		}
	}

	// simple upward recompute until stable
	queue := list.New()
	seen := make(map[NodeID]struct{})
	for id := range affected {
		queue.PushBack(id)
	}
	for queue.Len() > 0 {
		e := queue.Front()
		queue.Remove(e)
		id := e.Value.(NodeID)
		if _, ok := seen[id]; ok {
			continue
		}
		if err := t.recomputeNodeHashLocked(id); err != nil {
			return err
		}
		seen[id] = struct{}{}
		if p := t.nodes[id].parent; p != nil {
			queue.PushBack(*p)
		}
	}

	if t.cfg.EnableMetrics {
		t.metrics.LastUpdateTimeNS = uint64(time.Since(start).Nanoseconds())
		t.metrics.TotalUpdates += uint64(len(updates))
	}
	return nil
}

// ---------------------- Proofs ----------------------

func (t *TernaryMeshTree) GenerateProof(leafIndex int) (VerificationProof, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if leafIndex < 0 || leafIndex >= t.leafCount {
		return VerificationProof{}, fmt.Errorf("%w: %d", ErrInvalidIndex, leafIndex)
	}
	return t.generateProofInternalLocked(leafIndex)
}

func (t *TernaryMeshTree) VerifyProof(proof VerificationProof, leafData []byte) (bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.rootID == nil {
		return false, ErrUninitialized
	}
	leafHash := ComputeHash(leafData)
	return t.verifyProofInternalLocked(proof, leafHash, *t.rootID), nil
}

func (t *TernaryMeshTree) generateProofInternalLocked(leafIndex int) (VerificationProof, error) {
	var sibs []SiblingHash
	var arities []int
	var pathIndices []int
	cur := NodeID(leafIndex)
	path := 0

	for {
		n := t.nodes[cur]
		if n.parent == nil {
			break
		}
		pid := *n.parent
		parent := t.nodes[pid]

		pos := -1
		for i, cid := range parent.children {
			if cid == cur {
				pos = i
				break
			}
		}
		if pos < 0 {
			return VerificationProof{}, ErrMissingParent
		}

		for i, cid := range parent.children {
			if i == pos {
				continue
			}
			sibs = append(sibs, SiblingHash{Pos: i, Hash: t.nodes[cid].hash})
		}
		arities = append(arities, len(parent.children))
		pathIndices = append(pathIndices, pos)

		cur = pid
		path++
	}

	return VerificationProof{
		LeafIndex:     leafIndex,
		SiblingHashes: sibs,
		PathLength:    path,
		NodeArities:   arities,
		PathIndices:   pathIndices,
	}, nil
}

func (t *TernaryMeshTree) verifyProofInternalLocked(proof VerificationProof, leafHash Hash, root NodeID) bool {
	curID := NodeID(proof.LeafIndex)
	curHash := leafHash
	si := 0

	for step := 0; step < proof.PathLength; step++ {
		n := t.nodes[curID]
		if n.parent == nil {
			return false
		}
		pid := *n.parent
		parent := t.nodes[pid]

		pos := -1
		for i, cid := range parent.children {
			if cid == curID {
				pos = i
				break
			}
		}
		if pos < 0 {
			return false
		}

		childHashes := make([]Hash, len(parent.children))
		childHashes[pos] = curHash

		need := len(parent.children) - 1
		for i := 0; i < need; i++ {
			if si >= len(proof.SiblingHashes) {
				return false
			}
			sh := proof.SiblingHashes[si]
			si++
			if sh.Pos == pos || sh.Pos < 0 || sh.Pos >= len(childHashes) {
				return false
			}
			childHashes[sh.Pos] = sh.Hash
		}

		curHash = CombineHashes(childHashes)
		curID = pid
	}

	return si == len(proof.SiblingHashes) && bytes.Equal(curHash[:], t.nodes[root].hash[:])
}

// VerifyProofWithRoot verifies a proof for a given leaf data and root hash.
func VerifyProofWithRoot(proof VerificationProof, leafData []byte, rootHash Hash) (bool, error) {
	leafHash := ComputeHash(leafData)

	if len(proof.NodeArities) != proof.PathLength || len(proof.PathIndices) != proof.PathLength {
		return false, ErrInvalidProof
	}

	curHash := leafHash
	si := 0

	for step := 0; step < proof.PathLength; step++ {
		arity := proof.NodeArities[step]
		if arity < 1 || arity > 3 {
			return false, ErrInvalidProof
		}

		pos := proof.PathIndices[step]
		if pos < 0 || pos >= arity {
			return false, ErrInvalidProof
		}

		childHashes := make([]Hash, arity)
		childHashes[pos] = curHash

		need := arity - 1
		for i := 0; i < need; i++ {
			if si >= len(proof.SiblingHashes) {
				return false, ErrInvalidProof
			}
			sh := proof.SiblingHashes[si]
			si++
			if sh.Pos == pos || sh.Pos < 0 || sh.Pos >= arity {
				return false, ErrInvalidProof
			}
			childHashes[sh.Pos] = sh.Hash
		}

		curHash = CombineHashes(childHashes)
	}

	return si == len(proof.SiblingHashes) && bytes.Equal(curHash[:], rootHash[:]), nil
}

// ---------------------- Serialization ----------------------

type serializedBlob struct {
	Nodes     []SerializableNode
	LeafData  [][]byte
	RootID    *NodeID
	LeafCount int
}

func (t *TernaryMeshTree) Serialize() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	snodes := make([]SerializableNode, 0, len(t.nodes))
	for id, n := range t.nodes {
		snodes = append(snodes, SerializableNode{
			ID:       NodeID(id),
			Hash:     n.hash,
			Children: append([]NodeID(nil), n.children...),
			IsLeaf:   n.isLeaf,
			Parent:   n.parent,
		})
	}
	cpLeaves := make([][]byte, len(t.leafData))
	for i := range t.leafData {
		cpLeaves[i] = append([]byte(nil), t.leafData[i]...)
	}

	blob := serializedBlob{
		Nodes:     snodes,
		LeafData:  cpLeaves,
		RootID:    t.rootID,
		LeafCount: t.leafCount,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&blob); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSerialization, err)
	}
	return buf.Bytes(), nil
}

func Deserialize(b []byte, cfg Config) (*TernaryMeshTree, error) {
	var blob serializedBlob
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&blob); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSerialization, err)
	}

	nodes := make([]internalNode, 0, len(blob.Nodes))
	for _, n := range blob.Nodes {
		nodes = append(nodes, internalNode{
			hash:     n.Hash,
			children: append([]NodeID(nil), n.Children...),
			isLeaf:   n.IsLeaf,
			parent:   n.Parent,
		})
	}

	return &TernaryMeshTree{
		nodes:     nodes,
		leafData:  blob.LeafData,
		rootID:    blob.RootID,
		leafCount: blob.LeafCount,
		cfg:       cfg,
		hashCache: make(map[string]Hash, min(256, cfg.MaxCacheSize)),
	}, nil
}

// ---------------------- Getters ----------------------

func (t *TernaryMeshTree) Metrics() Metrics {
	if !t.cfg.EnableMetrics {
		return Metrics{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.metrics.clone()
}

func (t *TernaryMeshTree) RootHash() (Hash, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.rootID == nil {
		return Hash{}, false
	}
	return t.nodes[*t.rootID].hash, true
}

func (t *TernaryMeshTree) Height() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.rootID == nil {
		return 0
	}
	return t.calculateHeightLocked(*t.rootID)
}

// ---------------------- internals ----------------------

func (t *TernaryMeshTree) getCachedHashLocked(data []byte) Hash {
	if !t.cfg.EnableCaching {
		return ComputeHash(data)
	}
	key := string(data)
	t.cacheMu.RLock()
	h, ok := t.hashCache[key]
	t.cacheMu.RUnlock()
	if ok {
		return h
	}
	h = ComputeHash(data)
	t.cacheMu.Lock()
	if len(t.hashCache) < t.cfg.MaxCacheSize {
		t.hashCache[key] = h
	}
	t.cacheMu.Unlock()
	return h
}

func (t *TernaryMeshTree) updateAncestorsLocked(cur NodeID) error {
	var anc []NodeID
	for {
		n := t.nodes[cur]
		if n.parent == nil {
			break
		}
		p := *n.parent
		anc = append(anc, p)
		cur = p
	}
	for i := len(anc) - 1; i >= 0; i-- {
		if err := t.recomputeNodeHashLocked(anc[i]); err != nil {
			return err
		}
	}
	return nil
}

func (t *TernaryMeshTree) collectAncestorsLocked(cur NodeID, set map[NodeID]struct{}) error {
	for {
		n := t.nodes[cur]
		if n.parent == nil {
			break
		}
		p := *n.parent
		set[p] = struct{}{}
		cur = p
	}
	return nil
}

func (t *TernaryMeshTree) recomputeNodeHashLocked(id NodeID) error {
	if int(id) >= len(t.nodes) {
		return fmt.Errorf("%w: %d", ErrInvalidIndex, id)
	}
	n := t.nodes[id]
	if n.isLeaf {
		return nil
	}
	childHashes := make([]Hash, 0, len(n.children))
	for _, cid := range n.children {
		childHashes = append(childHashes, t.nodes[cid].hash)
	}
	t.nodes[id].hash = CombineHashes(childHashes)
	return nil
}

func (t *TernaryMeshTree) calculateHeightLocked(id NodeID) int {
	n := t.nodes[id]
	if n.isLeaf {
		return 1
	}
	maxH := 0
	for _, cid := range n.children {
		if h := t.calculateHeightLocked(cid); h > maxH {
			maxH = h
		}
	}
	return maxH + 1
}

func (t *TernaryMeshTree) estimateMemoryUsageLocked() int {
	// quick estimate: node header + leaves
	const approxNodeBytes = 80 // rough avg on 64-bit
	total := approxNodeBytes * len(t.nodes)
	for _, d := range t.leafData {
		total += len(d)
	}
	return total
}

// ---------------------- helpers ----------------------

func nextMultipleOf(n, m int) int {
	if m <= 0 {
		return n
	}
	r := n % m
	if r == 0 {
		return n
	}
	return n + (m - r)
}

func chunkBy[T any](in []T, k int) [][]T {
	if k <= 0 {
		return [][]T{in}
	}
	out := make([][]T, 0, (len(in)+k-1)/k)
	for i := 0; i < len(in); i += k {
		j := i + k
		if j > len(in) {
			j = len(in)
		}
		out = append(out, in[i:j])
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------- Self-test (optional) ----------------------

// SelfTest runs a minimal sequence similar to the Rust tests.
func SelfTest() error {
	t := NewDefault()
	data := [][]byte{[]byte("block1"), []byte("block2"), []byte("block3")}
	if err := t.Build(data); err != nil {
		return err
	}
	if ok, err := t.Verify(0, []byte("block1")); err != nil || !ok {
		return fmt.Errorf("verify before update failed: %v", err)
	}
	if err := t.Update(0, []byte("new_block1")); err != nil {
		return err
	}
	if ok, err := t.Verify(0, []byte("new_block1")); err != nil || !ok {
		return fmt.Errorf("verify after update failed: %v", err)
	}
	blob, err := t.Serialize()
	if err != nil {
		return err
	}
	t2, err := Deserialize(blob, DefaultConfig())
	if err != nil {
		return err
	}
	r1, _ := t.RootHash()
	r2, _ := t2.RootHash()
	if r1 != r2 {
		return fmt.Errorf("root hash mismatch after deserialize")
	}
	return nil
}
