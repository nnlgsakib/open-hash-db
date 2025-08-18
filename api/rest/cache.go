package rest

import "sync"

// ChunkCache represents an LRU cache for chunks
type ChunkCache struct {
	mu      sync.RWMutex
	cache   map[string][]byte
	order   []string
	maxSize int
}

// NewChunkCache creates a new chunk cache
func NewChunkCache(size int) *ChunkCache {
	return &ChunkCache{
		cache:   make(map[string][]byte),
		order:   make([]string, 0, size),
		maxSize: size,
	}
}

// Get retrieves a chunk from cache
func (cc *ChunkCache) Get(key string) ([]byte, bool) {
	cc.mu.RLock()
	data, exists := cc.cache[key]
	cc.mu.RUnlock()
	if exists {
		// Move to front (most recently used)
		cc.mu.Lock()
		cc.moveToFront(key)
		cc.mu.Unlock()
	}
	return data, exists
}

// Put adds a chunk to cache
func (cc *ChunkCache) Put(key string, data []byte) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if _, exists := cc.cache[key]; exists {
		cc.moveToFront(key)
		return
	}

	if len(cc.cache) >= cc.maxSize {
		// Evict least recently used
		oldest := cc.order[len(cc.order)-1]
		delete(cc.cache, oldest)
		cc.order = cc.order[:len(cc.order)-1]
	}

	cc.cache[key] = data
	cc.order = append([]string{key}, cc.order...)
}

// moveToFront moves key to front of order slice
func (cc *ChunkCache) moveToFront(key string) {
	for i, k := range cc.order {
		if k == key {
			cc.order = append([]string{key}, append(cc.order[:i], cc.order[i+1:]...)...)
			break
		}
	}
}
