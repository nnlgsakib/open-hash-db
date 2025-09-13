package gateway

import (
    "container/list"
    "sync"
)

// ChunkCache is an LRU cache bounded by total bytes.
type ChunkCache struct {
    mu       sync.Mutex
    maxBytes int64
    curBytes int64
    ll       *list.List
    cache    map[string]*list.Element
}

type entry struct {
    key  string
    data []byte
}

func NewChunkCacheBytes(maxBytes int64) *ChunkCache {
    if maxBytes <= 0 {
        maxBytes = 64 << 20 // default 64MB
    }
    return &ChunkCache{maxBytes: maxBytes, ll: list.New(), cache: make(map[string]*list.Element)}
}

func (cc *ChunkCache) Get(key string) ([]byte, bool) {
    cc.mu.Lock()
    defer cc.mu.Unlock()
    if ele, ok := cc.cache[key]; ok {
        cc.ll.MoveToFront(ele)
        return ele.Value.(*entry).data, true
    }
    return nil, false
}

func (cc *ChunkCache) Put(key string, data []byte) {
    if data == nil {
        return
    }
    cc.mu.Lock()
    defer cc.mu.Unlock()
    if ele, ok := cc.cache[key]; ok {
        // update in place
        e := ele.Value.(*entry)
        cc.curBytes -= int64(len(e.data))
        e.data = data
        cc.curBytes += int64(len(data))
        cc.ll.MoveToFront(ele)
    } else {
        ele := cc.ll.PushFront(&entry{key: key, data: data})
        cc.cache[key] = ele
        cc.curBytes += int64(len(data))
    }
    cc.evict()
}

func (cc *ChunkCache) evict() {
    for cc.curBytes > cc.maxBytes {
        ele := cc.ll.Back()
        if ele == nil {
            return
        }
        e := ele.Value.(*entry)
        delete(cc.cache, e.key)
        cc.curBytes -= int64(len(e.data))
        cc.ll.Remove(ele)
    }
}

