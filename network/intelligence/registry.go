package intelligence

import (
    "sort"
    "sync"
    "time"

    "github.com/libp2p/go-libp2p/core/peer"
    "openhashdb/core/hasher"
)

// Registry maintains a content-to-peer mapping with peer classification and scoring.
type Registry struct {
    mu sync.RWMutex

    // content -> peer -> meta
    providers map[hasher.Hash]map[peer.ID]*ProviderMeta

    // peer bloom filter hint about content they likely have
    peerBlooms map[peer.ID]*Bloom

    // peer -> last type classification
    peerTypes map[peer.ID]PeerType
}

func NewRegistry() *Registry {
    return &Registry{
        providers: make(map[hasher.Hash]map[peer.ID]*ProviderMeta),
        peerBlooms: make(map[peer.ID]*Bloom),
        peerTypes: make(map[peer.ID]PeerType),
    }
}

// ClassifyPeer records the classification of a peer.
func (r *Registry) ClassifyPeer(p peer.ID, t PeerType) {
    r.mu.Lock()
    r.peerTypes[p] = t
    if _, ok := r.peerBlooms[p]; !ok {
        r.peerBlooms[p] = NewBloom(4096, 3)
    }
    r.mu.Unlock()
}

// RegisterHave marks that a peer indicated it has a block.
func (r *Registry) RegisterHave(p peer.ID, h hasher.Hash) {
    r.mu.Lock()
    defer r.mu.Unlock()
    if _, ok := r.providers[h]; !ok {
        r.providers[h] = make(map[peer.ID]*ProviderMeta)
    }
    pm, ok := r.providers[h][p]
    if !ok {
        pm = &ProviderMeta{PeerID: p, Type: r.peerTypes[p], Score: 1}
        r.providers[h][p] = pm
    }
    pm.LastSeen = time.Now()
    // Update bloom
    if bloom := r.peerBlooms[p]; bloom != nil {
        bloom.Add(h[:])
    }
}

// NoteBlockServed increases score and updates latency for a peer that served a block.
func (r *Registry) NoteBlockServed(p peer.ID, latency time.Duration) {
    r.mu.Lock()
    defer r.mu.Unlock()
    for _, peers := range r.providers {
        if pm, ok := peers[p]; ok {
            // Simple EWMA for latency and additive score
            if pm.Latency == 0 {
                pm.Latency = latency
            } else {
                pm.Latency = time.Duration(0.7*float64(pm.Latency) + 0.3*float64(latency))
            }
            pm.Score += 1
            pm.LastSeen = time.Now()
        }
    }
}

// NoteFailure penalizes a peer after failure.
func (r *Registry) NoteFailure(p peer.ID) {
    r.mu.Lock()
    defer r.mu.Unlock()
    for _, peers := range r.providers {
        if pm, ok := peers[p]; ok {
            pm.Score *= 0.9
            pm.LastSeen = time.Now()
        }
    }
}

// GetPreferredProviders returns peers ordered by preference: direct + high score, then relayed.
func (r *Registry) GetPreferredProviders(h hasher.Hash) []peer.ID {
    r.mu.RLock()
    defer r.mu.RUnlock()
    peers := r.providers[h]
    if len(peers) == 0 {
        return nil
    }
    type item struct{ p peer.ID; pm *ProviderMeta }
    items := make([]item, 0, len(peers))
    for pid, pm := range peers {
        items = append(items, item{p: pid, pm: pm})
    }
    sort.Slice(items, func(i, j int) bool {
        // prefer direct
        if items[i].pm.Type != items[j].pm.Type {
            return items[i].pm.Type == PeerTypeDirect
        }
        // then higher score
        if items[i].pm.Score != items[j].pm.Score {
            return items[i].pm.Score > items[j].pm.Score
        }
        // then lower latency
        return items[i].pm.Latency < items[j].pm.Latency
    })
    out := make([]peer.ID, len(items))
    for i, it := range items {
        out[i] = it.p
    }
    return out
}

// ShouldProbePeerFor returns true when it makes sense to send a want-have probe to a peer.
func (r *Registry) ShouldProbePeerFor(p peer.ID, h hasher.Hash) bool {
    r.mu.RLock()
    defer r.mu.RUnlock()
    // If we already know, no need to probe
    if mp, ok := r.providers[h]; ok {
        if _, ok2 := mp[p]; ok2 {
            return true // still okay to probe for freshness
        }
    }
    // Use bloom filter hint if available
    if bloom := r.peerBlooms[p]; bloom != nil {
        return bloom.Test(h[:])
    }
    // No hint, allow probe
    return true
}

