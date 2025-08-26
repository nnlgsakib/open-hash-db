package peer_registry

import (
    "sort"
    "strings"
    "sync"
    "time"

    "github.com/libp2p/go-libp2p/core/peer"
)

// ConnectionType indicates how we are currently connected to a peer.
type ConnectionType int

const (
    ConnUnknown ConnectionType = iota
    ConnDirect
    ConnRelayed
)

// PeerQuality holds rolling counters and derived score for a peer.
type PeerQuality struct {
    SuccessBlocks uint64
    FailedBlocks  uint64
    HeartbeatsOK  uint64
    HeartbeatsErr uint64
    // last success latencies avg with simple EMA to keep it cheap
    LatencyEMA      float64
    lastSampledTime time.Time
}

func (q *PeerQuality) Score(now time.Time) float64 {
    // Reliability score favors success and penalizes failures; lower latency improves score.
    // Keep it simple and monotonic for ordering purposes.
    successes := float64(q.SuccessBlocks + q.HeartbeatsOK)
    failures := float64(q.FailedBlocks + q.HeartbeatsErr)
    denom := successes + failures
    rel := 0.0
    if denom > 0 {
        rel = successes / denom
    }
    // Latency term: map to [0,1] with soft cap at ~1s
    lat := q.LatencyEMA
    if lat <= 0 {
        lat = 0.5 // neutral when unknown
    } else {
        if lat > 1.5 {
            lat = 1.5
        }
        lat = 1.5 - lat
        lat = lat / 1.5
    }
    return 0.75*rel + 0.25*lat
}

// PeerRecord holds per-peer metadata.
type PeerRecord struct {
    ID             peer.ID
    Addrs          []string
    ConnType       ConnectionType
    LastSeen       time.Time
    Quality        PeerQuality
    // A small set of known content hashes the peer is believed to have.
    // Use string keys (content hash string) for compactness.
    content map[string]struct{}
}

// ProviderInfo is returned from lookups for a given content hash.
type ProviderInfo struct {
    ID       peer.ID
    ConnType ConnectionType
    Score    float64
    Addrs    []string
}

// Registry maintains mappings between peers and content, and peer connection state.
type Registry struct {
    mu sync.RWMutex
    // peerID -> record
    peers map[peer.ID]*PeerRecord
    // contentHash -> set(peerID)
    contentToPeers map[string]map[peer.ID]struct{}
}

func New() *Registry {
    return &Registry{
        peers:         make(map[peer.ID]*PeerRecord),
        contentToPeers: make(map[string]map[peer.ID]struct{}),
    }
}

// OnConnected records a connection event. Addrs may be nil. connType can be ConnUnknown.
func (r *Registry) OnConnected(pid peer.ID, addrs []string, connType ConnectionType) {
    now := time.Now()
    r.mu.Lock()
    defer r.mu.Unlock()
    rec, ok := r.peers[pid]
    if !ok {
        rec = &PeerRecord{ID: pid, content: make(map[string]struct{})}
        r.peers[pid] = rec
    }
    if len(addrs) > 0 {
        rec.Addrs = append([]string(nil), addrs...)
    }
    if connType != ConnUnknown {
        rec.ConnType = connType
    }
    rec.LastSeen = now
}

// OnDisconnected marks a peer as disconnected; we keep the record but update last seen.
func (r *Registry) OnDisconnected(pid peer.ID) {
    r.mu.Lock()
    if rec, ok := r.peers[pid]; ok {
        rec.LastSeen = time.Now()
    }
    r.mu.Unlock()
}

// UpdateConnectionType updates the current connection classification.
func (r *Registry) UpdateConnectionType(pid peer.ID, connType ConnectionType) {
    r.mu.Lock()
    if rec, ok := r.peers[pid]; ok {
        rec.ConnType = connType
        rec.LastSeen = time.Now()
    }
    r.mu.Unlock()
}

// RecordHave notes that a peer claims to have the content hash.
func (r *Registry) RecordHave(pid peer.ID, contentHash string) {
    r.mu.Lock()
    rec, ok := r.peers[pid]
    if !ok {
        rec = &PeerRecord{ID: pid, content: make(map[string]struct{})}
        r.peers[pid] = rec
    }
    rec.content[contentHash] = struct{}{}
    if _, ok := r.contentToPeers[contentHash]; !ok {
        r.contentToPeers[contentHash] = make(map[peer.ID]struct{})
    }
    r.contentToPeers[contentHash][pid] = struct{}{}
    rec.LastSeen = time.Now()
    r.mu.Unlock()
}

// RecordBlockSuccess updates quality metrics when we successfully receive a block from a peer.
func (r *Registry) RecordBlockSuccess(pid peer.ID, latency time.Duration) {
    r.mu.Lock()
    if rec, ok := r.peers[pid]; ok {
        rec.Quality.SuccessBlocks++
        // EMA update; smoothing factor 0.3
        l := latency.Seconds()
        if rec.Quality.LatencyEMA == 0 {
            rec.Quality.LatencyEMA = l
        } else {
            rec.Quality.LatencyEMA = 0.7*rec.Quality.LatencyEMA + 0.3*l
        }
        rec.LastSeen = time.Now()
    }
    r.mu.Unlock()
}

// RecordBlockFailure updates failure counters for a peer.
func (r *Registry) RecordBlockFailure(pid peer.ID) {
    r.mu.Lock()
    if rec, ok := r.peers[pid]; ok {
        rec.Quality.FailedBlocks++
        rec.LastSeen = time.Now()
    }
    r.mu.Unlock()
}

// RecordHeartbeat records result of a heartbeat attempt and optional RTT.
func (r *Registry) RecordHeartbeat(pid peer.ID, ok bool, rtt time.Duration) {
    r.mu.Lock()
    if rec, exists := r.peers[pid]; exists {
        if ok {
            rec.Quality.HeartbeatsOK++
            l := rtt.Seconds()
            if l > 0 {
                if rec.Quality.LatencyEMA == 0 {
                    rec.Quality.LatencyEMA = l
                } else {
                    rec.Quality.LatencyEMA = 0.8*rec.Quality.LatencyEMA + 0.2*l
                }
            }
        } else {
            rec.Quality.HeartbeatsErr++
        }
        rec.LastSeen = time.Now()
    }
    r.mu.Unlock()
}

// GetPeersForContent returns providers ordered by quality score, optionally filtering by direct connections.
func (r *Registry) GetPeersForContent(contentHash string, onlyDirect bool) []ProviderInfo {
    now := time.Now()
    r.mu.RLock()
    provSet := r.contentToPeers[contentHash]
    if len(provSet) == 0 {
        r.mu.RUnlock()
        return nil
    }
    out := make([]ProviderInfo, 0, len(provSet))
    for pid := range provSet {
        rec := r.peers[pid]
        if rec == nil {
            continue
        }
        if onlyDirect && rec.ConnType == ConnRelayed {
            continue
        }
        out = append(out, ProviderInfo{
            ID:       pid,
            ConnType: rec.ConnType,
            Score:    rec.Quality.Score(now),
            Addrs:    append([]string(nil), rec.Addrs...),
        })
    }
    r.mu.RUnlock()

    sort.Slice(out, func(i, j int) bool {
        // Prefer direct over relayed when scores are close; otherwise by score desc
        if out[i].ConnType != out[j].ConnType {
            if out[i].ConnType == ConnDirect && out[j].ConnType == ConnRelayed {
                return true
            }
            if out[j].ConnType == ConnDirect && out[i].ConnType == ConnRelayed {
                return false
            }
        }
        return out[i].Score > out[j].Score
    })
    return out
}

// GetPeerSnapshot returns a snapshot of all known peers with minimal details.
type PeerSnapshot struct {
    ID       string        `json:"id"`
    Addrs    []string      `json:"addrs"`
    Conn     string        `json:"conn_type"`
    Score    float64       `json:"score"`
    LastSeen time.Time     `json:"last_seen"`
    Content  []string      `json:"content,omitempty"`
}

func (r *Registry) Snapshot() []PeerSnapshot {
    now := time.Now()
    r.mu.RLock()
    out := make([]PeerSnapshot, 0, len(r.peers))
    for _, rec := range r.peers {
        conn := "unknown"
        switch rec.ConnType {
        case ConnDirect:
            conn = "direct"
        case ConnRelayed:
            conn = "relayed"
        }
        cs := make([]string, 0, len(rec.content))
        for h := range rec.content {
            cs = append(cs, h)
        }
        out = append(out, PeerSnapshot{
            ID:       rec.ID.String(),
            Addrs:    append([]string(nil), rec.Addrs...),
            Conn:     conn,
            Score:    rec.Quality.Score(now),
            LastSeen: rec.LastSeen,
            Content:  cs,
        })
    }
    r.mu.RUnlock()
    // Sort by score desc, direct first
    sort.Slice(out, func(i, j int) bool {
        if out[i].Conn != out[j].Conn {
            if out[i].Conn == "direct" && out[j].Conn == "relayed" {
                return true
            }
            if out[j].Conn == "direct" && out[i].Conn == "relayed" {
                return false
            }
        }
        return out[i].Score > out[j].Score
    })
    return out
}

// Helper to infer connection type by inspecting an address string.
func InferConnTypeFromAddr(addr string) ConnectionType {
    if strings.Contains(addr, "/p2p-circuit") {
        return ConnRelayed
    }
    return ConnDirect
}

