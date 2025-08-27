package intelligence

import (
    "time"

    "github.com/libp2p/go-libp2p/core/peer"
    "openhashdb/core/hasher"
)

// PeerType indicates whether a peer is directly connected or reachable only via relay
type PeerType int

const (
    PeerTypeUnknown PeerType = iota
    PeerTypeDirect
    PeerTypeRelayed
)

// ProviderMeta stores metadata about a peer that can provide certain content
type ProviderMeta struct {
    PeerID   peer.ID
    Type     PeerType
    Score    float64
    Latency  time.Duration
    LastSeen time.Time
}

// ProviderHint captures a peer -> content relationship hint
type ProviderHint struct {
    PeerID peer.ID
    Hash   hasher.Hash
    Type   PeerType
}

