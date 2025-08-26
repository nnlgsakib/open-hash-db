package rest

import (
    "encoding/json"
    "net/http"

    "github.com/gorilla/mux"
    "openhashdb/network/libp2p"
)

// getPeersSnapshot returns a JSON snapshot of known peers and their scores/connection types.
func (s *Server) getPeersSnapshot(w http.ResponseWriter, r *http.Request) {
    node, ok := s.node.(*libp2p.Node)
    if !ok || node == nil {
        http.Error(w, "node not available", http.StatusServiceUnavailable)
        return
    }
    reg := node.PeerRegistry()
    if reg == nil {
        http.Error(w, "peer registry not available", http.StatusServiceUnavailable)
        return
    }
    snap := reg.Snapshot()
    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(snap)
}

// getProvidersForHash lists peers believed to have a given content hash.
func (s *Server) getProvidersForHash(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    h := vars["hash"]
    node, ok := s.node.(*libp2p.Node)
    if !ok || node == nil {
        http.Error(w, "node not available", http.StatusServiceUnavailable)
        return
    }
    reg := node.PeerRegistry()
    if reg == nil {
        http.Error(w, "peer registry not available", http.StatusServiceUnavailable)
        return
    }
    providers := reg.GetPeersForContent(h, false)
    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(providers)
}

