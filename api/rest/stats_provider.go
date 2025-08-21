package rest

import (
	"net/http"

	"openhashdb/protobuf/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// getNetworkStats returns network statistics
func (s *Server) getNetworkStats(w http.ResponseWriter, r *http.Request) {
	if s.node == nil {
		s.writeError(w, http.StatusServiceUnavailable, "Network node not available", nil)
		return
	}

	if nodeWithStats, ok := s.node.(interface{ GetNetworkStats() *pb.NetworkStatsResponse }); ok {
		stats := nodeWithStats.GetNetworkStats()
		s.writeJSON(w, http.StatusOK, stats)
	} else {
		s.writeError(w, http.StatusServiceUnavailable, "Network stats not supported by this node type", nil)
	}
}

// getStats returns system statistics
func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	content, err := s.storage.ListContent()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to get storage stats", err)
		return
	}
	storageStats := &pb.StorageStats{ContentCount: int64(len(content))}

	replicationStats := s.replicator.GetStats()

	stats := &pb.StatsResponse{
		Storage:     storageStats,
		Replication: replicationStats,
		Timestamp:   timestamppb.Now(),
	}

	s.writeJSON(w, http.StatusOK, stats)
}