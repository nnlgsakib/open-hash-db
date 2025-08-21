package rest

import (
	"net/http"

	"openhashdb/protobuf/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// healthCheck returns server health status
func (s *Server) healthCheck(w http.ResponseWriter, r *http.Request) {
	health := &pb.HealthResponse{
		Status:    "healthy",
		Timestamp: timestamppb.Now(),
		Version:   "1.0.0",
	}

	s.writeJSON(w, http.StatusOK, health)
}