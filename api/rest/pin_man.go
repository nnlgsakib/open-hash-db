package rest

import (
    "net/http"

    cid "github.com/ipfs/go-cid"
    "openhashdb/core/cidutil"
    "openhashdb/core/hasher"
    "openhashdb/protobuf/pb"

    "github.com/gorilla/mux"
)

// pinContent pins content
func (s *Server) pinContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	if err := s.replicator.PinContent(hash); err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to pin content", err)
		return
	}

	response := &pb.PinResponse{
		Hash:    hash.String(),
		Message: "Content pinned successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// unpinContent unpins content
func (s *Server) unpinContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	if err := s.replicator.UnpinContent(hash); err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to unpin content", err)
		return
	}

	response := &pb.PinResponse{
		Hash:    hash.String(),
		Message: "Content unpinned successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// listPins lists all pinned content hashes
func (s *Server) listPins(w http.ResponseWriter, r *http.Request) {
    pins := s.replicator.GetPinnedContent()
    s.writeJSON(w, http.StatusOK, pins)
}

// pinContentByCID resolves CID to hash and pins the content
func (s *Server) pinContentByCID(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    cidStr := vars["cid"]
    c, err := cid.Parse(cidStr)
    if err != nil { s.writeError(w, http.StatusBadRequest, "Invalid cid", err); return }
    h, err := cidutil.ToHash(c)
    if err != nil { s.writeError(w, http.StatusBadRequest, "Unsupported cid", err); return }
    r = mux.SetURLVars(r, map[string]string{"hash": h.String()})
    s.pinContent(w, r)
}

// unpinContentByCID resolves CID to hash and unpins the content
func (s *Server) unpinContentByCID(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    cidStr := vars["cid"]
    c, err := cid.Parse(cidStr)
    if err != nil { s.writeError(w, http.StatusBadRequest, "Invalid cid", err); return }
    h, err := cidutil.ToHash(c)
    if err != nil { s.writeError(w, http.StatusBadRequest, "Unsupported cid", err); return }
    r = mux.SetURLVars(r, map[string]string{"hash": h.String()})
    s.unpinContent(w, r)
}
