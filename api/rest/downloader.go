package rest

import (
    "fmt"
    "log"
    "net/http"
    "strconv"

    "openhashdb/core/hasher"
    cid "github.com/ipfs/go-cid"
    "openhashdb/core/cidutil"

	"github.com/gorilla/mux"
)

// downloadContent handles content downloads with optimized streaming
func (s *Server) downloadContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	metadata, err := s.storage.GetContent(hash)
	if err != nil {
		// If not found locally, try to fetch from the network
		log.Printf("Content %s not found locally, attempting to fetch from network...", hashStr)
		if err := s.replicator.FetchAndStore(hash); err != nil {
			s.writeError(w, http.StatusNotFound, "Content not found on the network", err)
			return
		}
		// Try getting content again after fetching
		metadata, err = s.storage.GetContent(hash)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to get content after fetching", err)
			return
		}
	}

	if metadata.IsDirectory {
		s.streamDirectoryAsZip(w, r, metadata)
		return
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Filename))
	w.Header().Set("Content-Type", metadata.MimeType)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))

	s.streamChunksOptimized(w, r, metadata)
}

// downloadContentByCID resolves CID to hash and uses the same logic as downloadContent.
func (s *Server) downloadContentByCID(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    cidStr := vars["cid"]
    c, err := cid.Parse(cidStr)
    if err != nil {
        s.writeError(w, http.StatusBadRequest, "Invalid cid", err)
        return
    }
    h, err := cidutil.ToHash(c)
    if err != nil {
        s.writeError(w, http.StatusBadRequest, "Unsupported cid multihash", err)
        return
    }
    r = mux.SetURLVars(r, map[string]string{"hash": h.String()})
    s.downloadContent(w, r)
}
