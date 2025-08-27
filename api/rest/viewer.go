package rest

import (
    "log"
    "net/http"
    "strconv"

    "openhashdb/core/hasher"
    cid "github.com/ipfs/go-cid"
    "openhashdb/core/cidutil"

	"github.com/gorilla/mux"
)

// viewContent handles content viewing with optimization
func (s *Server) viewContent(w http.ResponseWriter, r *http.Request) {
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
		s.showDirectoryListing(w, r, metadata)
		return
	}

	if !s.isMimeTypeRenderable(metadata.MimeType) {
		s.showDownloadPage(w, hashStr, metadata.Filename)
		return
	}

	w.Header().Set("Content-Disposition", "inline")
	w.Header().Set("Content-Type", metadata.MimeType)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))

	s.streamChunksOptimized(w, r, metadata)
}

// viewContentByCID resolves CID to hash and uses the same logic as viewContent.
func (s *Server) viewContentByCID(w http.ResponseWriter, r *http.Request) {
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
    s.viewContent(w, r)
}
