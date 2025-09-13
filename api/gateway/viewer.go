package gateway

import (
    "log"
    "net/http"
    "strconv"

    "openhashdb/core/hasher"

    "github.com/gorilla/mux"
)

// viewContent handles content viewing with optimization
func (s *Server) viewContent(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    hashStr := vars["hash"]

    hash, err := hasher.HashFromString(hashStr)
    if err != nil { s.writeError(w, http.StatusBadRequest, "Invalid hash", err); return }

    metadata, err := s.storage.GetContent(hash)
    if err != nil {
        log.Printf("Content %s not found locally, attempting to fetch from network...", hashStr)
        if err := s.replicator.FetchAndStore(hash); err != nil { s.writeError(w, http.StatusNotFound, "Content not found on the network", err); return }
        metadata, err = s.storage.GetContent(hash)
        if err != nil { s.writeError(w, http.StatusInternalServerError, "Failed to get content after fetching", err); return }
    }
    if metadata.IsDirectory { s.showDirectoryListing(w, r, metadata); return }
    if !s.isMimeTypeRenderable(metadata.MimeType) { s.showDownloadPage(w, hashStr, metadata.Filename); return }
    w.Header().Set("Content-Disposition", "inline")
    w.Header().Set("Content-Type", metadata.MimeType)
    w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
    w.Header().Set("ETag", hashStr)
    s.streamChunksOptimized(w, r, metadata)
}

