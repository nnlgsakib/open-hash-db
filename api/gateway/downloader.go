package gateway

import (
    "fmt"
    "log"
    "net/http"
    "strconv"

    "openhashdb/core/hasher"

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

    // Common headers
    w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Filename))
    w.Header().Set("Content-Type", metadata.MimeType)
    w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
    w.Header().Set("ETag", hashStr)

    if r.Method == http.MethodHead {
        // HEAD: headers only
        w.WriteHeader(http.StatusOK)
        return
    }

    s.streamChunksOptimized(w, r, metadata)
}

