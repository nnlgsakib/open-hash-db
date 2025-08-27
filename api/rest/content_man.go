package rest

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"

	"openhashdb/core/hasher"
	"openhashdb/network/libp2p"
	"openhashdb/protobuf/pb"

	"github.com/gorilla/mux"
	"google.golang.org/protobuf/proto"
)

// getContentInfo returns information about content
func (s *Server) getContentInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	// Try to get content from local storage first
	metadata, err := s.storage.GetContent(hash)
	if err == nil {
		// Content found locally
		chunks := make([]*JSONChunkInfo, len(metadata.Chunks))
		for i, c := range metadata.Chunks {
			chunks[i] = &JSONChunkInfo{
				Hash: hex.EncodeToString(c.Hash),
				Size: c.Size,
			}
		}

		links := make([]*JSONLink, len(metadata.Links))
		for i, l := range metadata.Links {
			links[i] = &JSONLink{
				Name: l.Name,
				Hash: hex.EncodeToString(l.Hash),
				Size: l.Size,
				Type: l.Type,
			}
		}

		info := &JSONContentInfo{
			Hash:        hex.EncodeToString(metadata.Hash),
			Filename:    metadata.Filename,
			MimeType:    metadata.MimeType,
			Size:        metadata.Size,
			ModTime:     metadata.ModTime.AsTime(),
			IsDirectory: metadata.IsDirectory,
			CreatedAt:   metadata.CreatedAt.AsTime(),
			RefCount:    metadata.RefCount,
			Chunks:      chunks,
			Links:       links,
		}
		s.writeJSON(w, http.StatusOK, info)
		return
	}

	// Content not found locally, try to fetch metadata via delegation
	log.Printf("Content %s not found locally, checking network for metadata...", hashStr)
	fetchedMetadata, err := s.fetchMetadata(r.Context(), hash)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "Content not found on the network", err)
		return
	}
	log.Printf("Metadata for %s found on network", hashStr)

	// Store content metadata to make it available for future local lookups
	if err := s.storage.StoreContent(fetchedMetadata); err != nil {
		log.Printf("Warning: failed to store fetched metadata for %s: %v", hash.String(), err)
	}

	// Start background replication of all content chunks
	go func() {
		log.Printf("Starting background replication for %s...", hash.String())
		if err := s.replicator.FetchAndStore(hash); err != nil {
			log.Printf("Background replication for %s failed: %v", hash.String(), err)
		} else {
			log.Printf("Background replication for %s completed successfully.", hash.String())
		}
	}()

	// Respond to the user immediately
	chunks := make([]*JSONChunkInfo, len(fetchedMetadata.Chunks))
	for i, c := range fetchedMetadata.Chunks {
		chunks[i] = &JSONChunkInfo{
			Hash: hex.EncodeToString(c.Hash),
			Size: c.Size,
		}
	}

	links := make([]*JSONLink, len(fetchedMetadata.Links))
	for i, l := range fetchedMetadata.Links {
		links[i] = &JSONLink{
			Name: l.Name,
			Hash: hex.EncodeToString(l.Hash),
			Size: l.Size,
			Type: l.Type,
		}
	}
	info := &JSONContentInfo{
		Hash:        hex.EncodeToString(fetchedMetadata.Hash),
		Filename:    fetchedMetadata.Filename,
		MimeType:    fetchedMetadata.MimeType,
		Size:        fetchedMetadata.Size,
		ModTime:     fetchedMetadata.ModTime.AsTime(),
		IsDirectory: fetchedMetadata.IsDirectory,
		CreatedAt:   fetchedMetadata.CreatedAt.AsTime(),
		RefCount:    fetchedMetadata.RefCount,
		Chunks:      chunks,
		Links:       links,
		Message:     "Not found locally. Found on network, replicating in background.",
	}

	s.writeJSON(w, http.StatusOK, info)
}

// fetchMetadata attempts to retrieve metadata for a content root using delegated metadata requests.
func (s *Server) fetchMetadata(ctx context.Context, hash hasher.Hash) (*pb.ContentMetadata, error) {
	// If already present, return it
	if s.storage.HasContent(hash) {
		return s.storage.GetContent(hash)
	}
	libp2pNode, ok := s.node.(*libp2p.Node)
	if !ok {
		return nil, fmt.Errorf("node does not support delegated metadata")
	}
	// Prefer connected peers; try a small fanout
	for _, p := range libp2pNode.ConnectedPeers() {
		data, err := libp2pNode.RequestDelegatedMetadata(ctx, p, hash)
		if err != nil || len(data) == 0 {
			continue
		}
		var meta pb.ContentMetadata
		if err := proto.Unmarshal(data, &meta); err != nil {
			continue
		}
		return &meta, nil
	}
	return nil, fmt.Errorf("metadata not found via delegation")
}
