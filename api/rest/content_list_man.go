package rest

import (
	"encoding/hex"
	"net/http"
)

// listContent lists all content
func (s *Server) listContent(w http.ResponseWriter, r *http.Request) {
	hashes, err := s.storage.ListContent()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to list content", err)
		return
	}

	jsonContentList := make([]*JSONContentInfo, 0, len(hashes))
	for _, hash := range hashes {
		metadata, err := s.storage.GetContent(hash)
		if err != nil {
			continue // Skip invalid entries
		}

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
		jsonContentList = append(jsonContentList, info)
	}

	s.writeJSON(w, http.StatusOK, jsonContentList)
}