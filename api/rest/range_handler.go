package rest

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"openhashdb/core/hasher"
	"openhashdb/protobuf/pb"
)

// handleRangeRequestOptimized handles range requests with optimization
func (s *Server) handleRangeRequestOptimized(ctx context.Context, w http.ResponseWriter, r *http.Request, metadata *pb.ContentMetadata, rangeHeader string) {
	flusher, hasFlusher := w.(http.Flusher)
	start, end, err := parseRangeHeader(rangeHeader, metadata.Size)
	if err != nil {
		s.writeError(w, http.StatusRequestedRangeNotSatisfiable, "Invalid Range header", err)
		return
	}
	contentLength := end - start + 1

	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, metadata.Size))
	w.Header().Set("Cache-Control", "public, max-age=31536000")
	w.WriteHeader(http.StatusPartialContent)

	// Identify chunks needed for this range and prefetch them
	neededChunks := s.getChunksForRange(metadata.Chunks, start, end)
	if err := s.prefetchChunks(ctx, neededChunks); err != nil {
		log.Printf("Warning: Failed to prefetch range chunks for %s: %v", string(metadata.Hash), err)
	}

	buffer := s.bufferPool.Get().([]byte)
	defer s.bufferPool.Put(buffer)

	var currentOffset int64
	for _, chunkInfo := range metadata.Chunks {
		chunkStart := currentOffset
		chunkEnd := currentOffset + int64(chunkInfo.Size) - 1

		// Check if this chunk is within the requested range
		if chunkStart <= end && chunkEnd >= start {
			streamStart := max(chunkStart, start)
			streamEnd := min(chunkEnd, end)
			offsetInChunk := streamStart - chunkStart
			lengthToStream := streamEnd - streamStart + 1

			chunkHash, err := hasher.HashFromBytes(chunkInfo.Hash)
			if err != nil {
				log.Printf("Aborting ranged stream for %s due to invalid chunk hash: %v", string(metadata.Hash), err)
				return
			}

			if err := s.streamChunkWithBuffer(ctx, w, chunkHash, int(offsetInChunk), int(lengthToStream), buffer); err != nil {
				if !isClientClosedError(err) {
					log.Printf("Aborting ranged stream for %s due to error: %v", string(metadata.Hash), err)
				}
				return
			}
		}

		currentOffset += int64(chunkInfo.Size)
		if currentOffset > end {
			break
		}
	}

	if hasFlusher {
		flusher.Flush()
	}
}