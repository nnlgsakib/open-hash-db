package rest

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"openhashdb/core/blockstore"
	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"openhashdb/network/bitswap"
	"strconv"
	"sync"
	"time"
)

// streamChunksOptimized provides optimized chunk streaming with prefetching
func (s *Server) streamChunksOptimized(w http.ResponseWriter, r *http.Request, metadata *blockstore.ContentMetadata) {
	ctx := r.Context()
	flusher, hasFlusher := w.(http.Flusher)

	// Set common headers
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", metadata.MimeType)
	w.Header().Set("Last-Modified", metadata.ModTime.UTC().Format(http.TimeFormat))
	w.Header().Set("Cache-Control", "public, max-age=31536000") // Cache for 1 year (immutable content)

	// Handle Range requests
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		s.handleRangeRequestOptimized(ctx, w, r, metadata, rangeHeader)
		return
	}

	// No Range header, stream the full content
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	w.WriteHeader(http.StatusOK)

	// Prefetch chunks for better performance
	if err := s.prefetchChunks(ctx, metadata.Chunks); err != nil {
		log.Printf("Warning: Failed to prefetch chunks for %s: %v", metadata.Hash, err)
		// Continue without prefetching
	}

	// Stream chunks with optimized buffering
	buffer := s.bufferPool.Get().([]byte)
	defer s.bufferPool.Put(buffer)

	for _, chunkInfo := range metadata.Chunks {
		if err := s.streamChunkWithBuffer(ctx, w, chunkInfo.Hash, 0, int(chunkInfo.Size), buffer); err != nil {
			if !isClientClosedError(err) {
				log.Printf("Aborting stream for %s due to error: %v", metadata.Hash, err)
			}
			return
		}
		if hasFlusher {
			flusher.Flush()
		}
	}
}

// getChunksForRange returns chunks needed for a specific byte range
func (s *Server) getChunksForRange(chunks []chunker.ChunkInfo, start, end int64) []chunker.ChunkInfo {
	var result []chunker.ChunkInfo
	var offset int64

	for _, chunk := range chunks {
		chunkEnd := offset + int64(chunk.Size) - 1
		if offset <= end && chunkEnd >= start {
			result = append(result, chunk)
		}
		offset += int64(chunk.Size)
		if offset > end {
			break
		}
	}
	return result
}

// prefetchChunks fetches multiple chunks concurrently
func (s *Server) prefetchChunks(ctx context.Context, chunks []chunker.ChunkInfo) error {
	if len(chunks) == 0 {
		return nil
	}

	// Use semaphore to limit concurrent fetches
	sem := make(chan struct{}, maxConcurrentOps)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, chunkInfo := range chunks {
		// Skip if already in cache
		if _, exists := s.chunkCache.Get(chunkInfo.Hash.String()); exists {
			continue
		}

		// Skip if already in local storage
		if has, _ := s.storage.Has(chunkInfo.Hash); has {
			continue
		}

		wg.Add(1)
		go func(hash hasher.Hash) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
				defer func() { <-sem }()
			}

			if err := s.fetchChunk(ctx, hash); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				log.Printf("Failed to prefetch chunk %s: %v", hash, err)
			}
		}(chunkInfo.Hash)
	}

	wg.Wait()
	return firstErr
}

// fetchChunk fetches a single chunk and stores it
func (s *Server) fetchChunk(ctx context.Context, hash hasher.Hash) error {
	if bitswapNode, ok := s.node.(interface{ GetBitswap() *bitswap.Engine }); ok {
		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		if _, err := bitswapNode.GetBitswap().GetBlock(fetchCtx, hash); err != nil {
			return fmt.Errorf("failed to fetch chunk %s: %w", hash, err)
		}
		return nil
	}
	return fmt.Errorf("node does not support bitswap, cannot fetch chunk %s", hash)
}

// streamChunkWithBuffer streams a chunk using a reusable buffer
func (s *Server) streamChunkWithBuffer(ctx context.Context, w io.Writer, hash hasher.Hash, offset, length int, buffer []byte) error {
	// Check cache first
	if data, exists := s.chunkCache.Get(hash.String()); exists {
		_, err := w.Write(data[offset : offset+length])
		return err
	}

	// Check if the block is in local storage
	if has, _ := s.storage.Has(hash); !has {
		if err := s.fetchChunk(ctx, hash); err != nil {
			return err
		}
	}

	// Retrieve the block from storage
	blk, err := s.storage.Get(hash)
	if err != nil {
		return fmt.Errorf("failed to get block %s from storage: %w", hash, err)
	}

	data := blk.RawData()

	// Add to cache for future use
	s.chunkCache.Put(hash.String(), data)

	// Write the requested part of the chunk
	if _, err := w.Write(data[offset : offset+length]); err != nil {
		return err
	}

	return nil
}

// fetchAndStreamChunkOptimized is an optimized version of fetchAndStreamChunk
func (s *Server) fetchAndStreamChunkOptimized(ctx context.Context, w io.Writer, hash hasher.Hash, offset, length int) error {
	buffer := s.bufferPool.Get().([]byte)
	defer s.bufferPool.Put(buffer)

	return s.streamChunkWithBuffer(ctx, w, hash, offset, length, buffer)
}
