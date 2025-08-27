package rest

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"openhashdb/core/hasher"
	"openhashdb/network/bitswap"
	"openhashdb/protobuf/pb"
)

// streamChunksOptimized provides optimized chunk streaming with prefetching
func (s *Server) streamChunksOptimized(w http.ResponseWriter, r *http.Request, metadata *pb.ContentMetadata) {
	ctx := r.Context()

	if metadata.IsErasureCoded {
		s.streamErasureCodedContent(w, r, metadata)
		return
	}

	flusher, hasFlusher := w.(http.Flusher)

	// Set common headers
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", metadata.MimeType)
	w.Header().Set("Last-Modified", metadata.ModTime.AsTime().UTC().Format(http.TimeFormat))
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
		log.Printf("Warning: Failed to prefetch chunks for %s: %v", string(metadata.Hash), err)
		// Continue without prefetching
	}

	// Stream chunks with optimized buffering
	buffer := s.bufferPool.Get().([]byte)
	defer s.bufferPool.Put(buffer)

	for _, chunkInfo := range metadata.Chunks {
		chunkHash, err := hasher.HashFromBytes(chunkInfo.Hash)
		if err != nil {
			log.Printf("Aborting stream for %s due to invalid chunk hash: %v", string(metadata.Hash), err)
			return
		}
		if err := s.streamChunkWithBuffer(ctx, w, chunkHash, 0, int(chunkInfo.Size), buffer); err != nil {
			if !isClientClosedError(err) {
				log.Printf("Aborting stream for %s due to error: %v", string(metadata.Hash), err)
			}
			return
		}
		if hasFlusher {
			flusher.Flush()
		}
	}
}

func (s *Server) streamErasureCodedContent(w http.ResponseWriter, r *http.Request, metadata *pb.ContentMetadata) {
	log.Printf("Streaming erasure-coded content %s", string(metadata.Hash))

	// 1. Collect all shard hashes
	shardHashes := make([]hasher.Hash, len(metadata.Chunks))
	for i, shardInfo := range metadata.Chunks {
		h, err := hasher.HashFromBytes(shardInfo.Hash)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Invalid shard hash", err)
			return
		}
		shardHashes[i] = h
	}

	// 2. Fetch enough shards to reconstruct
	shardsToFetch := int(metadata.DataShards)
	bitswapNode, ok := s.node.(interface{ GetBitswap() *bitswap.Engine })
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "Node does not support bitswap", nil)
		return
	}

	blockChannel, err := bitswapNode.GetBitswap().GetBlocks(r.Context(), shardHashes)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to start bitswap session for shards", err)
		return
	}

	// 3. Collect shards
	availableShards := make([][]byte, len(shardHashes))
	shardMap := make(map[hasher.Hash]int)
	for i, h := range shardHashes {
		shardMap[h] = i
	}

	receivedCount := 0
	for blk := range blockChannel {
		if idx, ok := shardMap[blk.Hash()]; ok {
			availableShards[idx] = blk.RawData()
			receivedCount++
			if receivedCount >= shardsToFetch {
				go func() {
					for range blockChannel {}
				}()
				break
			}
		}
	}

	if receivedCount < shardsToFetch {
		s.writeError(w, http.StatusNotFound, "Could not retrieve enough shards to reconstruct file", nil)
		return
	}

	// 4. Reconstruct the data
	reconstructedData, err := s.sharder.Reconstruct(availableShards)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to reconstruct file from shards", err)
		return
	}

	// Trim padding to original file size
	if int64(len(reconstructedData)) > metadata.Size {
		reconstructedData = reconstructedData[:metadata.Size]
	}

	// 5. Handle range request and stream
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		start, end, err := parseRangeHeader(rangeHeader, metadata.Size)
		if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid range header", err)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, metadata.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(reconstructedData[start : end+1])
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(reconstructedData)
	}
}

// getChunksForRange returns chunks needed for a specific byte range
func (s *Server) getChunksForRange(chunks []*pb.ChunkInfo, start, end int64) []*pb.ChunkInfo {
	var result []*pb.ChunkInfo
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
func (s *Server) prefetchChunks(ctx context.Context, chunks []*pb.ChunkInfo) error {
	if len(chunks) == 0 {
		return nil
	}

	// Use semaphore to limit concurrent fetches
	sem := make(chan struct{}, maxConcurrentOps)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, chunkInfo := range chunks {
		chunkHash, err := hasher.HashFromBytes(chunkInfo.Hash)
		if err != nil {
			continue
		}
		// Skip if already in cache
		if _, exists := s.chunkCache.Get(chunkHash.String()); exists {
			continue
		}

		// Skip if already in local storage
		if has, _ := s.storage.Has(chunkHash); has {
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
		}(chunkHash)
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

    // Clamp bounds to prevent panics in case of size mismatches
    if offset < 0 {
        offset = 0
    }
    if offset > len(data) {
        offset = len(data)
    }
    end := offset + length
    if end > len(data) {
        end = len(data)
    }
    // Write the requested part of the chunk
    if _, err := w.Write(data[offset:end]); err != nil {
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
