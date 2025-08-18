package rest

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"openhashdb/core/blockstore"
	"openhashdb/core/merkle"
	"path/filepath"
)

// streamDirectoryAsZip creates zip files with optimized streaming
func (s *Server) streamDirectoryAsZip(w http.ResponseWriter, r *http.Request, metadata *blockstore.ContentMetadata) {
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", metadata.Filename))

	// Use Transfer-Encoding: chunked for streaming
	w.Header().Set("Transfer-Encoding", "chunked")

	// Create a pipe for streaming zip data
	pr, pw := io.Pipe()
	defer pr.Close()

	// Start zip creation in a goroutine
	go func() {
		defer pw.Close()
		zipWriter := zip.NewWriter(pw)
		defer zipWriter.Close()

		if err := s.addFilesToZipOptimized(r.Context(), zipWriter, metadata.Links, ""); err != nil {
			log.Printf("Error creating zip archive for %s: %v", metadata.Hash.String(), err)
		}
	}()

	// Stream the zip data
	buffer := s.bufferPool.Get().([]byte)
	defer s.bufferPool.Put(buffer)

	for {
		n, err := pr.Read(buffer)
		if n > 0 {
			if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
				if !isClientClosedError(writeErr) {
					log.Printf("Error writing zip data: %v", writeErr)
				}
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			if !isClientClosedError(err) {
				log.Printf("Error reading zip data: %v", err)
			}
			return
		}
	}
}

// addFilesToZipOptimized adds files to zip with concurrent processing
func (s *Server) addFilesToZipOptimized(ctx context.Context, zipWriter *zip.Writer, links []merkle.Link, basePath string) error {
	// Use semaphore to limit concurrent operations
	sem := make(chan struct{}, maxConcurrentOps)

	for _, link := range links {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		pathInZip := filepath.Join(basePath, link.Name)

		if link.Type == "directory" {
			// Create a directory entry in the zip file
			_, err := zipWriter.Create(pathInZip + "/")
			if err != nil {
				<-sem
				return fmt.Errorf("failed to create directory in zip: %w", err)
			}

			// Get the metadata for the subdirectory to recurse
			dirMetadata, err := s.storage.GetContent(link.Hash)
			if err != nil {
				<-sem
				return fmt.Errorf("could not get metadata for subdirectory %s (%s): %w", link.Name, link.Hash.String(), err)
			}

			<-sem
			if err := s.addFilesToZipOptimized(ctx, zipWriter, dirMetadata.Links, pathInZip); err != nil {
				return err
			}
		} else { // "file"
			fileWriter, err := zipWriter.Create(pathInZip)
			if err != nil {
				<-sem
				return fmt.Errorf("failed to create file in zip: %w", err)
			}

			// Get file metadata to access its chunks
			fileMetadata, err := s.storage.GetContent(link.Hash)
			if err != nil {
				<-sem
				return fmt.Errorf("could not get metadata for file %s (%s): %w", link.Name, link.Hash.String(), err)
			}

			// Prefetch all chunks for this file concurrently
			if err := s.prefetchChunks(ctx, fileMetadata.Chunks); err != nil {
				<-sem
				return fmt.Errorf("failed to prefetch chunks for file %s: %w", pathInZip, err)
			}

			// Stream the chunks of the file into the zip writer
			for _, chunkInfo := range fileMetadata.Chunks {
				if err := s.fetchAndStreamChunkOptimized(ctx, fileWriter, chunkInfo.Hash, 0, int(chunkInfo.Size)); err != nil {
					<-sem
					return fmt.Errorf("failed to stream chunk to zip for file %s: %w", pathInZip, err)
				}
			}
			<-sem
		}
	}
	return nil
}
