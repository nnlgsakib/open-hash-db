package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"openhashdb/api/pages"
	"openhashdb/core/block"
	"openhashdb/core/blockstore"
	"openhashdb/core/hasher"
	"openhashdb/core/merkle"
	"openhashdb/core/utils"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func isClientClosedError(err error) bool {
	// Check for common client-side disconnect errors
	return strings.Contains(err.Error(), "forcibly closed by the remote host") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "An established connection was aborted by the software in your host machine") ||
		strings.Contains(err.Error(), "use of closed network connection")
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func parseRangeHeader(s string, size int64) (int64, int64, error) {
	if s == "" {
		return 0, 0, nil // No range header
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return 0, 0, fmt.Errorf("invalid range header format")
	}
	s = s[len(b):]
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range header format")
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start value")
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		// If the end is empty, it means "to the end"
		if parts[1] == "" {
			end = size - 1
		} else {
			return 0, 0, fmt.Errorf("invalid end value")
		}
	}
	if start > end || start >= size {
		return 0, 0, fmt.Errorf("invalid range")
	}
	return start, end, nil
}

// isMimeTypeRenderable checks if a MIME type can be displayed directly by most browsers.
func (s *Server) isMimeTypeRenderable(mimeType string) bool {
	return strings.HasPrefix(mimeType, "text/") ||
		strings.HasPrefix(mimeType, "image/") ||
		strings.HasPrefix(mimeType, "audio/") ||
		strings.HasPrefix(mimeType, "video/") ||
		strings.HasPrefix(mimeType, "font/") ||
		mimeType == "application/pdf" ||
		mimeType == "application/javascript" ||
		mimeType == "application/json" ||
		mimeType == "application/ld+json" ||
		mimeType == "application/vnd.ms-fontobject" ||
		mimeType == "application/xml" ||
		mimeType == "application/xhtml+xml" ||
		mimeType == "application/wasm" ||
		mimeType == "application/vnd.apple.mpegurl"
}

// showDownloadPage displays a simple HTML page with a download button for non-renderable content.
func (s *Server) showDownloadPage(w http.ResponseWriter, hashStr, filename string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, pages.DownloadPage,
		hashStr, filename, hashStr, filename)
}

func (s *Server) showDirectoryListing(w http.ResponseWriter, r *http.Request, metadata *blockstore.ContentMetadata) {
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		s.writeJSON(w, http.StatusOK, metadata.Links)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	var html strings.Builder
	html.WriteString(fmt.Sprintf(pages.DirViewerPage, metadata.Filename, metadata.Filename, metadata.Hash.String()))

	for _, link := range metadata.Links {
		var linkHref, nameDisplay string
		if link.Type == "directory" {
			linkHref = fmt.Sprintf("/view/%s", link.Hash.String())
			nameDisplay = link.Name + "/"
		} else {
			linkHref = fmt.Sprintf("/download/%s", link.Hash.String())
			nameDisplay = link.Name
		}
		html.WriteString(fmt.Sprintf(`
			<tr>
				<td>%s</td>
				<td><a href="%s" title="%s">%s</a></td>
				<td>%d bytes</td>
				<td><a href="/info/%s" title="View details of %s">%s...</a></td>
			</tr>
		`, link.Type, linkHref, link.Name, nameDisplay, link.Size, link.Hash.String(), link.Name, link.Hash.String()[:16]))
	}

	html.WriteString(
		`
			</table>
		</body>
		</html>
	`)

	fmt.Fprint(w, html.String())
}

func (s *Server) storeUploadedFile(filename string, reader io.Reader, useEC bool) (hasher.Hash, int64, error) {
	if useEC {
		// Erasure Coding Path
		merkleFile, shards, err := merkle.BuildErasureCodedFileTree(reader, s.sharder)
		if err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to build erasure-coded merkle tree: %w", err)
		}

		for _, shard := range shards {
			if has, _ := s.storage.Has(shard.Hash()); !has {
				if err := s.storage.Put(shard); err != nil {
					return hasher.Hash{}, 0, fmt.Errorf("failed to store shard %s: %w", shard.Hash().String(), err)
				}
			}
		}

		metadata := &blockstore.ContentMetadata{
			Hash:            merkleFile.Root,
			Filename:        filename,
			MimeType:        utils.GetMimeType(filename),
			Size:            merkleFile.TotalSize,
			ModTime:         time.Now(),
			IsDirectory:     false,
			CreatedAt:       time.Now(),
			RefCount:        1,
			IsErasureCoded:  true,
			DataShards:      s.sharder.DataShardCount(),
			ParityShards:    s.sharder.ParityShardCount(),
			Chunks:          merkleFile.Chunks, // Shard info
		}

		// Store the metadata itself as a block
		metaBytes, err := json.Marshal(metadata)
		if err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to marshal metadata: %w", err)
		}
		if err := s.storage.Put(block.NewBlockWithHash(metadata.Hash, metaBytes)); err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata block: %w", err)
		}

		if err := s.storage.StoreContent(metadata); err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata: %w", err)
		}

		log.Printf("Successfully stored erasure-coded file %s with Merkle root %s", filename, merkleFile.Root.String())
		return merkleFile.Root, merkleFile.TotalSize, nil

	} else {
		// Chunking Path (existing logic)
		merkleFile, chunks, err := merkle.BuildFileTree(reader, s.chunker)
		if err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to build merkle tree: %w", err)
		}

		for _, chunk := range chunks {
			if has, _ := s.storage.Has(chunk.Hash); !has {
				if err := s.storage.Put(block.NewBlock(chunk.Data)); err != nil {
					return hasher.Hash{}, 0, fmt.Errorf("failed to store chunk %s: %w", chunk.Hash.String(), err)
				}
			}
		}

		metadata := &blockstore.ContentMetadata{
			Hash:        merkleFile.Root,
			Filename:    filename,
			MimeType:    utils.GetMimeType(filename),
			Size:        merkleFile.TotalSize,
			ModTime:     time.Now(),
			IsDirectory: false,
			CreatedAt:   time.Now(),
			RefCount:    1,
			Chunks:      merkleFile.Chunks,
		}

		// Store the metadata itself as a block
		metaBytes, err := json.Marshal(metadata)
		if err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to marshal metadata: %w", err)
		}
		if err := s.storage.Put(block.NewBlockWithHash(metadata.Hash, metaBytes)); err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata block: %w", err)
		}

		if err := s.storage.StoreContent(metadata); err != nil {
			return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata: %w", err)
		}

		log.Printf("Successfully stored file %s with Merkle root %s", filename, merkleFile.Root.String())
		return merkleFile.Root, merkleFile.TotalSize, nil
	}
}

func (s *Server) storeUploadedDirectory(path string, name string) (*merkle.Link, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var links []merkle.Link

	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		var link *merkle.Link

		if entry.IsDir() {
			link, err = s.storeUploadedDirectory(entryPath, entry.Name())
			if err != nil {
				return nil, err
			}
		} else {
			file, err := os.Open(entryPath)
			if err != nil {
				return nil, err
			}

			hash, size, err := s.storeUploadedFile(entry.Name(), file, false)
			file.Close()
			if err != nil {
				return nil, err
			}

			link = &merkle.Link{
				Name: entry.Name(),
				Hash: hash,
				Size: size,
				Type: "file",
			}
		}
		links = append(links, *link)
	}

	dirHash, err := merkle.BuildDirectoryTree(links)
	if err != nil {
		return nil, err
	}

	var totalSize int64
	for _, l := range links {
		totalSize += l.Size
	}

	dirMetadata := &blockstore.ContentMetadata{
		Hash:        dirHash,
		Filename:    name,
		MimeType:    "inode/directory",
		Size:        totalSize,
		ModTime:     time.Now(),
		IsDirectory: true,
		CreatedAt:   time.Now(),
		RefCount:    1,
		Links:       links,
	}

	// Store the metadata itself as a block
	metaBytes, err := json.Marshal(dirMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal directory metadata: %w", err)
	}
	if err := s.storage.Put(block.NewBlockWithHash(dirMetadata.Hash, metaBytes)); err != nil {
		return nil, fmt.Errorf("failed to store directory metadata block: %w", err)
	}

	if err := s.storage.StoreContent(dirMetadata); err != nil {
		return nil, err
	}

	return &merkle.Link{
		Name: name,
		Hash: dirHash,
		Size: totalSize,
		Type: "directory",
	}, nil
}

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
	}
}

// writeError writes an error response
func (s *Server) writeError(w http.ResponseWriter, status int, message string, err error) {
	errorMsg := message
	if err != nil {
		errorMsg = fmt.Sprintf("%s: %v", message, err)
		log.Printf("API Error: %s", errorMsg)
	}

	response := ErrorResponse{
		Error:   http.StatusText(status),
		Code:    status,
		Message: errorMsg,
	}

	s.writeJSON(w, status, response)
}
