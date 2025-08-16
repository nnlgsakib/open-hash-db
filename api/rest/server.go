package rest

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"openhashdb/core/block"
	"openhashdb/core/blockstore"
	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"openhashdb/core/merkle"
	"openhashdb/core/utils"
	"openhashdb/network/bitswap"
	"openhashdb/network/libp2p"
	"openhashdb/network/replicator"
	"openhashdb/network/streammanager"
	"openhashdb/openhashdb-ui"

	"github.com/gorilla/mux"
)

// Server represents the REST API server
type Server struct {
	storage    *blockstore.Blockstore
	replicator *replicator.Replicator
	streamer   *streammanager.StreamManager
	chunker    *chunker.Chunker
	node       interface{} // libp2p node for network stats
	router     *mux.Router
	server     *http.Server
}

// UploadResponse represents the response from upload operations
type UploadResponse struct {
	Hash     string `json:"hash"`
	Size     int64  `json:"size"`
	Filename string `json:"filename,omitempty"`
	Message  string `json:"message"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ContentInfo represents content information
type ContentInfo struct {
	Hash        string              `json:"hash"`
	Filename    string              `json:"filename"`
	MimeType    string              `json:"mime_type"`
	Size        int64               `json:"size"`
	ModTime     time.Time           `json:"mod_time"`
	IsDirectory bool                `json:"is_directory"`
	CreatedAt   time.Time           `json:"created_at"`
	RefCount    int                 `json:"ref_count"`
	Chunks      []chunker.ChunkInfo `json:"chunks,omitempty"`
	Links       []merkle.Link       `json:"links,omitempty"`
}

// NewServer creates a new REST API server
func NewServer(bs *blockstore.Blockstore, replicator *replicator.Replicator, node interface{}) *Server {
	s := &Server{
		storage:    bs,
		replicator: replicator,
		chunker:    chunker.NewChunker(),
		node:       node,
		router:     mux.NewRouter(),
	}

	if libp2pNode, ok := node.(*libp2p.Node); ok {
		s.streamer = streammanager.NewStreamManager(libp2pNode)
	}

	s.setupRoutes()
	return s
}

// setupRoutes sets up the API routes
func (s *Server) setupRoutes() {
	// Enable CORS for all routes
	s.router.Use(s.corsMiddleware)

	// Upload endpoints
	s.router.HandleFunc("/upload/file", s.uploadFile).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/upload/folder", s.uploadFolder).Methods("POST", "OPTIONS")

	// Download endpoints
	s.router.HandleFunc("/download/{hash}", s.downloadContent).Methods("GET", "OPTIONS")
	s.router.HandleFunc("/view/{hash}", s.viewContent).Methods("GET", "OPTIONS")

	// Info endpoints
	s.router.HandleFunc("/info/{hash}", s.getContentInfo).Methods("GET", "OPTIONS")
	s.router.HandleFunc("/list", s.listContent).Methods("GET", "OPTIONS")
	s.router.HandleFunc("/stats", s.getStats).Methods("GET", "OPTIONS")
	s.router.HandleFunc("/network", s.getNetworkStats).Methods("GET", "OPTIONS")

	// Pin endpoints
	s.router.HandleFunc("/pin/{hash}", s.pinContent).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/unpin/{hash}", s.unpinContent).Methods("DELETE", "OPTIONS")
	s.router.HandleFunc("/pins", s.listPins).Methods("GET", "OPTIONS")

	// Health check
	s.router.HandleFunc("/health", s.healthCheck).Methods("GET", "OPTIONS")
	//web
	s.router.PathPrefix("/").Handler(openhashdb.GetHandler())
}

// corsMiddleware adds CORS headers
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Start starts the server
func (s *Server) Start(addr string) error {
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Printf("Starting REST API server on %s", addr)
	return s.server.ListenAndServe()
}

// Stop stops the server
func (s *Server) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// uploadFile handles single file uploads
func (s *Server) uploadFile(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "No file provided", err)
		return
	}
	defer file.Close()

	hash, size, err := s.storeUploadedFile(header.Filename, file)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store file", err)
		return
	}

	// Announce to network
	if err := s.replicator.AnnounceContent(hash, size); err != nil {
		log.Printf("Failed to announce content: %v", err)
	}

	response := UploadResponse{
		Hash:     hash.String(),
		Size:     size,
		Filename: header.Filename,
		Message:  "File uploaded successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// uploadFolder handles folder uploads
func (s *Server) uploadFolder(w http.ResponseWriter, r *http.Request) {
	// Create a temporary directory to reconstruct the folder structure
	tempDir, err := os.MkdirTemp("", "openhash-upload-*")
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to create temp dir", err)
		return
	}
	defer os.RemoveAll(tempDir)

	reader, err := r.MultipartReader()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to read multipart request", err)
		return
	}

	var firstPartName string
	var hasFiles bool

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to read multipart part", err)
			return
		}

		if part.FileName() == "" {
			continue
		}
		hasFiles = true
		if firstPartName == "" {
			firstPartName = part.FileName()
		}

		filePath := filepath.Join(tempDir, filepath.FromSlash(part.FileName()))

		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to create directory structure", err)
			return
		}

		dst, err := os.Create(filePath)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to create file", err)
			return
		}

		_, err = io.Copy(dst, part)
		dst.Close()
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to save file part", err)
			return
		}
	}

	if !hasFiles {
		s.writeError(w, http.StatusBadRequest, "No files provided for folder upload", nil)
		return
	}

	var rootPath string
	var folderName string

	parts := strings.Split(firstPartName, "/")
	if len(parts) > 1 {
		folderName = parts[0]
		rootPath = filepath.Join(tempDir, folderName)
	} else {
		folderName = "upload"
		rootPath = tempDir
	}

	link, err := s.storeUploadedDirectory(rootPath, folderName)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store directory", err)
		return
	}

	if err := s.replicator.AnnounceContent(link.Hash, link.Size); err != nil {
		log.Printf("Failed to announce content: %v", err)
	}

	response := UploadResponse{
		Hash:     link.Hash.String(),
		Size:     link.Size,
		Filename: folderName,
		Message:  "Folder uploaded successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// downloadContent handles content downloads.
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

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Filename))
	w.Header().Set("Content-Type", metadata.MimeType)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))

	s.streamChunks(w, r, metadata)
}

func (s *Server) streamDirectoryAsZip(w http.ResponseWriter, r *http.Request, metadata *blockstore.ContentMetadata) {
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", metadata.Filename))

	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()

	err := s.addDirectoryToZip(r.Context(), zipWriter, metadata.Links, "")
	if err != nil {
		log.Printf("Error creating zip archive for %s: %v", metadata.Hash.String(), err)
		// We can't write a proper error response if headers have been sent.
	}
}

func (s *Server) addDirectoryToZip(ctx context.Context, zipWriter *zip.Writer, links []merkle.Link, basePath string) error {
	for _, link := range links {
		pathInZip := link.Name
		if basePath != "" {
			pathInZip = basePath + "/" + link.Name
		}

		if link.Type == "directory" {
			dirMetadata, err := s.storage.GetContent(link.Hash)
			if err != nil {
				return fmt.Errorf("could not get metadata for subdirectory %s (%s): %w", link.Name, link.Hash.String(), err)
			}

			header := &zip.FileHeader{
				Name:     pathInZip + "/",
				Modified: dirMetadata.ModTime,
			}
			header.SetMode(os.ModeDir | 0755)

			_, err = zipWriter.CreateHeader(header)
			if err != nil {
				return fmt.Errorf("failed to create directory header in zip for %s: %w", pathInZip, err)
			}

			if err := s.addDirectoryToZip(ctx, zipWriter, dirMetadata.Links, pathInZip); err != nil {
				return err
			}
		} else { // "file"
			fileMetadata, err := s.storage.GetContent(link.Hash)
			if err != nil {
				return fmt.Errorf("could not get metadata for file %s (%s): %w", link.Name, link.Hash.String(), err)
			}

			header := &zip.FileHeader{
				Name:     pathInZip,
				Modified: fileMetadata.ModTime,
				Method:   zip.Deflate,
			}
			header.SetMode(0644)

			fileWriter, err := zipWriter.CreateHeader(header)
			if err != nil {
				return fmt.Errorf("failed to create file in zip for %s: %w", pathInZip, err)
			}

			for _, chunkInfo := range fileMetadata.Chunks {
				data, err := s.getChunkData(ctx, chunkInfo.Hash)
				if err != nil {
					return fmt.Errorf("could not get chunk %s for file %s: %w", chunkInfo.Hash.String(), pathInZip, err)
				}
				if _, err = fileWriter.Write(data); err != nil {
					return fmt.Errorf("failed to write chunk data to zip for file %s: %w", pathInZip, err)
				}
			}
		}
	}
	return nil
}

// viewContent handles content viewing.
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

	s.streamChunks(w, r, metadata)
}

func (s *Server) streamChunks(w http.ResponseWriter, r *http.Request, metadata *blockstore.ContentMetadata) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	for i, chunkInfo := range metadata.Chunks {
		data, err := s.getChunkData(r.Context(), chunkInfo.Hash)
		if err != nil {
			log.Printf("Error getting chunk %d for content %s: %v", i, metadata.Hash.String(), err)
			// We can't recover from a missing chunk, so we stop.
			// The client will receive a truncated response.
			return
		}

		if _, err := w.Write(data); err != nil {
			// Check if the error is due to the client closing the connection.
			// This is a common occurrence and not necessarily a server error.
			if strings.Contains(err.Error(), "forcibly closed by the remote host") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "connection reset by peer") {
				log.Printf("Client closed connection while streaming %s. Download aborted.", metadata.Hash.String())
			} else {
				log.Printf("Error writing chunk %d to response for content %s: %v", i, metadata.Hash.String(), err)
			}
			// Stop streaming if we can't write to the client.
			return
		}

		// Flush the data to the client after each chunk.
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// getChunkData retrieves chunk data, fetching from network if necessary.
func (s *Server) getChunkData(ctx context.Context, chunkHash hasher.Hash) ([]byte, error) {
	has, err := s.storage.Has(chunkHash)
	if err != nil {
		return nil, fmt.Errorf("error checking blockstore for chunk %s: %w", chunkHash, err)
	}
	if has {
		blk, err := s.storage.Get(chunkHash)
		if err != nil {
			return nil, err
		}
		return blk.RawData(), nil
	}

	// Use bitswap to get the block
	if bitswapNode, ok := s.node.(interface{ GetBitswap() *bitswap.Engine }); ok {
		blk, err := bitswapNode.GetBitswap().GetBlock(ctx, chunkHash)
		if err != nil {
			return nil, err
		}
		return blk.RawData(), nil
	}

	return nil, fmt.Errorf("node does not support bitswap")
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
	fmt.Fprintf(w,
		`<!DOCTYPE html>
		<html>
		<head>
			<title>OpenHashDB - Content View</title>
			<style>
				body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Helvetica, Arial, sans-serif; text-align: center; margin-top: 50px; color: #333; }
				.container { max-width: 600px; margin: auto; padding: 20px; }
				strong { color: #0056b3; }
				a.button { display: inline-block; margin-top: 20px; padding: 12px 24px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px; font-weight: bold; }
				a.button:hover { background-color: #0056b3; }
			</style>
		</head>
		<body>
			<div class="container">
				<h2>Unable to Display Content</h2>
				<p>The content with hash <strong>%s</strong> (filename: %s) cannot be displayed directly in the browser.</p>
				<p>Click the button below to download the file instead.</p>
				<a href="/download/%s" download="%s" class="button">Download File</a>
			</div>
		</body>
		</html>`,
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
	html.WriteString(fmt.Sprintf(`
		<!DOCTYPE html>
		<html>
		<head>
			<title>Directory Listing: %s</title>
			<style>
				body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Helvetica, Arial, sans-serif; margin: 20px; color: #333; }
				h2 { border-bottom: 1px solid #ccc; padding-bottom: 10px; }
				a { color: #007bff; text-decoration: none; }
				a:hover { text-decoration: underline; }
				table { border-collapse: collapse; width: 100%%; margin-top: 20px; }
				th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }
				th { background-color: #f2f2f2; }
				.download-all { display: inline-block; margin-bottom: 20px; padding: 10px 20px; background-color: #28a745; color: white; border-radius: 5px; font-weight: bold; }
				.download-all:hover { background-color: #218838; }
			</style>
		</head>
		<body>
			<h2>Directory: %s</h2>
			<a href="/download/%s" class="download-all">Download All as .zip</a>
			<table>
				<tr>
					<th>Type</th>
					<th>Name</th>
					<th>Size</th>
					<th>Hash</th>
				</tr>
	`, metadata.Filename, metadata.Filename, metadata.Hash.String()))

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

// getContentInfo returns information about content
func (s *Server) getContentInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	metadata, err := s.storage.GetContent(hash)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "Content not found", err)
		return
	}

	info := ContentInfo{
		Hash:        metadata.Hash.String(),
		Filename:    metadata.Filename,
		MimeType:    metadata.MimeType,
		Size:        metadata.Size,
		ModTime:     metadata.ModTime,
		IsDirectory: metadata.IsDirectory,
		CreatedAt:   metadata.CreatedAt,
		RefCount:    metadata.RefCount,
		Chunks:      metadata.Chunks,
		Links:       metadata.Links,
	}

	s.writeJSON(w, http.StatusOK, info)
}

// listContent lists all content
func (s *Server) listContent(w http.ResponseWriter, r *http.Request) {
	hashes, err := s.storage.ListContent()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to list content", err)
		return
	}

	var contentList []ContentInfo
	for _, hash := range hashes {
		metadata, err := s.storage.GetContent(hash)
		if err != nil {
			continue // Skip invalid entries
		}

		info := ContentInfo{
			Hash:        metadata.Hash.String(),
			Filename:    metadata.Filename,
			MimeType:    metadata.MimeType,
			Size:        metadata.Size,
			ModTime:     metadata.ModTime,
			IsDirectory: metadata.IsDirectory,
			CreatedAt:   metadata.CreatedAt,
			RefCount:    metadata.RefCount,
		}
		contentList = append(contentList, info)
	}

	s.writeJSON(w, http.StatusOK, contentList)
}

// getStats returns system statistics
func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	content, err := s.storage.ListContent()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to get storage stats", err)
		return
	}
	storageStats := map[string]interface{}{"content_count": len(content)}

	replicationStats := s.replicator.GetStats()

	stats := map[string]interface{}{
		"storage":     storageStats,
		"replication": replicationStats,
		"timestamp":   time.Now(),
	}

	s.writeJSON(w, http.StatusOK, stats)
}

// pinContent pins content
func (s *Server) pinContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	if err := s.replicator.PinContent(hash); err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to pin content", err)
		return
	}

	response := map[string]interface{}{
		"hash":    hash.String(),
		"message": "Content pinned successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// unpinContent unpins content
func (s *Server) unpinContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	if err := s.replicator.UnpinContent(hash); err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to unpin content", err)
		return
	}

	response := map[string]interface{}{
		"hash":    hash.String(),
		"message": "Content unpinned successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// listPins lists all pinned content hashes
func (s *Server) listPins(w http.ResponseWriter, r *http.Request) {
	pins := s.replicator.GetPinnedContent()
	s.writeJSON(w, http.StatusOK, pins)
}

// healthCheck returns server health status
func (s *Server) healthCheck(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"version":   "1.0.0",
	}

	s.writeJSON(w, http.StatusOK, health)
}

func (s *Server) storeUploadedFile(filename string, reader io.Reader) (hasher.Hash, int64, error) {
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

			hash, size, err := s.storeUploadedFile(entry.Name(), file)
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

// getNetworkStats returns network statistics
func (s *Server) getNetworkStats(w http.ResponseWriter, r *http.Request) {
	if s.node == nil {
		stats := map[string]interface{}{
			"error": "Network node not available",
		}
		s.writeJSON(w, http.StatusServiceUnavailable, stats)
		return
	}

	if nodeWithStats, ok := s.node.(interface{ GetNetworkStats() map[string]interface{} }); ok {
		stats := nodeWithStats.GetNetworkStats()
		s.writeJSON(w, http.StatusOK, stats)
	} else {
		stats := map[string]interface{}{
			"error": "Network stats not supported by this node type",
		}
		s.writeJSON(w, http.StatusServiceUnavailable, stats)
	}
}
