package rest

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/libp2p"
	"openhashdb/network/replicator"
	"openhashdb/openhashdb-ui"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

const (
	LargeFileThreshold = 200 * 1024 * 1024 // 200MB
)

// Server represents the REST API server
type Server struct {
	storage    *storage.Storage
	replicator *replicator.Replicator
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
	Hash        string    `json:"hash"`
	Filename    string    `json:"filename"`
	MimeType    string    `json:"mime_type"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"mod_time"`
	ChunkCount  int       `json:"chunk_count,omitempty"`
	IsDirectory bool      `json:"is_directory"`
	CreatedAt   time.Time `json:"created_at"`
	RefCount    int       `json:"ref_count"`
}

// NewServer creates a new REST API server
func NewServer(storage *storage.Storage, replicator *replicator.Replicator, node interface{}) *Server {
	s := &Server{
		storage:    storage,
		replicator: replicator,
		chunker:    chunker.NewChunker(chunker.ChunkSize256KB),
		node:       node,
		router:     mux.NewRouter(),
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
	// Parse multipart form
	err := r.ParseMultipartForm(32 << 20) // 32MB max memory
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Failed to parse multipart form", err)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "No file provided", err)
		return
	}
	defer file.Close()

	// Read file content
	content, err := io.ReadAll(file)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to read file", err)
		return
	}

	// Handle large files
	if len(content) > LargeFileThreshold {
		s.handleLargeFile(w, header.Filename, content)
		return
	}

	// Store the file
	hash, err := s.storeFile(header.Filename, content)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store file", err)
		return
	}

	// Announce to network
	if err := s.replicator.AnnounceContent(hash, int64(len(content)), false); err != nil {
		log.Printf("Failed to announce content: %v", err)
	}

	response := UploadResponse{
		Hash:     hash.String(),
		Size:     int64(len(content)),
		Filename: header.Filename,
		Message:  "File uploaded successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleLargeFile chunks a large file, stores the chunks, and announces the root hash.
func (s *Server) handleLargeFile(w http.ResponseWriter, filename string, content []byte) {
	chunks, err := s.chunker.ChunkBytes(content)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to chunk file", err)
		return
	}

	chunkedFile := s.chunker.CreateChunkedFile(chunks)
	var chunkHashes []hasher.Hash
	for _, chunk := range chunks {
		if err := s.storage.StoreData(chunk.Hash, chunk.Data); err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to store chunk", err)
			return
		}
		chunkHashes = append(chunkHashes, chunk.Hash)
	}

	// Store the chunk hashes
	chunkHashesBytes, err := json.Marshal(chunkHashes)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to marshal chunk hashes", err)
		return
	}
	if err := s.storage.StoreData(chunkedFile.RootHash, chunkHashesBytes); err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store chunk hashes", err)
		return
	}

	// Store the DAG structure as metadata
	dagNode := &storage.ContentMetadata{
		Hash:        chunkedFile.RootHash,
		Filename:    filename,
		Size:        chunkedFile.TotalSize,
		ModTime:     time.Now(),
		IsDirectory: true, // Treat as a directory of chunks
		CreatedAt:   time.Now(),
		RefCount:    1,
		ChunkCount:  len(chunks),
	}
	if err := s.storage.StoreContent(dagNode); err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store DAG metadata", err)
		return
	}

	// Announce only the root hash, and mark it as a large file
	if err := s.replicator.AnnounceContent(chunkedFile.RootHash, chunkedFile.TotalSize, true); err != nil {
		log.Printf("Failed to announce large file content: %v", err)
	}

	response := UploadResponse{
		Hash:     chunkedFile.RootHash.String(),
		Size:     chunkedFile.TotalSize,
		Filename: filename,
		Message:  "Large file uploaded and chunked successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}


// uploadFolder handles folder uploads by accepting multiple files, zipping them in-memory, and storing the zip.
func (s *Server) uploadFolder(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB max memory
		s.writeError(w, http.StatusBadRequest, "Failed to parse multipart form", err)
		return
	}

	form := r.MultipartForm
	files := form.File["files"]
	if len(files) == 0 {
		s.writeError(w, http.StatusBadRequest, "No files provided for folder upload", nil)
		return
	}

	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	// Add files to the archive.
	for _, fileHeader := range files {
		file, err := fileHeader.Open()
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to open a file for zipping", err)
			return
		}
		defer file.Close()

		writer, err := zipWriter.Create(fileHeader.Filename)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to create zip entry", err)
			return
		}

		if _, err := io.Copy(writer, file); err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to copy file content to zip", err)
			return
		}
	}

	// Close the zip writer to finalize the archive.
	if err := zipWriter.Close(); err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to finalize zip archive", err)
		return
	}

	// Determine the folder name from the common base path of the files.
	folderName := "archive.zip"
	if len(files) > 0 {
		// A simple approach to name the zip file: use the directory of the first file.
		// This assumes a 'directory/file.txt' structure in the form submission.
		commonPath := filepath.Dir(files[0].Filename)
		if commonPath != "." && commonPath != "/" {
			folderName = commonPath + ".zip"
		}
	}

	// Store the zipped content.
	zipData := buf.Bytes()

	// Handle large files
	if len(zipData) > LargeFileThreshold {
		s.handleLargeFile(w, folderName, zipData)
		return
	}

	hash, err := s.storeFile(folderName, zipData)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store zipped folder", err)
		return
	}

	// Announce to network.
	if err := s.replicator.AnnounceContent(hash, int64(len(zipData)), false); err != nil {
		log.Printf("Failed to announce content: %v", err)
	}

	response := UploadResponse{
		Hash:     hash.String(),
		Size:     int64(len(zipData)),
		Filename: folderName,
		Message:  "Folder uploaded and zipped successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}

// downloadContent handles content downloads, fetching from the network if not available locally.
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
		// Content not found locally, check the network
		log.Printf("Content %s not found locally. Checking network...", hashStr)
		libp2pNode, ok := s.node.(*libp2p.Node)
		if !ok {
			s.writeError(w, http.StatusNotFound, "Content not found locally and network is unavailable", nil)
			return
		}

		providers, err := libp2pNode.FindContentProviders(hashStr)
		if err != nil || len(providers) == 0 {
			s.writeError(w, http.StatusNotFound, fmt.Sprintf("Content not found locally or on the network: no providers found for content %s", hashStr), nil)
			return
		}

		// Found providers. Assume it's a large file and fetch the manifest first.
		log.Printf("Found providers for %s. Fetching manifest...", hashStr)
		_, manifest, err := s.fetchContentFromNetwork(r.Context(), hash)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "Failed to fetch content manifest from network", err)
			return
		}
		metadata = manifest
	}

	// At this point, we have the metadata, either from local storage or the network.
	if metadata.IsDirectory && metadata.ChunkCount > 0 {
		s.downloadLargeFile(w, metadata)
		return
	}

	// Handle small files
	data, err := s.storage.GetData(hash)
	if err != nil {
		// If data is still not found (e.g., manifest was fetched but not the content), fetch it now.
		data, metadata, err = s.fetchContentFromNetwork(r.Context(), hash)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "Content data missing and could not be fetched from the network", err)
			return
		}
	}

	// Set headers
	w.Header().Set("Content-Type", metadata.MimeType)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	if metadata.Filename != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Filename))
	}

	// Write content
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// downloadLargeFile handles the download of a large file by fetching its chunks.
func (s *Server) downloadLargeFile(w http.ResponseWriter, metadata *storage.ContentMetadata) {
	chunkHashes, err := s.storage.GetChunkHashes(metadata.Hash)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to get chunk hashes", err)
		return
	}

	totalChunks := len(chunkHashes)
	log.Printf("Starting download of large file %s (%d chunks)", metadata.Filename, totalChunks)


	// Set headers for the file
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Filename))
	w.WriteHeader(http.StatusOK)

	// Fetch and stream chunks sequentially to ensure correct order
	for i, chunkHash := range chunkHashes {
		progress := float64(i+1) / float64(totalChunks) * 100
		log.Printf("Downloading chunk %d of %d (%.2f%%) for file %s", i+1, totalChunks, progress, metadata.Filename)

		chunkData, err := s.replicator.RequestChunk(chunkHash)
		if err != nil {
			log.Printf("Failed to fetch chunk %s: %v", chunkHash.String(), err)
			// Handling the error by stopping the download. A more robust solution might involve retries.
			http.Error(w, "Failed to download a chunk", http.StatusInternalServerError)
			return
		}
		_, err = w.Write(chunkData)
		if err != nil {
			// This error typically happens if the client closes the connection.
			log.Printf("Failed to write chunk data to response: %v", err)
			return
		}
	}
	log.Printf("Finished downloading large file %s", metadata.Filename)
}

// viewContent handles content viewing, fetching from the network if not available locally.
func (s *Server) viewContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashStr := vars["hash"]

	hash, err := hasher.HashFromString(hashStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid hash", err)
		return
	}

	var data []byte
	var metadata *storage.ContentMetadata

	// Try to get metadata from local storage first.
	metadata, err = s.storage.GetContent(hash)
	if err != nil {
		// Metadata not found locally, try to fetch from the network.
		log.Printf("Metadata for %s not found locally, attempting to fetch from network.", hashStr)
		data, metadata, err = s.fetchContentFromNetwork(r.Context(), hash)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "Content not found locally or on the network", err)
			return
		}
	} else {
		// Metadata found, now get the data.
		data, err = s.storage.GetData(hash)
		if err != nil {
			// Data not found locally, despite having metadata. This is an inconsistent state.
			// Let's try to recover by fetching from the network.
			log.Printf("Metadata for %s found, but data is missing. Attempting to fetch from network.", hashStr)
			data, metadata, err = s.fetchContentFromNetwork(r.Context(), hash)
			if err != nil {
				s.writeError(w, http.StatusNotFound, "Content data missing and could not be fetched from the network", err)
				return
			}
		}
	}

	// Determine if content is renderable in browser
	isRenderable := strings.HasPrefix(metadata.MimeType, "text/") ||
		strings.HasPrefix(metadata.MimeType, "image/") ||
		metadata.MimeType == "application/pdf"

	if isRenderable {
		// Set headers for inline viewing
		w.Header().Set("Content-Type", metadata.MimeType)
		w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
		w.Header().Set("Content-Disposition", "inline")
		// Write content
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	} else {
		// For non-renderable content, provide a download link
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `
			<!DOCTYPE html>
			<html>
			<head>
				<title>OpenHashDB - Unable to Render</title>
				<style>
					body { font-family: sans-serif; text-align: center; margin-top: 50px; }
					a { display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px; }
					a:hover { background-color: #0056b3; }
				</style>
			</head>
			<body>
				<h1>Unable to Render Content</h1>
				<p>The content with hash <strong>%s</strong> cannot be displayed directly in the browser.</p>
				<p>Click the button below to download the file:</p>
				<a href="/download/%s" download="%s">Click to Download</a>
			</body>
			</html>
		`, hashStr, hashStr, metadata.Filename)
	}
}

// fetchContentFromNetwork finds and retrieves content from peers.
func (s *Server) fetchContentFromNetwork(ctx context.Context, hash hasher.Hash) ([]byte, *storage.ContentMetadata, error) {
	libp2pNode, ok := s.node.(*libp2p.Node)
	if !ok {
		return nil, nil, fmt.Errorf("network node is not available or does not support fetching")
	}

	hashStr := hash.String()
	log.Printf("Searching for providers of content %s", hashStr)
	providers, err := libp2pNode.FindContentProviders(hashStr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find content providers: %w", err)
	}
	if len(providers) == 0 {
		return nil, nil, fmt.Errorf("no providers found for content %s", hashStr)
	}

	log.Printf("Found %d provider(s) for %s. Attempting to fetch...", len(providers), hashStr)
	var lastErr error
	for _, p := range providers {
		if p.ID == libp2pNode.ID() {
			log.Printf("Skipping self as provider: %s", p.ID)
			continue // Don't try to fetch from ourselves
		}
		log.Printf("Requesting content %s from peer %s", hashStr, p.ID)
		data, metadata, err := libp2pNode.RequestContentFromPeer(p.ID, hashStr)
		if err != nil {
			lastErr = fmt.Errorf("failed to fetch from peer %s: %w", p.ID, err)
			log.Printf("Error fetching from peer: %v", lastErr)
			continue
		}

		// The metadata.Hash from the peer should match the requested hash (random hash)
		if metadata.Hash != hash {
			lastErr = fmt.Errorf("requested hash mismatch: expected %s, got %s from peer %s",
				hash.String(), metadata.Hash.String(), p.ID)
			log.Printf("Warning: %v", lastErr)
			continue
		}

		// Compute the actual content hash of the received data
		actualContentHash := hasher.HashBytes(data)
		log.Printf("Received data from peer %s for hash %s: computed content hash=%s", p.ID, hashStr, actualContentHash.String())

		// Determine the expected content hash for verification
		var expectedContentHash hasher.Hash
		usePeerContentHashForVerification := false

		// Get local metadata to compare ContentHash
		localMetadata, err := s.storage.GetContent(hash)
		if err == nil && localMetadata.ContentHash != (hasher.Hash{}) {
			// If local metadata exists and has a content hash, use it
			expectedContentHash = localMetadata.ContentHash
			log.Printf("Local metadata found for %s: expected ContentHash=%s", hashStr, expectedContentHash.String())
		} else if metadata.ContentHash != (hasher.Hash{}) {
			// If no local content hash, but peer provides one, use peer's for verification
			expectedContentHash = metadata.ContentHash
			usePeerContentHashForVerification = true
			log.Printf("No local ContentHash for %s, using peer's ContentHash for verification: %s", hashStr, expectedContentHash.String())
		} else {
			// Neither local nor peer provides a content hash, skip verification
			log.Printf("Warning: no ContentHash provided by local storage or peer for %s, skipping content verification.", hashStr)
			// No expectedContentHash, so the verification block below will be skipped.
		}

		// Verify content-based hash if an expected hash is present
		if expectedContentHash != (hasher.Hash{}) {
			if !hasher.Verify(data, expectedContentHash) {
				lastErr = fmt.Errorf("content hash mismatch for %s from peer %s: expected %s, got %s",
					hashStr, p.ID, expectedContentHash.String(), actualContentHash.String())
				log.Printf("Error: %v", lastErr)
				continue
			}
		}

		// If we used the peer's content hash for verification and it passed,
		// or if no content hash was available and we accepted the data,
		// ensure the metadata stored locally has the correct content hash.
		if usePeerContentHashForVerification || expectedContentHash == (hasher.Hash{}) {
			// If we used peer's content hash and it passed, or if no content hash was available,
			// update the metadata with the actual content hash for future verification.
			metadata.ContentHash = actualContentHash
		}

		// Store fetched content
		log.Printf("Successfully fetched %d bytes for content %s from peer %s. Storing locally.",
			len(data), hashStr, p.ID)
		if err := s.storage.StoreContent(metadata); err != nil {
			log.Printf("Warning: failed to store fetched metadata for %s: %v", hashStr, err)
		}
		if err := s.storage.StoreData(hash, data); err != nil {
			log.Printf("Warning: failed to store fetched data for %s: %v", hashStr, err)
		}

		return data, metadata, nil
	}

	return nil, nil, fmt.Errorf("failed to fetch content from all found providers: %w", lastErr)
}

// func (s *Server) fetchContentFromNetwork(ctx context.Context, hash hasher.Hash) ([]byte, error) {
// 	libp2pNode, ok := s.node.(*libp2p.Node)
// 	if !ok {
// 		return nil, fmt.Errorf("network node is not available or does not support fetching")
// 	}

// 	hashStr := hash.String()
// 	log.Printf("Searching for providers of content %s", hashStr)
// 	providers, err := libp2pNode.FindContentProviders(hashStr)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to find content providers: %w", err)
// 	}
// 	if len(providers) == 0 {
// 		return nil, fmt.Errorf("no providers found for content")
// 	}

// 	log.Printf("Found %d provider(s) for %s. Attempting to fetch...", len(providers), hashStr)
// 	var lastErr error
// 	for _, p := range providers {
// 		if p.ID == libp2pNode.ID() {
// 			continue // Don't try to fetch from ourselves
// 		}
// 		log.Printf("Requesting content %s from peer %s", hashStr, p.ID)
// 		data, metadata, err := libp2pNode.RequestContentFromPeer(p.ID, hashStr)
// 		if err != nil {
// 			lastErr = fmt.Errorf("failed to fetch from peer %s: %w", p.ID, err)
// 			log.Printf("Error fetching from peer: %v", err)
// 			continue
// 		}

// 		// Content fetched successfully, now store it with its metadata.
// 		log.Printf("Successfully fetched %d bytes for content %s from peer %s. Storing locally.", len(data), hashStr, p.ID)
// 		if err := s.storage.StoreContent(metadata); err != nil {
// 			log.Printf("Warning: failed to store fetched metadata for %s: %v", hashStr, err)
// 			// We have the data but failed to store it. Log the error but still return the data.
// 		}
// 		if err := s.storage.StoreData(metadata.Hash, data); err != nil {
// 			log.Printf("Warning: failed to store fetched data for %s: %v", hashStr, err)
// 		}

// 		return data, nil
// 	}

// 	return nil, fmt.Errorf("failed to fetch content from all found providers: %w", lastErr)
// }

// viewContentPath handles viewing content at a specific path (for directories)
func (s *Server) viewContentPath(w http.ResponseWriter, r *http.Request) {
	s.writeError(w, http.StatusNotImplemented, "Path-based content viewing not yet implemented", nil)
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
		// Content not found locally, let's check the network.
		log.Printf("Content %s not found locally. Checking network for providers...", hashStr)
		libp2pNode, ok := s.node.(*libp2p.Node)
		if !ok {
			s.writeError(w, http.StatusNotFound, "Content not found locally and network is unavailable", nil)
			return
		}

		providers, err := libp2pNode.FindContentProviders(hashStr)
		if err != nil || len(providers) == 0 {
			s.writeError(w, http.StatusNotFound, fmt.Sprintf("Content not found locally or on the network: no providers found for content %s", hashStr), nil)
			return
		}

		// Providers found, so the content exists on the network.
		s.writeJSON(w, http.StatusAccepted, map[string]string{
			"message": "Content is available on the network. Use the /download endpoint to retrieve it.",
			"hash":    hashStr,
		})
		return
	}

	info := ContentInfo{
		Hash:        metadata.Hash.String(),
		Filename:    metadata.Filename,
		MimeType:    metadata.MimeType,
		Size:        metadata.Size,
		ModTime:     metadata.ModTime,
		ChunkCount:  metadata.ChunkCount,
		IsDirectory: metadata.IsDirectory,
		CreatedAt:   metadata.CreatedAt,
		RefCount:    metadata.RefCount,
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
			ChunkCount:  metadata.ChunkCount,
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
	storageStats, err := s.storage.GetStats()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to get storage stats", err)
		return
	}

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

// storeFile stores a file and returns its hash
func (s *Server) storeFile(filename string, content []byte) (hasher.Hash, error) {
	// Generate a random 32-byte hash
	randomBytes := make([]byte, 32)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return hasher.Hash{}, fmt.Errorf("failed to generate random hash: %w", err)
	}

	// Create random hash using hasher.HashBytes
	hash := hasher.HashBytes(randomBytes) // Changed from sha256.Sum256 to hasher.HashBytes

	// Compute content-based hash for verification
	contentHash := hasher.HashBytes(content)
	log.Printf("Storing file %s: random hash=%s, content hash=%s", filename, hash.String(), contentHash.String())

	// Check if content with this random hash already exists
	if s.storage.HasContent(hash) {
		log.Printf("Content with hash %s already exists", hash.String())
		return hash, nil
	}

	// Create metadata
	metadata := &storage.ContentMetadata{
		Hash:        hash,
		ContentHash: contentHash, // Store content-based hash
		Filename:    filename,
		MimeType:    getMimeType(filename),
		Size:        int64(len(content)),
		ModTime:     time.Now(),
		IsDirectory: false,
		CreatedAt:   time.Now(),
		RefCount:    1,
	}

	// Store metadata
	if err := s.storage.StoreContent(metadata); err != nil {
		return hasher.Hash{}, fmt.Errorf("failed to store metadata: %w", err)
	}

	// Store data
	if err := s.storage.StoreData(hash, content); err != nil {
		return hasher.Hash{}, fmt.Errorf("failed to store data: %w", err)
	}

	log.Printf("Successfully stored file %s with random hash %s and content hash %s", filename, hash.String(), contentHash.String())
	return hash, nil
}

// func (s *Server) storeFile(filename string, content []byte) (hasher.Hash, error) {
// 	// Compute hash
// 	// Generate 32 random bytes
// 	randomBytes := make([]byte, 32)
// 	_, err := rand.Read(randomBytes)
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Hash the random bytes using SHA-256
// 	hash := sha256.Sum256(randomBytes)
// 	// Check if already exists
// 	if s.storage.HasContent(hash) {
// 		return hash, nil
// 	}

// 	// Create metadata
// 	metadata := &storage.ContentMetadata{
// 		Hash:        hash,
// 		Filename:    filename,
// 		MimeType:    getMimeType(filename),
// 		Size:        int64(len(content)),
// 		ModTime:     time.Now(),
// 		IsDirectory: false,
// 		CreatedAt:   time.Now(),
// 		RefCount:    1,
// 	}

// 	// Store metadata
// 	if err := s.storage.StoreContent(metadata); err != nil {
// 		return hasher.Hash{}, fmt.Errorf("failed to store metadata: %w", err)
// 	}

// 	// Store data
// 	if err := s.storage.StoreData(hash, content); err != nil {
// 		return hasher.Hash{}, fmt.Errorf("failed to store data: %w", err)
// 	}

// 	return hash, nil
// }

// getMimeType determines MIME type from filename
func getMimeType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".txt":
		return "text/plain"
	case ".html", ".htm":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js":
		return "application/javascript"
	case ".json":
		return "application/json"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".pdf":
		return "application/pdf"
	default:
		return "application/octet-stream"
	}
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

	// Type assert to get network stats
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
