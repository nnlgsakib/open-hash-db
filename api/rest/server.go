package rest

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
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
	"openhashdb/network/streammanager"
	"openhashdb/openhashdb-ui"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// Server represents the REST API server
type Server struct {
	storage    *storage.Storage
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

	// Store the file by streaming
	hash, size, err := s.storeFile(header.Filename, file)
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
	zipDataReader := bytes.NewReader(buf.Bytes())
	hash, size, err := s.storeFile(folderName, zipDataReader)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store zipped folder", err)
		return
	}

	// Announce to network.
	if err := s.replicator.AnnounceContent(hash, size); err != nil {
		log.Printf("Failed to announce content: %v", err)
	}

	response := UploadResponse{
		Hash:     hash.String(),
		Size:     size,
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

	var metadata *storage.ContentMetadata
	var dataStream io.ReadCloser

	metadata, err = s.storage.GetContent(hash)
	if err == nil {
		dataStream, err = s.storage.GetDataStream(hash)
		if err != nil {
			log.Printf("Metadata for %s found, but data stream is missing. Attempting to fetch from network.", hashStr)
			// Fall through to network fetch
		}
	}

	if err != nil { // Either metadata or data stream missing
		log.Printf("Content %s not available locally, attempting to fetch from network.", hashStr)
		dataStream, metadata, err = s.fetchContentStreamFromNetwork(r.Context(), hash)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "Content not found locally or on the network", err)
			return
		}
	}

	defer dataStream.Close()

	// Set headers
	w.Header().Set("Content-Type", metadata.MimeType)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	if metadata.Filename != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", metadata.Filename))
	}

	// Write content by streaming
	w.WriteHeader(http.StatusOK)

	// Use a buffered copy to handle large files and client disconnects
	buf := make([]byte, 32*1024) // 32KB buffer
	for {
		select {
		case <-r.Context().Done():
			log.Printf("Client disconnected before content %s could be fully sent.", hashStr)
			return
		default:
			nr, readErr := dataStream.Read(buf)
			if nr > 0 {
				_, writeErr := w.Write(buf[0:nr])
				if writeErr != nil {
					// Don't log error if client disconnected
					if r.Context().Err() == nil {
						log.Printf("Failed to write chunk of content %s to client: %v", hashStr, writeErr)
					}
					return
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			if readErr == io.EOF {
				return // End of file
			}
			if readErr != nil {
				log.Printf("Failed to read content %s for streaming: %v", hashStr, readErr)
				return
			}
		}
	}
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

	var metadata *storage.ContentMetadata
	var dataStream io.ReadCloser

	metadata, err = s.storage.GetContent(hash)
	if err == nil {
		dataStream, err = s.storage.GetDataStream(hash)
		if err != nil {
			log.Printf("Metadata for %s found, but data stream is missing. Attempting to fetch from network.", hashStr)
			// Fall through to network fetch
		}
	}

	if err != nil { // Either metadata or data stream missing
		log.Printf("Content %s not available locally, attempting to fetch from network.", hashStr)
		dataStream, metadata, err = s.fetchContentStreamFromNetwork(r.Context(), hash)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "Content not found locally or on the network", err)
			return
		}
	}

	defer dataStream.Close()

	// Determine if content is renderable in browser
	isRenderable := strings.HasPrefix(metadata.MimeType, "text/") || // Text: plain, html, css, csv, vtt, markdown, vcard, calendar
		strings.HasPrefix(metadata.MimeType, "image/") || // Images: png, jpg, jpeg, gif, svg, webp, ico, bmp, avif, heif, tiff
		strings.HasPrefix(metadata.MimeType, "audio/") || // Audio: mp3, wav, ogg, flac, aac
		strings.HasPrefix(metadata.MimeType, "video/") || // Videos: mp4, webm, mpeg, ogv, avi, mov, ts
		strings.HasPrefix(metadata.MimeType, "font/") || // Fonts: woff, woff2, ttf, otf
		metadata.MimeType == "application/pdf" || // PDF
		metadata.MimeType == "application/javascript" || // JavaScript: js, mjs
		metadata.MimeType == "application/json" || // JSON
		metadata.MimeType == "application/ld+json" || // JSON-LD
		metadata.MimeType == "application/vnd.ms-fontobject" || // Font: eot
		metadata.MimeType == "application/xml" || // XML
		metadata.MimeType == "application/xhtml+xml" || // XHTML
		metadata.MimeType == "application/wasm" || // WebAssembly
		metadata.MimeType == "application/vnd.apple.mpegurl" // Streaming: m3u8

	if isRenderable {
		// Set headers for inline viewing
		w.Header().Set("Content-Type", metadata.MimeType)
		w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
		w.Header().Set("Content-Disposition", "inline")
		// Write content by streaming
		w.WriteHeader(http.StatusOK)

		// Use a buffered copy to handle large files and client disconnects
		buf := make([]byte, 32*1024) // 32KB buffer
		for {
			select {
			case <-r.Context().Done():
				log.Printf("Client disconnected before content %s could be fully sent.", hashStr)
				return
			default:
				nr, readErr := dataStream.Read(buf)
				if nr > 0 {
					_, writeErr := w.Write(buf[0:nr])
					if writeErr != nil {
						// Don't log error if client disconnected
						if r.Context().Err() == nil {
							log.Printf("Failed to write chunk of content %s to client: %v", hashStr, writeErr)
						}
						return
					}
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				}
				if readErr == io.EOF {
					return // End of file
				}
				if readErr != nil {
					log.Printf("Failed to read content %s for streaming: %v", hashStr, readErr)
					return
				}
			}
		}
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

// fetchContentStreamFromNetwork finds and retrieves a content stream from peers.
func (s *Server) fetchContentStreamFromNetwork(ctx context.Context, hash hasher.Hash) (io.ReadCloser, *storage.ContentMetadata, error) {
	libp2pNode, ok := s.node.(*libp2p.Node)
	if !ok || s.streamer == nil {
		return nil, nil, fmt.Errorf("network node is not available or does not support streaming")
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

	log.Printf("Found %d provider(s) for %s. Attempting to fetch stream...", len(providers), hashStr)
	var lastErr error
	for _, p := range providers {
		if p.ID == libp2pNode.ID() {
			log.Printf("Skipping self as provider: %s", p.ID)
			continue // Don't try to fetch from ourselves
		}

		stream, err := s.streamer.RequestTransfer(ctx, hash, p.ID)
		if err != nil {
			lastErr = fmt.Errorf("failed to fetch stream from peer %s: %w", p.ID, err)
			log.Printf("Error fetching stream from peer: %v", lastErr)
			continue
		}

		// Since RequestTransfer now returns metadata, we need to get it.
		// This is a bit of a hack, as we don't have the metadata yet.
		// We'll get it from the stream itself.
		var metadata *storage.ContentMetadata

		// TODO: Add content verification for streams if possible, though it's tricky.

		// Store content in the background while streaming
		go func(stream io.ReadCloser) {
			// Create a tee reader to save the content as it's read
			pr, pw := io.Pipe()
			tee := io.TeeReader(stream, pw)

			// Goroutine to read from the tee and store the data
			go func() {
				data, err := io.ReadAll(tee)
				if err != nil {
					log.Printf("Error reading from tee for storage: %v", err)
					return
				}

				if metadata != nil { // metadata should be available here
					if err := s.storage.StoreContent(metadata); err != nil {
						log.Printf("Warning: failed to store fetched metadata for %s: %v", hashStr, err)
					}
					if err := s.storage.StoreData(hash, data); err != nil {
						log.Printf("Warning: failed to store fetched data for %s: %v", hashStr, err)
					}
					log.Printf("Successfully stored streamed content %s in the background", hashStr)
				}
			}()

			// now we can read the metadata from the pipe reader
			// and then the rest of the data will be read by the other goroutine
			// and sent to the client
			// This is a bit of a hack, but it works for now.
			// A better solution would be to have the metadata sent first.
			// But that would require a change in the protocol.
			// So we'll stick with this for now.

			// read metadata from the stream
			var metaLen uint32
			if err := binary.Read(pr, binary.BigEndian, &metaLen); err != nil {
				log.Printf("Error reading metadata length from stream: %v", err)
				return
			}

			metaBytes := make([]byte, metaLen)
			if _, err := io.ReadFull(pr, metaBytes); err != nil {
				log.Printf("Error reading metadata from stream: %v", err)
				return
			}

			if err := json.Unmarshal(metaBytes, &metadata); err != nil {
				log.Printf("Error unmarshaling metadata from stream: %v", err)
				return
			}

		}(stream)

		return stream, metadata, nil
	}

	return nil, nil, fmt.Errorf("failed to fetch content stream from all found providers: %w", lastErr)
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
		// If not found locally, check the network
		log.Printf("Content %s not found locally, checking network...", hashStr)
		libp2pNode, ok := s.node.(*libp2p.Node)
		if !ok {
			s.writeError(w, http.StatusNotFound, "Content not found", err)
			return
		}

		providers, err := libp2pNode.FindContentProviders(hashStr)
		if err != nil || len(providers) == 0 {
			s.writeError(w, http.StatusNotFound, "Content not found on the network", err)
			return
		}

		// Content found on network, start background replication
		go s.fetchContentFromNetwork(context.Background(), hash)

		s.writeJSON(w, http.StatusAccepted, map[string]string{
			"message": "Content found on network, replication started in background.",
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

// storeFile stores a file and returns its hash and size
func (s *Server) storeFile(filename string, reader io.Reader) (hasher.Hash, int64, error) {
	// Generate a random 32-byte hash for the primary identifier
	randomBytes := make([]byte, 32)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return hasher.Hash{}, 0, fmt.Errorf("failed to generate random hash: %w", err)
	}
	hash := hasher.HashBytes(randomBytes)

	// Create a buffer to read the content and calculate its hash and size
	var b bytes.Buffer
	size, err := io.Copy(&b, reader)
	if err != nil {
		return hasher.Hash{}, 0, fmt.Errorf("failed to read content for hashing: %w", err)
	}
	contentHash := hasher.HashBytes(b.Bytes())

	log.Printf("Storing file %s: random hash=%s, content hash=%s, size=%d", filename, hash.String(), contentHash.String(), size)

	// Check if content with this random hash already exists
	if s.storage.HasContent(hash) {
		log.Printf("Content with hash %s already exists", hash.String())
		return hash, size, nil
	}

	// Create metadata
	metadata := &storage.ContentMetadata{
		Hash:        hash,
		ContentHash: contentHash, // Store content-based hash
		Filename:    filename,
		MimeType:    getMimeType(filename),
		Size:        size,
		ModTime:     time.Now(),
		IsDirectory: false,
		CreatedAt:   time.Now(),
		RefCount:    1,
	}

	// Store metadata
	if err := s.storage.StoreContent(metadata); err != nil {
		return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata: %w", err)
	}

	// Store data using the buffered content
	if err := s.storage.StoreDataStream(hash, bytes.NewReader(b.Bytes()), size); err != nil {
		return hasher.Hash{}, 0, fmt.Errorf("failed to store data stream: %w", err)
	}

	log.Printf("Successfully stored file %s with random hash %s and content hash %s", filename, hash.String(), contentHash.String())
	return hash, size, nil
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
	case ".js", ".mjs":
		return "application/javascript"
	case ".json":
		return "application/json"
	case ".jsonld":
		return "application/ld+json"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	case ".webp":
		return "image/webp"
	case ".ico":
		return "image/x-icon"
	case ".bmp":
		return "image/bmp"
	case ".avif":
		return "image/avif"
	case ".heif", ".heic":
		return "image/heif"
	case ".tiff", ".tif":
		return "image/tiff"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".ogg":
		return "audio/ogg"
	case ".flac":
		return "audio/flac"
	case ".aac":
		return "audio/aac"
	case ".mp4":
		return "video/mp4"
	case ".webm":
		return "video/webm"
	case ".mpeg", ".mpg":
		return "video/mpeg"
	case ".ogv":
		return "video/ogg"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	case ".woff":
		return "font/woff"
	case ".woff2":
		return "font/woff2"
	case ".ttf":
		return "font/ttf"
	case ".otf":
		return "font/otf"
	case ".eot":
		return "application/vnd.ms-fontobject"
	case ".xml":
		return "application/xml"
	case ".xhtml":
		return "application/xhtml+xml"
	case ".wasm":
		return "application/wasm"
	case ".csv":
		return "text/csv"
	case ".vtt":
		return "text/vtt"
	case ".md", ".markdown":
		return "text/markdown"
	case ".ts":
		return "video/mp2t"
	case ".m3u8":
		return "application/vnd.apple.mpegurl"

	// Browser-unrenderable MIME types
	case ".pdf":
		return "application/pdf"
	case ".zip":
		return "application/zip"
	case ".rar":
		return "application/x-rar-compressed"
	case ".7z":
		return "application/x-7z-compressed"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	case ".bz2":
		return "application/x-bzip2"
	case ".xz":
		return "application/x-xz"
	case ".zst":
		return "application/zstd"
	case ".exe":
		return "application/x-msdownload"
	case ".doc":
		return "application/msword"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".ppt":
		return "application/vnd.ms-powerpoint"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case ".odt":
		return "application/vnd.oasis.opendocument.text"
	case ".ods":
		return "application/vnd.oasis.opendocument.spreadsheet"
	case ".odp":
		return "application/vnd.oasis.opendocument.presentation"
	case ".odg":
		return "application/vnd.oasis.opendocument.graphics"
	case ".rtf":
		return "application/rtf"
	case ".epub":
		return "application/epub+zip"
	case ".jar":
		return "application/java-archive"
	case ".war":
		return "application/x-webarchive"
	case ".bin":
		return "application/octet-stream"
	case ".iso":
		return "application/x-iso9660-image"
	case ".dmg":
		return "application/x-apple-diskimage"
	case ".torrent":
		return "application/x-bittorrent"
	case ".sql":
		return "application/sql"
	case ".db", ".sqlite":
		return "application/x-sqlite3"
	case ".psd":
		return "image/vnd.adobe.photoshop"
	case ".ai":
		return "application/postscript"
	case ".eps":
		return "application/postscript"
	case ".vcf", ".vcard":
		return "text/vcard"
	case ".ics", ".ical":
		return "text/calendar"
	case ".apk":
		return "application/vnd.android.package-archive"
	case ".deb":
		return "application/vnd.debian.binary-package"
	case ".rpm":
		return "application/x-rpm"
	case ".swf":
		return "application/x-shockwave-flash"
	case ".mkv":
		return "video/x-matroska"
	case ".flv":
		return "video/x-flv"
	case ".dwg":
		return "image/vnd.dwg"
	case ".kml":
		return "application/vnd.google-earth.kml+xml"
	case ".kmz":
		return "application/vnd.google-earth.kmz"
	case ".gpx":
		return "application/gpx+xml"

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
