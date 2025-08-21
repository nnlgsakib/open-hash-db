package rest

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"openhashdb/protobuf/pb"
)

// uploadFile handles single file uploads
func (s *Server) uploadFile(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "No file provided", err)
		return
	}
	defer file.Close()

	useEC := r.URL.Query().Get("ec") == "true"
	hash, size, err := s.storeUploadedFile(header.Filename, file, useEC)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to store file", err)
		return
	}

	// Announce to network
	if err := s.replicator.AnnounceContent(hash, size); err != nil {
		log.Printf("Failed to announce content: %v", err)
	}

	response := &pb.UploadResponse{
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

	response := &pb.UploadResponse{
		Hash:     link.Hash.String(),
		Size:     link.Size,
		Filename: folderName,
		Message:  "Folder uploaded successfully",
	}

	s.writeJSON(w, http.StatusOK, response)
}