package rest

import (
	"net/http"
	"openhashdb/core/blockstore"
	"openhashdb/core/chunker"
	"openhashdb/core/merkle"
	"openhashdb/core/sharder"
	"openhashdb/network/replicator"
	"openhashdb/network/streammanager"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// Server represents the REST API server
type Server struct {
	storage    *blockstore.Blockstore
	replicator *replicator.Replicator
	streamer   *streammanager.StreamManager
	chunker    *chunker.Chunker
	sharder    sharder.ErasureCoder
	node       interface{} // libp2p node for network stats
	router     *mux.Router
	server     *http.Server
	chunkCache *ChunkCache
	bufferPool sync.Pool
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
	Message     string              `json:"message,omitempty"`
}
