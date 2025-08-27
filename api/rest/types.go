package rest

import (
	"net/http"
	"sync"
	"time"

	"openhashdb/core/blockstore"
	"openhashdb/core/chunker"
	"openhashdb/core/sharder"
	"openhashdb/network/replicator"
	"openhashdb/network/streammanager"

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

// JSONContentInfo is a struct for JSON responses
type JSONContentInfo struct {
    Hash        string           `json:"hash"`
    CID         string           `json:"cid,omitempty"`
    Filename    string           `json:"filename"`
    MimeType    string           `json:"mime_type"`
    Size        int64            `json:"size"`
    ModTime     time.Time        `json:"mod_time"`
    IsDirectory bool             `json:"is_directory"`
	CreatedAt   time.Time        `json:"created_at"`
	RefCount    int32            `json:"ref_count"`
	Chunks      []*JSONChunkInfo `json:"chunks,omitempty"`
	Links       []*JSONLink      `json:"links,omitempty"`
	Message     string           `json:"message,omitempty"`
}

// JSONChunkInfo is a struct for JSON responses
type JSONChunkInfo struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// JSONLink is a struct for JSON responses
type JSONLink struct {
	Name string `json:"name"`
	Hash string `json:"hash"`
	Size int64  `json:"size"`
	Type string `json:"type"`
}
