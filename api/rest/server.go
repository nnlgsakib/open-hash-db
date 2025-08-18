package rest

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"openhashdb/core/blockstore"
	"openhashdb/core/chunker"
	"openhashdb/network/libp2p"
	"openhashdb/network/replicator"
	"openhashdb/network/streammanager"

	"github.com/gorilla/mux"
)

const (
	// Buffer sizes and limits
	bufferSize       = 64 * 1024 // 64KB buffer for streaming
	maxConcurrentOps = 10        // Maximum concurrent chunk operations
	chunkCacheSize   = 100       // Number of chunks to keep in memory
	prefetchAhead    = 5         // Number of chunks to prefetch ahead
)

// NewServer creates a new REST API server
func NewServer(bs *blockstore.Blockstore, replicator *replicator.Replicator, node interface{}) *Server {
	s := &Server{
		storage:    bs,
		replicator: replicator,
		chunker:    chunker.NewChunker(),
		node:       node,
		router:     mux.NewRouter(),
		chunkCache: NewChunkCache(chunkCacheSize),
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, bufferSize)
			},
		},
	}

	if libp2pNode, ok := node.(*libp2p.Node); ok {
		s.streamer = streammanager.NewStreamManager(libp2pNode)
	}

	s.setupRoutes()
	return s
}

// Start starts the server with optimized timeouts
func (s *Server) Start(addr string) error {
	s.server = &http.Server{
		Addr:           addr,
		Handler:        s.router,
		ReadTimeout:    5 * time.Minute,  // Increased for large file operations
		WriteTimeout:   10 * time.Minute, // Increased for large downloads
		IdleTimeout:    2 * time.Minute,  // Connection keepalive
		MaxHeaderBytes: 1 << 20,          // 1MB max headers
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
