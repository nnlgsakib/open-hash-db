package rest

import (
    "context"
    "log"
    "net/http"
    "sync"
    "time"

    "openhashdb/core/blockstore"
    "openhashdb/core/chunker"
    "openhashdb/core/sharder"
    "openhashdb/network/libp2p"
    "openhashdb/network/replicator"
    "openhashdb/network/streammanager"

    "github.com/gorilla/mux"
    "os"
    "strconv"
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
	sharder, err := sharder.NewReedSolomon(sharder.DefaultDataShards, sharder.DefaultParityShards)
	if err != nil {
		log.Fatalf("Failed to create sharder: %v", err)
	}
    // Chunk cache size in bytes (default 128MB)
    var cacheBytes int64 = 128 << 20
    if v := os.Getenv("OPENHASHDB_CHUNK_CACHE_BYTES"); v != "" {
        if parsed, err := parseBytesEnv(v); err == nil {
            cacheBytes = parsed
        }
    }

    s := &Server{
        storage:    bs,
        replicator: replicator,
        chunker:    chunker.NewChunker(),
        sharder:    sharder,
        node:       node,
        router:     mux.NewRouter(),
        chunkCache: NewChunkCacheBytes(cacheBytes),
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

// parseBytesEnv parses strings like "134217728" (bytes) or suffixes like "128M".
func parseBytesEnv(s string) (int64, error) {
    // simple parser: support K/M/G suffix
    var mult int64 = 1
    if len(s) > 0 {
        switch s[len(s)-1] {
        case 'K', 'k':
            mult = 1 << 10
            s = s[:len(s)-1]
        case 'M', 'm':
            mult = 1 << 20
            s = s[:len(s)-1]
        case 'G', 'g':
            mult = 1 << 30
            s = s[:len(s)-1]
        }
    }
    n, err := strconv.ParseInt(s, 10, 64)
    if err != nil {
        return 0, err
    }
    return n * mult, nil
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
