package gateway

import (
    "net/http"
    "os"

    "github.com/prometheus/client_golang/prometheus/promhttp"
    "openhashdb/openhashdb-ui"
)

// setupRoutes sets up the API routes
func (s *Server) setupRoutes() {
    // Middlewares: CORS, optional read-only, and no-buffering for streaming
    s.router.Use(s.corsMiddleware)
    s.router.Use(noBufferMiddleware)
    if os.Getenv("OPENHASHDB_READ_ONLY") == "true" {
        s.router.Use(readOnlyMiddleware)
    }

    // Upload endpoints
    s.router.HandleFunc("/upload/file", s.uploadFile).Methods("POST", "OPTIONS")
    s.router.HandleFunc("/upload/folder", s.uploadFolder).Methods("POST", "OPTIONS")

    // Download endpoints
    s.router.HandleFunc("/download/{hash}", s.downloadContent).Methods("GET", "HEAD", "OPTIONS")
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

    // Health check and metrics
    s.router.HandleFunc("/health", s.healthCheck).Methods("GET", "OPTIONS")
    s.router.Handle("/metrics", promhttp.Handler()).Methods("GET")

    // UI
    s.router.PathPrefix("/").Handler(openhashdb.GetHandler())
}

// readOnlyMiddleware rejects mutating methods when read-only mode is enabled.
func readOnlyMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        switch r.Method {
        case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
            http.Error(w, "read-only mode", http.StatusForbidden)
            return
        }
        next.ServeHTTP(w, r)
    })
}

// noBufferMiddleware disables proxy buffering to improve large transfer latency.
func noBufferMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Nginx, Traefik, etc. honor X-Accel-Buffering when configured
        w.Header().Set("X-Accel-Buffering", "no")
        next.ServeHTTP(w, r)
    })
}

