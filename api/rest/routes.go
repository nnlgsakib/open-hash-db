package rest

import "openhashdb/openhashdb-ui"

// setupRoutes sets up the API routes
func (s *Server) setupRoutes() {
	// Enable CORS for all routes
	s.router.Use(s.corsMiddleware)

	// Upload endpoints
	s.router.HandleFunc("/upload/file", s.uploadFile).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/upload/folder", s.uploadFolder).Methods("POST", "OPTIONS")

    // Download endpoints (hash and CID)
    s.router.HandleFunc("/download/{hash}", s.downloadContent).Methods("GET", "OPTIONS")
    s.router.HandleFunc("/view/{hash}", s.viewContent).Methods("GET", "OPTIONS")
    s.router.HandleFunc("/cid/download/{cid}", s.downloadContentByCID).Methods("GET", "OPTIONS")
    s.router.HandleFunc("/cid/view/{cid}", s.viewContentByCID).Methods("GET", "OPTIONS")

    // Info endpoints (hash and CID)
    s.router.HandleFunc("/info/{hash}", s.getContentInfo).Methods("GET", "OPTIONS")
    s.router.HandleFunc("/cid/info/{cid}", s.getContentInfoByCID).Methods("GET", "OPTIONS")
	s.router.HandleFunc("/list", s.listContent).Methods("GET", "OPTIONS")
	s.router.HandleFunc("/stats", s.getStats).Methods("GET", "OPTIONS")
	s.router.HandleFunc("/network", s.getNetworkStats).Methods("GET", "OPTIONS")

    // Pin endpoints (hash and CID)
    s.router.HandleFunc("/pin/{hash}", s.pinContent).Methods("POST", "OPTIONS")
    s.router.HandleFunc("/unpin/{hash}", s.unpinContent).Methods("DELETE", "OPTIONS")
    s.router.HandleFunc("/cid/pin/{cid}", s.pinContentByCID).Methods("POST", "OPTIONS")
    s.router.HandleFunc("/cid/unpin/{cid}", s.unpinContentByCID).Methods("DELETE", "OPTIONS")
	s.router.HandleFunc("/pins", s.listPins).Methods("GET", "OPTIONS")

	// Health check
	s.router.HandleFunc("/health", s.healthCheck).Methods("GET", "OPTIONS")
	//web
	s.router.PathPrefix("/").Handler(openhashdb.GetHandler())
}
