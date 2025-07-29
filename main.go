package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"openhashdb/api/rest"
	"openhashdb/core/dag"
	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/libp2p"
	"openhashdb/network/replicator"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	// Global flags
	dbPath     string
	apiPort    int
	p2pPort    int
	verbose    bool
	bootnodes  string
	apiURL     string
	
	// Global instances
	store      *storage.Storage
	node       *libp2p.Node
	repl       *replicator.Replicator
	apiServer  *rest.Server
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "openhash",
	Short: "OpenHashDB is a content-addressable, distributed database system",
	Long: `OpenHashDB is a blazing-fast, content-addressable, distributed database system
designed as a modern, developer-friendly alternative to IPFS.

It provides:
- Content-addressable storage using SHA-256 hashes
- Distributed peer-to-peer networking via libp2p
- File and folder upload with automatic chunking
- REST API for web integration
- CLI for direct command-line usage`, 
	Run: func(cmd *cobra.Command, args []string) {
		// Default behavior when no subcommand is provided
		cmd.Help()
	},
}

// addCmd represents the add command
var addCmd = &cobra.Command{
	Use:   "add [file/folder]",
	Short: "Add a file or folder to OpenHashDB",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]
		
		// Check if using API mode
		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}
			
			// Check if path is a file or directory
			info, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("failed to stat path: %w", err)
			}
			
			if info.IsDir() {
				return fmt.Errorf("folder upload via API not yet implemented, use daemon mode")
			}
			
			return uploadFileViaAPI(path)
		}
		
		// Direct database mode
		// Initialize storage
		if err := initStorage(); err != nil {
			return err
		}
		defer store.Close()
		
		// Check if path exists
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("path does not exist: %s", path)
		}
		
		if info.IsDir() {
			return addFolder(path)
		} else {
			return addFile(path)
		}
	},
}

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get [hash]",
	Short: "Retrieve content by hash",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hashStr := args[0]
		
		// Check if using API mode
		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}
			
			return getContentViaAPI(hashStr)
		}
		
		// Direct database mode
		// Initialize storage
		if err := initStorage(); err != nil {
			return err
		}
		defer store.Close()
		
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			return fmt.Errorf("invalid hash: %w", err)
		}
		
		return getContent(hash)
	},
}

// viewCmd represents the view command
var viewCmd = &cobra.Command{
	Use:   "view [hash]",
	Short: "View content information",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hashStr := args[0]
		
		// Check if using API mode
		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}
			
			return getContentViaAPI(hashStr) // Same as get for now
		}
		
		// Direct database mode
		// Initialize storage
		if err := initStorage(); err != nil {
			return err
		}
		defer store.Close()
		
		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			return fmt.Errorf("invalid hash: %w", err)
		}
		
		return viewContent(hash)
	},
}

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all stored content",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check if using API mode
		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}
			
			return listContentViaAPI()
		}
		
		// Direct database mode
		// Initialize storage
		if err := initStorage(); err != nil {
			return err
		}
		defer store.Close()
		
		return listContent()
	},
}

// daemonCmd represents the daemon command
var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Start OpenHashDB daemon with REST API",
	RunE: func(cmd *cobra.Command, args []string) error {
		enableRest, _ := cmd.Flags().GetBool("enable-rest")
		
		// Initialize all components
		if err := initAll(); err != nil {
			return err
		}
		defer cleanup()
		
		fmt.Printf("OpenHashDB daemon started\n")
		fmt.Printf("Node ID: %s\n", node.ID().String())
		fmt.Printf("Addresses:\n")
		for _, addr := range node.Addrs() {
			fmt.Printf("  %s\n", addr)
		}
		
			if enableRest {
				// Initialize API server
				apiServer = rest.NewServer(store, repl, node)
				addr := fmt.Sprintf("0.0.0.0:%d", apiPort)
				fmt.Printf("REST API available at: http://%s\n", addr)
			
			go func() {
				if err := apiServer.Start(addr); err != nil {
					log.Printf("REST API server error: %v", err)
				}
			}()
		}
		
		// Wait for interrupt
		select {}
	},
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&dbPath, "db", "./openhash.db", "Database path")
	rootCmd.PersistentFlags().IntVar(&apiPort, "api-port", 8080, "REST API port")
	rootCmd.PersistentFlags().IntVar(&p2pPort, "p2p-port", 0, "P2P port (0 for random)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	rootCmd.PersistentFlags().StringVar(&bootnodes, "bootnode", "", "Comma-separated list of bootnode addresses (e.g., /ip4/1.2.3.4/tcp/4001/p2p/Qm...)")
	rootCmd.PersistentFlags().StringVar(&apiURL, "api", "", "REST API URL for remote operations (e.g., http://localhost:8080)")
	
	// Command-specific flags
	daemonCmd.Flags().Bool("enable-rest", true, "Enable REST API")
	
	// Add commands
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(viewCmd)
	rootCmd.AddCommand(daemonCmd)
	rootCmd.AddCommand(listCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// initStorage initializes the storage layer
func initStorage() error {
	var err error
	store, err = storage.NewStorage(dbPath)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	return nil
}

// initAll initializes all components
func initAll() error {
	// Initialize storage
	if err := initStorage(); err != nil {
		return err
	}
	
	// Parse bootnode addresses
	var bootnodeAddrs []string
	if bootnodes != "" {
		bootnodeAddrs = strings.Split(bootnodes, ",")
		for i, addr := range bootnodeAddrs {
			bootnodeAddrs[i] = strings.TrimSpace(addr)
		}
	}
	
	// Initialize libp2p node
	ctx := context.Background()
	var err error
	node, err = libp2p.NewNode(ctx, bootnodeAddrs)
	if err != nil {
		return fmt.Errorf("failed to initialize libp2p node: %w", err)
	}
	
	// Initialize replicator
	repl = replicator.NewReplicator(store, node, replicator.DefaultReplicationFactor)
	
	return nil
}

// cleanup closes all components
func cleanup() {
	if repl != nil {
		repl.Close()
	}
	if node != nil {
		node.Close()
	}
	if store != nil {
		store.Close()
	}
	if apiServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		apiServer.Stop(ctx)
	}
}

// addFile adds a single file
func addFile(path string) error {
	// Read file
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	
	// Get file info
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	
	// Compute hash
	hash := hasher.HashBytes(content)
	
	// Create metadata
	metadata := &storage.ContentMetadata{
		Hash:        hash,
		Filename:    filepath.Base(path),
		MimeType:    getMimeType(path),
		Size:        info.Size(),
		ModTime:     info.ModTime(),
		IsDirectory: false,
		CreatedAt:   time.Now(),
		RefCount:    1,
	}
	
	// Store metadata
	if err := store.StoreContent(metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}
	
	// Store data
	if err := store.StoreData(hash, content); err != nil {
		return fmt.Errorf("failed to store data: %w", err)
	}
	
	// Announce to DHT if node is available
	if node != nil {
		if err := node.AnnounceContent(hash.String()); err != nil {
			log.Printf("Warning: failed to announce content to DHT: %v", err)
		}
	}
	
	fmt.Printf("✅ File added: %s\n", hash.String())
	fmt.Printf("   Size: %d bytes\n", len(content))
	fmt.Printf("   Name: %s\n", filepath.Base(path))
	
	return nil
}

// addFolder adds a folder (simplified implementation)
func addFolder(path string) error {
	// Build DAG
	builder := dag.NewDAGBuilder()
	if verbose {
		builder.ProgressCallback = func(path string, processed, total int) {
			fmt.Printf("Processing: %s (%d/%d)\n", path, processed, total)
		}
	}
	
	dagNode, err := builder.BuildFromPath(path)
	if err != nil {
		return fmt.Errorf("failed to build DAG: %w", err)
	}
	
	// Serialize DAG
	dagData, err := dagNode.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize DAG: %w", err)
	}
	
	// Create metadata
	metadata := &storage.ContentMetadata{
		Hash:        dagNode.Hash,
		Filename:    filepath.Base(path),
		MimeType:    "application/x-directory",
		Size:        dagNode.Size,
		ModTime:     time.Now(),
		IsDirectory: true,
		CreatedAt:   time.Now(),
		RefCount:    1,
	}
	
	// Store metadata
	if err := store.StoreContent(metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}
	
	// Store DAG data
	if err := store.StoreData(dagNode.Hash, dagData); err != nil {
		return fmt.Errorf("failed to store DAG data: %w", err)
	}
	
	fmt.Printf("✅ Folder added: %s\n", dagNode.Hash.String())
	fmt.Printf("   Size: %d bytes\n", dagNode.Size)
	fmt.Printf("   Name: %s\n", filepath.Base(path))
	fmt.Printf("   Files: %d\n", len(dagNode.Links))
	
	return nil
}

// getContent retrieves and displays content
func getContent(hash hasher.Hash) error {
	// First try to get from local storage
	metadata, err := store.GetContent(hash)
	if err == nil {
		// Content found locally
		data, err := store.GetData(hash)
		if err == nil {
			return displayContent(metadata, data, hash)
		}
	}
	
	// Content not found locally, try DHT lookup if node is available
	if node != nil {
		log.Printf("Content not found locally, searching DHT...")
		providers, err := node.FindContentProviders(hash.String())
		if err != nil {
			return fmt.Errorf("failed to find content providers: %w", err)
		}
		
		if len(providers) == 0 {
			return fmt.Errorf("content not found: no providers available for hash %s", hash.String())
		}
		
		// Try to retrieve content from providers
		for _, provider := range providers {
			log.Printf("Attempting to retrieve content from provider: %s", provider.ID.String())
			data, err := node.RequestContentFromPeer(provider.ID, hash.String())
			if err != nil {
				log.Printf("Failed to retrieve from provider %s: %v", provider.ID.String(), err)
				continue
			}
			
				// Verify hash
				if retrievedHash := hasher.HashBytes(data); retrievedHash != hash {
					log.Printf("Hash mismatch from provider %s", provider.ID.String())
					continue
				}
			
			// Store locally for future use
			metadata = &storage.ContentMetadata{
				Hash:        hash,
				Filename:    fmt.Sprintf("retrieved_%s", hash.String()[:8]),
				MimeType:    "application/octet-stream",
				Size:        int64(len(data)),
				ModTime:     time.Now(),
				IsDirectory: false,
				CreatedAt:   time.Now(),
				RefCount:    1,
			}
			
			if err := store.StoreContent(metadata); err != nil {
				log.Printf("Warning: failed to store retrieved metadata: %v", err)
			}
			
			if err := store.StoreData(hash, data); err != nil {
				log.Printf("Warning: failed to store retrieved data: %v", err)
			}
			
			return displayContent(metadata, data, hash)
		}
		
		return fmt.Errorf("failed to retrieve content from any provider")
	}
	
	return fmt.Errorf("content not found: %w", err)
}

// displayContent displays content information and data
func displayContent(metadata *storage.ContentMetadata, data []byte, hash hasher.Hash) error {
	if metadata.IsDirectory {
		// Parse as DAG
		dagNode, err := dag.Deserialize(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize DAG: %w", err)
		}
		
		fmt.Printf("📁 Directory: %s\n", metadata.Filename)
		fmt.Printf("   Hash: %s\n", hash.String())
		fmt.Printf("   Size: %d bytes\n", metadata.Size)
		fmt.Printf("   Files:\n")
		
		for _, link := range dagNode.Links {
			typeIcon := "📄"
			if link.Type == dag.NodeTypeDirectory {
				typeIcon = "📁"
			}
			fmt.Printf("     %s %s (%s, %d bytes)\n", typeIcon, link.Name, link.Hash.String()[:8], link.Size)
		}
	} else {
		// Regular file
		fmt.Printf("📄 File: %s\n", metadata.Filename)
		fmt.Printf("   Hash: %s\n", hash.String())
		fmt.Printf("   Size: %d bytes\n", metadata.Size)
		fmt.Printf("   MIME: %s\n", metadata.MimeType)
		
		// If it's a text file and small enough, show content
		if isTextFile(metadata.MimeType) && len(data) < 1024 {
			fmt.Printf("   Content:\n")
			fmt.Printf("   %s\n", string(data))
		}
	}
	
	return nil
}

// viewContent displays content information
func viewContent(hash hasher.Hash) error {
	metadata, err := store.GetContent(hash)
	if err != nil {
		return fmt.Errorf("content not found: %w", err)
	}
	
	fmt.Printf("Hash: %s\n", metadata.Hash.String())
	fmt.Printf("Filename: %s\n", metadata.Filename)
	fmt.Printf("MIME Type: %s\n", metadata.MimeType)
	fmt.Printf("Size: %d bytes\n", metadata.Size)
	fmt.Printf("Modified: %s\n", metadata.ModTime.Format(time.RFC3339))
	fmt.Printf("Created: %s\n", metadata.CreatedAt.Format(time.RFC3339))
	fmt.Printf("Reference Count: %d\n", metadata.RefCount)
	fmt.Printf("Is Directory: %t\n", metadata.IsDirectory)
	
	if metadata.ChunkCount > 0 {
		fmt.Printf("Chunk Count: %d\n", metadata.ChunkCount)
	}
	
	return nil
}

// listContent lists all stored content
func listContent() error {
	hashes, err := store.ListContent()
	if err != nil {
		return fmt.Errorf("failed to list content: %w", err)
	}
	
	if len(hashes) == 0 {
		fmt.Println("No content stored")
		return nil
	}
	
	fmt.Printf("Stored content (%d items):\n", len(hashes))
	fmt.Println()
	
	for _, hash := range hashes {
		metadata, err := store.GetContent(hash)
		if err != nil {
			continue
		}
		
		typeIcon := "📄"
		if metadata.IsDirectory {
			typeIcon = "📁"
		}
		
		fmt.Printf("%s %s\n", typeIcon, metadata.Filename)
		fmt.Printf("   Hash: %s\n", hash.String())
		fmt.Printf("   Size: %d bytes\n", metadata.Size)
		fmt.Printf("   Created: %s\n", metadata.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Println()
	}
	
	return nil
}

// Helper functions

func getMimeType(path string) string {
	ext := filepath.Ext(path)
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

func isTextFile(mimeType string) bool {
	return mimeType == "text/plain" || 
		   mimeType == "text/html" || 
		   mimeType == "application/json" ||
		   mimeType == "text/css" ||
		   mimeType == "application/javascript"
}

// HTTP client functions for API operations

// getAPIURL returns the API URL to use, with fallback to default
func getAPIURL() string {
	if apiURL != "" {
		return strings.TrimSuffix(apiURL, "/")
	}
	return fmt.Sprintf("http://localhost:%d", apiPort)
}

// uploadFileViaAPI uploads a file via REST API
func uploadFileViaAPI(filePath string) error {
	baseURL := getAPIURL()
	
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	
	// Create multipart form
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	
	// Add file field
	fileWriter, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}
	
	if _, err := io.Copy(fileWriter, file); err != nil {
		return fmt.Errorf("failed to copy file data: %w", err)
	}
	
	writer.Close()
	
	// Make request
	resp, err := http.Post(baseURL+"/upload/file", writer.FormDataContentType(), &buf)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	defer resp.Body.Close()
	
	// Parse response
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}
	
	if resp.StatusCode != http.StatusOK {
		if errMsg, ok := result["error"].(string); ok {
			return fmt.Errorf("upload failed: %s", errMsg)
		}
		return fmt.Errorf("upload failed with status: %d", resp.StatusCode)
	}
	
	// Display result
	if hash, ok := result["hash"].(string); ok {
		fmt.Printf("✅ File uploaded: %s\n", hash)
		if size, ok := result["size"].(float64); ok {
			fmt.Printf("   Size: %.0f bytes\n", size)
		}
		if filename, ok := result["filename"].(string); ok {
			fmt.Printf("   Name: %s\n", filename)
		}
	}
	
	return nil
}

// getContentViaAPI retrieves content via REST API
func getContentViaAPI(hash string) error {
	baseURL := getAPIURL()
	
	// First get content info
	resp, err := http.Get(baseURL + "/info/" + hash)
	if err != nil {
		return fmt.Errorf("failed to get content info: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("content not found: %s", hash)
	}
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get content info, status: %d", resp.StatusCode)
	}
	
	var info map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fmt.Errorf("failed to parse content info: %w", err)
	}
	
	// Display content info
	fmt.Printf("📄 File: %s\n", info["filename"])
	fmt.Printf("   Hash: %s\n", hash)
	if size, ok := info["size"].(float64); ok {
		fmt.Printf("   Size: %.0f bytes\n", size)
	}
	if mimeType, ok := info["mime_type"].(string); ok {
		fmt.Printf("   MIME: %s\n", mimeType)
	}
	
	// If it's a small text file, show content
	if mimeType, ok := info["mime_type"].(string); ok && strings.HasPrefix(mimeType, "text/") {
		if size, ok := info["size"].(float64); ok && size < 1024 {
			// Download content
			resp, err := http.Get(baseURL + "/download/" + hash)
			if err != nil {
				return fmt.Errorf("failed to download content: %w", err)
			}
			defer resp.Body.Close()
			
			if resp.StatusCode == http.StatusOK {
				content, err := io.ReadAll(resp.Body)
				if err == nil {
					fmt.Printf("   Content:\n")
					fmt.Printf("   %s\n", string(content))
				}
			}
		}
	}
	
	return nil
}

// listContentViaAPI lists content via REST API
func listContentViaAPI() error {
	baseURL := getAPIURL()
	
	resp, err := http.Get(baseURL + "/list")
	if err != nil {
		return fmt.Errorf("failed to list content: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to list content, status: %d", resp.StatusCode)
	}
	
	var items []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}
	
	fmt.Printf("📋 Stored Content (%d items):\n\n", len(items))
	
	for _, item := range items {
		if hash, ok := item["hash"].(string); ok {
			fmt.Printf("📄 %s\n", hash)
			if filename, ok := item["filename"].(string); ok {
				fmt.Printf("   Name: %s\n", filename)
			}
			if size, ok := item["size"].(float64); ok {
				fmt.Printf("   Size: %.0f bytes\n", size)
			}
			if mimeType, ok := item["mime_type"].(string); ok {
				fmt.Printf("   Type: %s\n", mimeType)
			}
			fmt.Println()
		}
	}
	
	if len(items) == 0 {
		fmt.Println("📋 No content stored")
	}
	
	return nil
}

// shouldUseAPI determines if API mode should be used
func shouldUseAPI() bool {
	// If --api flag was provided (even with empty value), use API mode
	cmd := rootCmd
	if cmd.Flags().Changed("api") {
		return true
	}
	return apiURL != ""
}

// checkAPIConnection verifies API connectivity
func checkAPIConnection() error {
	baseURL := getAPIURL()
	
	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		return fmt.Errorf("failed to connect to API at %s: %w", baseURL, err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API health check failed, status: %d", resp.StatusCode)
	}
	
	return nil
}

