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
	"openhashdb/core/chunker"
	"openhashdb/core/dag"
	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/bootnode"
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
	dbPath    string
	keyPath   string
	apiPort   int
	p2pPort   int
	verbose   bool
	bootnodes string
	apiURL    string

	// Global instances (initialize as nil)
	store     *storage.Storage
	node      *libp2p.Node
	repl      *replicator.Replicator
	apiServer *rest.Server
)

// rootCmd represents the base command
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
		cmd.Help()
	},
}

// addCmd adds a file or folder
var addCmd = &cobra.Command{
	Use:   "add [file/folder]",
	Short: "Add a file or folder to OpenHashDB",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]
		pin, _ := cmd.Flags().GetBool("pin")

		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}

			info, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("failed to stat path: %w", err)
			}

			if info.IsDir() {
				return uploadFolderViaAPI(path, pin)
			}
			return uploadFileViaAPI(path, pin)
		}

		if err := initStorage(); err != nil {
			return err
		}
		defer store.Close()

		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("path does not exist: %w", err)
		}

		if info.IsDir() {
			return addFolder(path)
		}
		return addFile(path)
	},
}

// getCmd retrieves content by hash
var getCmd = &cobra.Command{
	Use:   "get [hash]",
	Short: "Retrieve content by hash",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hashStr := args[0]

		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}
			return getContentViaAPI(hashStr)
		}

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

// viewCmd views content information
var viewCmd = &cobra.Command{
	Use:   "view [hash]",
	Short: "View content information",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hashStr := args[0]

		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}
			return getContentViaAPI(hashStr)
		}

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

// listCmd lists all stored content
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all stored content",
	RunE: func(cmd *cobra.Command, args []string) error {
		if shouldUseAPI() {
			if err := checkAPIConnection(); err != nil {
				return fmt.Errorf("API connection failed: %w", err)
			}
			return listContentViaAPI()
		}

		if err := initStorage(); err != nil {
			return err
		}
		defer store.Close()

		return listContent()
	},
}

// daemonCmd starts the OpenHashDB daemon
var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Start OpenHashDB daemon with REST API",
	RunE: func(cmd *cobra.Command, args []string) error {
		enableRest, _ := cmd.Flags().GetBool("enable-rest")

		if err := initAll(); err != nil {
			return err
		}
		defer cleanup()

		fmt.Printf("OpenHashDB daemon started\n")
		fmt.Printf("Node ID: %s\n", node.ID().String())
		fmt.Println("Addresses:")
		for _, addr := range node.Addrs() {
			fmt.Printf("  %s\n", addr)
		}

		if enableRest {
			apiServer = rest.NewServer(store, repl, node)
			addr := fmt.Sprintf("0.0.0.0:%d", apiPort)
			fmt.Printf("REST API available at: http://%s\n", addr)

			go func() {
				if err := apiServer.Start(addr); err != nil {
					log.Printf("REST API server error: %v", err)
				}
			}()
		}

		select {}
	},
}

// bootnodeCmd runs a standalone bootnode
var bootnodeCmd = &cobra.Command{
	Use:   "bootnode",
	Short: "Run a standalone bootnode and relayer",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		actualKeyPath := keyPath
		if actualKeyPath == "" {
			actualKeyPath = filepath.Join(dbPath, "peer.key")
		}

		var bootnodeAddrs []string
		if bootnodes != "" {
			bootnodeAddrs = strings.Split(bootnodes, ",")
			for i, addr := range bootnodeAddrs {
				bootnodeAddrs[i] = strings.TrimSpace(addr)
			}
		}

		node, err := bootnode.NewBootNode(ctx, actualKeyPath, bootnodeAddrs, p2pPort)
		if err != nil {
			return fmt.Errorf("failed to create bootnode: %w", err)
		}
		defer node.Close()

		fmt.Println("Bootnode started successfully")
		fmt.Printf("Node ID: %s\n", node.ID().String())
		fmt.Println("Addresses:")
		for _, addr := range node.Addrs() {
			fmt.Printf("  %s\n", addr)
		}

		select {}
	},
}

// pinCmd pins content
var pinCmd = &cobra.Command{
	Use:   "pin [hash]",
	Short: "Pin content to prevent garbage collection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hashStr := args[0]

		if !shouldUseAPI() {
			return fmt.Errorf("pin command only available in API mode")
		}
		if err := checkAPIConnection(); err != nil {
			return fmt.Errorf("API connection failed: %w", err)
		}

		return pinViaAPI(hashStr)
	},
}

// unpinCmd unpins content
var unpinCmd = &cobra.Command{
	Use:   "unpin [hash]",
	Short: "Unpin content to allow garbage collection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hashStr := args[0]

		if !shouldUseAPI() {
			return fmt.Errorf("unpin command only available in API mode")
		}
		if err := checkAPIConnection(); err != nil {
			return fmt.Errorf("API connection failed: %w", err)
		}

		return unpinViaAPI(hashStr)
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dbPath, "db", "./openhash.db", "Database path")
	rootCmd.PersistentFlags().StringVar(&keyPath, "key-path", "", "Path to the node's private key file")
	rootCmd.PersistentFlags().IntVar(&apiPort, "api-port", 8080, "REST API port")
	rootCmd.PersistentFlags().IntVar(&p2pPort, "p2p-port", 0, "P2P port (0 for random)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	rootCmd.PersistentFlags().StringVar(&bootnodes, "bootnode", "", "Comma-separated list of bootnode addresses")
	rootCmd.PersistentFlags().StringVar(&apiURL, "api", "", "REST API URL for remote operations")

	daemonCmd.Flags().Bool("enable-rest", true, "Enable REST API")
	addCmd.Flags().Bool("pin", false, "Pin the added content")

	rootCmd.AddCommand(addCmd, getCmd, viewCmd, daemonCmd, listCmd, bootnodeCmd, pinCmd, unpinCmd)
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
	if err := initStorage(); err != nil {
		return err
	}

	var bootnodeAddrs []string
	if bootnodes != "" {
		bootnodeAddrs = strings.Split(bootnodes, ",")
		for i, addr := range bootnodeAddrs {
			bootnodeAddrs[i] = strings.TrimSpace(addr)
		}
	}

	actualKeyPath := keyPath
	if actualKeyPath == "" {
		actualKeyPath = filepath.Join(dbPath, "peer.key")
	}

	ctx := context.Background()
	var err error
	node, err = libp2p.NewNodeWithKeyPath(ctx, bootnodeAddrs, actualKeyPath, p2pPort)
	if err != nil {
		return fmt.Errorf("failed to initialize libp2p node: %w", err)
	}
	node.SetStorage(store)

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
		if err := apiServer.Stop(ctx); err != nil {
			log.Printf("Failed to stop API server: %v", err)
		}
	}
}

// addFile adds a single file
func addFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	chunker := chunker.NewChunker(chunker.ChunkSize1MB)
	chunks, err := chunker.ChunkReader(file)
	if err != nil {
		return fmt.Errorf("failed to chunk file: %w", err)
	}

	var chunkHashes []hasher.Hash
	for _, chunk := range chunks {
		if err := store.StoreData(chunk.Hash, chunk.Data); err != nil {
			return fmt.Errorf("failed to store chunk %s: %w", chunk.Hash.String(), err)
		}
		chunkHashes = append(chunkHashes, chunk.Hash)
	}

	contentHash := hasher.HashMultiple(chunkHashes...)
	metadata := &storage.ContentMetadata{
		Hash:        contentHash,
		ContentHash: contentHash,
		Filename:    filepath.Base(path),
		MimeType:    getMimeType(path),
		Size:        info.Size(),
		ModTime:     info.ModTime(),
		ChunkHashes: chunkHashes,
		ChunkCount:  len(chunks),
		IsDirectory: false,
		CreatedAt:   time.Now(),
		RefCount:    1,
	}

	if err := store.StoreContent(metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	if node != nil {
		if err := node.AnnounceContent(contentHash.String()); err != nil {
			log.Printf("Warning: failed to announce content to DHT: %v", err)
		}
	}

	fmt.Printf("✅ File added: %s\n", contentHash.String())
	fmt.Printf("   Size: %d bytes\n", info.Size())
	fmt.Printf("   Name: %s\n", filepath.Base(path))
	fmt.Printf("   Chunks: %d\n", len(chunks))
	return nil
}

// addFolder adds a folder
func addFolder(path string) error {
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

	dagData, err := dagNode.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize DAG: %w", err)
	}

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

	if err := store.StoreContent(metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

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
	metadata, err := store.GetContent(hash)
	if err == nil {
		data, err := store.GetData(hash)
		if err == nil {
			return displayContent(metadata, data, hash)
		}
	}

	if node != nil {
		log.Printf("Content not found locally, searching DHT...")
		providers, err := node.FindContentProviders(hash.String())
		if err != nil {
			return fmt.Errorf("failed to find content providers: %w", err)
		}

		if len(providers) == 0 {
			return fmt.Errorf("content not found: no providers available for hash %s", hash.String())
		}

		for _, provider := range providers {
			if provider.ID == node.ID() {
				continue
			}
			log.Printf("Attempting to retrieve content from provider: %s", provider.ID.String())
			data, retrievedMetadata, err := node.RequestContentFromPeer(provider.ID, hash.String())
			if err != nil {
				log.Printf("Failed to retrieve from provider %s: %v", provider.ID.String(), err)
				continue
			}

			if err := store.StoreContent(retrievedMetadata); err != nil {
				log.Printf("Warning: failed to store retrieved metadata: %v", err)
			}

			if err := store.StoreData(hash, data); err != nil {
				log.Printf("Warning: failed to store retrieved data: %v", err)
			}

			log.Printf("Successfully retrieved and stored content from %s", provider.ID.String())
			return displayContent(retrievedMetadata, data, hash)
		}

		return fmt.Errorf("failed to retrieve content from any provider")
	}

	return fmt.Errorf("content not found: %w", err)
}

// displayContent displays content information and data
func displayContent(metadata *storage.ContentMetadata, data []byte, hash hasher.Hash) error {
	if metadata.IsDirectory {
		dagNode, err := dag.Deserialize(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize DAG: %w", err)
		}

		fmt.Printf("📁 Directory: %s\n", metadata.Filename)
		fmt.Printf("   Hash: %s\n", hash.String())
		fmt.Printf("   Size: %d bytes\n", metadata.Size)
		fmt.Println("   Files:")
		for _, link := range dagNode.Links {
			typeIcon := "📄"
			if link.Type == dag.NodeTypeDirectory {
				typeIcon = "📁"
			}
			fmt.Printf("     %s %s (%s, %d bytes)\n", typeIcon, link.Name, link.Hash.String()[:8], link.Size)
		}
	} else {
		fmt.Printf("📄 File: %s\n", metadata.Filename)
		fmt.Printf("   Hash: %s\n", hash.String())
		fmt.Printf("   Size: %d bytes\n", metadata.Size)
		fmt.Printf("   MIME: %s\n", metadata.MimeType)

		if isTextFile(metadata.MimeType) && len(data) < 1024 {
			fmt.Println("   Content:")
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

	fmt.Printf("Stored content (%d items):\n\n", len(hashes))
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
		fmt.Printf("   Created: %s\n\n", metadata.CreatedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

// getMimeType determines MIME type based on file extension
func getMimeType(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".html", ".htm", ".shtml", ".xhtml":
		return "text/html"
	case ".css", ".scss", ".sass", ".less":
		return "text/css"
	case ".js", ".mjs", ".jsx":
		return "application/javascript"
	case ".ts", ".tsx":
		return "application/typescript"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".svg":
		return "image/svg+xml"
	case ".csv":
		return "text/csv"
	case ".md", ".markdown":
		return "text/markdown"
	case ".webmanifest":
		return "application/manifest+json"
	case ".vtt":
		return "text/vtt"
	case ".txt":
		return "text/plain"
	case ".php":
		return "text/x-php"
	case ".py":
		return "text/x-python"
	case ".rb":
		return "text/x-ruby"
	case ".java":
		return "text/x-java-source"
	case ".c":
		return "text/x-c"
	case ".cpp":
		return "text/x-c++src"
	case ".cs":
		return "text/x-csharp"
	case ".go":
		return "text/x-go"
	case ".sh":
		return "text/x-shellscript"
	case ".sql":
		return "text/x-sql"
	case ".yaml", ".yml":
		return "text/yaml"
	case ".toml":
		return "text/x-toml"
	case ".ini":
		return "text/x-ini"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".avif":
		return "image/avif"
	case ".ico":
		return "image/x-icon"
	case ".bmp":
		return "image/bmp"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".ogg":
		return "audio/ogg"
	case ".aac":
		return "audio/aac"
	case ".flac":
		return "audio/flac"
	case ".m4a":
		return "audio/mp4"
	case ".mp4":
		return "video/mp4"
	case ".webm":
		return "video/webm"
	case ".ogv":
		return "video/ogg"
	case ".mov":
		return "video/quicktime"
	case ".mkv":
		return "video/x-matroska"
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
	case ".pdf":
		return "application/pdf"
	case ".wasm":
		return "application/wasm"
	default:
		return "application/octet-stream"
	}
}

// isTextFile checks if the MIME type is text-based
func isTextFile(mimeType string) bool {
	return mimeType == "text/plain" ||
		mimeType == "text/html" ||
		mimeType == "application/json" ||
		mimeType == "text/css" ||
		mimeType == "application/javascript"
}

// getAPIURL returns the API URL
func getAPIURL() string {
	if apiURL != "" {
		return strings.TrimSuffix(apiURL, "/")
	}
	return fmt.Sprintf("http://localhost:%d", apiPort)
}

// uploadFileViaAPI uploads a file via REST API
func uploadFileViaAPI(filePath string, pin bool) error {
	baseURL := getAPIURL()

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	fileWriter, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := io.Copy(fileWriter, file); err != nil {
		return fmt.Errorf("failed to copy file data: %w", err)
	}
	writer.Close()

	resp, err := http.Post(baseURL+"/upload/file", writer.FormDataContentType(), &buf)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	defer resp.Body.Close()

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

	if hash, ok := result["hash"].(string); ok {
		fmt.Printf("✅ File uploaded: %s\n", hash)
		if size, ok := result["size"].(float64); ok {
			fmt.Printf("   Size: %.0f bytes\n", size)
		}
		if filename, ok := result["filename"].(string); ok {
			fmt.Printf("   Name: %s\n", filename)
		}
		if pin {
			if err := pinViaAPI(hash); err != nil {
				return fmt.Errorf("failed to pin content: %w", err)
			}
		}
	}
	return nil
}

// uploadFolderViaAPI uploads a folder via REST API
func uploadFolderViaAPI(dirPath string, pin bool) error {
	baseURL := getAPIURL()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			relPath, err := filepath.Rel(dirPath, path)
			if err != nil {
				return err
			}

			part, err := writer.CreateFormFile("files", relPath)
			if err != nil {
				return err
			}
			_, err = io.Copy(part, file)
			return err
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}
	writer.Close()

	resp, err := http.Post(baseURL+"/upload/folder", writer.FormDataContentType(), &buf)
	if err != nil {
		return fmt.Errorf("failed to upload folder: %w", err)
	}
	defer resp.Body.Close()

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

	if hash, ok := result["hash"].(string); ok {
		fmt.Printf("✅ Folder uploaded: %s\n", hash)
		if size, ok := result["size"].(float64); ok {
			fmt.Printf("   Size: %.0f bytes\n", size)
		}
		if filename, ok := result["filename"].(string); ok {
			fmt.Printf("   Name: %s\n", filename)
		}
		if pin {
			if err := pinViaAPI(hash); err != nil {
				return fmt.Errorf("failed to pin content: %w", err)
			}
		}
	}
	return nil
}

// getContentViaAPI retrieves content via REST API
func getContentViaAPI(hash string) error {
	baseURL := getAPIURL()

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

	fmt.Printf("📄 File: %s\n", info["filename"])
	fmt.Printf("   Hash: %s\n", hash)
	if size, ok := info["size"].(float64); ok {
		fmt.Printf("   Size: %.0f bytes\n", size)
	}
	if mimeType, ok := info["mime_type"].(string); ok {
		fmt.Printf("   MIME: %s\n", mimeType)
	}

	if mimeType, ok := info["mime_type"].(string); ok && strings.HasPrefix(mimeType, "text/") {
		if size, ok := info["size"].(float64); ok && size < 1024 {
			resp, err := http.Get(baseURL + "/download/" + hash)
			if err != nil {
				return fmt.Errorf("failed to download content: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				content, err := io.ReadAll(resp.Body)
				if err != nil {
					return fmt.Errorf("failed to read content: %w", err)
				}
				fmt.Println("   Content:")
				fmt.Printf("   %s\n", string(content))
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
	return rootCmd.Flags().Changed("api") || apiURL != ""
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

// pinViaAPI pins content via REST API
func pinViaAPI(hash string) error {
	baseURL := getAPIURL()
	req, err := http.NewRequest("POST", baseURL+"/pin/"+hash, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to pin content: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			if errMsg, ok := result["message"].(string); ok {
				return fmt.Errorf("pin failed: %s", errMsg)
			}
		}
		return fmt.Errorf("pin failed with status: %d", resp.StatusCode)
	}

	fmt.Printf("✅ Content pinned: %s\n", hash)
	return nil
}

// unpinViaAPI unpins content via REST API
func unpinViaAPI(hash string) error {
	baseURL := getAPIURL()
	req, err := http.NewRequest("DELETE", baseURL+"/unpin/"+hash, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to unpin content: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			if errMsg, ok := result["message"].(string); ok {
				return fmt.Errorf("unpin failed: %s", errMsg)
			}
		}
		return fmt.Errorf("unpin failed with status: %d", resp.StatusCode)
	}

	fmt.Printf("✅ Content unpinned: %s\n", hash)
	return nil
}
