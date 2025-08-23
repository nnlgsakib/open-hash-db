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
	"os"
	"path/filepath"
	"strings"
	"time"

	"encoding/hex"
	"openhashdb/api/rest"
	"openhashdb/core/block"
	"openhashdb/core/blockstore"
	"openhashdb/core/chunker"
	"openhashdb/core/hasher"
	"openhashdb/core/merkle"
	"openhashdb/core/utils"
	"openhashdb/network/bitswap"
	"openhashdb/network/libp2p"
	"openhashdb/network/replicator"
	"openhashdb/protobuf/pb"
	"openhashdb/version"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// Global flags
	dbPath    string
	keyPath   string // New flag for key path
	apiPort   int
	p2pPort   int
	verbose   bool
	bootnodes string
	relays    string
	apiURL    string

	// Global instances
	bs        *blockstore.Blockstore
	node      *libp2p.Node
	repl      *replicator.Replicator
	apiServer *rest.Server
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
				return uploadFolderViaAPI(path)
			}

			return uploadFileViaAPI(path)
		}

		// Direct database mode
		// Initialize storage
		if err := initStorage(); err != nil {
			return err
		}
		defer bs.Close()

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
		defer bs.Close()

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
		defer bs.Close()

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
		defer bs.Close()

		return listContent()
	},
}
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of OpenHashDB",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Version: %s\n", version.Version)
		fmt.Printf("Author: %s\n", version.Author)
		fmt.Printf("Branch: %s\n", version.Branch)
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
			apiServer = rest.NewServer(bs, repl, node)
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
	rootCmd.PersistentFlags().StringVar(&dbPath, "db", "./.openhashdb", "Database root path")
	rootCmd.PersistentFlags().StringVar(&keyPath, "key-path", "", "Path to the node's private key file (defaults to <db-path>/peer.key)")
	rootCmd.PersistentFlags().IntVar(&apiPort, "api-port", 8080, "REST API port")
	rootCmd.PersistentFlags().IntVar(&p2pPort, "p2p-port", 0, "P2P port (0 for random)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	rootCmd.PersistentFlags().StringVar(&bootnodes, "bootnode", "", "Comma-separated list of bootnode addresses (e.g., /ip4/1.2.3.4/tcp/4001/p2p/Qm...)")
	rootCmd.PersistentFlags().StringVar(&relays, "relays", "", "Comma-separated list of static relay addresses (hop relays) for AutoRelay")
	rootCmd.PersistentFlags().StringVar(&apiURL, "api", "", "REST API URL for remote operations (e.g., http://localhost:8080)")

	// Command-specific flags

	daemonCmd.Flags().Bool("enable-rest", true, "Enable REST API")

	// Add commands
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(viewCmd)
	rootCmd.AddCommand(daemonCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(versionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// initStorage initializes the blockstore layer
func initStorage() error {
	var err error
	bs, err = blockstore.NewBlockstore(dbPath)
	if err != nil {
		return fmt.Errorf("failed to initialize blockstore: %w", err)
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

	// Parse relay addresses
	var relayAddrs []string
	if relays != "" {
		relayAddrs = strings.Split(relays, ",")
		for i, addr := range relayAddrs {
			relayAddrs[i] = strings.TrimSpace(addr)
		}
	}

	// Determine key path
	actualKeyPath := keyPath
	if actualKeyPath == "" {
		actualKeyPath = filepath.Join(dbPath, "peer.key")
	}

	// Initialize libp2p node
	ctx := context.Background()
	var err error
	node, err = libp2p.NewNodeWithKeyPath(ctx, bootnodeAddrs, relayAddrs, actualKeyPath, p2pPort)
	if err != nil {
		return fmt.Errorf("failed to initialize libp2p node: %w", err)
	}
	node.SetBlockstore(bs) // Set the blockstore on the node

	// Initialize Bitswap Engine
	bitswapEngine := bitswap.NewEngine(ctx, node.Host(), bs)
	node.SetBitswap(bitswapEngine)

	// Initialize replicator
	repl = replicator.NewReplicator(bs, node, bitswapEngine, replicator.DefaultReplicationFactor)

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
	if bs != nil {
		bs.Close()
	}
	if apiServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		apiServer.Stop(ctx)
	}
}

// addFile adds a single file
func addFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	c := chunker.NewChunker()
	merkleFile, chunks, err := merkle.BuildFileTree(file, c)
	if err != nil {
		return fmt.Errorf("failed to build merkle tree: %w", err)
	}

	for _, chunk := range chunks {
		if has, _ := bs.Has(chunk.Hash); !has {
			if err := bs.Put(block.NewBlock(chunk.Data)); err != nil {
				return fmt.Errorf("failed to store chunk %s: %w", chunk.Hash.String(), err)
			}
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	pbChunks := make([]*pb.ChunkInfo, len(chunks))
	for i, chunk := range chunks {
		pbChunks[i] = &pb.ChunkInfo{
			Hash: chunk.Hash[:],
			Size: int64(len(chunk.Data)),
		}
	}

	metadata := &pb.ContentMetadata{
		Hash:        merkleFile.Root[:],
		Filename:    filepath.Base(path),
		MimeType:    utils.GetMimeType(path),
		Size:        merkleFile.TotalSize,
		ModTime:     timestamppb.New(info.ModTime()),
		IsDirectory: false,
		CreatedAt:   timestamppb.Now(),
		RefCount:    1,
		Chunks:      pbChunks,
	}

	if err := bs.StoreContent(metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	if node != nil {
		if err := node.AnnounceContent(merkleFile.Root.String()); err != nil {
			log.Printf("Warning: failed to announce content to DHT: %v", err)
		}
	}

	fmt.Printf("✅ File added: %s\n", merkleFile.Root.String())
	fmt.Printf("   Size: %d bytes\n", merkleFile.TotalSize)
	fmt.Printf("   Name: %s\n", filepath.Base(path))

	return nil
}

// addFolder adds a folder
func addFolder(path string) error {
	link, err := storeDirectoryRecursive(path, filepath.Base(path))
	if err != nil {
		return err
	}

	fmt.Printf("✅ Folder added: %s\n", link.Hash.String())
	fmt.Printf("   Size: %d bytes\n", link.Size)
	fmt.Printf("   Name: %s\n", link.Name)

	return nil
}

func storeDirectoryRecursive(path string, name string) (*merkle.Link, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var links []merkle.Link
	c := chunker.NewChunker()

	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		var link *merkle.Link

		if entry.IsDir() {
			link, err = storeDirectoryRecursive(entryPath, entry.Name())
			if err != nil {
				return nil, err
			}
		} else {
			file, err := os.Open(entryPath)
			if err != nil {
				return nil, err
			}

			merkleFile, chunks, err := merkle.BuildFileTree(file, c)
			file.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to build merkle tree for %s: %w", entry.Name(), err)
			}

			for _, chunk := range chunks {
				if has, _ := bs.Has(chunk.Hash); !has {
					if err := bs.Put(block.NewBlock(chunk.Data)); err != nil {
						return nil, fmt.Errorf("failed to store chunk %s: %w", chunk.Hash.String(), err)
					}
				}
			}

			info, _ := entry.Info()

			pbChunks := make([]*pb.ChunkInfo, len(chunks))
			for i, chunk := range chunks {
				pbChunks[i] = &pb.ChunkInfo{
					Hash: chunk.Hash[:],
					Size: int64(len(chunk.Data)),
				}
			}

			fileMetadata := &pb.ContentMetadata{
				Hash:        merkleFile.Root[:],
				Filename:    entry.Name(),
				MimeType:    utils.GetMimeType(entry.Name()),
				Size:        merkleFile.TotalSize,
				ModTime:     timestamppb.New(info.ModTime()),
				IsDirectory: false,
				CreatedAt:   timestamppb.Now(),
				RefCount:    1,
				Chunks:      pbChunks,
			}
			if err := bs.StoreContent(fileMetadata); err != nil {
				return nil, fmt.Errorf("failed to store metadata for %s: %w", entry.Name(), err)
			}

			link = &merkle.Link{
				Name: entry.Name(),
				Hash: merkleFile.Root,
				Size: merkleFile.TotalSize,
				Type: "file",
			}
		}
		links = append(links, *link)
	}

	dirHash, err := merkle.BuildDirectoryTree(links)
	if err != nil {
		return nil, err
	}

	var totalSize int64
	for _, l := range links {
		totalSize += l.Size
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	pbLinks := make([]*pb.Link, len(links))
	for i, link := range links {
		pbLinks[i] = &pb.Link{
			Name: link.Name,
			Hash: link.Hash[:],
			Size: link.Size,
			Type: link.Type,
		}
	}

	dirMetadata := &pb.ContentMetadata{
		Hash:        dirHash[:],
		Filename:    name,
		MimeType:    "inode/directory",
		Size:        totalSize,
		ModTime:     timestamppb.New(info.ModTime()),
		IsDirectory: true,
		CreatedAt:   timestamppb.Now(),
		RefCount:    1,
		Links:       pbLinks,
	}

	if err := bs.StoreContent(dirMetadata); err != nil {
		return nil, err
	}

	return &merkle.Link{
		Name: name,
		Hash: dirHash,
		Size: totalSize,
		Type: "directory",
	}, nil
}

// getContent retrieves content from local storage or the network
func getContent(hash hasher.Hash) error {
	// First try to get from local storage
	metadata, err := bs.GetContent(hash)
	if err == nil {
		return displayContent(metadata, hash)
	}

	// Content not found locally, try DHT lookup if node is available
	if node != nil {
		log.Printf("Content not found locally, searching network...")
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// This is a simplification. In a real scenario, we would fetch the metadata first.
		blk, err := node.GetBitswap().GetBlock(ctx, hash)
		if err != nil {
			return fmt.Errorf("failed to get content from network: %w", err)
		}
		log.Printf("Got block: %s", blk.Hash())
		// We can't display full content info as we only have the root block.
		fmt.Printf("✅ Content retrieved from network: %s\n", hash.String())
		return nil
	}

	return fmt.Errorf("content not found: %w", err)
}

// displayContent displays content information and data
func displayContent(metadata *pb.ContentMetadata, hash hasher.Hash) error {
	if metadata.IsDirectory {
		fmt.Printf("📁 Directory: %s\n", metadata.Filename)
		fmt.Printf("   Hash: %s\n", hash.String())
		fmt.Printf("   Size: %d bytes\n", metadata.Size)
		fmt.Printf("   Files:\n")

		for _, link := range metadata.Links {
			typeIcon := "📄"
			if link.Type == "directory" {
				typeIcon = "📁"
			}
			fmt.Printf("     %s %s (%s, %d bytes)\n", typeIcon, link.Name, hex.EncodeToString(link.Hash)[:8], link.Size)
		}
	} else {
		// Regular file
		fmt.Printf("📄 File: %s\n", metadata.Filename)
		fmt.Printf("   Hash: %s\n", hash.String())
		fmt.Printf("   Size: %d bytes\n", metadata.Size)
		fmt.Printf("   MIME: %s\n", metadata.MimeType)
	}

	return nil
}

// viewContent displays content information
func viewContent(hash hasher.Hash) error {
	metadata, err := bs.GetContent(hash)
	if err != nil {
		return fmt.Errorf("content not found: %w", err)
	}

	fmt.Printf("Hash: %s\n", hex.EncodeToString(metadata.Hash))
	fmt.Printf("Filename: %s\n", metadata.Filename)
	fmt.Printf("MIME Type: %s\n", metadata.MimeType)
	fmt.Printf("Size: %d bytes\n", metadata.Size)
	fmt.Printf("Modified: %s\n", metadata.ModTime.AsTime().Format(time.RFC3339))
	fmt.Printf("Created: %s\n", metadata.CreatedAt.AsTime().Format(time.RFC3339))
	fmt.Printf("Reference Count: %d\n", metadata.RefCount)
	fmt.Printf("Is Directory: %t\n", metadata.IsDirectory)

	if len(metadata.Chunks) > 0 {
		fmt.Printf("Chunk Count: %d\n", len(metadata.Chunks))
	}

	return nil
}

// listContent lists all stored content
func listContent() error {
	hashes, err := bs.ListContent()
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
		metadata, err := bs.GetContent(hash)
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
		fmt.Printf("   Created: %s\n", metadata.CreatedAt.AsTime().Format("2006-01-02 15:04:05"))
		fmt.Println()
	}

	return nil
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

// uploadFolderViaAPI uploads a folder via REST API
func uploadFolderViaAPI(dirPath string) error {
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
			if err != nil {
				return err
			}
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
