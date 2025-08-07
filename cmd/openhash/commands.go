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
	keyPath   string // Path to the node's private key file
	apiPort   int    // REST API port
	p2pPort   int    // P2P port
	verbose   bool   // Verbose output
	bootnodes string // Comma-separated list of bootnode addresses
	apiURL    string // REST API URL

	// Global instances (only used in daemon mode)
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
- CLI for command-line usage via REST API

Run 'openhash daemon' to start the server before using other commands.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

// addCmd adds a file or folder via the REST API
var addCmd = &cobra.Command{
	Use:   "add [file/folder]",
	Short: "Add a file or folder to OpenHashDB",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkAPIConnection(); err != nil {
			return fmt.Errorf("API connection failed: %w (ensure 'openhash daemon' is running)", err)
		}

		path := args[0]
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("failed to stat path: %w", err)
		}

		if info.IsDir() {
			return uploadFolderViaAPI(path)
		}
		return uploadFileViaAPI(path)
	},
}

// getCmd retrieves content by hash via the REST API
var getCmd = &cobra.Command{
	Use:   "get [hash]",
	Short: "Retrieve content by hash",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkAPIConnection(); err != nil {
			return fmt.Errorf("API connection failed: %w (ensure 'openhash daemon' is running)", err)
		}
		return getContentViaAPI(args[0])
	},
}

// viewCmd views content information via the REST API
var viewCmd = &cobra.Command{
	Use:   "view [hash]",
	Short: "View content information",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkAPIConnection(); err != nil {
			return fmt.Errorf("API connection failed: %w (ensure 'openhash daemon' is running)", err)
		}
		return viewContentViaAPI(args[0])
	},
}

// listCmd lists all stored content via the REST API
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all stored content",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkAPIConnection(); err != nil {
			return fmt.Errorf("API connection failed: %w (ensure 'openhash daemon' is running)", err)
		}
		return listContentViaAPI()
	},
}

// daemonCmd starts the OpenHashDB daemon with REST API
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
		fmt.Printf("Addresses:\n")
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

// bootnodeCmd runs a standalone bootnode and relayer
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
		fmt.Printf("Addresses:\n")
		for _, addr := range node.Addrs() {
			fmt.Printf("  %s\n", addr)
		}

		select {}
	},
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&dbPath, "db", "./openhash.db", "Database path")
	rootCmd.PersistentFlags().StringVar(&keyPath, "key-path", "", "Path to the node's private key file (defaults to <db-path>/peer.key)")
	rootCmd.PersistentFlags().IntVar(&apiPort, "api-port", 8080, "REST API port")
	rootCmd.PersistentFlags().IntVar(&p2pPort, "p2p-port", 0, "P2P port (0 for random)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	rootCmd.PersistentFlags().StringVar(&bootnodes, "bootnode", "", "Comma-separated list of bootnode addresses (e.g., /ip4/1.2.3.4/tcp/4001/p2p/Qm...)")
	rootCmd.PersistentFlags().StringVar(&apiURL, "api", "", "REST API URL (e.g., http://localhost:8080)")

	// Daemon-specific flags
	daemonCmd.Flags().Bool("enable-rest", true, "Enable REST API")

	// Add commands
	rootCmd.AddCommand(addCmd, getCmd, viewCmd, listCmd, daemonCmd, bootnodeCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// initAll initializes all components for daemon mode
func initAll() error {
	var err error
	store, err = storage.NewStorage(dbPath)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
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
	node, err = libp2p.NewNodeWithKeyPath(ctx, bootnodeAddrs, actualKeyPath, p2pPort)
	if err != nil {
		store.Close() // Clean up storage if node initialization fails
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
		apiServer.Stop(ctx)
	}
}

// API interaction functions

// getAPIURL returns the API URL, defaulting to localhost if not set
func getAPIURL() string {
	if apiURL != "" {
		return strings.TrimSuffix(apiURL, "/")
	}
	return fmt.Sprintf("http://localhost:%d", apiPort)
}

// checkAPIConnection verifies API connectivity
func checkAPIConnection() error {
	resp, err := http.Get(getAPIURL() + "/health")
	if err != nil {
		return fmt.Errorf("failed to connect to API at %s: %w", getAPIURL(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API health check failed, status: %d", resp.StatusCode)
	}
	return nil
}

// uploadFileViaAPI uploads a file via REST API
func uploadFileViaAPI(filePath string) error {
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

	resp, err := http.Post(getAPIURL()+"/upload/file", writer.FormDataContentType(), &buf)
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
	}
	return nil
}

// uploadFolderViaAPI uploads a folder via REST API
func uploadFolderViaAPI(dirPath string) error {
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

	resp, err := http.Post(getAPIURL()+"/upload/folder", writer.FormDataContentType(), &buf)
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
	resp, err := http.Get(getAPIURL() + "/download/" + hash)
	if err != nil {
		return fmt.Errorf("failed to download content: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("content not found: %s", hash)
	}
	if resp.StatusCode != http.StatusOK {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			if errMsg, ok := result["error"].(string); ok {
				return fmt.Errorf("failed to get content: %s", errMsg)
			}
		}
		return fmt.Errorf("failed to get content, status: %d", resp.StatusCode)
	}

	// Get content info for metadata
	infoResp, err := http.Get(getAPIURL() + "/info/" + hash)
	if err != nil {
		return fmt.Errorf("failed to get content info: %w", err)
	}
	defer infoResp.Body.Close()

	var info map[string]interface{}
	if err := json.NewDecoder(infoResp.Body).Decode(&info); err != nil {
		return fmt.Errorf("failed to parse content info: %w", err)
	}

	// Display metadata
	filename := info["filename"].(string)
	fmt.Printf("📄 File: %s\n", filename)
	fmt.Printf("   Hash: %s\n", hash)
	if size, ok := info["size"].(float64); ok {
		fmt.Printf("   Size: %.0f bytes\n", size)
	}
	if mimeType, ok := info["mime_type"].(string); ok {
		fmt.Printf("   MIME: %s\n", mimeType)
	}

	// Save content to file
	outputPath := filename
	if _, err := os.Stat(outputPath); err == nil {
		outputPath = fmt.Sprintf("%s-%s%s", strings.TrimSuffix(filename, filepath.Ext(filename)), hash[:8], filepath.Ext(filename))
	}
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	if _, err := io.Copy(outputFile, resp.Body); err != nil {
		return fmt.Errorf("failed to save content: %w", err)
	}
	fmt.Printf("   Saved to: %s\n", outputPath)
	return nil
}

// viewContentViaAPI views content information via REST API
func viewContentViaAPI(hash string) error {
	resp, err := http.Get(getAPIURL() + "/info/" + hash)
	if err != nil {
		return fmt.Errorf("failed to get content info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("content not found: %s", hash)
	}
	if resp.StatusCode != http.StatusOK {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			if errMsg, ok := result["error"].(string); ok {
				return fmt.Errorf("failed to get content info: %s", errMsg)
			}
		}
		return fmt.Errorf("failed to get content info, status: %d", resp.StatusCode)
	}

	var info map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fmt.Errorf("failed to parse content info: %w", err)
	}

	fmt.Printf("Hash: %s\n", hash)
	if filename, ok := info["filename"].(string); ok {
		fmt.Printf("Filename: %s\n", filename)
	}
	if mimeType, ok := info["mime_type"].(string); ok {
		fmt.Printf("MIME Type: %s\n", mimeType)
	}
	if size, ok := info["size"].(float64); ok {
		fmt.Printf("Size: %.0f bytes\n", size)
	}
	if modTime, ok := info["mod_time"].(string); ok {
		if t, err := time.Parse(time.RFC3339, modTime); err == nil {
			fmt.Printf("Modified: %s\n", t.Format(time.RFC3339))
		}
	}
	if createdAt, ok := info["created_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			fmt.Printf("Created: %s\n", t.Format(time.RFC3339))
		}
	}
	if refCount, ok := info["ref_count"].(float64); ok {
		fmt.Printf("Reference Count: %d\n", int(refCount))
	}
	if isDir, ok := info["is_directory"].(bool); ok {
		fmt.Printf("Is Directory: %t\n", isDir)
	}
	if chunkCount, ok := info["chunk_count"].(float64); ok {
		fmt.Printf("Chunk Count: %d\n", int(chunkCount))
	}
	return nil
}

// listContentViaAPI lists content via REST API
func listContentViaAPI() error {
	resp, err := http.Get(getAPIURL() + "/list")
	if err != nil {
		return fmt.Errorf("failed to list content: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			if errMsg, ok := result["error"].(string); ok {
				return fmt.Errorf("failed to list content: %s", errMsg)
			}
		}
		return fmt.Errorf("failed to list content, status: %d", resp.StatusCode)
	}

	var items []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	fmt.Printf("📋 Stored Content (%d items):\n\n", len(items))
	for _, item := range items {
		if hash, ok := item["hash"].(string); ok {
			typeIcon := "📄"
			if isDir, ok := item["is_directory"].(bool); ok && isDir {
				typeIcon = "📁"
			}
			if filename, ok := item["filename"].(string); ok {
				fmt.Printf("%s %s\n", typeIcon, filename)
				fmt.Printf("   Hash: %s\n", hash)
				if size, ok := item["size"].(float64); ok {
					fmt.Printf("   Size: %.0f bytes\n", size)
				}
				if createdAt, ok := item["created_at"].(string); ok {
					if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
						fmt.Printf("   Created: %s\n", t.Format("2006-01-02 15:04:05"))
					}
				}
				fmt.Println()
			}
		}
	}

	if len(items) == 0 {
		fmt.Println("📋 No content stored")
	}
	return nil
}
