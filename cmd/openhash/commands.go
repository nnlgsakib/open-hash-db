package main

import (
	"context"
	"fmt"
	"log"
	"openhashdb/api/rest"
	"openhashdb/core/dag"
	"openhashdb/core/hasher"
	"openhashdb/core/storage"
	"openhashdb/network/libp2p"
	"openhashdb/network/replicator"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

var (
	// Global flags
	dbPath  string
	keyPath string // New flag for key path
	apiPort int
	p2pPort int
	verbose bool

	// Global instances
	store     *storage.Storage
	node      *libp2p.Node
	repl      *replicator.Replicator
	apiServer *rest.Server
)

// addCmd represents the add command
var addCmd = &cobra.Command{
	Use:   "add [file/folder]",
	Short: "Add a file or folder to OpenHashDB",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

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

// pinCmd represents the pin command
var pinCmd = &cobra.Command{
	Use:   "pin [hash]",
	Short: "Pin content to prevent garbage collection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		hashStr := args[0]
		priority, _ := cmd.Flags().GetInt("priority")

		// Initialize storage and networking
		if err := initAll(); err != nil {
			return err
		}
		defer cleanup()

		hash, err := hasher.HashFromString(hashStr)
		if err != nil {
			return fmt.Errorf("invalid hash: %w", err)
		}

		return pinContent(hash, priority)
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
			// Start REST API server
			apiServer = rest.NewServer(store, repl)
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

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all stored content",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize storage
		if err := initStorage(); err != nil {
			return err
		}
		defer store.Close()

		return listContent()
	},
}

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show system statistics",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize all components
		if err := initAll(); err != nil {
			return err
		}
		defer cleanup()

		return showStats()
	},
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&dbPath, "db", "./openhash.db", "Database path")
	rootCmd.PersistentFlags().StringVar(&keyPath, "key-path", "", "Path to the node's private key file (defaults to <db-path>/peer.key)")
	rootCmd.PersistentFlags().IntVar(&apiPort, "api-port", 8080, "REST API port")
	rootCmd.PersistentFlags().IntVar(&p2pPort, "p2p-port", 0, "P2P port (0 for random)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")

	// Command-specific flags
	pinCmd.Flags().Int("priority", 1, "Pin priority (higher = more important)")
	daemonCmd.Flags().Bool("enable-rest", true, "Enable REST API")

	// Add commands
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(viewCmd)
	rootCmd.AddCommand(pinCmd)
	rootCmd.AddCommand(daemonCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(statsCmd)
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

	// Determine key path
	actualKeyPath := keyPath
	if actualKeyPath == "" {
		actualKeyPath = filepath.Join(dbPath, "peer.key")
	}

	// Initialize libp2p node
	ctx := context.Background()
	var err error
	node, err = libp2p.NewNodeWithKeyPath(ctx, libp2p.DefaultBootnodes, actualKeyPath)
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
	// Get metadata
	metadata, err := store.GetContent(hash)
	if err != nil {
		return fmt.Errorf("content not found: %w", err)
	}

	// Get data
	data, err := store.GetData(hash)
	if err != nil {
		return fmt.Errorf("data not found: %w", err)
	}

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

// pinContent pins content
func pinContent(hash hasher.Hash, priority int) error {
	if err := repl.PinContent(hash, priority); err != nil {
		return fmt.Errorf("failed to pin content: %w", err)
	}

	fmt.Printf("✅ Content pinned: %s (priority: %d)\n", hash.String(), priority)
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

// showStats displays system statistics
func showStats() error {
	// Storage stats
	storageStats, err := store.GetStats()
	if err != nil {
		return fmt.Errorf("failed to get storage stats: %w", err)
	}

	// Network stats
	networkStats := node.GetNetworkStats()

	// Replication stats
	replicationStats := repl.GetStats()

	fmt.Println("📊 OpenHashDB Statistics")
	fmt.Println("========================")
	fmt.Println()

	fmt.Println("Storage:")
	fmt.Printf("  Content items: %v\n", storageStats["content_count"])
	fmt.Printf("  Chunks: %v\n", storageStats["chunk_count"])
	fmt.Println()

	fmt.Println("Network:")
	fmt.Printf("  Peer ID: %v\n", networkStats["peer_id"])
	fmt.Printf("  Connected peers: %v\n", networkStats["connected_peers"])
	if peers, ok := networkStats["peer_list"].([]string); ok && len(peers) > 0 {
		fmt.Println("  Peer list:")
		for _, peer := range peers {
			fmt.Printf("    %s\n", peer)
		}
	}
	fmt.Println()

	fmt.Println("Replication:")
	fmt.Printf("  Replication factor: %v\n", replicationStats["replication_factor"])
	fmt.Printf("  Pinned content: %v\n", replicationStats["pinned_content"])
	fmt.Printf("  Pending requests: %v\n", replicationStats["pending_requests"])

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
