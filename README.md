# OpenHashDB

**A blazing-fast, content-addressable, distributed database system designed as a modern, developer-friendly alternative to IPFS.**

## Overview

OpenHashDB is a revolutionary distributed storage system that combines the power of content-addressable storage with modern peer-to-peer networking technologies. Built with Go and leveraging libp2p for networking, OpenHashDB provides both CLI and REST API interfaces for maximum flexibility and integration potential.

## Key Features

- **Content-Addressable Storage**: Every piece of data is identified by its SHA-256 hash, ensuring data integrity and enabling efficient deduplication
- **Distributed Architecture**: Peer-to-peer networking via libp2p enables truly decentralized storage without single points of failure
- **Dual Interface**: Both command-line interface (CLI) and REST API for different use cases and integration scenarios
- **Automatic Chunking**: Large files are automatically split into manageable chunks for efficient storage and transfer
- **Merkle DAG Support**: Directory structures are represented as Merkle Directed Acyclic Graphs for efficient verification
- **Content Replication**: Configurable replication factors ensure data availability across the network
- **Pinning System**: Explicit content pinning prevents garbage collection of important data
- **High Performance**: Optimized for speed with concurrent processing and efficient storage backends

## Quick Start

### Installation

1. **Prerequisites**
   - Go 1.21 or later
   - Git

2. **Build from Source**
   ```bash
   git clone <repository-url>
   cd openhash
   go build -o openhash .
   ```

3. **Verify Installation**
   ```bash
   ./openhash --help
   ```

### Basic Usage

#### Adding Files

Add a single file to OpenHashDB:
```bash
./openhash add myfile.txt
```

Add a directory:
```bash
./openhash add ./my-directory
```

#### Retrieving Content

List all stored content:
```bash
./openhash list
```

Retrieve content by hash:
```bash
./openhash get <hash>
```

View content metadata:
```bash
./openhash view <hash>
```

#### Starting the Daemon

Start OpenHashDB with REST API:
```bash
./openhash daemon
```

The REST API will be available at `http://localhost:8080` by default.

## Architecture

OpenHashDB follows a modular architecture with distinct layers:

### Core Components

- **Hasher**: SHA-256 content addressing and integrity verification
- **Storage**: LevelDB-based persistent storage with metadata management
- **Chunker**: Configurable file chunking with Merkle tree construction
- **DAG Builder**: Merkle DAG construction for directory structures

### Network Layer

- **libp2p Node**: Peer-to-peer networking and protocol handling
- **Replicator**: Content replication and availability management
- **Discovery**: Peer discovery via mDNS and DHT

### API Layer

- **REST Server**: HTTP API for web integration
- **CLI Interface**: Command-line tools for direct usage

## CLI Reference

### Global Flags

- `--db string`: Database path (default "./openhash.db")
- `--api-port int`: REST API port (default 8080)
- `--p2p-port int`: P2P port (0 for random)
- `--verbose`: Enable verbose output

### Commands

#### `add [file/folder]`
Add a file or folder to OpenHashDB.

**Examples:**
```bash
./openhash add document.pdf
./openhash add ./website --verbose
```

#### `get [hash]`
Retrieve and display content by hash.

**Examples:**
```bash
./openhash get 8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e
```

#### `view [hash]`
Display detailed metadata about content.

**Examples:**
```bash
./openhash view 8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e
```

#### `list`
List all stored content with metadata.

#### `daemon`
Start the OpenHashDB daemon with networking and REST API.

**Flags:**
- `--enable-rest`: Enable REST API (default true)

**Examples:**
```bash
./openhash daemon
./openhash daemon --api-port 9000
```

## REST API Reference

### Base URL
```
http://localhost:8080
```

### Endpoints

#### Health Check
```http
GET /health
```

Returns server health status.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-07-28T11:23:53.745950516-04:00",
  "version": "1.0.0"
}
```

#### Upload File
```http
POST /upload/file
Content-Type: multipart/form-data
```

Upload a single file.

**Parameters:**
- `file`: File to upload (form data)

**Response:**
```json
{
  "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
  "size": 611,
  "filename": "test_file.txt",
  "message": "File uploaded successfully"
}
```

#### Download Content
```http
GET /download/{hash}
```

Download content by hash.

**Response:** Raw file content with appropriate headers.

#### View Content
```http
GET /view/{hash}
```

View content inline (CDN-style).

**Response:** Content with inline viewing headers.

#### Get Content Info
```http
GET /info/{hash}
```

Get detailed information about content.

**Response:**
```json
{
  "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
  "filename": "test_file.txt",
  "mime_type": "text/plain",
  "size": 611,
  "mod_time": "2025-07-28T11:14:53.004720466-04:00",
  "is_directory": false,
  "created_at": "2025-07-28T11:21:49.814410096-04:00",
  "ref_count": 1
}
```

#### List Content
```http
GET /list
```

List all stored content.

**Response:** Array of content information objects.

#### Pin Content
```http
POST /pin/{hash}?priority=1
```

Pin content to prevent garbage collection.

**Query Parameters:**
- `priority`: Pin priority (default 1)

#### Unpin Content
```http
DELETE /unpin/{hash}
```

Remove pin from content.

#### List Pins
```http
GET /pins
```

List all pinned content.

#### System Statistics
```http
GET /stats
```

Get system statistics.

**Response:**
```json
{
  "storage": {
    "content_count": 2,
    "chunk_count": 0
  },
  "replication": {
    "replication_factor": 3,
    "pinned_content": 0,
    "pending_requests": 0
  },
  "timestamp": "2025-07-28T11:24:31.585560549-04:00"
}
```

## Configuration

### Database Configuration

OpenHashDB uses LevelDB for persistent storage. The database path can be configured using the `--db` flag:

```bash
./openhash --db /path/to/database add myfile.txt
```

### Network Configuration

Configure networking ports:

```bash
./openhash --p2p-port 4001 --api-port 8080 daemon
```

### Replication Settings

The default replication factor is 3, meaning content will be replicated to 3 nodes in the network. This can be adjusted in the source code by modifying the `DefaultReplicationFactor` constant.

## Development

### Project Structure

```
openhash/
├── cmd/openhash/          # CLI command implementations
├── core/
│   ├── dag/              # Merkle DAG implementation
│   ├── chunker/          # File chunking logic
│   ├── hasher/           # SHA-256 hashing utilities
│   └── storage/          # LevelDB storage layer
├── network/
│   ├── libp2p/           # libp2p networking
│   └── replicator/       # Content replication
├── api/
│   └── rest/             # REST API server
├── gateway/
│   ├── cdn/              # CDN-style content delivery
│   └── raw/              # Raw content handlers
└── main.go               # Application entry point
```

### Building

```bash
go build -o openhash .
```

### Testing

Run the test suite:
```bash
go test ./...
```

### Dependencies

Key dependencies include:
- `github.com/spf13/cobra`: CLI framework
- `github.com/syndtr/goleveldb/leveldb`: LevelDB storage
- `github.com/libp2p/go-libp2p`: P2P networking
- `github.com/gorilla/mux`: HTTP routing

## Use Cases

### Content Distribution Network (CDN)

OpenHashDB can serve as a decentralized CDN for static websites and assets:

1. Upload website files:
   ```bash
   ./openhash add ./website
   ```

2. Start daemon:
   ```bash
   ./openhash daemon
   ```

3. Access content via REST API:
   ```
   http://localhost:8080/view/{hash}/index.html
   ```

### Backup and Archival

Use OpenHashDB for distributed backup storage:

1. Add files to be backed up:
   ```bash
   ./openhash add ./important-documents
   ```

2. Pin critical content:
   ```bash
   curl -X POST http://localhost:8080/pin/{hash}?priority=10
   ```

### Data Sharing

Share files across a distributed network:

1. Add file to local node:
   ```bash
   ./openhash add shared-file.pdf
   ```

2. Share the hash with others
3. Others can retrieve the file:
   ```bash
   ./openhash get {hash}
   ```

## Security Considerations

### Content Integrity

All content is verified using SHA-256 hashes, ensuring data integrity. Any tampering with content will result in hash mismatches that are automatically detected.

### Network Security

- All peer-to-peer communications use libp2p's built-in security features
- Content is immutable once stored
- No authentication is required for read operations (by design)
- Write operations are local to each node

### Privacy

- Content hashes may reveal information about stored data
- All content is stored unencrypted
- Network traffic is visible to peers
- Consider encryption at the application layer for sensitive data

## Performance Optimization

### Storage Optimization

- Use SSD storage for better performance
- Configure appropriate chunk sizes based on content type
- Regular garbage collection to remove unreferenced content

### Network Optimization

- Ensure good network connectivity between peers
- Use appropriate replication factors (higher for critical content)
- Monitor peer connections and network health

### Memory Usage

- LevelDB uses configurable write buffers (default 64MB)
- In-memory caching improves access times for frequently used content
- Monitor memory usage in production deployments

## Troubleshooting

### Common Issues

#### "Database locked" Error
- Ensure only one OpenHashDB instance is running per database
- Check file permissions on the database directory

#### Network Connection Issues
- Verify firewall settings allow P2P traffic
- Check if ports are available and not blocked
- Ensure proper network connectivity between peers

#### High Memory Usage
- Reduce write buffer size in storage configuration
- Implement regular garbage collection
- Monitor chunk sizes and adjust if necessary

### Debugging

Enable verbose logging:
```bash
./openhash --verbose add myfile.txt
```

Check daemon logs:
```bash
./openhash daemon > openhash.log 2>&1
```

## Contributing

We welcome contributions to OpenHashDB! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Make your changes with appropriate tests
4. Submit a pull request with a clear description

### Development Setup

1. Install Go 1.21+
2. Clone the repository
3. Install dependencies: `go mod download`
4. Run tests: `go test ./...`
5. Build: `go build -o openhash .`

## License

[License information would go here]

## Support

For support and questions:
- Create an issue on the project repository
- Check the documentation and troubleshooting guide
- Review existing issues for similar problems

## Roadmap

Future enhancements planned:
- IPFS compatibility bridge
- Enhanced storage backends (S3, distributed filesystems)
- Web-based management interface
- Advanced analytics and monitoring
- Smart prefetching based on usage patterns
- Enhanced security features and access controls



## Enhanced Peer Discovery and DHT Integration

OpenHashDB now includes advanced peer discovery capabilities and a Distributed Hash Table (DHT) for efficient content lookup across the network. These enhancements significantly improve the system's ability to connect to peers and locate content, even when it's not directly stored on connected nodes.

### Bootnode Support

Bootnodes serve as initial connection points for nodes joining the OpenHashDB network. This feature provides reliable network bootstrapping beyond local mDNS discovery.

#### Using Bootnodes

You can specify bootnodes when starting any OpenHashDB command using the `--bootnode` flag:

```bash
# Single bootnode
./openhash daemon --bootnode /ip4/192.168.1.100/tcp/4001/p2p/12D3KooWExample...

# Multiple bootnodes (comma-separated)
./openhash daemon --bootnode /ip4/192.168.1.100/tcp/4001/p2p/12D3KooWExample...,/ip4/192.168.1.101/tcp/4001/p2p/12D3KooWAnother...

# Add content with bootnode connection
./openhash add myfile.txt --bootnode /ip4/192.168.1.100/tcp/4001/p2p/12D3KooWExample...
```

#### Hardcoded Bootnodes

When no bootnodes are specified, OpenHashDB will attempt to connect to a set of hardcoded, well-known bootnodes maintained by the project. This ensures out-of-the-box connectivity to the OpenHashDB network without requiring manual configuration.

### Distributed Hash Table (DHT)

The integrated Kademlia-based DHT enables efficient content discovery across the network. When content is added to OpenHashDB, the node announces itself as a provider for that content's hash. Other nodes can then query the DHT to find providers for specific content.

#### How DHT Works in OpenHashDB

1. **Content Announcement**: When you add content, your node automatically announces to the DHT that it can provide that content
2. **Content Discovery**: When requesting content not available locally, your node queries the DHT to find providers
3. **Automatic Retrieval**: Once providers are found, your node automatically retrieves the content from available peers
4. **Local Caching**: Retrieved content is automatically cached locally for future access

#### DHT Network Statistics

You can monitor DHT status through the REST API:

```bash
curl http://localhost:8080/network
```

This returns detailed network information including:
- Connected peers
- DHT status and peer count
- Node addresses and peer ID
- K-bucket information

### Network Architecture

The enhanced OpenHashDB network architecture now includes:

- **libp2p Host**: Core networking layer for peer-to-peer connections
- **Kademlia DHT**: Distributed hash table for content discovery
- **Bootnode Integration**: Reliable network bootstrapping
- **mDNS Discovery**: Local network peer discovery
- **Content Replication**: Automatic content distribution across peers

### Performance Benefits

The DHT integration provides several performance improvements:

- **Efficient Content Lookup**: O(log N) lookup complexity instead of exhaustive peer searches
- **Network Resilience**: Content remains accessible even if original providers go offline
- **Automatic Load Distribution**: Popular content is automatically replicated across multiple nodes
- **Reduced Network Traffic**: Targeted queries instead of broadcast searches

### Security Considerations

The DHT implementation includes several security features:

- **Content Verification**: All retrieved content is verified against its hash before use
- **Peer Authentication**: libp2p's built-in peer authentication prevents impersonation
- **Rate Limiting**: DHT queries are rate-limited to prevent abuse
- **Tamper Detection**: Merkle DAG structure ensures content integrity



## API Mode for CLI Commands

OpenHashDB now supports an `--api` flag for CLI commands that resolves database access conflicts when a daemon is running. This feature allows you to interact with a running OpenHashDB daemon via its REST API instead of directly accessing the database files.

### The Database Lock Problem

When the OpenHashDB daemon is running, it maintains an exclusive lock on the database files to ensure data consistency. This means that CLI commands like `add`, `get`, `list`, and `view` cannot directly access the database and will fail with errors like:

```
Error: failed to initialize storage: failed to open database: The process cannot access the file because it is being used by another process.
```

### Solution: API Mode

The `--api` flag enables CLI commands to communicate with the running daemon through its REST API instead of directly accessing the database. This provides several benefits:

- **No database conflicts**: Commands work seamlessly while daemon is running
- **Network operations**: Can interact with remote OpenHashDB instances
- **Consistent behavior**: Same commands work in both direct and API modes
- **Automatic fallback**: Uses default localhost URL when no URL is specified

### Usage Examples

#### Basic API Mode

```bash
# Start the daemon
./openhash daemon

# In another terminal, use API mode for commands
./openhash add myfile.txt --api http://localhost:8080
./openhash list --api http://localhost:8080
./openhash get <hash> --api http://localhost:8080
./openhash view <hash> --api http://localhost:8080
```

#### Default API URL

When using `--api` with an empty string, OpenHashDB automatically uses the default localhost URL based on the `--api-port` setting:

```bash
# These are equivalent when --api-port is 8080 (default)
./openhash list --api ""
./openhash list --api http://localhost:8080
```

#### Remote API Access

You can interact with OpenHashDB instances running on other machines:

```bash
# Connect to remote OpenHashDB instance
./openhash add myfile.txt --api http://192.168.1.100:8080
./openhash list --api http://remote-server:8080
```

#### Different Port Configurations

```bash
# Daemon running on custom port
./openhash daemon --api-port 9090

# CLI commands using custom port
./openhash add myfile.txt --api http://localhost:9090
```

### Command Behavior in API Mode

#### Add Command
- Uploads files via multipart form data to `/upload/file` endpoint
- Displays upload confirmation with hash, size, and filename
- Folder uploads via API are not yet implemented (use daemon mode)

#### Get Command
- Retrieves content information via `/info/<hash>` endpoint
- Downloads and displays small text files automatically
- Shows file metadata including size, MIME type, and content preview

#### List Command
- Fetches content list via `/list` endpoint
- Displays all stored content with metadata
- Shows file/folder icons and creation timestamps

#### View Command
- Currently behaves identically to `get` command in API mode
- Retrieves and displays content information and preview

### Error Handling

API mode includes comprehensive error handling:

#### Connection Errors
```bash
$ ./openhash list --api http://localhost:8080
Error: API connection failed: failed to connect to API at http://localhost:8080: dial tcp: connect: connection refused
```

#### Invalid Hash Errors
```bash
$ ./openhash get invalid-hash --api http://localhost:8080
Error: content not found: invalid-hash
```

#### Network Timeouts
API requests include reasonable timeouts to prevent hanging operations.

### Performance Considerations

#### API Mode vs Direct Mode

| Aspect | Direct Mode | API Mode |
|--------|-------------|----------|
| **Speed** | Fastest (direct DB access) | Slightly slower (HTTP overhead) |
| **Concurrency** | Single process only | Multiple concurrent clients |
| **Network** | Local only | Local and remote access |
| **Daemon Required** | No | Yes |
| **Database Locks** | Exclusive access needed | No conflicts |

#### When to Use Each Mode

**Use Direct Mode when:**
- No daemon is running
- Maximum performance is required
- Working with local files only
- Single-user scenarios

**Use API Mode when:**
- Daemon is already running
- Multiple users need access
- Remote access is required
- Building applications that integrate with OpenHashDB

### Integration with Applications

The API mode makes OpenHashDB CLI commands suitable for integration with scripts and applications:

```bash
#!/bin/bash
# Example script using API mode

# Start daemon if not running
if ! curl -s http://localhost:8080/health > /dev/null; then
    ./openhash daemon &
    sleep 3
fi

# Upload files using API mode
for file in *.txt; do
    ./openhash add "$file" --api ""
done

# List all content
./openhash list --api ""
```

### Troubleshooting API Mode

#### Common Issues

1. **Daemon not running**: Ensure the daemon is started before using API mode
2. **Wrong port**: Verify the daemon is running on the expected port
3. **Firewall blocking**: Check that the API port is accessible
4. **Network connectivity**: Ensure network connection to remote instances

#### Debug Commands

```bash
# Check if daemon is running
curl http://localhost:8080/health

# Check daemon logs
./openhash daemon --verbose

# Test API connectivity
./openhash list --api http://localhost:8080 --verbose
```

