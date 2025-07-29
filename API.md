# OpenHashDB REST API Documentation

**Version:** 1.0.0  
**Base URL:** `http://localhost:8080`  
**Author:** Manus AI  
**Date:** July 28, 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Authentication](#authentication)
3. [Request/Response Format](#requestresponse-format)
4. [Error Handling](#error-handling)
5. [Rate Limiting](#rate-limiting)
6. [Endpoints](#endpoints)
7. [Examples](#examples)
8. [SDKs and Libraries](#sdks-and-libraries)

## Introduction

The OpenHashDB REST API provides a comprehensive HTTP interface for interacting with the content-addressable distributed database system. This API enables web applications, services, and tools to integrate with OpenHashDB for storing, retrieving, and managing content across a distributed network.

The API follows RESTful principles and uses standard HTTP methods and status codes. All responses are returned in JSON format unless otherwise specified. The API is designed to be stateless, with each request containing all necessary information for processing.

## Authentication

Currently, the OpenHashDB REST API does not require authentication for any operations. This design choice reflects the open, distributed nature of the system where content is identified by cryptographic hashes rather than access controls.

**Security Note:** In production deployments, consider implementing authentication and authorization mechanisms at the network or proxy level if access control is required.

## Request/Response Format

### Content Types

- **Request Content-Type:** 
  - `application/json` for JSON payloads
  - `multipart/form-data` for file uploads
  - `application/x-www-form-urlencoded` for form data

- **Response Content-Type:** 
  - `application/json` for API responses
  - Original MIME type for content downloads

### Headers

#### Standard Headers

- `Content-Type`: Specifies the media type of the request body
- `Accept`: Specifies the media types acceptable for the response
- `User-Agent`: Identifies the client application

#### CORS Headers

The API includes Cross-Origin Resource Sharing (CORS) headers to enable browser-based applications:

- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type, Authorization`

### Response Structure

All API responses follow a consistent structure:

#### Success Response
```json
{
  "data": {
    // Response data
  },
  "status": "success",
  "timestamp": "2025-07-28T11:23:53.745950516-04:00"
}
```

#### Error Response
```json
{
  "error": "Bad Request",
  "code": 400,
  "message": "Detailed error description",
  "timestamp": "2025-07-28T11:23:53.745950516-04:00"
}
```

## Error Handling

The API uses standard HTTP status codes to indicate the success or failure of requests:

### Success Codes

- **200 OK**: Request successful
- **201 Created**: Resource created successfully

### Client Error Codes

- **400 Bad Request**: Invalid request format or parameters
- **404 Not Found**: Requested resource does not exist
- **405 Method Not Allowed**: HTTP method not supported for endpoint
- **413 Payload Too Large**: Request body exceeds size limits

### Server Error Codes

- **500 Internal Server Error**: Unexpected server error
- **503 Service Unavailable**: Service temporarily unavailable

### Error Response Details

Error responses include detailed information to help diagnose issues:

```json
{
  "error": "Not Found",
  "code": 404,
  "message": "Content not found: 8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
  "details": {
    "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
    "operation": "content_retrieval"
  }
}
```

## Rate Limiting

Currently, the OpenHashDB REST API does not implement rate limiting. In production deployments, consider implementing rate limiting at the reverse proxy or load balancer level to prevent abuse.

**Recommended Limits:**
- Upload operations: 10 requests per minute per IP
- Download operations: 100 requests per minute per IP
- Metadata operations: 60 requests per minute per IP

## Endpoints

### Health and Status

#### GET /health

Returns the health status of the OpenHashDB service.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-07-28T11:23:53.745950516-04:00",
  "version": "1.0.0"
}
```

**Status Codes:**
- `200`: Service is healthy
- `503`: Service is unhealthy

---

### Content Upload

#### POST /upload/file

Upload a single file to OpenHashDB.

**Request:**
- **Content-Type:** `multipart/form-data`
- **Body Parameters:**
  - `file` (required): File to upload

**Example Request:**
```bash
curl -X POST \
  -F "file=@document.pdf" \
  http://localhost:8080/upload/file
```

**Response:**
```json
{
  "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
  "size": 1024,
  "filename": "document.pdf",
  "message": "File uploaded successfully"
}
```

**Status Codes:**
- `200`: File uploaded successfully
- `400`: Invalid request or missing file
- `413`: File too large
- `500`: Upload failed

#### POST /upload/folder

Upload a folder structure to OpenHashDB.

**Status:** Not yet implemented

**Response:**
```json
{
  "error": "Not Implemented",
  "code": 501,
  "message": "Folder upload not yet implemented"
}
```

---

### Content Retrieval

#### GET /download/{hash}

Download content by its hash.

**Parameters:**
- `hash` (path, required): SHA-256 hash of the content

**Response:** Raw content with appropriate headers:
- `Content-Type`: Original MIME type
- `Content-Length`: File size in bytes
- `Content-Disposition`: Attachment with original filename

**Example Request:**
```bash
curl -X GET \
  http://localhost:8080/download/8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e \
  -o downloaded_file.txt
```

**Status Codes:**
- `200`: Content retrieved successfully
- `400`: Invalid hash format
- `404`: Content not found

#### GET /view/{hash}

View content inline (CDN-style delivery).

**Parameters:**
- `hash` (path, required): SHA-256 hash of the content

**Response:** Raw content with inline viewing headers:
- `Content-Type`: Original MIME type
- `Content-Length`: File size in bytes

**Example Request:**
```bash
curl -X GET \
  http://localhost:8080/view/8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e
```

**Status Codes:**
- `200`: Content retrieved successfully
- `400`: Invalid hash format
- `404`: Content not found

#### GET /view/{hash}/{path}

View content at a specific path within a directory structure.

**Status:** Not yet implemented

**Parameters:**
- `hash` (path, required): SHA-256 hash of the directory
- `path` (path, required): Path within the directory

---

### Content Information

#### GET /info/{hash}

Get detailed metadata about content.

**Parameters:**
- `hash` (path, required): SHA-256 hash of the content

**Response:**
```json
{
  "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
  "filename": "document.pdf",
  "mime_type": "application/pdf",
  "size": 1024,
  "mod_time": "2025-07-28T11:14:53.004720466-04:00",
  "chunk_count": 0,
  "is_directory": false,
  "created_at": "2025-07-28T11:21:49.814410096-04:00",
  "ref_count": 1
}
```

**Response Fields:**
- `hash`: SHA-256 hash of the content
- `filename`: Original filename
- `mime_type`: MIME type of the content
- `size`: Size in bytes
- `mod_time`: Last modification time
- `chunk_count`: Number of chunks (for large files)
- `is_directory`: Whether content is a directory
- `created_at`: Creation timestamp in OpenHashDB
- `ref_count`: Reference count for garbage collection

**Status Codes:**
- `200`: Information retrieved successfully
- `400`: Invalid hash format
- `404`: Content not found

#### GET /list

List all stored content.

**Response:**
```json
[
  {
    "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
    "filename": "document.pdf",
    "mime_type": "application/pdf",
    "size": 1024,
    "mod_time": "2025-07-28T11:14:53.004720466-04:00",
    "is_directory": false,
    "created_at": "2025-07-28T11:21:49.814410096-04:00",
    "ref_count": 1
  }
]
```

**Status Codes:**
- `200`: List retrieved successfully
- `500`: Failed to retrieve list

---

### Content Pinning

#### POST /pin/{hash}

Pin content to prevent garbage collection.

**Parameters:**
- `hash` (path, required): SHA-256 hash of the content
- `priority` (query, optional): Pin priority (default: 1)

**Example Request:**
```bash
curl -X POST \
  http://localhost:8080/pin/8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e?priority=5
```

**Response:**
```json
{
  "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
  "priority": 5,
  "message": "Content pinned successfully"
}
```

**Status Codes:**
- `200`: Content pinned successfully
- `400`: Invalid hash format
- `404`: Content not found
- `500`: Pin operation failed

#### DELETE /unpin/{hash}

Remove pin from content.

**Parameters:**
- `hash` (path, required): SHA-256 hash of the content

**Example Request:**
```bash
curl -X DELETE \
  http://localhost:8080/unpin/8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e
```

**Response:**
```json
{
  "hash": "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e",
  "message": "Content unpinned successfully"
}
```

**Status Codes:**
- `200`: Content unpinned successfully
- `400`: Invalid hash format
- `500`: Unpin operation failed

#### GET /pins

List all pinned content.

**Response:**
```json
{
  "8c449c94ffd47419a47710dbcdc2d0ab84e10e8a294fdc4b4d70a5be8608df1e": 5,
  "b51afd15acac4cbd03e1bfaca718b390d699bbba500e9f86ecd1a0b6b0fefc77": 1
}
```

**Response Format:** Object with hash keys and priority values.

**Status Codes:**
- `200`: Pin list retrieved successfully
- `500`: Failed to retrieve pin list

---

### System Information

#### GET /stats

Get system statistics and metrics.

**Response:**
```json
{
  "storage": {
    "content_count": 42,
    "chunk_count": 128
  },
  "replication": {
    "replication_factor": 3,
    "pinned_content": 5,
    "pending_requests": 2
  },
  "network": {
    "peer_id": "12D3KooWLyxzumqRjpM8DtpCnEQPkvhLRZve9a4Bd8MSz1NL5gPr",
    "connected_peers": 3,
    "addresses": [
      "/ip4/127.0.0.1/tcp/37477/p2p/12D3KooWLyxzumqRjpM8DtpCnEQPkvhLRZve9a4Bd8MSz1NL5gPr"
    ]
  },
  "timestamp": "2025-07-28T11:24:31.585560549-04:00"
}
```

**Response Fields:**
- `storage`: Storage-related statistics
  - `content_count`: Number of content items stored
  - `chunk_count`: Number of chunks stored
- `replication`: Replication system statistics
  - `replication_factor`: Configured replication factor
  - `pinned_content`: Number of pinned items
  - `pending_requests`: Number of pending replication requests
- `network`: Network-related information
  - `peer_id`: Local peer identifier
  - `connected_peers`: Number of connected peers
  - `addresses`: List of local addresses
- `timestamp`: Response generation time

**Status Codes:**
- `200`: Statistics retrieved successfully
- `500`: Failed to retrieve statistics

## Examples

### Complete Upload and Download Workflow

```bash
# 1. Upload a file
curl -X POST \
  -F "file=@example.txt" \
  http://localhost:8080/upload/file

# Response: {"hash":"abc123...","size":1024,"filename":"example.txt","message":"File uploaded successfully"}

# 2. Get file information
curl -X GET \
  http://localhost:8080/info/abc123...

# 3. Download the file
curl -X GET \
  http://localhost:8080/download/abc123... \
  -o downloaded_example.txt

# 4. Pin the file for permanent storage
curl -X POST \
  http://localhost:8080/pin/abc123...?priority=10
```

### JavaScript/Node.js Example

```javascript
const FormData = require('form-data');
const fs = require('fs');
const axios = require('axios');

const baseURL = 'http://localhost:8080';

// Upload file
async function uploadFile(filePath) {
  const form = new FormData();
  form.append('file', fs.createReadStream(filePath));
  
  try {
    const response = await axios.post(`${baseURL}/upload/file`, form, {
      headers: form.getHeaders()
    });
    return response.data;
  } catch (error) {
    console.error('Upload failed:', error.response.data);
    throw error;
  }
}

// Download file
async function downloadFile(hash, outputPath) {
  try {
    const response = await axios.get(`${baseURL}/download/${hash}`, {
      responseType: 'stream'
    });
    
    const writer = fs.createWriteStream(outputPath);
    response.data.pipe(writer);
    
    return new Promise((resolve, reject) => {
      writer.on('finish', resolve);
      writer.on('error', reject);
    });
  } catch (error) {
    console.error('Download failed:', error.response.data);
    throw error;
  }
}

// Usage
(async () => {
  try {
    const uploadResult = await uploadFile('./example.txt');
    console.log('Uploaded:', uploadResult);
    
    await downloadFile(uploadResult.hash, './downloaded.txt');
    console.log('Downloaded successfully');
  } catch (error) {
    console.error('Error:', error);
  }
})();
```

### Python Example

```python
import requests
import json

BASE_URL = 'http://localhost:8080'

def upload_file(file_path):
    """Upload a file to OpenHashDB"""
    with open(file_path, 'rb') as f:
        files = {'file': f}
        response = requests.post(f'{BASE_URL}/upload/file', files=files)
        response.raise_for_status()
        return response.json()

def download_file(hash_value, output_path):
    """Download a file from OpenHashDB"""
    response = requests.get(f'{BASE_URL}/download/{hash_value}')
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        f.write(response.content)

def get_file_info(hash_value):
    """Get file information"""
    response = requests.get(f'{BASE_URL}/info/{hash_value}')
    response.raise_for_status()
    return response.json()

def pin_content(hash_value, priority=1):
    """Pin content to prevent garbage collection"""
    response = requests.post(f'{BASE_URL}/pin/{hash_value}', 
                           params={'priority': priority})
    response.raise_for_status()
    return response.json()

# Usage example
if __name__ == '__main__':
    try:
        # Upload file
        result = upload_file('example.txt')
        print(f"Uploaded: {result}")
        
        hash_value = result['hash']
        
        # Get file info
        info = get_file_info(hash_value)
        print(f"File info: {json.dumps(info, indent=2)}")
        
        # Pin the file
        pin_result = pin_content(hash_value, priority=5)
        print(f"Pinned: {pin_result}")
        
        # Download file
        download_file(hash_value, 'downloaded_example.txt')
        print("Downloaded successfully")
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
```

## SDKs and Libraries

Currently, there are no official SDKs for OpenHashDB. However, the REST API can be easily integrated using standard HTTP libraries in any programming language.

### Recommended Libraries

#### JavaScript/Node.js
- `axios` or `fetch` for HTTP requests
- `form-data` for file uploads

#### Python
- `requests` for HTTP requests
- `urllib3` for advanced HTTP features

#### Go
- `net/http` (standard library)
- `github.com/go-resty/resty` for enhanced features

#### Java
- `OkHttp` or `Apache HttpClient`
- `Retrofit` for REST API integration

#### C#
- `HttpClient` (built-in)
- `RestSharp` for enhanced features

### Community Contributions

We welcome community contributions for official SDKs and client libraries. Please follow the project's contribution guidelines and ensure comprehensive test coverage for any submitted libraries.

## Versioning

The OpenHashDB REST API follows semantic versioning (SemVer). The current version is 1.0.0.

### Version History

- **1.0.0** (2025-07-28): Initial release
  - Basic file upload/download functionality
  - Content metadata and listing
  - Pinning system
  - System statistics

### Backward Compatibility

We strive to maintain backward compatibility within major versions. Breaking changes will only be introduced in new major versions with appropriate migration guides.

## Support and Feedback

For API-related questions, issues, or feedback:

1. Check the documentation and examples
2. Review existing issues in the project repository
3. Create a new issue with detailed information
4. Include API version, request/response details, and error messages

We appreciate feedback and contributions to improve the API documentation and functionality.


## Network Statistics Endpoint

### GET /network

Returns detailed network statistics including peer connections, DHT status, and node information.

**Response Format:**
```json
{
  "peer_id": "12D3KooWExample...",
  "connected_peers": 5,
  "peer_list": [
    "12D3KooWPeer1...",
    "12D3KooWPeer2..."
  ],
  "addresses": [
    "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWExample...",
    "/ip4/192.168.1.100/tcp/4001/p2p/12D3KooWExample..."
  ],
  "dht": {
    "enabled": true,
    "peer_count": 15,
    "bucket_info": [...]
  }
}
```

**Response Fields:**
- `peer_id`: Unique identifier for this node
- `connected_peers`: Number of directly connected peers
- `peer_list`: Array of connected peer IDs
- `addresses`: List of network addresses this node is listening on
- `dht.enabled`: Whether DHT is active
- `dht.peer_count`: Number of peers in the DHT routing table
- `dht.bucket_info`: Detailed K-bucket information for DHT routing

**Example Usage:**
```bash
curl http://localhost:8080/network
```

**Use Cases:**
- Monitor network connectivity status
- Debug peer discovery issues
- Analyze DHT performance
- Track node reachability

### Enhanced Upload Behavior

All upload endpoints now automatically announce content to the DHT after successful storage. This ensures that uploaded content becomes discoverable by other nodes in the network.

### Enhanced Download Behavior

Download endpoints now implement DHT-based content discovery. If content is not available locally, the system will:

1. Query the DHT for content providers
2. Attempt to retrieve content from discovered providers
3. Verify content integrity using hash validation
4. Cache content locally for future access
5. Return the requested content to the client

This process is transparent to API users - the same endpoints work whether content is local or remote.


## CLI Integration with REST API

OpenHashDB CLI commands can interact with the REST API using the `--api` flag, enabling seamless integration between command-line operations and running daemon instances.

### CLI Command Mapping

The following table shows how CLI commands map to REST API endpoints:

| CLI Command | API Endpoint | HTTP Method | Description |
|-------------|--------------|-------------|-------------|
| `add <file>` | `/upload/file` | POST | Upload file via multipart form |
| `get <hash>` | `/info/<hash>` | GET | Get content information |
| `get <hash>` | `/download/<hash>` | GET | Download content (for preview) |
| `list` | `/list` | GET | List all stored content |
| `view <hash>` | `/info/<hash>` | GET | View content information |

### API Request Examples

#### File Upload via CLI
```bash
./openhash add myfile.txt --api http://localhost:8080
```

Equivalent curl command:
```bash
curl -X POST -F "file=@myfile.txt" http://localhost:8080/upload/file
```

#### Content Retrieval via CLI
```bash
./openhash get abc123... --api http://localhost:8080
```

Equivalent curl commands:
```bash
# Get content info
curl http://localhost:8080/info/abc123...

# Download content
curl http://localhost:8080/download/abc123...
```

#### Content Listing via CLI
```bash
./openhash list --api http://localhost:8080
```

Equivalent curl command:
```bash
curl http://localhost:8080/list
```

### Response Format Handling

The CLI automatically handles different response formats from the API:

#### Upload Response
```json
{
  "hash": "abc123...",
  "size": 1024,
  "filename": "myfile.txt",
  "message": "File uploaded successfully"
}
```

CLI Output:
```
✅ File uploaded: abc123...
   Size: 1024 bytes
   Name: myfile.txt
```

#### Content Info Response
```json
{
  "hash": "abc123...",
  "filename": "myfile.txt",
  "mime_type": "text/plain",
  "size": 1024,
  "mod_time": "2025-07-28T12:00:00Z",
  "is_directory": false,
  "created_at": "2025-07-28T12:00:00Z",
  "ref_count": 1
}
```

CLI Output:
```
📄 File: myfile.txt
   Hash: abc123...
   Size: 1024 bytes
   MIME: text/plain
   Content:
   [file content preview for small text files]
```

#### Content List Response
```json
[
  {
    "hash": "abc123...",
    "filename": "file1.txt",
    "mime_type": "text/plain",
    "size": 1024,
    "is_directory": false
  },
  {
    "hash": "def456...",
    "filename": "folder1",
    "mime_type": "application/x-directory",
    "size": 2048,
    "is_directory": true
  }
]
```

CLI Output:
```
📋 Stored Content (2 items):

📄 abc123...
   Name: file1.txt
   Size: 1024 bytes
   Type: text/plain

📁 def456...
   Name: folder1
   Size: 2048 bytes
   Type: application/x-directory
```

### Error Handling

The CLI provides user-friendly error messages for common API errors:

#### Connection Errors
```bash
Error: API connection failed: failed to connect to API at http://localhost:8080: dial tcp: connect: connection refused
```

#### HTTP Status Errors
```bash
Error: upload failed with status: 400
Error: content not found: abc123...
Error: failed to list content, status: 500
```

#### JSON Parsing Errors
```bash
Error: failed to parse response: invalid character '<' looking for beginning of value
```

### Authentication and Security

Currently, the CLI API integration does not include authentication. For production deployments, consider:

1. **Network Security**: Use HTTPS for remote connections
2. **Firewall Rules**: Restrict API access to authorized networks
3. **Reverse Proxy**: Use nginx or similar for SSL termination and access control
4. **VPN Access**: Require VPN connection for remote API access

### Performance Optimization

#### Connection Reuse
The CLI creates a new HTTP connection for each command. For high-frequency operations, consider:

1. **Batch Operations**: Group multiple files into single requests
2. **Keep-Alive**: Use HTTP/1.1 keep-alive for connection reuse
3. **HTTP/2**: Enable HTTP/2 for multiplexed connections

#### Timeout Configuration
Default timeouts are set for reasonable operation:

- **Connection Timeout**: 30 seconds
- **Read Timeout**: 30 seconds
- **Write Timeout**: 30 seconds

### Integration Patterns

#### Script Integration
```bash
#!/bin/bash
API_URL="http://localhost:8080"

# Function to check if API is available
check_api() {
    curl -s "$API_URL/health" > /dev/null
    return $?
}

# Upload multiple files
upload_files() {
    for file in "$@"; do
        if [ -f "$file" ]; then
            ./openhash add "$file" --api "$API_URL"
        fi
    done
}

# Main script
if check_api; then
    upload_files *.txt
    ./openhash list --api "$API_URL"
else
    echo "API not available, starting daemon..."
    ./openhash daemon &
    sleep 3
    upload_files *.txt
fi
```

#### Application Integration
```python
import subprocess
import json

class OpenHashDBClient:
    def __init__(self, api_url="http://localhost:8080"):
        self.api_url = api_url
    
    def add_file(self, filepath):
        result = subprocess.run([
            "./openhash", "add", filepath, "--api", self.api_url
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            # Parse CLI output to extract hash
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if line.startswith('✅ File uploaded:'):
                    return line.split(': ')[1]
        return None
    
    def list_content(self):
        result = subprocess.run([
            "./openhash", "list", "--api", self.api_url
        ], capture_output=True, text=True)
        
        return result.stdout if result.returncode == 0 else None

# Usage
client = OpenHashDBClient()
hash_value = client.add_file("myfile.txt")
content_list = client.list_content()
```

