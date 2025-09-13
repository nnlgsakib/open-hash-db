package gateway

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "strings"

    "openhashdb/api/pages"
    "openhashdb/core/block"
    "openhashdb/core/chunker"
    "openhashdb/core/hasher"
    "openhashdb/core/tmt"
    "openhashdb/core/tree"
    "openhashdb/core/utils"
    "openhashdb/protobuf/pb"

    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/types/known/timestamppb"
)

func isClientClosedError(err error) bool {
    // Check for common client-side disconnect errors
    return strings.Contains(err.Error(), "forcibly closed by the remote host") ||
        strings.Contains(err.Error(), "broken pipe") ||
        strings.Contains(err.Error(), "connection reset by peer") ||
        strings.Contains(err.Error(), "An established connection was aborted by the software in your host machine") ||
        strings.Contains(err.Error(), "use of closed network connection")
}

func max(a, b int64) int64 { if a > b { return a }; return b }
func min(a, b int64) int64 { if a < b { return a }; return b }

func parseRangeHeader(s string, size int64) (int64, int64, error) {
    if s == "" { return 0, 0, nil }
    const b = "bytes="
    if !strings.HasPrefix(s, b) { return 0, 0, fmt.Errorf("invalid range header format") }
    s = s[len(b):]
    parts := strings.Split(s, "-")
    if len(parts) != 2 { return 0, 0, fmt.Errorf("invalid range header format") }
    start, err := strconv.ParseInt(parts[0], 10, 64)
    if err != nil { return 0, 0, fmt.Errorf("invalid start value") }
    end, err := strconv.ParseInt(parts[1], 10, 64)
    if err != nil {
        if parts[1] == "" { end = size - 1 } else { return 0, 0, fmt.Errorf("invalid end value") }
    }
    if start > end || start >= size { return 0, 0, fmt.Errorf("invalid range") }
    return start, end, nil
}

// isMimeTypeRenderable checks if a MIME type can be displayed directly by most browsers.
func (s *Server) isMimeTypeRenderable(mimeType string) bool {
    return strings.HasPrefix(mimeType, "text/") ||
        strings.HasPrefix(mimeType, "image/") ||
        strings.HasPrefix(mimeType, "audio/") ||
        strings.HasPrefix(mimeType, "video/") ||
        strings.HasPrefix(mimeType, "font/") ||
        mimeType == "application/pdf" ||
        mimeType == "application/javascript" ||
        mimeType == "application/json" ||
        mimeType == "application/ld+json" ||
        mimeType == "application/vnd.ms-fontobject" ||
        mimeType == "application/xml" ||
        mimeType == "application/xhtml+xml" ||
        mimeType == "application/wasm" ||
        mimeType == "application/vnd.apple.mpegurl"
}

// showDownloadPage displays a simple HTML page with a download button for non-renderable content.
func (s *Server) showDownloadPage(w http.ResponseWriter, hashStr, filename string) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    w.WriteHeader(http.StatusOK)
    fmt.Fprintf(w, pages.DownloadPage, hashStr, filename, hashStr, filename)
}

func (s *Server) showDirectoryListing(w http.ResponseWriter, r *http.Request, metadata *pb.ContentMetadata) {
    if strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
        s.writeJSON(w, http.StatusOK, metadata)
        return
    }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    w.WriteHeader(http.StatusOK)

    var html strings.Builder
    html.WriteString(fmt.Sprintf(pages.DirViewerPage, metadata.Filename, metadata.Filename, fmt.Sprintf("%x", metadata.Hash)))
    for _, link := range metadata.Links {
        hashHex := fmt.Sprintf("%x", link.Hash)
        var linkHref, nameDisplay string
        typeIcon := "file"
        if link.Type == "directory" {
            linkHref = fmt.Sprintf("/view/%s", hashHex)
            nameDisplay = link.Name + "/"
            typeIcon = "folder"
        } else {
            linkHref = fmt.Sprintf("/download/%s", hashHex)
            nameDisplay = link.Name
        }
        html.WriteString(fmt.Sprintf(`
            <tr>
                <td class="file-type">
                    <span class="type-icon type-%s"></span>
                    <span>%s</span>
                </td>
                <td class="file-name"><a href="%s" title="%s">%s</a></td>
                <td class="file-size">%d bytes</td>
                <td><a href="/view/%s" title="View details of %s" class="file-hash">%s</a></td>
            </tr>
        `, typeIcon, link.Type, linkHref, link.Name, nameDisplay, link.Size, hashHex, link.Name, hashHex))
    }
    html.WriteString(`
            </table>
        </body>
        </html>
    `)
    fmt.Fprint(w, html.String())
}

func (s *Server) storeUploadedFile(filename string, reader io.Reader, useEC bool) (hasher.Hash, int64, error) {
    if useEC {
        // Erasure Coding Path
        treeFile, shards, err := tree.BuildErasureCodedFileTree(reader, s.sharder)
        if err != nil {
            return hasher.Hash{}, 0, fmt.Errorf("failed to build erasure-coded merkle tree: %w", err)
        }
        for _, shard := range shards {
            if has, _ := s.storage.Has(shard.Hash()); !has {
                if err := s.storage.Put(shard); err != nil {
                    return hasher.Hash{}, 0, fmt.Errorf("failed to store shard %s: %w", tmt.HashToHex(shard.Hash()), err)
                }
            }
        }
        chunks := make([]*pb.ChunkInfo, len(treeFile.Chunks))
        for i, c := range treeFile.Chunks {
            chunks[i] = &pb.ChunkInfo{Hash: c.Hash[:], Size: int64(c.Size)}
        }
        metadata := &pb.ContentMetadata{
            Hash:           treeFile.Root[:],
            Filename:       filename,
            MimeType:       utils.GetMimeType(filename),
            Size:           treeFile.TotalSize,
            ModTime:        timestamppb.Now(),
            IsDirectory:    false,
            CreatedAt:      timestamppb.Now(),
            RefCount:       1,
            IsErasureCoded: true,
            DataShards:     int32(s.sharder.DataShardCount()),
            ParityShards:   int32(s.sharder.ParityShardCount()),
            Chunks:         chunks,
        }
        metaBytes, err := proto.Marshal(metadata)
        if err != nil { return hasher.Hash{}, 0, fmt.Errorf("failed to marshal metadata: %w", err) }
        if err := s.storage.Put(block.NewBlockWithHash(treeFile.Root, metaBytes)); err != nil { return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata block: %w", err) }
        if err := s.storage.StoreContent(metadata); err != nil { return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata: %w", err) }
        log.Printf("Successfully stored erasure-coded file %s with TMT root %s", filename, tmt.HashToHex(treeFile.Root))
        return treeFile.Root, treeFile.TotalSize, nil
    }

    // Streaming chunking path to avoid high memory usage
    var chunkInfos []chunker.ChunkInfo
    var leafHashes [][]byte
    var totalSize int64
    err := s.chunker.Stream(reader, func(ch chunker.Chunk) error {
        if has, _ := s.storage.Has(ch.Hash); !has {
            if err := s.storage.Put(block.NewBlock(ch.Data)); err != nil {
                return fmt.Errorf("failed to store chunk %s: %w", tmt.HashToHex(ch.Hash), err)
            }
        }
        chunkInfos = append(chunkInfos, chunker.ChunkInfo{Hash: ch.Hash, Size: ch.Size})
        leafHashes = append(leafHashes, ch.Hash[:])
        totalSize += int64(ch.Size)
        return nil
    })
    if err != nil { return hasher.Hash{}, 0, fmt.Errorf("failed to chunk and store: %w", err) }

    if len(leafHashes) == 0 {
        root := tmt.ComputeHash(nil)
        metadata := &pb.ContentMetadata{
            Hash:        root[:],
            Filename:    filename,
            MimeType:    utils.GetMimeType(filename),
            Size:        0,
            ModTime:     timestamppb.Now(),
            IsDirectory: false,
            CreatedAt:   timestamppb.Now(),
            RefCount:    1,
            Chunks:      []*pb.ChunkInfo{},
        }
        metaBytes, _ := proto.Marshal(metadata)
        _ = s.storage.Put(block.NewBlockWithHash(root, metaBytes))
        if err := s.storage.StoreContent(metadata); err != nil { return hasher.Hash{}, 0, err }
        return root, 0, nil
    }

    tree := tmt.NewDefault()
    if err := tree.Build(leafHashes); err != nil { return hasher.Hash{}, 0, fmt.Errorf("tmt build error: %w", err) }
    root, _ := tree.RootHash()
    pbChunks := make([]*pb.ChunkInfo, len(chunkInfos))
    for i, c := range chunkInfos {
        pbChunks[i] = &pb.ChunkInfo{Hash: c.Hash[:], Size: int64(c.Size)}
    }
    metadata := &pb.ContentMetadata{
        Hash:        root[:],
        Filename:    filename,
        MimeType:    utils.GetMimeType(filename),
        Size:        totalSize,
        ModTime:     timestamppb.Now(),
        IsDirectory: false,
        CreatedAt:   timestamppb.Now(),
        RefCount:    1,
        Chunks:      pbChunks,
    }
    metaBytes, err := proto.Marshal(metadata)
    if err != nil { return hasher.Hash{}, 0, fmt.Errorf("failed to marshal metadata: %w", err) }
    if err := s.storage.Put(block.NewBlockWithHash(root, metaBytes)); err != nil { return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata block: %w", err) }
    if err := s.storage.StoreContent(metadata); err != nil { return hasher.Hash{}, 0, fmt.Errorf("failed to store metadata: %w", err) }
    log.Printf("Successfully stored file %s with TMT root %s", filename, tmt.HashToHex(root))
    return root, totalSize, nil
}

func (s *Server) storeUploadedDirectory(path string, name string) (*tree.Link, error) {
    entries, err := os.ReadDir(path)
    if err != nil { return nil, err }
    var links []tree.Link
    for _, entry := range entries {
        entryPath := filepath.Join(path, entry.Name())
        var link *tree.Link
        if entry.IsDir() {
            link, err = s.storeUploadedDirectory(entryPath, entry.Name())
            if err != nil { return nil, err }
        } else {
            file, err := os.Open(entryPath)
            if err != nil { return nil, err }
            hash, size, err := s.storeUploadedFile(entry.Name(), file, false)
            file.Close()
            if err != nil { return nil, err }
            link = &tree.Link{Name: entry.Name(), Hash: hash, Size: size, Type: "file"}
        }
        links = append(links, *link)
    }
    dirHash, err := tree.BuildDirectoryTree(links)
    if err != nil { return nil, err }
    var totalSize int64
    for _, l := range links { totalSize += l.Size }
    pbLinks := make([]*pb.Link, len(links))
    for i, l := range links { pbLinks[i] = &pb.Link{Name: l.Name, Hash: l.Hash[:], Size: l.Size, Type: l.Type} }
    dirMetadata := &pb.ContentMetadata{
        Hash:        dirHash[:],
        Filename:    name,
        MimeType:    "inode/directory",
        Size:        totalSize,
        ModTime:     timestamppb.Now(),
        IsDirectory: true,
        CreatedAt:   timestamppb.Now(),
        RefCount:    1,
        Links:       pbLinks,
    }
    metaBytes, err := proto.Marshal(dirMetadata)
    if err != nil { return nil, fmt.Errorf("failed to marshal directory metadata: %w", err) }
    if err := s.storage.Put(block.NewBlockWithHash(dirHash, metaBytes)); err != nil { return nil, fmt.Errorf("failed to store directory metadata block: %w", err) }
    if err := s.storage.StoreContent(dirMetadata); err != nil { return nil, err }
    return &tree.Link{Name: name, Hash: dirHash, Size: totalSize, Type: "directory"}, nil
}

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    if err := json.NewEncoder(w).Encode(data); err != nil {
        log.Printf("Failed to encode JSON response: %v", err)
    }
}

// writeError writes a JSON error response
func (s *Server) writeError(w http.ResponseWriter, status int, message string, err error) {
    errorMsg := message
    if err != nil {
        errorMsg = fmt.Sprintf("%s: %v", message, err)
        log.Printf("API Error: %s", errorMsg)
    }
    response := map[string]interface{}{
        "error": map[string]interface{}{
            "code":    status,
            "message": errorMsg,
        },
    }
    s.writeJSON(w, status, response)
}

