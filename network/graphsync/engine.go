package graphsync

import (
    "bufio"
    "context"
    "encoding/binary"
    "fmt"
    "io"
    "log"
    "time"

    cid "github.com/ipfs/go-cid"
    "github.com/libp2p/go-libp2p/core/host"
    "github.com/libp2p/go-libp2p/core/network"
    "openhashdb/core/block"
    "openhashdb/core/blockstore"
    "openhashdb/core/cidutil"
    "openhashdb/core/hasher"
    "openhashdb/network/protocols"
    "openhashdb/protobuf/pb"
    "google.golang.org/protobuf/proto"
    "openhashdb/core/unixfs"
)

// Engine is a minimal GraphSync-like engine with a single-request handler.
// It accepts a Request and replies with a Response that can include the root metadata block.
type Engine struct{
    h   host.Host
    ctx context.Context
    bs  *blockstore.Blockstore
}

func New(ctx context.Context, h host.Host) *Engine {
    e := &Engine{h: h, ctx: ctx}
    h.SetStreamHandler(protocols.GraphSyncProtocolID, e.handleStream)
    return e
}

// WithBlockstore attaches a blockstore to serve data
func (e *Engine) WithBlockstore(bs *blockstore.Blockstore) *Engine { e.bs = bs; return e }

func (e *Engine) handleStream(s network.Stream) {
    defer s.Close()
    remote := s.Conn().RemotePeer()
    reader := bufio.NewReader(s)
    // Read varint length then payload
    msgLen, err := binary.ReadUvarint(reader)
    if err != nil { if err != io.EOF && err != network.ErrReset { log.Printf("[GraphSync] len read err from %s: %v", remote, err) }; return }
    buf := make([]byte, msgLen)
    if _, err := io.ReadFull(reader, buf); err != nil { log.Printf("[GraphSync] read err from %s: %v", remote, err); return }
    var req pb.Request
    if err := proto.Unmarshal(buf, &req); err != nil { log.Printf("[GraphSync] bad request from %s: %v", remote, err); e.writeResponse(s, &pb.Response{Id: req.Id, Status: "ERROR", ErrorMessage: "bad request"}); return }

    // Parse root CID
    rootCID, err := parseCIDBytes(req.RootCid)
    if err != nil { e.writeResponse(s, &pb.Response{Id: req.Id, Status: "ERROR", ErrorMessage: "invalid cid"}); return }
    // Convert to internal hash
    h, err := cidutil.ToHash(rootCID)
    if err != nil { e.writeResponse(s, &pb.Response{Id: req.Id, Status: "ERROR", ErrorMessage: "unsupported cid"}); return }

    // If no blockstore, reply with partial
    if e.bs == nil { e.writeResponse(s, &pb.Response{Id: req.Id, Status: "PARTIAL"}); return }

    // Traverse DAG and stream blocks
    blocks, status := e.collectBlocks(rootCID, h, req.Selector)
    e.writeResponse(s, &pb.Response{Id: req.Id, Status: status, Blocks: blocks})
}

func (e *Engine) tryGetBlock(h hasher.Hash) (block.Block, error) {
    if e.bs == nil { return nil, fmt.Errorf("no blockstore") }
    return e.bs.Get(h)
}

// collectBlocks walks a simple DAG rooted at hash/ CID.
// If selector is empty, stream root metadata and all file chunks; for directories, include child metadata one level deep.
func (e *Engine) collectBlocks(rootCID cid.Cid, h hasher.Hash, selector []byte) ([]*pb.GSBlock, string) {
    var out []*pb.GSBlock
    cfg := parseSelector(selector)
    // Add root metadata block if available
    rootBlk, err := e.tryGetBlock(h)
    if err == nil {
        if !cfg.FileOnly {
            out = append(out, &pb.GSBlock{Link: rootCID.Bytes(), Data: rootBlk.RawData()})
        }
    }
    // Decode metadata to decide traversal
    var meta pb.ContentMetadata
    if rootBlk != nil {
        if err := proto.Unmarshal(rootBlk.RawData(), &meta); err == nil {
            // If file, stream chunks
            if !meta.IsDirectory {
                // Limit total blocks
                const maxBlocks = 2048
                for _, ch := range meta.Chunks {
                    if len(out) >= maxBlocks { break }
                    chHash, err := hasher.HashFromBytes(ch.Hash)
                    if err != nil { continue }
                    if b, err := e.bs.Get(chHash); err == nil {
                        // child CID
                        cc, _ := cidutil.FromHash(chHash, cidutil.Raw)
                        out = append(out, &pb.GSBlock{Link: cc.Bytes(), Data: b.RawData()})
                    }
                }
            } else {
                // Directory: include child metadata one level deep
                depthLeft := cfg.Depth
                if depthLeft <= 0 { depthLeft = 1 }
                visited := map[hasher.Hash]struct{}{}
                e.walkDir(h, depthLeft, cfg, visited, &out)
            }
            // Also include a dag-cbor encoded metadata block for UnixFS/IPLD compatibility if not fileOnly
            if !cfg.FileOnly {
                if cbor, err := unixfs.EncodeMetadataDagCBOR(&meta); err == nil {
                    mh := hasher.HashBytes(cbor)
                    cc, _ := cidutil.FromHash(mh, cidutil.DagCBOR)
                    out = append(out, &pb.GSBlock{Link: cc.Bytes(), Data: cbor})
                }
            }
        }
    }
    status := "OK"
    if len(out) == 0 { status = "PARTIAL" }
    // Prevent overly large responses in one go
    if len(out) > 0 {
        // backpressure: small sleep to avoid clogging relayed connections
        time.Sleep(5 * time.Millisecond)
    }
    return out, status
}

type selectorConfig struct {
    Depth   int // how many directory levels to traverse (0=none, 1=direct children, etc.)
    FileOnly bool // if true, stream only file data blocks; exclude metadata
}

// parseSelector supports a simple ASCII format: "depth=N;fileOnly=true". Empty means defaults.
func parseSelector(b []byte) selectorConfig {
    cfg := selectorConfig{Depth: 1, FileOnly: false}
    if len(b) == 0 { return cfg }
    s := string(b)
    // very simple parser
    for _, part := range []string{";"} {
        _ = part // placeholder to keep style; real parsing below
    }
    // split by ';'
    start := 0
    for i := 0; i <= len(s); i++ {
        if i == len(s) || s[i] == ';' {
            kv := s[start:i]
            if eq := indexRune(kv, '='); eq > 0 {
                k := kv[:eq]
                v := kv[eq+1:]
                if k == "depth" {
                    var n int
                    _, _ = fmt.Sscanf(v, "%d", &n)
                    if n >= 0 { cfg.Depth = n }
                } else if k == "fileOnly" {
                    if v == "true" || v == "1" { cfg.FileOnly = true }
                }
            }
            start = i+1
        }
    }
    return cfg
}

func indexRune(s string, r byte) int { for i := 0; i < len(s); i++ { if s[i]==r { return i } } ; return -1 }

func (e *Engine) walkDir(root hasher.Hash, depth int, cfg selectorConfig, visited map[hasher.Hash]struct{}, out *[]*pb.GSBlock) {
    if depth < 0 { return }
    if _, ok := visited[root]; ok { return }
    visited[root] = struct{}{}
    // get metadata block
    b, err := e.bs.Get(root)
    if err != nil { return }
    var meta pb.ContentMetadata
    if err := proto.Unmarshal(b.RawData(), &meta); err != nil { return }
    // include metadata unless fileOnly
    if !cfg.FileOnly {
        cc, _ := cidutil.FromHash(root, cidutil.Raw)
        *out = append(*out, &pb.GSBlock{Link: cc.Bytes(), Data: b.RawData()})
    }
    if !meta.IsDirectory {
        for _, ch := range meta.Chunks {
            chh, err := hasher.HashFromBytes(ch.Hash)
            if err != nil { continue }
            if blk, err := e.bs.Get(chh); err == nil {
                cc, _ := cidutil.FromHash(chh, cidutil.Raw)
                *out = append(*out, &pb.GSBlock{Link: cc.Bytes(), Data: blk.RawData()})
            }
        }
        return
    }
    if depth == 0 { return }
    for _, l := range meta.Links {
        lh, err := hasher.HashFromBytes(l.Hash)
        if err != nil { continue }
        e.walkDir(lh, depth-1, cfg, visited, out)
    }
}

func (e *Engine) writeResponse(s network.Stream, resp *pb.Response) {
    data, _ := proto.Marshal(resp)
    writer := bufio.NewWriter(s)
    lenBuf := make([]byte, binary.MaxVarintLen64)
    n := binary.PutUvarint(lenBuf, uint64(len(data)))
    if _, err := writer.Write(lenBuf[:n]); err == nil {
        _, err = writer.Write(data)
    }
    if err := writer.Flush(); err != nil {
        _ = s.Reset()
    }
}

func parseCIDBytes(b []byte) (cid.Cid, error) {
    if len(b) == 0 { return cid.Cid{}, fmt.Errorf("empty cid") }
    // First try direct cast from raw bytes
    if c, err := cid.Cast(b); err == nil { return c, nil }
    // Fallback: interpret as string and parse
    if c, err := cid.Parse(string(b)); err == nil { return c, nil }
    return cid.Cid{}, fmt.Errorf("invalid cid bytes")
}
