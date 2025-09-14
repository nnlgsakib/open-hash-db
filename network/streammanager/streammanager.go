package streammanager

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "log"
    "sync"
    "time"

    "openhashdb/core/block"
    "openhashdb/core/hasher"
    "openhashdb/network/libp2p"
    "openhashdb/protobuf/pb"

    "github.com/libp2p/go-libp2p/core/peer"
    "google.golang.org/protobuf/proto"
)

const (
	MaxConcurrentStreams = 10
	StreamTimeout        = 5 * time.Minute
	QueueTimeout         = 10 * time.Minute
)

// TransferRequest represents a request to transfer a large file.
type TransferRequest struct {
    Hash      hasher.Hash
    PeerID    peer.ID
    Response  chan io.ReadCloser
    Error     chan error
    Ctx       context.Context
}

// StreamManager handles the queuing and processing of large file transfers.
type StreamManager struct {
	node          *libp2p.Node
	requests      chan *TransferRequest
	activeStreams map[string]context.CancelFunc
	streamSlots   chan struct{}
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager(node *libp2p.Node) *StreamManager {
	ctx, cancel := context.WithCancel(context.Background())
	sm := &StreamManager{
		node:          node,
		requests:      make(chan *TransferRequest, 100),
		activeStreams: make(map[string]context.CancelFunc),
		streamSlots:   make(chan struct{}, MaxConcurrentStreams),
		ctx:           ctx,
		cancel:        cancel,
	}
    go sm.processRequests()
    return sm
}

// Close shuts down the StreamManager.
func (sm *StreamManager) Close() {
	sm.cancel()
	close(sm.requests)
}

// RequestTransfer queues a new transfer request.
func (sm *StreamManager) RequestTransfer(ctx context.Context, hash hasher.Hash, peerID peer.ID) (io.ReadCloser, error) {
	req := &TransferRequest{
		Hash:     hash,
		PeerID:   peerID,
		Response: make(chan io.ReadCloser, 1),
		Error:    make(chan error, 1),
		Ctx:      ctx,
	}

	select {
	case sm.requests <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

    select {
    case stream := <-req.Response:
        return stream, nil
    case err := <-req.Error:
        return nil, err
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

// processRequests processes queued transfer requests.
func (sm *StreamManager) processRequests() {
	for {
		select {
		case req := <-sm.requests:
			sm.streamSlots <- struct{}{} // Acquire a slot for a concurrent stream
			go sm.handleTransfer(req)
		case <-sm.ctx.Done():
			return
		}
	}
}

// handleTransfer manages a single file transfer, including resuming.
func (sm *StreamManager) handleTransfer(req *TransferRequest) {
    defer func() {
        <-sm.streamSlots // Release the stream slot
    }()

	streamID := fmt.Sprintf("%s-%s", req.PeerID.String(), req.Hash.String())
	sm.mu.Lock()
	if _, exists := sm.activeStreams[streamID]; exists {
		sm.mu.Unlock()
		req.Error <- fmt.Errorf("transfer for %s from %s already in progress", req.Hash.String(), req.PeerID.String())
		return
	}

	_, cancel := context.WithCancel(req.Ctx)
	sm.activeStreams[streamID] = cancel
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.activeStreams, streamID)
		sm.mu.Unlock()
		cancel()
	}()

    log.Printf("[StreamManager] Starting stream for %s (peer hint: %s)", req.Hash.String(), req.PeerID.String())

    // Fetch metadata block first
    bs := sm.node.GetBitswap()
    if bs == nil { req.Error <- fmt.Errorf("bitswap not available"); return }

    metaCtx, cancel := context.WithTimeout(req.Ctx, 60*time.Second)
    defer cancel()
    metaBlk, err := bs.GetBlock(metaCtx, req.Hash)
    if err != nil {
        req.Error <- fmt.Errorf("failed to fetch metadata block %s: %w", req.Hash, err)
        return
    }
    var metadata pb.ContentMetadata
    if err := proto.Unmarshal(metaBlk.RawData(), &metadata); err != nil {
        // Treat as raw block stream
        r := io.NopCloser(bytes.NewReader(metaBlk.RawData()))
        req.Response <- r
        return
    }
    if metadata.IsDirectory {
        req.Error <- fmt.Errorf("cannot stream directory content: %s", req.Hash)
        return
    }

    // Prepare ordered list of chunks
    chunks := metadata.Chunks
    if len(chunks) == 0 {
        // No chunks means the metadata block itself is the content
        r := io.NopCloser(bytes.NewReader(metaBlk.RawData()))
        req.Response <- r
        return
    }

    // Start concurrent fetch of chunks via bitswap and return a reader over them
    stream, err := newChunkStream(req.Ctx, bs, chunks)
    if err != nil { req.Error <- err; return }
    req.Response <- stream
}

// chunkStream implements io.ReadCloser to stream a sequence of chunks fetched via bitswap
type chunkStream struct {
    ctx    context.Context
    cancel context.CancelFunc

    mu        sync.Mutex
    chunks    []*pb.ChunkInfo
    nextIndex int
    buf       *bytes.Reader

    recvMu   sync.Mutex
    recv     map[hasher.Hash][]byte
    waitCh   chan struct{}

    errOnce sync.Once
    err     error
}

func newChunkStream(ctx context.Context, bs interface{ GetBlocks(context.Context, []hasher.Hash) (<-chan block.Block, error) }, chunks []*pb.ChunkInfo) (io.ReadCloser, error) {
    if len(chunks) == 0 { return io.NopCloser(bytes.NewReader(nil)), nil }
    sctx, cancel := context.WithCancel(ctx)
    cs := &chunkStream{
        ctx:     sctx,
        cancel:  cancel,
        chunks:  chunks,
        recv:    make(map[hasher.Hash][]byte),
        waitCh:  make(chan struct{}, 1),
        buf:     bytes.NewReader(nil),
    }
    // Launch fetcher
    go func() {
        hashes := make([]hasher.Hash, 0, len(chunks))
        for _, c := range chunks { if h, err := hasher.HashFromBytes(c.Hash); err == nil { hashes = append(hashes, h) } }
        ch, err := bs.GetBlocks(sctx, hashes)
        if err != nil { cs.finishWithError(err); return }
        for {
            select {
            case <-sctx.Done():
                return
            case blk, ok := <-ch:
                if !ok { return }
                h := blk.Hash()
                data := blk.RawData()
                cs.recvMu.Lock()
                cs.recv[h] = data
                cs.recvMu.Unlock()
                cs.signal()
            }
        }
    }()
    return cs, nil
}

func (cs *chunkStream) signal() {
    select { case cs.waitCh <- struct{}{}: default: }
}

func (cs *chunkStream) Read(p []byte) (int, error) {
    for {
        // Drain current buffer if any
        if cs.buf != nil {
            if n, err := cs.buf.Read(p); n > 0 || (err != nil && err != io.EOF) {
                return n, err
            }
        }
        // Load next chunk data
        if cs.nextIndex >= len(cs.chunks) {
            return 0, io.EOF
        }
        next := cs.chunks[cs.nextIndex]
        h, err := hasher.HashFromBytes(next.Hash)
        if err != nil { cs.finishWithError(err); return 0, err }
        cs.recvMu.Lock()
        data, ok := cs.recv[h]
        cs.recvMu.Unlock()
        if ok {
            cs.buf = bytes.NewReader(data)
            cs.nextIndex++
            continue
        }
        // Wait for more data or context done
        select {
        case <-cs.waitCh:
            continue
        case <-cs.ctx.Done():
            if cs.err != nil { return 0, cs.err }
            return 0, io.EOF
        }
    }
}

func (cs *chunkStream) finishWithError(err error) {
    cs.errOnce.Do(func() { cs.err = err; cs.cancel() })
}

func (cs *chunkStream) Close() error {
    cs.cancel()
    return nil
}
