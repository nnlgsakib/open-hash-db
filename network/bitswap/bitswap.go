package bitswap

import (
    "bufio"
    "context"
    "encoding/binary"
    "fmt"
    "io"
    "log"
    "strings"
    "sync"
    "time"

    "openhashdb/core/block"
    "openhashdb/core/blockstore"
    "openhashdb/core/cidutil"
    "openhashdb/core/hasher"
    "openhashdb/network/intelligence"
    "openhashdb/network/protocols"
    "openhashdb/protobuf/pb"

    "github.com/google/uuid"
    cid "github.com/ipfs/go-cid"
    "github.com/libp2p/go-libp2p/core/host"
    "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/libp2p/go-libp2p/core/protocol"
    "google.golang.org/protobuf/proto"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "sort"
)

const (
    ProtocolBitswap        = protocol.ID("/openhashdb/bitswap/1.2.0")
    sendWantlistInterval   = 10 * time.Second
    presenceCacheTTL       = 1 * time.Minute
    maxConcurrentDownloads = 8
    providerSearchTimeout  = 5 * time.Minute
    maxConcurrentUpper     = 16
)

// Metrics for delegated fetching
var (
    delegatedFetchAttempts = promauto.NewCounter(prometheus.CounterOpts{
        Name: "openhashdb_bitswap_delegated_fetch_attempts_total",
        Help: "Number of delegated fetch attempts initiated by this node",
    })
    delegatedFetchSuccess = promauto.NewCounter(prometheus.CounterOpts{
        Name: "openhashdb_bitswap_delegated_fetch_success_total",
        Help: "Number of successful delegated fetches",
    })
    delegatedFetchFailures = promauto.NewCounter(prometheus.CounterOpts{
        Name: "openhashdb_bitswap_delegated_fetch_failures_total",
        Help: "Number of failed delegated fetches",
    })
)

// Engine is the main bitswap engine.
type Engine struct {
	host        host.Host
	blockstore  *blockstore.Blockstore
	wantlist    *WantlistManager
	peers       map[peer.ID]*peerLedger
	downloadMgr *DownloadManager
	registry    *intelligence.Registry
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewEngine creates a new bitswap engine.
func NewEngine(ctx context.Context, h host.Host, bs *blockstore.Blockstore) *Engine {
    ctx, cancel := context.WithCancel(ctx)
    e := &Engine{
        host:        h,
        blockstore:  bs,
        wantlist:    NewWantlistManager(),
        peers:       make(map[peer.ID]*peerLedger),
        downloadMgr: NewDownloadManager(),
        registry:    intelligence.NewRegistry(),
        ctx:         ctx,
        cancel:      cancel,
    }
    h.SetStreamHandler(ProtocolBitswap, e.handleNewStream)
    go e.periodicWantlistBroadcast()
    return e
}

// SetRegistry allows plugging a custom registry (primarily for tests)
func (e *Engine) SetRegistry(r *intelligence.Registry) { e.registry = r }

// UpdatePeerLatency allows external observers (e.g., ping) to feed latency into provider scoring
func (e *Engine) UpdatePeerLatency(p peer.ID, lat time.Duration) {
    if e.registry != nil {
        e.registry.UpdatePeerLatency(p, lat)
    }
}

// GetBlock fetches a single block, waiting for it to become available from the network.
func (e *Engine) GetBlock(ctx context.Context, h hasher.Hash) (block.Block, error) {
	if has, _ := e.blockstore.Has(h); has {
		return e.blockstore.Get(h)
	}

	blocks, err := e.GetBlocks(ctx, []hasher.Hash{h})
	if err != nil {
		return nil, err
	}

	select {
	case b, ok := <-blocks:
		if !ok {
			return nil, fmt.Errorf("failed to get block %s, channel closed", h)
		}
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetBlocks fetches multiple blocks concurrently from the network.
func (e *Engine) GetBlocks(ctx context.Context, hashes []hasher.Hash) (<-chan block.Block, error) {
    session := e.downloadMgr.NewSession(ctx, hashes)
    output := make(chan block.Block)

    go func() {
		defer close(output)
		defer e.downloadMgr.CloseSession(session.id)

		var initialWants []hasher.Hash
		for _, h := range hashes {
			if has, _ := e.blockstore.Has(h); !has {
				initialWants = append(initialWants, h)
			} else {
				blk, err := e.blockstore.Get(h)
				if err == nil {
					session.MarkAsDone(h)
					output <- blk
				}
			}
		}

		if len(initialWants) == 0 {
			return
		}

		log.Printf("[Bitswap] GetBlocks: Starting session %s for %d blocks", session.id, len(initialWants))

		// Add wants to global wantlist and broadcast
		for _, h := range initialWants {
			e.wantlist.Add(h, 1, pb.Message_Wantlist_Entry_Have)
		}
		e.broadcastWantlist()

        // Adaptive concurrency based on provider counts (rarest-first heuristic)
        totalProviders := 0
        for _, h := range initialWants {
            totalProviders += e.registry.ProviderCount(h)
        }
        conc := 2 + totalProviders*2
        if conc > maxConcurrentUpper { conc = maxConcurrentUpper }
        if conc < 2 { conc = 2 }
        var wg sync.WaitGroup
        for i := 0; i < conc; i++ {
            wg.Add(1)
            go e.downloadWorker(session, &wg)
        }

		// Collect results
		for i := 0; i < len(initialWants); i++ {
			select {
			case blk := <-session.output:
				select {
				case output <- blk:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				log.Printf("[Bitswap] GetBlocks context done for session %s", session.id)
				return
			}
		}
		log.Printf("[Bitswap] GetBlocks finished for session %s", session.id)
	}()

	return output, nil
}

func (e *Engine) downloadWorker(session *DownloadSession, wg *sync.WaitGroup) {
    defer wg.Done()
    for {
        select {
        case <-session.ctx.Done():
            return
        default:
        }

        hash, ok := session.NextWant()
        if !ok {
            return // No more blocks to download
        }

        // Probe direct peers first
        e.probePeersForWant(hash, true)
        waitCtx, cancel := context.WithTimeout(session.ctx, 3*time.Second)
        prov, err := session.WaitForProvider(waitCtx, hash)
        cancel()
        if err != nil {
            // Expand to relayed peers
            e.probePeersForWant(hash, false)
            prov, err = session.WaitForProvider(session.ctx, hash)
        }

        if err != nil {
            // Try delegated fetch via direct peers before giving up
            if e.tryDelegatedFetch(session, hash) {
                // delegated path succeeded, move to next want
                continue
            }
            log.Printf("[Bitswap Worker] Could not find provider for block %s: %v", hash, err)
            session.RequeueWant(hash) // Re-queue to try again later
            time.Sleep(1 * time.Second)
            continue
        }

        // log.Printf("[Bitswap Worker] Requesting block %s from peer %s", hash, prov)
        e.sendWantBlockToPeer(prov, hash)
        session.NoteRequest(hash, prov)
        // Watchdog: if not fulfilled within deadline, requeue (handles slow or unresponsive peers)
        go func(h hasher.Hash, pid peer.ID) {
            // Longer timeout for relayed peers
            timeout := 6 * time.Second
            if e.classifyPeer(pid) == intelligence.PeerTypeRelayed {
                timeout = 12 * time.Second
            }
            t := time.NewTimer(timeout)
            defer t.Stop()
            select {
            case <-t.C:
                if !session.IsDone(h) {
                    session.RequeueWant(h)
                }
            case <-session.ctx.Done():
                return
            }
        }(hash, prov)
    }
}

// tryDelegatedFetch asks a few direct peers to fetch the block and return bytes.
func (e *Engine) tryDelegatedFetch(session *DownloadSession, h hasher.Hash) bool {
    delegatedFetchAttempts.Inc()
    // collect direct peers
    var direct []peer.ID
    for _, p := range e.host.Network().Peers() {
        if e.classifyPeer(p) == intelligence.PeerTypeDirect {
            direct = append(direct, p)
        }
    }
    if len(direct) == 0 {
        return false
    }
    // limit fanout
    if len(direct) > 4 { direct = direct[:4] }

    // send in parallel; first success wins
    type result struct{ data []byte; ok bool }
    resCh := make(chan result, len(direct))
    var wg sync.WaitGroup
    for _, p := range direct {
        p := p
        wg.Add(1)
        go func() {
            defer wg.Done()
            ctx, cancel := context.WithTimeout(session.ctx, 10*time.Second)
            defer cancel()
            // Build request using custom wire format
            req := &protocols.DelegationRequest{Hash: h[:]}
            payload := req.Encode()
            s, err := e.host.NewStream(network.WithAllowLimitedConn(ctx, "delegate"), p, protocol.ID(protocols.DelegateProtocolID))
            if err != nil { resCh <- result{nil, false}; return }
            writer := bufio.NewWriter(s)
            lenBuf := make([]byte, binary.MaxVarintLen64)
            n := binary.PutUvarint(lenBuf, uint64(len(payload)))
            if _, err = writer.Write(lenBuf[:n]); err == nil { _, err = writer.Write(payload) }
            if err == nil { err = writer.Flush() }
            if err != nil { s.Close(); resCh <- result{nil, false}; return }
            reader := bufio.NewReader(s)
            msgLen, err := binary.ReadUvarint(reader)
            if err != nil { s.Close(); resCh <- result{nil, false}; return }
            buf := make([]byte, msgLen)
            if _, err = io.ReadFull(reader, buf); err != nil { s.Close(); resCh <- result{nil, false}; return }
            s.Close()
            resp, err := protocols.DecodeDelegationResponse(buf)
            if err != nil || resp.Error != "" || len(resp.Data) == 0 { resCh <- result{nil, false}; return }
            resCh <- result{resp.Data, true}
        }()
    }
    go func(){ wg.Wait(); close(resCh) }()
    for r := range resCh {
        if r.ok {
            // Insert block and deliver
            blk := block.NewBlockWithHash(h, r.data)
            _ = e.blockstore.Put(blk)
            e.wantlist.Remove(h)
            e.downloadMgr.DistributeBlock(blk)
            delegatedFetchSuccess.Inc()
            return true
        }
    }
    delegatedFetchFailures.Inc()
    return false
}

// probePeersForWant sends targeted want-have probes. If directOnly, limit to direct peers.
func (e *Engine) probePeersForWant(h hasher.Hash, directOnly bool) {
    var peersList []peer.ID
    if cand := e.registry.GetPreferredProviders(h); len(cand) > 0 {
        // Query a few top candidates
        limit := 4
        if len(cand) < limit { limit = len(cand) }
        for _, p := range cand[:limit] {
            if directOnly && e.classifyPeer(p) == intelligence.PeerTypeRelayed { continue }
            peersList = append(peersList, p)
        }
    } else {
        for _, p := range e.host.Network().Peers() {
            if directOnly && e.classifyPeer(p) == intelligence.PeerTypeRelayed { continue }
            peersList = append(peersList, p)
        }
    }
    if len(peersList) == 0 { return }
    entry := &pb.Message_Wantlist_Entry{Hash: h[:], Priority: 10, WantType: pb.Message_Wantlist_Entry_Have}
    msg := &pb.Message{Wantlist: &pb.Message_Wantlist{Entries: []*pb.Message_Wantlist_Entry{entry}}}
    for _, pid := range peersList {
        e.sendMessage(pid, msg)
    }
}

// classifyPeer returns whether a peer is direct or relayed using connection/addrs
func (e *Engine) classifyPeer(p peer.ID) intelligence.PeerType {
    conns := e.host.Network().ConnsToPeer(p)
    for _, c := range conns {
        if c.RemoteMultiaddr() != nil && !strings.Contains(c.RemoteMultiaddr().String(), "/p2p-circuit") {
            return intelligence.PeerTypeDirect
        }
    }
    for _, a := range e.host.Peerstore().Addrs(p) {
        if !strings.Contains(a.String(), "/p2p-circuit") {
            return intelligence.PeerTypeDirect
        }
    }
    return intelligence.PeerTypeRelayed
}

// handleNewStream handles incoming bitswap streams.
func (e *Engine) handleNewStream(s network.Stream) {
	remotePeer := s.Conn().RemotePeer()
	ledger := e.getOrCreateLedger(remotePeer)

	defer s.Close()
	reader := bufio.NewReader(s)

	for {
		msgLen, err := binary.ReadUvarint(reader)
		if err != nil {
			if err != io.EOF && err != network.ErrReset {
				log.Printf("[Bitswap] Failed to read message length from %s: %v", remotePeer, err)
			}
			return
		}

		buf := make([]byte, msgLen)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			log.Printf("[Bitswap] Failed to read message from %s: %v", remotePeer, err)
			return
		}

		var msg pb.Message
		if err := proto.Unmarshal(buf, &msg); err != nil {
			log.Printf("[Bitswap] Failed to decode message from %s: %v", remotePeer, err)
			continue
		}

		ledger.BytesRecv(uint64(msgLen))

		if msg.Wantlist != nil && len(msg.Wantlist.Entries) > 0 {
			go e.sendMatchingBlocks(remotePeer, msg.Wantlist)
		}
		if len(msg.Blocks) > 0 {
			go e.handleIncomingBlocks(msg.Blocks, remotePeer)
		}
		if len(msg.BlockPresences) > 0 {
			go e.handleIncomingPresences(msg.BlockPresences, remotePeer)
		}
	}
}

func (e *Engine) handleIncomingBlocks(blocks []*pb.Message_Block, remotePeer peer.ID) {
    for _, b := range blocks {
        var expected hasher.Hash
        var err error
        if len(b.Hash) == len(expected) {
            expected, err = hasher.HashFromBytes(b.Hash)
        } else {
            expected, err = parseWantToHash(b.Hash)
        }
        if err != nil {
            expected = hasher.HashBytes(b.Data)
        }
        if lat, ok := e.downloadMgr.NoteProviderResponse(expected, remotePeer); ok {
            e.registry.NoteBlockServed(remotePeer, lat)
        }
        newBlock := block.NewBlockWithHash(expected, b.Data)
        log.Printf("[Bitswap] Received block %s from %s", newBlock.Hash().String(), remotePeer.String())
        e.blockstore.Put(newBlock)
        e.wantlist.Remove(newBlock.Hash())
        e.downloadMgr.DistributeBlock(newBlock)
        e.registry.NoteBlockServed(remotePeer, 0)
    }
}

func (e *Engine) handleIncomingPresences(presences []*pb.Message_BlockPresence, remotePeer peer.ID) {
    for _, pres := range presences {
        hash, err := parseWantToHash(pres.Hash)
        if err != nil {
            continue
        }
        if pres.Type == pb.Message_BlockPresence_Have {
            // classify and record provider hint
            pType := e.classifyPeer(remotePeer)
            e.registry.ClassifyPeer(remotePeer, pType)
            e.registry.RegisterHave(remotePeer, hash)
            e.downloadMgr.DistributeHave(hash, remotePeer)
        }
    }
}

// parseWantToHash tries to resolve a want identifier to a local hash.
// Accepts either 32-byte digest or CID bytes (binary or string-encoded).
func parseWantToHash(b []byte) (hasher.Hash, error) {
    if len(b) == len(hasher.Hash{}) {
        return hasher.HashFromBytes(b)
    }
    if c, err := cid.Cast(b); err == nil {
        return cidutil.ToHash(c)
    }
    if c, err := cid.Parse(string(b)); err == nil {
        return cidutil.ToHash(c)
    }
    return hasher.Hash{}, fmt.Errorf("unknown want identifier")
}

func (e *Engine) getOrCreateLedger(p peer.ID) *peerLedger {
	e.mu.Lock()
	defer e.mu.Unlock()
	ledger, exists := e.peers[p]
	if !exists {
		ledger = newPeerLedger(p, e.ctx, e.host)
		e.peers[p] = ledger
	}
	return ledger
}

func (e *Engine) HandlePeerDisconnect(p peer.ID) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ledger, exists := e.peers[p]; exists {
		log.Printf("[Bitswap] Peer %s disconnected, cleaning up ledger.", p)
		close(ledger.done)
		delete(e.peers, p)
	}
}

func (e *Engine) HandleNewPeer(p peer.ID) {
	e.getOrCreateLedger(p)
	go e.sendWantlistToPeer(p, true)
}

func (e *Engine) periodicWantlistBroadcast() {
    ticker := time.NewTicker(sendWantlistInterval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            e.broadcastWantlistPrioritized()
        case <-e.ctx.Done():
            return
        }
    }
}

func (e *Engine) broadcastWantlist() {
    wl := e.wantlist.GetWantlist()
    if len(wl) == 0 {
        return
    }
    // log.Printf("[Bitswap] Broadcasting wantlist with %d items", len(wl))
    for _, p := range e.host.Network().Peers() {
        go e.sendWantlistToPeer(p, false)
    }
}

// broadcastWantlistPrioritized sends want-haves to direct peers first, then a small subset of relayed peers.
func (e *Engine) broadcastWantlistPrioritized() {
    wl := e.wantlist.GetWantlist()
    if len(wl) == 0 {
        return
    }
    var direct []peer.ID
    var relayed []peer.ID
    for _, p := range e.host.Network().Peers() {
        if e.classifyPeer(p) == intelligence.PeerTypeDirect {
            direct = append(direct, p)
        } else {
            relayed = append(relayed, p)
        }
    }
    for _, p := range direct {
        go e.sendWantlistToPeer(p, false)
    }
    const maxRelayedProbes = 4
    for i, p := range relayed {
        if i >= maxRelayedProbes {
            break
        }
        go e.sendWantlistToPeer(p, false)
    }
}

func (e *Engine) sendWantlistToPeer(p peer.ID, full bool) {
    wl := e.wantlist.GetWantlist()
    if len(wl) == 0 {
        return
    }
    // Rarest-first: order by provider count ascending
    // Filter wants using registry hints to avoid spamming peers that likely don't have the data
    type wantWrap struct{ entry WantlistEntry; providers int }
    wants := make([]wantWrap, 0, len(wl))
    for _, e1 := range wl {
        if e.registry.ShouldProbePeerFor(p, e1.Hash) {
            wants = append(wants, wantWrap{entry: e1, providers: e.registry.ProviderCount(e1.Hash)})
        }
    }
    sort.Slice(wants, func(i, j int) bool {
        if wants[i].providers != wants[j].providers { return wants[i].providers < wants[j].providers }
        return wants[i].entry.Priority > wants[j].entry.Priority
    })
    var entries []*pb.Message_Wantlist_Entry
    for _, w := range wants {
        c, _ := cidutil.FromHash(w.entry.Hash, cidutil.Raw)
        entries = append(entries, &pb.Message_Wantlist_Entry{
            Hash:     c.Bytes(),
            Priority: int32(w.entry.Priority),
            WantType: w.entry.WantType,
        })
    }
    if len(entries) == 0 {
        return
    }
    msg := &pb.Message{
        Wantlist: &pb.Message_Wantlist{Entries: entries, Full: full},
    }
    e.sendMessage(p, msg)
}

func (e *Engine) sendWantBlockToPeer(p peer.ID, h hasher.Hash) {
    c, _ := cidutil.FromHash(h, cidutil.Raw)
    entry := &pb.Message_Wantlist_Entry{
        Hash:     c.Bytes(),
        Priority: 100,
        WantType: pb.Message_Wantlist_Entry_Block,
    }
    msg := &pb.Message{Wantlist: &pb.Message_Wantlist{Entries: []*pb.Message_Wantlist_Entry{entry}}}
    e.sendMessage(p, msg)
}

func (e *Engine) sendMatchingBlocks(p peer.ID, wl *pb.Message_Wantlist) {
    var blocksToSend []*pb.Message_Block
    var presencesToSend []*pb.Message_BlockPresence
    ledger := e.getOrCreateLedger(p)
    // Simple reciprocity policy: deprioritize block sending for poor reciprocators
    ledger.mu.RLock()
    sent, recv := ledger.bytesSent, ledger.bytesRecv
    ledger.mu.RUnlock()
    leech := recv > 0 && sent > 4*recv

    for _, entry := range wl.Entries {
        hash, err := parseWantToHash(entry.Hash)
        if err != nil {
            continue
        }
        has, _ := e.blockstore.Has(hash)
        hasMeta := e.blockstore.HasContent(hash)

        if entry.WantType == pb.Message_Wantlist_Entry_Block {
            if has && !leech {
                blk, err := e.blockstore.Get(hash)
                if err != nil {
                    log.Printf("[Bitswap] Core Error: Failed to get block %s from blockstore, but Has() was true: %v", hash, err)
                    continue
                }
                blockHash := blk.Hash()
                log.Printf("[Bitswap] Sending block %s to %s", blk.Hash().String(), p.String())
                blocksToSend = append(blocksToSend, &pb.Message_Block{
                    Hash: blockHash[:],
                    Data: blk.RawData(),
                })
            }
        } else if entry.WantType == 2 { // Meta (extension)
            if hasMeta {
                // Serve content metadata bytes as a block for the root/content hash
                meta, err := e.blockstore.GetContent(hash)
                if err != nil {
                    log.Printf("[Bitswap] Failed to load metadata %s for sending: %v", hash, err)
                    continue
                }
                data, err := proto.Marshal(meta)
                if err != nil {
                    log.Printf("[Bitswap] Failed to marshal metadata %s: %v", hash, err)
                    continue
                }
                log.Printf("[Bitswap] Sending metadata for %s to %s", hash.String(), p.String())
                blocksToSend = append(blocksToSend, &pb.Message_Block{
                    Hash: hash[:],
                    Data: data,
                })
            }
        } else if entry.WantType == pb.Message_Wantlist_Entry_Have {
            if ledger.hasSentPresenceRecently(hash) {
                continue
            }
            presenceType := pb.Message_BlockPresence_DontHave
            if has || hasMeta {
                presenceType = pb.Message_BlockPresence_Have
            }
            presencesToSend = append(presencesToSend, &pb.Message_BlockPresence{
                Hash: entry.Hash,
                Type: presenceType,
            })
            ledger.addSentPresence(hash)
        }
    }

    if len(blocksToSend) > 0 || len(presencesToSend) > 0 {
        msg := &pb.Message{Blocks: blocksToSend, BlockPresences: presencesToSend}
        e.sendMessage(p, msg)
    }
}

func (e *Engine) sendMessage(p peer.ID, msg *pb.Message) {
	ledger := e.getOrCreateLedger(p)
	select {
	case ledger.outgoing <- msg:
	case <-e.ctx.Done():
	}
}

// --- Download Manager and Session ---
type DownloadManager struct {
	sessions map[string]*DownloadSession
	mu       sync.RWMutex
}

func NewDownloadManager() *DownloadManager {
	return &DownloadManager{
		sessions: make(map[string]*DownloadSession),
	}
}

func (dm *DownloadManager) NewSession(ctx context.Context, hashes []hasher.Hash) *DownloadSession {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	sessCtx, cancel := context.WithCancel(ctx)
    s := &DownloadSession{
        id:         uuid.New().String(),
        ctx:        sessCtx,
        cancel:     cancel,
        wants:      make(chan hasher.Hash, len(hashes)),
        providers:  make(map[hasher.Hash]map[peer.ID]struct{}),
        provChans:  make(map[hasher.Hash]chan peer.ID),
        output:     make(chan block.Block, len(hashes)),
        doneBlocks: make(map[hasher.Hash]struct{}),
        reqTime:    make(map[hasher.Hash]time.Time),
        reqPeer:    make(map[hasher.Hash]peer.ID),
    }

	for _, h := range hashes {
		s.wants <- h
		s.provChans[h] = make(chan peer.ID, 1)
	}

	dm.sessions[s.id] = s
	return s
}

func (dm *DownloadManager) CloseSession(id string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if s, ok := dm.sessions[id]; ok {
		s.cancel()
		delete(dm.sessions, id)
	}
}

func (dm *DownloadManager) DistributeBlock(b block.Block) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	for _, s := range dm.sessions {
		s.handleBlock(b)
	}
}

func (dm *DownloadManager) DistributeHave(h hasher.Hash, p peer.ID) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	for _, s := range dm.sessions {
		s.addProvider(h, p)
	}
}

// NoteProviderResponse informs sessions that a response arrived from a peer for a given hash.
// Returns the first latency found.
func (dm *DownloadManager) NoteProviderResponse(h hasher.Hash, p peer.ID) (time.Duration, bool) {
    dm.mu.RLock()
    defer dm.mu.RUnlock()
    for _, s := range dm.sessions {
        if lat, ok := s.NoteResponse(h, p); ok {
            return lat, true
        }
    }
    return 0, false
}


type DownloadSession struct {
    id         string
    ctx        context.Context
    cancel     context.CancelFunc
    wants      chan hasher.Hash
    providers  map[hasher.Hash]map[peer.ID]struct{}
    provChans  map[hasher.Hash]chan peer.ID
    output     chan block.Block
    doneBlocks map[hasher.Hash]struct{}
    mu         sync.RWMutex
    reqTime    map[hasher.Hash]time.Time
    reqPeer    map[hasher.Hash]peer.ID
}

func (s *DownloadSession) NextWant() (hasher.Hash, bool) {
	select {
	case h := <-s.wants:
		return h, true
	case <-s.ctx.Done():
		return hasher.Hash{}, false
	}
}

func (s *DownloadSession) RequeueWant(h hasher.Hash) {
	select {
	case s.wants <- h:
	default:
	}
}

func (s *DownloadSession) MarkAsDone(h hasher.Hash) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.doneBlocks[h] = struct{}{}
}

func (s *DownloadSession) IsDone(h hasher.Hash) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    _, ok := s.doneBlocks[h]
    return ok
}

func (s *DownloadSession) NoteRequest(h hasher.Hash, p peer.ID) {
    s.mu.Lock()
    s.reqTime[h] = time.Now()
    s.reqPeer[h] = p
    s.mu.Unlock()
}

// NoteResponse returns latency if we had a request recorded for this hash and peer.
func (s *DownloadSession) NoteResponse(h hasher.Hash, p peer.ID) (time.Duration, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if tp, ok := s.reqPeer[h]; ok && tp == p {
        if t0, ok2 := s.reqTime[h]; ok2 {
            delete(s.reqTime, h)
            delete(s.reqPeer, h)
            return time.Since(t0), true
        }
    }
    return 0, false
}

func (s *DownloadSession) addProvider(h hasher.Hash, p peer.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.providers[h]; !ok {
		s.providers[h] = make(map[peer.ID]struct{})
	}
	s.providers[h][p] = struct{}{}

	if ch, ok := s.provChans[h]; ok {
		select {
		case ch <- p:
		default:
		}
	}
}

func (s *DownloadSession) WaitForProvider(ctx context.Context, h hasher.Hash) (peer.ID, error) {
	s.mu.RLock()
	// Check if we already have a provider
	if provs, ok := s.providers[h]; ok {
		for p := range provs {
			s.mu.RUnlock()
			return p, nil
		}
	}
	// Wait for a new provider
	ch, ok := s.provChans[h]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("no provider channel for hash %s", h)
	}

	select {
	case p := <-ch:
		return p, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *DownloadSession) handleBlock(b block.Block) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.doneBlocks[b.Hash()]; ok {
		return // Already handled this block
	}

	// Check if the block is part of this session by checking the provider chans map
	if _, ok := s.provChans[b.Hash()]; ok {
		s.doneBlocks[b.Hash()] = struct{}{}
		select {
		case s.output <- b:
		case <-s.ctx.Done():
		}
	}
}

// --- WantlistManager ---
type WantlistEntry struct {
	Hash     hasher.Hash
	Priority int
	WantType pb.Message_Wantlist_Entry_WantType
}
type WantlistManager struct {
	wants map[hasher.Hash]WantlistEntry
	mu    sync.RWMutex
}

func NewWantlistManager() *WantlistManager {
	return &WantlistManager{wants: make(map[hasher.Hash]WantlistEntry)}
}
func (wm *WantlistManager) Add(h hasher.Hash, priority int, wantType pb.Message_Wantlist_Entry_WantType) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.wants[h] = WantlistEntry{Hash: h, Priority: priority, WantType: wantType}
}
func (wm *WantlistManager) Remove(h hasher.Hash) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	delete(wm.wants, h)
}
func (wm *WantlistManager) GetWantlist() []WantlistEntry {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	wl := make([]WantlistEntry, 0, len(wm.wants))
	for _, entry := range wm.wants {
		wl = append(wl, entry)
	}
	return wl
}

// --- PeerLedger ---
type peerLedger struct {
	peer         peer.ID
	bytesSent    uint64
	bytesRecv    uint64
	sentPresence map[hasher.Hash]time.Time
	outgoing     chan *pb.Message
	done         chan struct{}
	mu           sync.RWMutex
}

func newPeerLedger(p peer.ID, ctx context.Context, h host.Host) *peerLedger {
	pl := &peerLedger{
		peer:         p,
		sentPresence: make(map[hasher.Hash]time.Time),
		outgoing:     make(chan *pb.Message, 16),
		done:         make(chan struct{}),
	}
	go pl.sender(ctx, h)
	return pl
}

func (pl *peerLedger) sender(ctx context.Context, h host.Host) {
    var stream network.Stream
    var writer *bufio.Writer

	defer func() {
		if stream != nil {
			stream.Close()
		}
	}()

	for {
		select {
        case msg := <-pl.outgoing:
            var err error
            if stream == nil {
                log.Printf("[Bitswap Sender] Opening new stream to peer %s", pl.peer)
                // Allow opening a stream over limited connections (e.g. relayed)
                stream, err = h.NewStream(network.WithAllowLimitedConn(ctx, "bitswap"), pl.peer, ProtocolBitswap)

				if err != nil {
					log.Printf("[Bitswap Sender] Failed to open stream to %s: %v", pl.peer, err)
					continue
				}
				log.Printf("[Bitswap Sender] Successfully opened new stream to %s (remote addr: %s)", pl.peer, stream.Conn().RemoteMultiaddr())
				writer = bufio.NewWriter(stream)
			}

			data, err := proto.Marshal(msg)
			if err != nil {
				log.Printf("[Bitswap Sender] Failed to marshal message for %s: %v", pl.peer, err)
				continue
			}

			lenBuf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(lenBuf, uint64(len(data)))

			_, err = writer.Write(lenBuf[:n])
			if err == nil {
				_, err = writer.Write(data)
			}
			if err == nil {
				err = writer.Flush()
			}

            if err != nil {
                log.Printf("[Bitswap Sender] Failed to send message to %s: %v", pl.peer, err)
                stream.Reset()
                stream = nil
                writer = nil
            } else {
                pl.BytesSent(uint64(len(data)))
            }

		case <-pl.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (pl *peerLedger) addSentPresence(h hasher.Hash) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	pl.sentPresence[h] = time.Now()
}

func (pl *peerLedger) hasSentPresenceRecently(h hasher.Hash) bool {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	if t, ok := pl.sentPresence[h]; ok {
		return time.Since(t) < presenceCacheTTL
	}
	return false
}

func (pl *peerLedger) BytesSent(n uint64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	pl.bytesSent += n
}

func (pl *peerLedger) BytesRecv(n uint64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	pl.bytesRecv += n
}
