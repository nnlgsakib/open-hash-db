package bitswap

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"openhashdb/core/block"
	"openhashdb/core/blockstore"
	"openhashdb/core/hasher"
	"openhashdb/protobuf/pb"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	ProtocolBitswap        = protocol.ID("/openhashdb/bitswap/1.2.0")
	sendWantlistInterval   = 10 * time.Second
	presenceCacheTTL       = 1 * time.Minute
	maxConcurrentDownloads = 8
	providerSearchTimeout  = 30 * time.Second
	baseHedgeDelay         = 400 * time.Millisecond
)

// Engine is the main bitswap engine.
type Engine struct {
    host        host.Host
    blockstore  *blockstore.Blockstore
    wantlist    *WantlistManager
    peers       map[peer.ID]*peerLedger
    downloadMgr *DownloadManager
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
		ctx:         ctx,
		cancel:      cancel,
	}
    h.SetStreamHandler(ProtocolBitswap, e.handleNewStream)
    go e.periodicWantlistBroadcast()
    return e
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
    // Provide session with peer-selection logic and hedging config
    session := e.downloadMgr.NewSession(ctx, hashes, func(candidates map[peer.ID]struct{}) (peer.ID, bool) {
        return e.selectBestPeer(candidates)
    })
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

		var wg sync.WaitGroup
        // Scale workers based on outstanding wants up to max
        workerCount := maxConcurrentDownloads
        if l := len(initialWants); l > 0 && l < workerCount {
            workerCount = l
        }
        for i := 0; i < workerCount; i++ {
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

        providerCtx, cancel := context.WithTimeout(session.ctx, providerSearchTimeout)
        p, err := session.WaitForProvider(providerCtx, hash)
        cancel()

        if err != nil {
            log.Printf("[Bitswap Worker] Could not find provider for block %s: %v", hash, err)
            session.RequeueWant(hash) // Re-queue to try again later
            time.Sleep(1 * time.Second)
            continue
        }

        // Request from selected peer, and optionally hedge to another peer if slow
        session.MarkAsked(hash, p)
        // log.Printf("[Bitswap Worker] Requesting block %s from peer %s", hash, p)
        e.sendWantBlockToPeer(p, hash)

        // Hedge after a small delay if not yet received and other providers exist
        go func(hsh hasher.Hash, first peer.ID) {
            // compute dynamic hedge delay using peer ledger EWMA if available
            hedgeDelay := baseHedgeDelay
            if pl := e.getLedger(first); pl != nil {
                if d := pl.getRTT(); d > 0 {
                    // hedge at ~50% of expected RTT, min 150ms
                    d2 := d / 2
                    if d2 < 150*time.Millisecond {
                        d2 = 150 * time.Millisecond
                    }
                    hedgeDelay = d2
                }
            }
            select {
            case <-time.After(hedgeDelay):
                if session.IsDone(hsh) { return }
                if next, ok := session.SelectNextProvider(hsh); ok {
                    if !session.HasAsked(hsh, next) {
                        session.MarkAsked(hsh, next)
                        // log.Printf("[Bitswap Worker] Hedging request for %s to %s", hsh, next)
                        e.sendWantBlockToPeer(next, hsh)
                    }
                }
            case <-session.ctx.Done():
                return
            }
        }(hash, p)
    }
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
        hash, err := hasher.HashFromBytes(b.Hash)
        if err != nil {
            continue
        }
        newBlock := block.NewBlockWithHash(hash, b.Data)
        e.blockstore.Put(newBlock)
        e.wantlist.Remove(newBlock.Hash())
        // update peer metrics for RTT and success
        if pl := e.getOrCreateLedger(remotePeer); pl != nil {
            pl.onBlockDelivered(hash)
        }
        e.downloadMgr.DistributeBlock(newBlock)
    }
}

func (e *Engine) handleIncomingPresences(presences []*pb.Message_BlockPresence, remotePeer peer.ID) {
	for _, pres := range presences {
		hash, err := hasher.HashFromBytes(pres.Hash)
		if err != nil {
			continue
		}
		if pres.Type == pb.Message_BlockPresence_Have {
			e.downloadMgr.DistributeHave(hash, remotePeer)
		}
	}
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

func (e *Engine) getLedger(p peer.ID) *peerLedger {
    e.mu.RLock()
    defer e.mu.RUnlock()
    return e.peers[p]
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
			e.broadcastWantlist()
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

func (e *Engine) sendWantlistToPeer(p peer.ID, full bool) {
	wl := e.wantlist.GetWantlist()
	if len(wl) == 0 {
		return
	}
	entries := make([]*pb.Message_Wantlist_Entry, len(wl))
	for i, entry := range wl {
		entries[i] = &pb.Message_Wantlist_Entry{
			Hash:     entry.Hash[:],
			Priority: int32(entry.Priority),
			WantType: entry.WantType,
		}
	}
	msg := &pb.Message{
		Wantlist: &pb.Message_Wantlist{Entries: entries, Full: full},
	}
	e.sendMessage(p, msg)
}

func (e *Engine) sendWantBlockToPeer(p peer.ID, h hasher.Hash) {
    entry := &pb.Message_Wantlist_Entry{
        Hash:     h[:],
        Priority: 100,
        WantType: pb.Message_Wantlist_Entry_Block,
    }
    msg := &pb.Message{
        Wantlist: &pb.Message_Wantlist{Entries: []*pb.Message_Wantlist_Entry{entry}},
    }
    // mark outbound request for RTT measurement
    if pl := e.getOrCreateLedger(p); pl != nil {
        pl.onRequestSent(h)
    }
    e.sendMessage(p, msg)
}

func (e *Engine) sendMatchingBlocks(p peer.ID, wl *pb.Message_Wantlist) {
    var blocksToSend []*pb.Message_Block
    var presencesToSend []*pb.Message_BlockPresence
    ledger := e.getOrCreateLedger(p)

	for _, entry := range wl.Entries {
		hash, err := hasher.HashFromBytes(entry.Hash)
		if err != nil {
			continue
		}
		has, _ := e.blockstore.Has(hash)

        if entry.WantType == pb.Message_Wantlist_Entry_Block && has {
            blk, err := e.blockstore.Get(hash)
            if err != nil {
                log.Printf("[Bitswap] Core Error: Failed to get block %s from blockstore, but Has() was true: %v", hash, err)
                continue
            }
            blockHash := blk.Hash()
            blocksToSend = append(blocksToSend, &pb.Message_Block{
                Hash: blockHash[:],
                Data: blk.RawData(),
            })
        } else if entry.WantType == pb.Message_Wantlist_Entry_Have {
            if ledger.hasSentPresenceRecently(hash) {
                continue
            }
            presenceType := pb.Message_BlockPresence_DontHave
            if has {
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

func (dm *DownloadManager) NewSession(ctx context.Context, hashes []hasher.Hash, selector func(map[peer.ID]struct{}) (peer.ID, bool)) *DownloadSession {
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
        askedPeers: make(map[hasher.Hash]map[peer.ID]struct{}),
        selector:   selector,
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
    askedPeers map[hasher.Hash]map[peer.ID]struct{}
    selector   func(map[peer.ID]struct{}) (peer.ID, bool)
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
        if s.selector != nil {
            if best, ok2 := s.selector(provs); ok2 {
                s.mu.RUnlock()
                return best, nil
            }
        } else {
            for p := range provs {
                s.mu.RUnlock()
                return p, nil
            }
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
        // On first provider, still consult selector among current providers, to pick best
        s.mu.RLock()
        var chosen peer.ID = p
        if provs, ok := s.providers[h]; ok && s.selector != nil {
            if best, ok2 := s.selector(provs); ok2 {
                chosen = best
            }
        }
        s.mu.RUnlock()
        return chosen, nil
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

// Track which peers we already asked for a given block
func (s *DownloadSession) MarkAsked(h hasher.Hash, p peer.ID) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if _, ok := s.askedPeers[h]; !ok { s.askedPeers[h] = make(map[peer.ID]struct{}) }
    s.askedPeers[h][p] = struct{}{}
}

func (s *DownloadSession) HasAsked(h hasher.Hash, p peer.ID) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    m, ok := s.askedPeers[h]
    if !ok { return false }
    _, ok = m[p]
    return ok
}

// SelectNextProvider returns another provider different from already asked peers
func (s *DownloadSession) SelectNextProvider(h hasher.Hash) (peer.ID, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    provs := s.providers[h]
    if len(provs) == 0 { return "", false }
    // filter out asked peers
    candidates := make(map[peer.ID]struct{}, len(provs))
    for p := range provs {
        if asked, ok := s.askedPeers[h]; ok {
            if _, already := asked[p]; already { continue }
        }
        candidates[p] = struct{}{}
    }
    if len(candidates) == 0 { return "", false }
    if s.selector != nil {
        return s.selector(candidates)
    }
    for p := range candidates { return p, true }
    return "", false
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
    inflight     map[hasher.Hash]time.Time // outbound requests we sent to this peer
    rttEWMA      time.Duration
}

func newPeerLedger(p peer.ID, ctx context.Context, h host.Host) *peerLedger {
    pl := &peerLedger{
        peer:         p,
        sentPresence: make(map[hasher.Hash]time.Time),
        outgoing:     make(chan *pb.Message, 16),
        done:         make(chan struct{}),
        inflight:     make(map[hasher.Hash]time.Time),
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
				stream, err = h.NewStream(ctx, pl.peer, ProtocolBitswap)

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

// onSendBlock marks the time we sent a block to this peer (for RTT measurement when it fetches from us)
// onRequestSent marks the time we sent a block request to this peer (for RTT measurement when it delivers)
func (pl *peerLedger) onRequestSent(h hasher.Hash) {
    pl.mu.Lock()
    pl.inflight[h] = time.Now()
    pl.mu.Unlock()
}

// onBlockDelivered records RTT for inflight request we made to this peer when block arrives
func (pl *peerLedger) onBlockDelivered(h hasher.Hash) {
    pl.mu.Lock()
    start, ok := pl.inflight[h]
    if ok { delete(pl.inflight, h) }
    pl.mu.Unlock()
    if ok {
        rtt := time.Since(start)
        pl.updateRTT(rtt)
    }
}

func (pl *peerLedger) updateRTT(sample time.Duration) {
    pl.mu.Lock()
    defer pl.mu.Unlock()
    const alpha = 0.2 // EWMA smoothing
    if pl.rttEWMA == 0 {
        pl.rttEWMA = sample
        return
    }
    // EWMA on durations via float64
    newVal := (1-alpha)*float64(pl.rttEWMA) + alpha*float64(sample)
    pl.rttEWMA = time.Duration(newVal)
}

func (pl *peerLedger) getRTT() time.Duration {
    pl.mu.RLock()
    defer pl.mu.RUnlock()
    return pl.rttEWMA
}

// Engine-level peer selection based on simple scoring
func (e *Engine) selectBestPeer(candidates map[peer.ID]struct{}) (peer.ID, bool) {
    var best peer.ID
    var bestScore float64 = -1
    now := time.Now()
    for p := range candidates {
        pl := e.getLedger(p)
        if pl == nil { return p, true }
        pl.mu.RLock()
        inflight := len(pl.inflight)
        rtt := pl.rttEWMA
        bytesRecv := pl.bytesRecv
        bytesSent := pl.bytesSent
        pl.mu.RUnlock()
        // Simple heuristic: prefer lower inflight, lower rtt, higher recv/sent ratio
        score := 0.0
        if rtt > 0 { score += 1.0 / (1 + float64(rtt/time.Millisecond)) } else { score += 0.5 }
        score += 1.0 / (1 + float64(inflight))
        if bytesSent > 0 { score += float64(bytesRecv) / float64(bytesSent) } else { score += 1.0 }
        // tiny jitter to avoid ties
        score += float64(now.UnixNano()%1000) / 1e12
        if score > bestScore { bestScore = score; best = p }
    }
    if best == "" { return "", false }
    return best, true
}
