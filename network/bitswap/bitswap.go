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
	"openhashdb/network/bitswap/pb"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	ProtocolBitswap      = protocol.ID("/openhashdb/bitswap/1.2.0")
	sendWantlistInterval = 10 * time.Second
	presenceCacheTTL     = 1 * time.Minute
)

// Engine is the main bitswap engine.
type Engine struct {
	host       host.Host
	blockstore *blockstore.Blockstore
	wantlist   *WantlistManager
	sessions   map[string]*Session
	peers      map[peer.ID]*peerLedger
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewEngine creates a new bitswap engine.
func NewEngine(ctx context.Context, h host.Host, bs *blockstore.Blockstore) *Engine {
	ctx, cancel := context.WithCancel(ctx)
	e := &Engine{
		host:       h,
		blockstore: bs,
		wantlist:   NewWantlistManager(),
		sessions:   make(map[string]*Session),
		peers:      make(map[peer.ID]*peerLedger),
		ctx:        ctx,
		cancel:     cancel,
	}
	h.SetStreamHandler(ProtocolBitswap, e.handleNewStream)
	go e.periodicWantlistBroadcast()
	return e
}

// GetBlock fetches a single block, waiting for it to become available from the network.
// The lifetime of this operation is controlled by the passed-in context.
func (e *Engine) GetBlock(ctx context.Context, h hasher.Hash) (block.Block, error) {
	// First, check if the block is already in the local blockstore.
	if has, _ := e.blockstore.Has(h); has {
		return e.blockstore.Get(h)
	}

	// Create a new bitswap session for this request.
	session := e.newSession(ctx)
	defer e.closeSession(session.id)

	// Add the desired block to the session's wantlist and the engine's global wantlist.
	// We initially ask for 'Have' to see which peers have the block.
	session.AddWant(h, pb.Message_Wantlist_Entry_Have)
	e.wantlist.Add(h, 1, pb.Message_Wantlist_Entry_Have)
	e.broadcastWantlist() // Broadcast the updated wantlist to connected peers.

	// This select waits for a block to be received or the context to be canceled.
	// It relies on the caller to provide a context with an appropriate deadline.
	select {
	case blk := <-session.blockChannel:
		// The block was received directly, possibly from a peer who had it cached
		// or responded with the block instead of a HAVE message.
		return blk, nil

	case peerWithBlock := <-session.peerHasBlockChannel:
		// A peer has indicated they have the block. Now, specifically request the block from them.
		e.sendWantBlockToPeer(peerWithBlock, h)

		// This inner select waits for the block to arrive from the specific peer.
		select {
		case blk := <-session.blockChannel:
			return blk, nil
		case <-ctx.Done():
			// The context expired or was canceled while waiting for the block from the specific peer.
			return nil, fmt.Errorf("context done while waiting for block %s from peer %s: %w", h, peerWithBlock, ctx.Err())
		}

	case <-ctx.Done():
		// The context was canceled or expired while waiting for any peer to have the block.
		return nil, fmt.Errorf("context done while waiting for a peer with block %s: %w", h, ctx.Err())
	}
}

// GetBlocks fetches multiple blocks.
func (e *Engine) GetBlocks(ctx context.Context, hashes []hasher.Hash) (<-chan block.Block, error) {
	output := make(chan block.Block)
	session := e.newSession(ctx)

	go func() {
		defer e.closeSession(session.id)
		defer close(output)

		wantsCount := 0
		for _, h := range hashes {
			if has, _ := e.blockstore.Has(h); !has {
				session.AddWant(h, pb.Message_Wantlist_Entry_Have)
				e.wantlist.Add(h, 1, pb.Message_Wantlist_Entry_Have)
				wantsCount++
			}
		}

		if wantsCount == 0 {
			return
		}

		e.broadcastWantlist()

		go func() {
			for {
				select {
				case peerWithBlock := <-session.peerHasBlockChannel:
					hashToGet := <-session.hashToGetChannel
					go e.sendWantBlockToPeer(peerWithBlock, hashToGet)
				case <-session.done:
					return
				case <-ctx.Done():
					return
				}
			}
		}()

		for i := 0; i < wantsCount; i++ {
			select {
			case blk := <-session.blockChannel:
				output <- blk
			case <-ctx.Done():
				return
			}
		}
	}()

	return output, nil
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
	// log.Printf("[Bitswap] Received %d blocks from %s", len(blocks), remotePeer)
	for _, b := range blocks {
		hash, err := hasher.HashFromBytes(b.Hash)
		if err != nil {
			continue
		}
		newBlock := block.NewBlockWithHash(hash, b.Data)
		e.blockstore.Put(newBlock)
		e.wantlist.Remove(newBlock.Hash())
		e.distributeBlock(newBlock)
	}
}

func (e *Engine) handleIncomingPresences(presences []*pb.Message_BlockPresence, remotePeer peer.ID) {
	for _, pres := range presences {
		hash, err := hasher.HashFromBytes(pres.Hash)
		if err != nil {
			continue
		}
		if pres.Type == pb.Message_BlockPresence_Have {
			e.distributeHave(hash, remotePeer)
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

// HandlePeerDisconnect cleans up resources associated with a disconnected peer.
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
	log.Printf("[Bitswap] Broadcasting wantlist with %d items", len(wl))
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

	if len(blocksToSend) > 0 {
		// log.Printf("[Bitswap] Fulfilling want-block request from %s for %d blocks", p, len(blocksToSend))
	}
	if len(presencesToSend) > 0 {
		log.Printf("[Bitswap] Responding to want-have request from %s with %d presences", p, len(presencesToSend))
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

// --- Session Management ---
type Session struct {
	id                  string
	wants               map[hasher.Hash]pb.Message_Wantlist_Entry_WantType
	blockChannel        chan block.Block
	peerHasBlockChannel chan peer.ID
	hashToGetChannel    chan hasher.Hash
	done                chan struct{}
	mu                  sync.RWMutex
}

func (e *Engine) newSession(ctx context.Context) *Session {
	e.mu.Lock()
	defer e.mu.Unlock()
	session := &Session{
		id:                  uuid.New().String(),
		wants:               make(map[hasher.Hash]pb.Message_Wantlist_Entry_WantType),
		blockChannel:        make(chan block.Block, 1),
		peerHasBlockChannel: make(chan peer.ID, 1),
		hashToGetChannel:    make(chan hasher.Hash, 1),
		done:                make(chan struct{}),
	}
	e.sessions[session.id] = session
	return session
}

func (e *Engine) closeSession(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if session, ok := e.sessions[id]; ok {
		close(session.done)
		delete(e.sessions, id)
	}
}

func (s *Session) AddWant(h hasher.Hash, wantType pb.Message_Wantlist_Entry_WantType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wants[h] = wantType
}

func (e *Engine) distributeBlock(b block.Block) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, session := range e.sessions {
		select {
		case <-session.done:
			continue
		default:
			session.mu.RLock()
			_, wanted := session.wants[b.Hash()]
			session.mu.RUnlock()
			if wanted {
				session.blockChannel <- b
				session.mu.Lock()
				delete(session.wants, b.Hash())
				session.mu.Unlock()
			}
		}
	}
}

func (e *Engine) distributeHave(h hasher.Hash, p peer.ID) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, session := range e.sessions {
		select {
		case <-session.done:
			continue
		default:
			session.mu.RLock()
			wantType, wanted := session.wants[h]
			session.mu.RUnlock()
			if wanted && wantType == pb.Message_Wantlist_Entry_Have {
				session.peerHasBlockChannel <- p
				session.hashToGetChannel <- h
				session.mu.Lock()
				session.wants[h] = pb.Message_Wantlist_Entry_Block
				session.mu.Unlock()
			}
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
				stream, err = h.NewStream(ctx, pl.peer, ProtocolBitswap)

				if err != nil {
					log.Printf("[Bitswap] Failed to open stream to %s: %v", pl.peer, err)
					// Don't return, just continue. The ledger can try again on the next message.
					continue
				}
				writer = bufio.NewWriter(stream)
			}

			data, err := proto.Marshal(msg)
			if err != nil {
				log.Printf("[Bitswap] Failed to marshal message for %s: %v", pl.peer, err)
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
				log.Printf("[Bitswap] Failed to send message to %s: %v", pl.peer, err)
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
