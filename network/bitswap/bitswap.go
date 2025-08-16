package bitswap

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"openhashdb/core/block"
	"openhashdb/core/blockstore"
	"openhashdb/core/hasher"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	ProtocolBitswap = protocol.ID("/openhashdb/bitswap/1.1.0")
	sendWantlistInterval = 10 * time.Second
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

// GetBlock fetches a single block.
func (e *Engine) GetBlock(ctx context.Context, h hasher.Hash) (block.Block, error) {
	if has, _ := e.blockstore.Has(h); has {
		return e.blockstore.Get(h)
	}

	session := e.newSession(ctx)
	defer e.closeSession(session.id)

	session.AddWant(h)
	e.wantlist.Add(h, 1)
	e.broadcastWantlist()

	select {
	case blk := <-session.blockChannel:
		return blk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetBlocks fetches multiple blocks.
func (e *Engine) GetBlocks(ctx context.Context, hashes []hasher.Hash) (<-chan block.Block, error) {
	output := make(chan block.Block)
	session := e.newSession(ctx)

	go func() {
		defer e.closeSession(session.id)
		defer close(output)

		for _, h := range hashes {
			if has, _ := e.blockstore.Has(h); !has {
				session.AddWant(h)
				e.wantlist.Add(h, 1)
			}
		}
		e.broadcastWantlist()

		for i := 0; i < len(session.wants); i++ {
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
	log.Printf("Received bitswap stream from %s", remotePeer)

	var msg Message
	if err := json.NewDecoder(s).Decode(&msg); err != nil {
		log.Printf("Failed to decode bitswap message from %s: %v", remotePeer, err)
		s.Reset()
		return
	}

	// Handle received blocks
	if len(msg.Blocks) > 0 {
		log.Printf("Received %d blocks from %s", len(msg.Blocks), remotePeer)
		for _, b := range msg.Blocks {
			e.blockstore.Put(b)
			e.wantlist.Remove(b.Hash())
			e.distributeBlock(b)
		}
	}

	// Handle received wantlist
	if len(msg.Wantlist.Entries) > 0 {
		log.Printf("Received wantlist with %d entries from %s", len(msg.Wantlist.Entries), remotePeer)
		go e.sendMatchingBlocks(remotePeer, msg.Wantlist)
	}
}

// HandleNewPeer is called when a new peer connects.
func (e *Engine) HandleNewPeer(p peer.ID) {
	e.mu.Lock()
	if _, exists := e.peers[p]; !exists {
		e.peers[p] = newPeerLedger()
		log.Printf("Tracking new bitswap peer: %s", p)
	}
	e.mu.Unlock()

	// Send our full wantlist to the new peer
	go e.sendWantlistToPeer(p, true)
}

// periodicWantlistBroadcast periodically sends the wantlist to all connected peers.
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

// broadcastWantlist sends the current wantlist to all connected peers.
func (e *Engine) broadcastWantlist() {
	wl := e.wantlist.GetWantlist()
	if len(wl) == 0 {
		return
	}

	log.Printf("Broadcasting wantlist with %d items", len(wl))
	for _, p := range e.host.Network().Peers() {
		go e.sendWantlistToPeer(p, false)
	}
}

// sendWantlistToPeer sends the wantlist to a specific peer.
func (e *Engine) sendWantlistToPeer(p peer.ID, full bool) {
	wl := e.wantlist.GetWantlist()
	if len(wl) == 0 {
		return
	}

	msg := Message{
		Wantlist: Wantlist{
			Entries: wl,
			Full:    full,
		},
	}

	e.sendMessage(p, &msg)
}

// sendMatchingBlocks finds blocks a peer wants and sends them.
func (e *Engine) sendMatchingBlocks(p peer.ID, wl Wantlist) {
	var blocksToSend []block.Block
	for _, entry := range wl.Entries {
		if has, _ := e.blockstore.Has(entry.Hash); has {
			blk, err := e.blockstore.Get(entry.Hash)
			if err == nil {
				blocksToSend = append(blocksToSend, blk)
			}
		}
	}

	if len(blocksToSend) > 0 {
		log.Printf("Sending %d matching blocks to %s", len(blocksToSend), p)
		msg := &Message{Blocks: blocksToSend}
		if err := e.sendMessage(p, msg); err != nil {
			log.Printf("Failed to send matching blocks to %s: %v", p, err)
		}
	}
}

// sendMessage sends a bitswap message to a peer.
func (e *Engine) sendMessage(p peer.ID, msg *Message) error {
	s, err := e.host.NewStream(e.ctx, p, ProtocolBitswap)
	if err != nil {
		return fmt.Errorf("failed to open stream to %s: %w", p, err)
	}
	defer s.Close()

	if err := json.NewEncoder(s).Encode(msg); err != nil {
		s.Reset()
		return fmt.Errorf("failed to send bitswap message to %s: %w", p, err)
	}

	return nil
}

// --- Session Management ---

// Session tracks a single GetBlocks operation.
type Session struct {
	id           string
	wants        map[hasher.Hash]struct{}
	blockChannel chan block.Block
	mu           sync.RWMutex
}

func (e *Engine) newSession(ctx context.Context) *Session {
	e.mu.Lock()
	defer e.mu.Unlock()

	session := &Session{
		id:           uuid.New().String(),
		wants:        make(map[hasher.Hash]struct{}),
		blockChannel: make(chan block.Block),
	}
	e.sessions[session.id] = session
	return session
}

func (e *Engine) closeSession(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.sessions, id)
}

func (s *Session) AddWant(h hasher.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wants[h] = struct{}{}
}

// distributeBlock finds the session that wants the block and sends it.
func (e *Engine) distributeBlock(b block.Block) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, session := range e.sessions {
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

// --- WantlistManager ---
type WantlistManager struct {
	wants map[hasher.Hash]WantlistEntry
	mu    sync.RWMutex
}

func NewWantlistManager() *WantlistManager {
	return &WantlistManager{
		wants: make(map[hasher.Hash]WantlistEntry),
	}
}

func (wm *WantlistManager) Add(h hasher.Hash, priority int) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.wants[h] = WantlistEntry{Hash: h, Priority: priority}
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

// --- PeerLedger (simplified) ---
type peerLedger struct {
	peer      peer.ID
	bytesSent uint64
	bytesRecv uint64
}

func newPeerLedger() *peerLedger {
	return &peerLedger{}
}