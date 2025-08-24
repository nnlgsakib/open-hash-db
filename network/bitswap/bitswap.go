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
	"openhashdb/network/types"
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
	providerSearchTimeout  = 5 * time.Minute
)

// Engine is the main bitswap engine.
type Engine struct {
	host               host.Host
	node               types.NodeConnector // Reference to the libp2p node for connection info
	blockstore         *blockstore.Blockstore
	wantlist           *WantlistManager
	peers              map[peer.ID]*peerLedger
	downloadMgr        *DownloadManager
	delegatedFetches   map[hasher.Hash]peer.ID
	delegatedFetchesMu sync.Mutex
	mu                 sync.RWMutex
	ctx                context.Context
	cancel             context.CancelFunc
}

// NewEngine creates a new bitswap engine.
func NewEngine(ctx context.Context, h host.Host, node types.NodeConnector, bs *blockstore.Blockstore) *Engine {
	ctx, cancel := context.WithCancel(ctx)
	e := &Engine{
		host:             h,
		node:             node,
		blockstore:       bs,
		wantlist:         NewWantlistManager(),
		peers:            make(map[peer.ID]*peerLedger),
		downloadMgr:      NewDownloadManager(),
		delegatedFetches: make(map[hasher.Hash]peer.ID),
		ctx:              ctx,
		cancel:           cancel,
	}
	e.downloadMgr.engine = e // Give download manager a reference to the engine
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

		var wg sync.WaitGroup
		for i := 0; i < maxConcurrentDownloads; i++ {
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

		// This loop will implement the strategy for a single hash
		fetchAttemptCtx, cancelAttempt := context.WithTimeout(session.ctx, providerSearchTimeout)

		_, err := e.fetchBlockWithStrategy(fetchAttemptCtx, hash, session)
		if err != nil {
			log.Printf("[Bitswap Worker] Failed to fetch block %s with strategy: %v. Re-queuing.", hash, err)
			session.RequeueWant(hash)
			time.Sleep(2 * time.Second) // Backoff before retrying
			cancelAttempt()
			continue
		}
		cancelAttempt()

		// If successful, the block is already in the blockstore and distributed to the session
		// via handleBlock, so we don't need to do anything else here.
	}
}

// fetchBlockWithStrategy implements the core logic for finding and fetching a block.
func (e *Engine) fetchBlockWithStrategy(ctx context.Context, hash hasher.Hash, session *DownloadSession) (block.Block, error) {
	// 1. Find providers
	providers := e.findProvidersForHash(ctx, hash, session)
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers found for %s", hash)
	}

	// 2. Categorize providers
	directPeers, relayedPeers := e.categorizeProviders(providers)
	allConnectedDirectPeers := e.getDirectlyConnectedPeers(e.node.ConnectedPeers())

	// STRATEGY 1: DIRECT FETCH
	log.Printf("[Bitswap Strategy] %s: Trying %d direct peers.", hash, len(directPeers))
	if len(directPeers) > 0 {
		for _, p := range directPeers {
			e.sendWantBlockToPeer(p, hash)
		}
		// Wait for a bit to see if the block arrives
		if blk, err := session.WaitForBlock(ctx, hash, 5*time.Second); err == nil {
			log.Printf("[Bitswap Strategy] %s: Success from direct peer.", hash)
			return blk, nil
		}
	}

	// STRATEGY 2: DELEGATED FETCH
	log.Printf("[Bitswap Strategy] %s: Trying delegated fetch via %d direct peers to %d relayed peers.", hash, len(allConnectedDirectPeers), len(relayedPeers))
	if len(relayedPeers) > 0 && len(allConnectedDirectPeers) > 0 {
		// Simple delegation: ask the first available direct peer to fetch from the first available relayed provider.
		delegate := allConnectedDirectPeers[0]
		target := relayedPeers[0]
		log.Printf("[Bitswap Strategy] %s: Delegating fetch to %s to get from %s", hash, delegate, target)
		e.sendDelegatedFetchRequestToPeer(delegate, target, hash)

		// Wait a bit longer for delegated fetch
		if blk, err := session.WaitForBlock(ctx, hash, 15*time.Second); err == nil {
			log.Printf("[Bitswap Strategy] %s: Success from delegated fetch.", hash)
			return blk, nil
		}
	}

	// STRATEGY 3: RELAYED FETCH (FALLBACK)
	log.Printf("[Bitswap Strategy] %s: Trying %d relayed peers as a fallback.", hash, len(relayedPeers))
	if len(relayedPeers) > 0 {
		for _, p := range relayedPeers {
			e.sendWantBlockToPeer(p, hash)
		}
		// Wait even longer for relayed fetch
		if blk, err := session.WaitForBlock(ctx, hash, 30*time.Second); err == nil {
			log.Printf("[Bitswap Strategy] %s: Success from relayed peer.", hash)
			return blk, nil
		}
	}

	return nil, fmt.Errorf("all fetch strategies failed for %s", hash)
}

func (e *Engine) findProvidersForHash(ctx context.Context, h hasher.Hash, session *DownloadSession) []peer.ID {
	var providers []peer.ID
	provChan := make(chan peer.ID, 10)
	var wg sync.WaitGroup

	// Goroutine to gather providers from the session
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Use a non-blocking read on the provider channel for this hash
			select {
			case p, ok := <-session.provChans[h]:
				if !ok {
					return
				}
				provChan <- p
			case <-ctx.Done():
				return
			default:
				// If no provider is immediately available, exit the loop.
				// New providers will trigger the have/want logic again.
				return
			}
		}
	}()

	// Wait for the initial set of providers to be gathered
	wg.Wait()
	close(provChan)

	for p := range provChan {
		providers = append(providers, p)
	}
	return providers
}

func (e *Engine) categorizeProviders(providers []peer.ID) (direct, relayed []peer.ID) {
	for _, p := range providers {
		if e.node.GetPeerConnectionType(p) == types.ConnectionTypeDirect {
			direct = append(direct, p)
		} else {
			relayed = append(relayed, p)
		}
	}
	return
}

func (e *Engine) getDirectlyConnectedPeers(peers []peer.ID) []peer.ID {
	var directPeers []peer.ID
	for _, p := range peers {
		if e.node.GetPeerConnectionType(p) == types.ConnectionTypeDirect {
			directPeers = append(directPeers, p)
		}
	}
	return directPeers
}

func (e *Engine) sendDelegatedFetchRequestToPeer(delegate peer.ID, target peer.ID, h hasher.Hash) {
	req := &pb.Message_DelegatedFetchRequest{
		Hash:            h[:],
		TargetPeerId:    target.String(),
		RequestorPeerId: e.host.ID().String(),
	}
	msg := &pb.Message{
		DelegatedRequests: []*pb.Message_DelegatedFetchRequest{req},
	}
	e.sendMessage(delegate, msg)
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
		if len(msg.DelegatedRequests) > 0 {
			go e.handleDelegatedFetchRequests(remotePeer, msg.DelegatedRequests)
		}
	}
}

func (e *Engine) handleDelegatedFetchRequests(requestor peer.ID, reqs []*pb.Message_DelegatedFetchRequest) {
	for _, req := range reqs {
		targetPeerID, err := peer.Decode(req.TargetPeerId)
		if err != nil {
			log.Printf("[Bitswap] Invalid target peer ID in delegated request from %s: %v", requestor, err)
			continue
		}
		hash, err := hasher.HashFromBytes(req.Hash)
		if err != nil {
			log.Printf("[Bitswap] Invalid hash in delegated request from %s: %v", requestor, err)
			continue
		}

		log.Printf("[Bitswap] Received delegated fetch request from %s to get block %s from %s", requestor, hash, targetPeerID)

		// Track that this hash is for the original requestor.
		e.delegatedFetchesMu.Lock()
		e.delegatedFetches[hash] = requestor
		e.delegatedFetchesMu.Unlock()

		// Set a timeout to clean up the entry if the block never arrives.
		time.AfterFunc(2*time.Minute, func() {
			e.delegatedFetchesMu.Lock()
			// Check if it's still the same requestor before deleting
			if p, ok := e.delegatedFetches[hash]; ok && p == requestor {
				delete(e.delegatedFetches, hash)
			}
			e.delegatedFetchesMu.Unlock()
		})

		// This node now acts as a client to get the block from the target.
		e.sendWantBlockToPeer(targetPeerID, hash)
	}
}

func (e *Engine) handleIncomingBlocks(blocks []*pb.Message_Block, remotePeer peer.ID) {
	for _, b := range blocks {
		hash, err := hasher.HashFromBytes(b.Hash)
		if err != nil {
			continue
		}
		newBlock := block.NewBlockWithHash(hash, b.Data)

		// Check if this is for a delegated fetch
		e.delegatedFetchesMu.Lock()
		requestor, isDelegated := e.delegatedFetches[newBlock.Hash()]
		if isDelegated {
			delete(e.delegatedFetches, newBlock.Hash())
		}
		e.delegatedFetchesMu.Unlock()

		if isDelegated {
			log.Printf("[Bitswap] Forwarding delegated block %s to original requestor %s", newBlock.Hash(), requestor)
			// Send the block to the original requestor
			msg := &pb.Message{
				Blocks: []*pb.Message_Block{b},
			}
			e.sendMessage(requestor, msg)
		}

		log.Printf("[Bitswap] Received block %s from %s", newBlock.Hash().String(), remotePeer.String())
		e.blockstore.Put(newBlock)
		e.wantlist.Remove(newBlock.Hash())
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
	engine   *Engine // Reference back to the engine
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
		id:             uuid.New().String(),
		ctx:            sessCtx,
		cancel:         cancel,
		wants:          make(chan hasher.Hash, len(hashes)),
		providers:      make(map[hasher.Hash]map[peer.ID]struct{}),
		provChans:      make(map[hasher.Hash]chan peer.ID, len(hashes)),
		output:         make(chan block.Block, len(hashes)),
		doneBlocks:     make(map[hasher.Hash]struct{}),
		receivedBlocks: make(map[hasher.Hash]block.Block),
		blockChans:     make(map[hasher.Hash]chan block.Block),
	}

	for _, h := range hashes {
		s.wants <- h
		s.provChans[h] = make(chan peer.ID, 10) // Increased buffer to hold more providers
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
	id             string
	ctx            context.Context
	cancel         context.CancelFunc
	wants          chan hasher.Hash
	providers      map[hasher.Hash]map[peer.ID]struct{}
	provChans      map[hasher.Hash]chan peer.ID
	output         chan block.Block
	doneBlocks     map[hasher.Hash]struct{}
	receivedBlocks map[hasher.Hash]block.Block      // Cache for received blocks
	blockChans     map[hasher.Hash]chan block.Block // For specific block waiters
	mu             sync.RWMutex
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

// WaitForBlock waits for a specific block to be received in the session.
func (s *DownloadSession) WaitForBlock(ctx context.Context, h hasher.Hash, timeout time.Duration) (block.Block, error) {
	s.mu.RLock()
	if blk, ok := s.receivedBlocks[h]; ok {
		s.mu.RUnlock()
		return blk, nil
	}
	s.mu.RUnlock()

	// If not found, create a channel and wait.
	ch := make(chan block.Block, 1)
	s.mu.Lock()
	// Re-check just in case it arrived between RUnlock and Lock
	if blk, ok := s.receivedBlocks[h]; ok {
		s.mu.Unlock()
		return blk, nil
	}
	s.blockChans[h] = ch
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.blockChans, h)
		s.mu.Unlock()
	}()

	select {
	case blk := <-ch:
		return blk, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for block %s", h)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *DownloadSession) handleBlock(b block.Block) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.doneBlocks[b.Hash()]; ok {
		return // Already handled this block
	}

	// Check if the block is wanted by this session
	if _, ok := s.provChans[b.Hash()]; !ok {
		return
	}

	s.receivedBlocks[b.Hash()] = b
	s.doneBlocks[b.Hash()] = struct{}{}

	// Notify any specific waiters
	if ch, ok := s.blockChans[b.Hash()]; ok {
		select {
		case ch <- b:
		default:
		}
		// The waiter is responsible for removing the channel from the map.
	}

	// Send to general output channel
	select {
	case s.output <- b:
	case <-s.ctx.Done():
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
