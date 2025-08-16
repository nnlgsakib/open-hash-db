package libp2p

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerExchanger handles exchanging peer lists.
type PeerExchanger struct {
	node         *Node
	ctx          context.Context
	connecting   map[peer.ID]bool // Keep track of in-flight connections
	connectingMu sync.Mutex
}

// NewPeerExchanger creates a new PeerExchanger.
func NewPeerExchanger(ctx context.Context, node *Node) *PeerExchanger {
	return &PeerExchanger{
		node:       node,
		ctx:        ctx,
		connecting: make(map[peer.ID]bool),
	}
}

// getPeerListJSON gets the list of connected peers as a JSON byte slice.
func (pe *PeerExchanger) getPeerListJSON() ([]byte, error) {
	peers := pe.node.Host().Peerstore().Peers()
	var addrInfos []peer.AddrInfo
	for _, p := range peers {
		if p == pe.node.Host().ID() {
			continue
		}
		// We only want to share peers we are connected to.
		if pe.node.Host().Network().Connectedness(p) == network.Connected {
			addrInfos = append(addrInfos, pe.node.Host().Peerstore().PeerInfo(p))
		}
	}
	return json.Marshal(addrInfos)
}

// connectToNewPeers takes a list of AddrInfo, filters out known/current peers,
// and attempts to connect to the new ones, using the source as a relay if direct connection fails.
func (pe *PeerExchanger) connectToNewPeers(addrInfos []peer.AddrInfo, sourcePeer peer.ID) {
	var wg sync.WaitGroup

	for _, pi := range addrInfos {
		// Don't connect to self or already connected peers
		if pi.ID == pe.node.Host().ID() || pe.node.Host().Network().Connectedness(pi.ID) == network.Connected {
			continue
		}

		// Check if a connection is already in progress
		pe.connectingMu.Lock()
		if pe.connecting[pi.ID] {
			pe.connectingMu.Unlock()
			continue
		}
		pe.connecting[pi.ID] = true
		pe.connectingMu.Unlock()

		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()

			// Double-check connection status inside the goroutine
			if pe.node.Host().Network().Connectedness(pi.ID) == network.Connected {
				return
			}

			log.Printf("Discovered new peer %s from %s, attempting to connect", pi.ID.String(), sourcePeer.String())

			// Attempt direct connection
			err := pe.node.Connect(pe.ctx, pi.Addrs[0].String())
			if err == nil {
				log.Printf("Successfully connected directly to new peer %s", pi.ID.String())
				// On success, leave the peer in the `connecting` map to prevent race conditions.
				// A disconnect notifier should eventually clear this flag.
				return
			}

			log.Printf("Failed to connect directly to %s: %v. Trying via relay.", pi.ID.String(), err)

			// If direct connection fails, try via relay
			// This part needs to be updated as ConnectViaRelay was removed.
			// For now, we just log the failure.
			log.Printf("Relay connection not implemented yet.")
			pe.connectingMu.Lock()
			delete(pe.connecting, pi.ID)
			pe.connectingMu.Unlock()
		}(pi)
	}
	wg.Wait()
}

// handleExchange handles the peer exchange on an incoming stream.
// It reads the peer list, connects to new peers, sends its own list back.
func (pe *PeerExchanger) handleExchange(stream network.Stream) {
	defer stream.Close()
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Handling peer exchange with %s", remotePeer.String())

	// 1. Receive their peers
	reader := bufio.NewReader(stream)
	theirPeersJSON, err := reader.ReadBytes('\n')
	if err != nil {
		log.Printf("Failed to read peer list from stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	var theirPeers []peer.AddrInfo
	if err := json.Unmarshal(theirPeersJSON, &theirPeers); err != nil {
		log.Printf("Failed to unmarshal peer list from %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	// 2. Connect to new peers, using the remote peer as a potential relay
	go pe.connectToNewPeers(theirPeers, remotePeer)

	// 3. Send our peers
	ourPeersJSON, err := pe.getPeerListJSON()
	if err != nil {
		log.Printf("Failed to get our peer list for exchange with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	writer := bufio.NewWriter(stream)
	if _, err := writer.Write(append(ourPeersJSON, '\n')); err != nil {
		log.Printf("Failed to write our peer list to stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}
	if err := writer.Flush(); err != nil {
		log.Printf("Failed to flush stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}
}

// initiateExchange initiates a peer exchange on an outgoing stream.
// It sends its own peer list, then reads the other's list and connects to new peers.
func (pe *PeerExchanger) initiateExchange(stream network.Stream) error {
	defer stream.Close()
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Initiating peer exchange with %s", remotePeer.String())

	// 1. Send our peers
	ourPeersJSON, err := pe.getPeerListJSON()
	if err != nil {
		log.Printf("Failed to get our peer list for exchange with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	writer := bufio.NewWriter(stream)
	if _, err := writer.Write(append(ourPeersJSON, '\n')); err != nil {
		log.Printf("Failed to write our peer list to stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}
	if err := writer.Flush(); err != nil {
		log.Printf("Failed to flush stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	// 2. Receive their peers
	reader := bufio.NewReader(stream)
	theirPeersJSON, err := reader.ReadBytes('\n')
	if err != nil {
		log.Printf("Failed to read peer list from stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	var theirPeers []peer.AddrInfo
	if err := json.Unmarshal(theirPeersJSON, &theirPeers); err != nil {
		log.Printf("Failed to unmarshal peer list from %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	// 3. Connect to new peers, using the remote peer as a potential relay
	go pe.connectToNewPeers(theirPeers, remotePeer)
	return nil
}