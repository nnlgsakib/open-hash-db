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
	peers := pe.node.host.Peerstore().Peers()
	var addrInfos []peer.AddrInfo
	for _, p := range peers {
		if p == pe.node.host.ID() {
			continue
		}
		// We only want to share peers we are connected to.
		if pe.node.host.Network().Connectedness(p) == network.Connected {
			addrInfos = append(addrInfos, pe.node.host.Peerstore().PeerInfo(p))
		}
	}
	return json.Marshal(addrInfos)
}

// connectToNewPeers takes a list of AddrInfo, filters out known/current peers,
// and attempts to connect to the new ones, using the source as a relay if direct connection fails.
func (pe *PeerExchanger) connectToNewPeers(addrInfos []peer.AddrInfo, sourcePeer peer.ID) {
	var wg sync.WaitGroup
	processed := make(map[peer.ID]bool)

	for _, pi := range addrInfos {
		// Don't connect to self
		if pi.ID == pe.node.host.ID() {
			continue
		}

		// Check if we have already processed this peer in this run
		if processed[pi.ID] {
			continue
		}

		// Check if we are already connected
		if pe.node.host.Network().Connectedness(pi.ID) == network.Connected {
			processed[pi.ID] = true
			continue
		}

		// Check if we are already trying to connect to avoid race conditions
		pe.connectingMu.Lock()
		if pe.connecting[pi.ID] {
			pe.connectingMu.Unlock()
			continue
		}
		pe.connecting[pi.ID] = true
		pe.connectingMu.Unlock()

		processed[pi.ID] = true
		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()
			defer func() {
				pe.connectingMu.Lock()
				delete(pe.connecting, pi.ID)
				pe.connectingMu.Unlock()
			}()

			// Re-check connectedness inside the goroutine to be safe
			if pe.node.host.Network().Connectedness(pi.ID) == network.Connected {
				return
			}

			log.Printf("Discovered new peer %s from %s, attempting to connect", pi.ID.String(), sourcePeer.String())

			// Attempt direct connection first
			if err := pe.node.host.Connect(pe.ctx, pi); err == nil {
				log.Printf("Successfully connected directly to new peer %s", pi.ID.String())
				return
			} else {
				log.Printf("Failed to connect directly to new peer %s: %v. Trying via relay.", pi.ID.String(), err)
			}

			// After a failed direct attempt, it's possible a connection was established concurrently.
			if pe.node.host.Network().Connectedness(pi.ID) == network.Connected {
				log.Printf("Already connected to %s, aborting relay attempt.", pi.ID.String())
				return
			}

			// If direct connection fails, try connecting via the source peer as a relay
			log.Printf("Attempting to connect to %s via relay %s", pi.ID.String(), sourcePeer.String())
			if err := pe.node.ConnectViaRelay(pe.ctx, pi.ID, sourcePeer); err != nil {
				log.Printf("Failed to connect to new peer %s via relay %s: %v", pi.ID.String(), sourcePeer.String(), err)
			} else {
				log.Printf("Successfully connected to new peer %s via relay %s", pi.ID.String(), sourcePeer.String())
			}
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
