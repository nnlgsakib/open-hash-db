package libp2p

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log"
	"sync"
    "time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
	"openhashdb/protobuf/pb"
)

// PeerExchanger handles exchanging peer lists.
type PeerExchanger struct {
    node         *Node
    ctx          context.Context
    connecting   map[peer.ID]bool // Keep track of in-flight connections
    connectingMu sync.Mutex
    // Backoff for failed targets to avoid repeatedly dialing offline peers
    failUntil    map[peer.ID]time.Time
    failCount    map[peer.ID]int
    failMu       sync.Mutex
}

// NewPeerExchanger creates a new PeerExchanger.
func NewPeerExchanger(ctx context.Context, node *Node) *PeerExchanger {
    return &PeerExchanger{
        node:       node,
        ctx:        ctx,
        connecting: make(map[peer.ID]bool),
        failUntil:  make(map[peer.ID]time.Time),
        failCount:  make(map[peer.ID]int),
    }
}

func addrInfoToProto(pi peer.AddrInfo) *pb.PeerInfo {
	addrs := make([]string, len(pi.Addrs))
	for i, addr := range pi.Addrs {
		addrs[i] = addr.String()
	}
	return &pb.PeerInfo{
		Id:    pi.ID.String(),
		Addrs: addrs,
	}
}

func protoToAddrInfo(pi *pb.PeerInfo) (peer.AddrInfo, error) {
	id, err := peer.Decode(pi.Id)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	addrs := make([]multiaddr.Multiaddr, len(pi.Addrs))
	for i, addrStr := range pi.Addrs {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return peer.AddrInfo{}, err
		}
		addrs[i] = addr
	}
	return peer.AddrInfo{
		ID:    id,
		Addrs: addrs,
	}, nil
}

// getPeerListProto gets the list of connected peers as a proto byte slice.
func (pe *PeerExchanger) getPeerListProto() ([]byte, error) {
    // Only share currently connected peers to avoid advertising offline nodes.
    peers := pe.node.Host().Network().Peers()
    self := pe.node.Host().ID()
    // Cap the list to avoid flooding
    const maxPeers = 64
    out := make([]*pb.PeerInfo, 0, maxPeers)
    for _, p := range peers {
        if p == self { continue }
        if pe.node.Host().Network().Connectedness(p) != network.Connected { continue }
        addrs := pe.node.Host().Peerstore().Addrs(p)
        if len(addrs) == 0 { continue }
        // Exclude relayed-only addresses if the connection is not direct and we don't have base addrs
        // but still include since we are connected (useful for NATed setups)
        pi := addrInfoToProto(pe.node.Host().Peerstore().PeerInfo(p))
        out = append(out, pi)
        if len(out) >= maxPeers { break }
    }
    return proto.Marshal(&pb.PeerInfoList{Peers: out})
}

// connectToNewPeers takes a list of AddrInfo, filters out known/current peers,
// and attempts to connect to the new ones, using the source as a relay if direct connection fails.
func (pe *PeerExchanger) connectToNewPeers(addrInfos []*pb.PeerInfo, sourcePeer peer.ID) {
    var wg sync.WaitGroup

    for _, pi := range addrInfos {
        addrInfo, err := protoToAddrInfo(pi)
        if err != nil {
            log.Printf("[PeerExchanger] Error converting proto to addr info: %v", err)
            continue
        }

        // Don't connect to self or already connected peers
        if pe.node.IsSelf(addrInfo) || pe.node.Host().Network().Connectedness(addrInfo.ID) == network.Connected {
            continue
        }

        // Backoff: skip if we've recently failed this target
        pe.failMu.Lock()
        if until, ok := pe.failUntil[addrInfo.ID]; ok && time.Now().Before(until) {
            pe.failMu.Unlock()
            continue
        }
        pe.failMu.Unlock()

		// Check if a connection is already in progress
		pe.connectingMu.Lock()
		if pe.connecting[addrInfo.ID] {
			pe.connectingMu.Unlock()
			continue
		}
        pe.connecting[addrInfo.ID] = true
        pe.connectingMu.Unlock()

        wg.Add(1)
        go func(pi peer.AddrInfo) {
            defer wg.Done()
            var connected bool

            // Always clear in-flight state at the end to allow future attempts
            defer func() {
                pe.connectingMu.Lock()
                delete(pe.connecting, pi.ID)
                pe.connectingMu.Unlock()
            }()

			if pe.node.Host().Network().Connectedness(pi.ID) == network.Connected {
				connected = true
				return
			}

			log.Printf("[PeerExchanger] Discovered new peer %s from %s", pi.ID, sourcePeer)

            if len(pi.Addrs) > 0 {
                log.Printf("[PeerExchanger] Attempting direct connection to %s with addrs: %v", pi.ID, pi.Addrs)
                err := pe.node.Host().Connect(pe.ctx, pi)
                if err == nil {
                    log.Printf("[PeerExchanger] Successfully connected directly to new peer %s", pi.ID)
                    connected = true
                    return
                }
                log.Printf("[PeerExchanger] Failed to connect directly to %s: %v. Trying via relay.", pi.ID, err)
            } else {
                log.Printf("[PeerExchanger] Peer %s has no public addresses, trying via relay.", pi.ID)
            }

        // Attempt relay dial via the source peer only if we are connected to it and it supports HOP
        if pe.node.Host().Network().Connectedness(sourcePeer) == network.Connected {
            protos, _ := pe.node.Host().Peerstore().GetProtocols(sourcePeer)
        supportsHop := false
        for _, pr := range protos {
            if pr == "/libp2p/circuit/relay/0.2.0/hop" {
                supportsHop = true
                break
            }
        }
        if supportsHop {
            // Use all known transport addrs of the source relay to build full relayed addrs
            relayBaseAddrs := pe.node.Host().Peerstore().Addrs(sourcePeer)
            circ, _ := multiaddr.NewMultiaddr("/p2p/" + sourcePeer.String() + "/p2p-circuit/p2p/" + pi.ID.String())
            for _, ra := range relayBaseAddrs {
                relayDial := ra.Encapsulate(circ)
                relayPeerInfo := peer.AddrInfo{ID: pi.ID, Addrs: []multiaddr.Multiaddr{relayDial}}
                log.Printf("[PeerExchanger] Attempting relay connection to %s via %s at %s", pi.ID, sourcePeer, relayDial)
                if err := pe.node.Host().Connect(pe.ctx, relayPeerInfo); err == nil {
                    log.Printf("[PeerExchanger] Successfully connected to %s via relay %s", pi.ID, sourcePeer)
                    connected = true
                    return
                }
            }
        }
        }

        // Fallback: try via our reserved relays
        pe.node.reservedRelaysMu.RLock()
        for rid, entry := range pe.node.reservedRelays {
            circ, _ := multiaddr.NewMultiaddr("/p2p/" + rid.String() + "/p2p-circuit/p2p/" + pi.ID.String())
            for _, ra := range entry.addrs {
                relayDial := ra.Encapsulate(circ)
                relayPeerInfo := peer.AddrInfo{ID: pi.ID, Addrs: []multiaddr.Multiaddr{relayDial}}
                log.Printf("[PeerExchanger] Attempting relay connection to %s via reserved relay %s at %s", pi.ID, rid, relayDial)
                if err := pe.node.Host().Connect(pe.ctx, relayPeerInfo); err == nil {
                    log.Printf("[PeerExchanger] Successfully connected to %s via reserved relay %s", pi.ID, rid)
                    pe.node.reservedRelaysMu.RUnlock()
                    connected = true
                    return
                }
            }
        }
        pe.node.reservedRelaysMu.RUnlock()

        // If still not connected, register a backoff for this target
        if !connected {
            pe.failMu.Lock()
            pe.failCount[pi.ID]++
            c := pe.failCount[pi.ID]
            if c > 6 {
                c = 6
            }
            // Exponential backoff: 30s * 2^(c-1), max ~32m
            pe.failUntil[pi.ID] = time.Now().Add(time.Duration(30*(1<<uint(c-1))) * time.Second)
            pe.failMu.Unlock()
        } else {
            pe.failMu.Lock()
            delete(pe.failCount, pi.ID)
            delete(pe.failUntil, pi.ID)
            pe.failMu.Unlock()
        }
    }(addrInfo)
    }
    wg.Wait()
}

// handleExchange handles the peer exchange on an incoming stream.
// It reads the peer list, connects to new peers, sends its own list back.
func (pe *PeerExchanger) handleExchange(stream network.Stream) {
	defer stream.Close()
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("[libp2p] Handling peer exchange with %s", remotePeer.String())

	// 1. Receive their peers
	reader := bufio.NewReader(stream)
	msgLen, err := binary.ReadUvarint(reader)
	if err != nil {
		log.Printf("[libp2p] Failed to read peer list length from stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	theirPeersProto := make([]byte, msgLen)
	_, err = io.ReadFull(reader, theirPeersProto)
	if err != nil {
		log.Printf("[libp2p] Failed to read peer list from stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	var theirPeers pb.PeerInfoList
	if err := proto.Unmarshal(theirPeersProto, &theirPeers); err != nil {
		log.Printf("[libp2p] Failed to unmarshal peer list from %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	// 2. Connect to new peers, using the remote peer as a potential relay
	go pe.connectToNewPeers(theirPeers.Peers, remotePeer)

	// 3. Send our peers
	ourPeersProto, err := pe.getPeerListProto()
	if err != nil {
		log.Printf("[libp2p] Failed to get our peer list for exchange with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	writer := bufio.NewWriter(stream)
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(len(ourPeersProto)))

	if _, err := writer.Write(lenBuf[:n]); err != nil {
		log.Printf("[libp2p] Failed to write our peer list length to stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}

	if _, err := writer.Write(ourPeersProto); err != nil {
		log.Printf("[libp2p] Failed to write our peer list to stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}
	if err := writer.Flush(); err != nil {
		log.Printf("[libp2p] Failed to flush stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return
	}
}

// initiateExchange initiates a peer exchange on an outgoing stream.
// It sends its own peer list, then reads the other's list and connects to new peers.
func (pe *PeerExchanger) initiateExchange(stream network.Stream) error {
	defer stream.Close()
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("[libp2p] Initiating peer exchange with %s", remotePeer.String())

	// 1. Send our peers
	ourPeersProto, err := pe.getPeerListProto()
	if err != nil {
		log.Printf("[libp2p] Failed to get our peer list for exchange with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	writer := bufio.NewWriter(stream)
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(len(ourPeersProto)))

	if _, err := writer.Write(lenBuf[:n]); err != nil {
		log.Printf("[libp2p] Failed to write our peer list length to stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	if _, err := writer.Write(ourPeersProto); err != nil {
		log.Printf("[libp2p] Failed to write our peer list to stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}
	if err := writer.Flush(); err != nil {
		log.Printf("[libp2p] Failed to flush stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	// 2. Receive their peers
	reader := bufio.NewReader(stream)
	msgLen, err := binary.ReadUvarint(reader)
	if err != nil {
		log.Printf("[libp2p] Failed to read peer list length from stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	theirPeersProto := make([]byte, msgLen)
	_, err = io.ReadFull(reader, theirPeersProto)
	if err != nil {
		log.Printf("[libp2p] Failed to read peer list from stream with %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	var theirPeers pb.PeerInfoList
	if err := proto.Unmarshal(theirPeersProto, &theirPeers); err != nil {
		log.Printf("[libp2p] Failed to unmarshal peer list from %s: %v", remotePeer.String(), err)
		stream.Reset()
		return err
	}

	// 3. Connect to new peers, using the remote peer as a potential relay
	go pe.connectToNewPeers(theirPeers.Peers, remotePeer)
	return nil
}
