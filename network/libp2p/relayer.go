package libp2p

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	relayv2client "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	circuit "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

// Relayer manages relay functionalities, like reserving slots.
type Relayer struct {
	host host.Host
}

// NewRelayer creates a new Relayer service and enables relay capabilities on the host.
func NewRelayer(h host.Host) (*Relayer, error) {
	// Enable the relay service on the host
	if _, err := circuit.New(h); err != nil {
		return nil, fmt.Errorf("failed to create circuit relay: %w", err)
	}
	return &Relayer{host: h}, nil
}

// ReserveSlot attempts to reserve a slot with a given peer.
func (r *Relayer) ReserveSlot(ctx context.Context, p peer.AddrInfo) {
	log.Printf("[relayer] Attempting to reserve slot with %s", p.ID)
	_, err := relayv2client.Reserve(ctx, r.host, p)
	if err != nil {
		log.Printf("[relayer] Failed to reserve slot with %s: %v", p.ID, err)
	} else {
		log.Printf("[relayer] Successfully reserved slot with %s.", p.ID)
	}
}

// DiscoverAndReserve checks if a peer is a relay and attempts to reserve a slot.
// This is typically called when a new peer connects.
func (r *Relayer) DiscoverAndReserve(p peer.ID) {
	go func() {
		// Use a background context because this is an async operation triggered by an event.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		protocols, err := r.host.Peerstore().GetProtocols(p)
		if err != nil {
			log.Printf("[relayer] Could not get protocols for peer %s: %v", p, err)
			return
		}

		hasRelay := false
		for _, proto := range protocols {
			if proto == "/libp2p/circuit/relay/0.2.0/hop" {
				hasRelay = true
				break
			}
		}

		if !hasRelay {
			return // Not a relay
		}

		log.Printf("[relayer] Peer %s is a relay, attempting to reserve a slot.", p)
		pinfo := r.host.Peerstore().PeerInfo(p)
		r.ReserveSlot(ctx, pinfo)
	}()
}
