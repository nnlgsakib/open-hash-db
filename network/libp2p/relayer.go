package libp2p

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"

    "openhashdb/protobuf/pb"

    ggio "github.com/gogo/protobuf/io"
    "github.com/libp2p/go-libp2p/core/host"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

const (
	RelayV2Hop                 = "/libp2p/circuit/relay/0.2.0/hop"
	ReservationRefreshInterval = 1 * time.Minute
	ReservationExpiryMargin    = 15 * time.Minute
)

// ReservationInfo holds information about a relay reservation.
type ReservationInfo struct {
	Relay      peer.ID
	Expiration time.Time
	Voucher    []byte
}

// Relayer manages relay functionalities, like reserving slots and refreshing them.
type Relayer struct {
	host           host.Host
	ctx            context.Context
	reservations   map[peer.ID]*ReservationInfo
	reservationsMu sync.Mutex
}

// NewRelayer creates a new Relayer service and enables relay capabilities on the host.
func NewRelayer(ctx context.Context, h host.Host) (*Relayer, error) {
	// Enable the relay service on the host, allowing it to function as a relay.
	if _, err := relay.New(h); err != nil {
		return nil, fmt.Errorf("failed to create circuit relay: %w", err)
	}
	r := &Relayer{
		host:         h,
		ctx:          ctx,
		reservations: make(map[peer.ID]*ReservationInfo),
	}
	go r.monitorReservations()
	return r, nil
}

// ReserveSlot attempts to reserve a slot with a given relay peer.
func (r *Relayer) ReserveSlot(ctx context.Context, p peer.AddrInfo) {
	log.Printf("[relayer] Attempting to reserve slot with %s", p.ID)

	stream, err := r.host.NewStream(ctx, p.ID, RelayV2Hop)
	if err != nil {
		log.Printf("[relayer] failed to open stream to relay %s: %v", p.ID, err)
		return
	}
	defer stream.Close()

	w := ggio.NewFullWriter(stream)
	var msg pb.HopMessage
	msg.Type = pb.HopMessage_RESERVE

	if err := w.WriteMsg(&msg); err != nil {
		log.Printf("[relayer] failed to send reservation request to %s: %v", p.ID, err)
		return
	}

	// Read response
	rdr := ggio.NewFullReader(stream, 2048)
	msg.Reset()
	if err := rdr.ReadMsg(&msg); err != nil {
		log.Printf("[relayer] failed to read reservation response from %s: %v", p.ID, err)
		return
	}

	if msg.Type != pb.HopMessage_STATUS {
		log.Printf("[relayer] Unexpected response from %s: expected STATUS, got %s", p.ID, msg.Type.String())
		return
	}

	if msg.Status != pb.Status_OK {
		log.Printf("[relayer] Failed to reserve slot with %s: status code %d", p.ID, msg.Status)
		return
	}

	log.Printf("[relayer] Successfully reserved slot with %s.", p.ID)
    if msg.Reservation != nil {
        r.reservationsMu.Lock()
        r.reservations[p.ID] = &ReservationInfo{
            Relay:      p.ID,
            Expiration: time.Unix(int64(msg.Reservation.Expire), 0),
            Voucher:    msg.Reservation.Voucher,
        }
        r.reservationsMu.Unlock()
        log.Printf("[relayer] Reservation with %s expires at %s", p.ID, time.Unix(int64(msg.Reservation.Expire), 0))
        // AutoRelay will advertise relayed addresses via Identify. No manual
        // mutation of self addresses here.
    }
}

// DiscoverAndReserve checks if a peer is a relay and attempts to reserve a slot.
func (r *Relayer) DiscoverAndReserve(p peer.ID) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		protocols, err := r.host.Peerstore().GetProtocols(p)
		if err != nil {
			log.Printf("[relayer] Could not get protocols for peer %s: %v", p, err)
			return
		}

		hasRelay := false
		for _, proto := range protocols {
			if proto == RelayV2Hop {
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

// monitorReservations periodically checks for reservations that are about to expire and refreshes them.
func (r *Relayer) monitorReservations() {
	ticker := time.NewTicker(ReservationRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.refreshSoonToExpireReservations()
		}
	}
}

func (r *Relayer) refreshSoonToExpireReservations() {
	r.reservationsMu.Lock()
	var toRefresh []peer.ID
	for pid, res := range r.reservations {
		if time.Until(res.Expiration) < ReservationExpiryMargin {
			toRefresh = append(toRefresh, pid)
		}
	}
	r.reservationsMu.Unlock()

	for _, pid := range toRefresh {
		log.Printf("[relayer] Reservation with %s is expiring soon, refreshing...", pid)
		pinfo := r.host.Peerstore().PeerInfo(pid)
		if len(pinfo.Addrs) == 0 {
			log.Printf("[relayer] No addresses for relay peer %s, cannot refresh reservation", pid)
			continue
		}
		// Use a background context for the async refresh operation.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		go func() {
			defer cancel()
			r.ReserveSlot(ctx, pinfo)
		}()
	}
}
