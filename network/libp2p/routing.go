package libp2p

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"openhashdb/core/hasher"
	"openhashdb/protobuf/pb"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// Routing encapsulates the DHT and related functionalities.
type Routing struct {
	node *Node
	dht  *dht.IpfsDHT
}

// NewRouting creates a new routing service for the node.
func NewRouting(node *Node, dht *dht.IpfsDHT) *Routing {
	r := &Routing{
		node: node,
		dht:  dht,
	}

	// Start background tasks for DHT maintenance.
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-r.node.ctx.Done():
				return
			case <-ticker.C:
				if err := r.Bootstrap(); err != nil {
					log.Printf("[routing] Failed to bootstrap DHT: %v", err)
				}
			}
		}
	}()

	return r
}

// Bootstrap connects to the DHT bootstrap nodes.
func (r *Routing) Bootstrap() error {
	if r.dht == nil {
		return fmt.Errorf("[routing] DHT not initialized")
	}
	log.Printf("[routing] Bootstrapping DHT...")
	ctx, cancel := context.WithTimeout(r.node.ctx, 30*time.Second)
	defer cancel()
	return r.dht.Bootstrap(ctx)
}

// AnnounceContent announces content availability to the DHT.
func (r *Routing) AnnounceContent(contentHashStr string) error {
	if r.dht == nil {
		return fmt.Errorf("[routing] DHT not initialized")
	}

	hash, err := hasher.HashFromString(contentHashStr)
	if err != nil {
		return fmt.Errorf("[routing] invalid content hash: %w", err)
	}

	if r.node.blockstore == nil {
		log.Printf("[routing] Blockstore not configured for content %s", contentHashStr)
		return fmt.Errorf("[routing] blockstore not configured")
	}
	if !r.node.blockstore.HasContent(hash) {
		log.Printf("[routing] Validation failed for content %s: content not in blockstore", contentHashStr)
		return fmt.Errorf("[routing] content not in blockstore")
	}

	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(r.node.ctx, 30*time.Second)
		defer cancel()

		mh, err := multihash.Encode(hash[:], multihash.SHA2_256)
		if err != nil {
			log.Printf("[routing] Attempt %d/%d: Failed to create multihash for %s: %v", attempt, maxRetries, contentHashStr, err)
			if attempt == maxRetries {
				return fmt.Errorf("[routing] failed to create multihash: %w", err)
			}
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		contentCID := cid.NewCidV1(cid.Raw, mh)

		log.Printf("[routing] Attempt %d/%d: Announcing content provider for hash: %s (CID: %s)", attempt, maxRetries, contentHashStr, contentCID.String())
		if err := r.dht.Provide(ctx, contentCID, true); err != nil {
			log.Printf("[routing] Attempt %d/%d: Failed to announce content %s: %v", attempt, maxRetries, contentHashStr, err)
			if attempt == maxRetries {
				return fmt.Errorf("[routing] failed to announce content after %d attempts: %w", maxRetries, err)
			}
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
			continue
		}
		return nil
	}
	return fmt.Errorf("[routing] failed to announce content %s: max retries exceeded", contentHashStr)
}

// FindContentProviders finds content providers from the DHT, prioritizing direct connections.
func (r *Routing) FindContentProviders(contentHash string) ([]peer.AddrInfo, error) {
	if r.dht == nil {
		return nil, fmt.Errorf("[routing] DHT not initialized")
	}

	ctx, cancel := context.WithTimeout(r.node.ctx, 90*time.Second)
	defer cancel()

	hash, err := hasher.HashFromString(contentHash)
	if err != nil {
		return nil, fmt.Errorf("[routing] invalid content hash: %w", err)
	}

	mh, err := multihash.Encode(hash[:], multihash.SHA2_256)
	if err != nil {
		log.Printf("[routing] Failed to create multihash for %s: %v", contentHash, err)
		return nil, fmt.Errorf("[routing] failed to create multihash: %w", err)
	}

	contentCID := cid.NewCidV1(cid.Raw, mh)
	log.Printf("[routing] Finding providers for content hash: %s (CID: %s)", contentHash, contentCID.String())

	providersCh := r.dht.FindProvidersAsync(ctx, contentCID, 20)

	var directProviders []peer.AddrInfo
	var relayedProviders []peer.AddrInfo

	for p := range providersCh {
		if p.ID == r.node.ID() || len(p.Addrs) == 0 {
			continue
		}

		isRelayed := false
		for _, addr := range p.Addrs {
			if strings.Contains(addr.String(), "/p2p-circuit/") {
				isRelayed = true
				break
			}
		}

		if isRelayed {
			log.Printf("[routing] Found relayed provider: %s for hash %s", p.ID.String(), contentHash)
			relayedProviders = append(relayedProviders, p)
		} else {
			log.Printf("[routing] Found direct provider: %s for hash %s", p.ID.String(), contentHash)
			directProviders = append(directProviders, p)
		}
	}

	// Prioritize direct providers by putting them first in the list.
	allProviders := append(directProviders, relayedProviders...)

	if len(allProviders) == 0 {
		log.Printf("[routing] Found no providers for hash: %s", contentHash)
		// It's not an error to find no providers, so we return an empty slice.
		return []peer.AddrInfo{}, nil
	}

	log.Printf("[routing] Found %d total providers (%d direct, %d relayed) for hash: %s", len(allProviders), len(directProviders), len(relayedProviders), contentHash)
	return allProviders, nil
}


// GetStats returns DHT statistics.
func (r *Routing) GetStats() *pb.DHTStats {
	if r.dht == nil {
		return &pb.DHTStats{
			Enabled: false,
		}
	}
	routingTable := r.dht.RoutingTable()
	return &pb.DHTStats{
		Enabled:    true,
		PeerCount:  int32(routingTable.Size()),
	}
}

// Close closes the DHT.
func (r *Routing) Close() error {
	if r.dht != nil {
		return r.dht.Close()
	}
	return nil
}