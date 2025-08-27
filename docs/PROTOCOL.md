OpenHash Protocol Redesign (IPFS-Aligned)

Overview

- Goal: Align OpenHash’s architecture and protocols with IPFS’s model for content addressing, exchange, and routing while preserving existing features. This includes adopting CIDv1, multihash, multicodec, IPLD DAGs (dag-pb and dag-cbor), Bitswap-like block exchange, optional GraphSync for traversals, and libp2p Kademlia DHT provider records.

Design Pillars

- Content Addressing: Use CIDv1 with multihash (sha2-256 default) and multicodec to identify blocks and DAG nodes. Raw blocks use codec `0x55`; dag-pb `0x70`; dag-cbor `0x71`.
- IPLD DAG: Represent files and directories using DAG-PB for compatibility; support DAG-CBOR for metadata and richer schemas. Leaves are chunked data blocks (Raw), linked by intermediate DAG-PB nodes.
- Block Exchange: Bitswap v1.2–compatible behavior with wantlist, want-have/want-block, block presence, and per-peer ledgers. Maintain priority and periodic broadcast; respond with presences to reduce bandwidth.
- Graph Traversal: Provide GraphSync-like request/response for efficient DAG traversal using IPLD selectors. Keep it optional; Bitswap remains the default data plane for blocks.
- Routing: libp2p Kademlia DHT provider records (Provide/FindProviders). Announce providers for root CIDs and optionally for popular chunks. Leverage peer classification (direct vs relayed) and relay reservations.
- Resilience: Torrent-inspired strategies: rarest-first chunk requests, multiple providers, parallelism with backoff, anti-leech policies, and tit-for-tat heuristics via peer ledgers.

Identifiers

- CID: CIDv1. Default multibase: base32 when stringified. Default multihash: sha2-256 (code 0x12, length 32). Default codecs:
  - Raw: 0x55 for leaf chunks
  - dag-pb: 0x70 for UnixFS-like trees
  - dag-cbor: 0x71 for metadata

Block Model

- Raw Block: opaque bytes addressed by the CID (mh=sha2-256 of raw data, codec=raw).
- DAG Node: IPLD-encoded node with links to children blocks by CID. Directory/file layout follows UnixFS v1 (dag-pb) initially. Metadata can be dag-cbor.

Network Protocols

- Bitswap: `/openhash/bitswap/1.2.0`
  - Wantlist: entries include CID multihash and WantType (Have|Block).
  - Block Presence: Have|DontHave.
  - Ledger: Track bytes sent/received; throttle/priority policies.
  - Provider hints: piggyback or out-of-band via DHT and local registry.

- GraphSync (Optional): `/openhash/graphsync/1.0.0`
  - Request: id, root CID, selector (IPLD selector bytes), extensions.
  - Response: id, blocks (link CID + bytes), metadata updates, errors, and completion status.
  - Transport: libp2p stream with length-prefixed protobuf messages.

- Gossip: `/openhash/gossip/1.0.0` retained for lightweight announcements.
- Delegation: `/openhash/delegate/1.0.0` retained for assisted fetching (fallback path).

Routing

- DHT: Use libp2p Kademlia. Provide root CIDs; optionally provide intermediate nodes based on policy. Use FindProviders during fetching, merge with learned provider registry. Respect connection quality; prefer direct paths.

REST API Shape

- Canonical identifiers are CIDs. For backward compatibility, support hex SHA-256 paths (`/content/{hash}`) by mapping to CIDv1 Raw codec.
- New fields: wherever `hash` appears, include `cid` string (base32) in responses. Accept `cid` param in requests.

Migration & Compatibility

- Phase 1: Dual support for `hasher.Hash` and `cid.Cid`. Introduce `core/cidutil` helpers. Protobufs gain CID-capable fields without removing existing ones.
- Phase 2: Switch internal storage/index keys to CID string; keep compatibility readers for hex hash lookups.
- Phase 3: Enable GraphSync for traversals; Bitswap continues handling block transport.
- Phase 4: Tighten policies: rarest-first and tit-for-tat, enforce presence caches, and smarter peer selection.

Security & Integrity

- All data identified by CID; verification by multihash. Reject mismatched blocks. Optional signed provider records in future phases.

Performance Considerations

- Parallel downloads with adaptive concurrency. Prefer direct peers; limited relayed probes. Small fanout on relays to avoid overload. Cache presences to reduce chatter. Opportunistic delegated fetch only as fallback.

Wire Formats

- Protobuf for control planes (Bitswap, GraphSync). Blocks are raw bytes. Length-prefixed framing per message on libp2p streams.

Next Steps (Implementation)

- Add `core/cidutil` and CID-backed helpers.
- Extend protobuf messages for CID and GraphSync.
- Register GraphSync protocol and stub engine.
- Add REST surfaces to consume/emit CIDs alongside hashes.
- Introduce provider advertisement/pinning routines keyed by CID.

