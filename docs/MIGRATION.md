Migration Plan to IPFS-Aligned Protocols

Phases

1) Dual-ID Support (Current)
- Introduce `core/cidutil` for conversion between `hasher.Hash` and `cid.Cid`.
- Keep all APIs working with hex SHA-256 while adding `cid` fields in responses.
- Extend protobufs without removing existing fields.

2) CID-First Internals
- Store blocks and metadata keyed by CID string.
- Maintain read-path compatibility for hex-hash via conversion to CIDv1 Raw codec.

3) GraphSync Enablement
- Register `/openhashdb/graphsync/1.0.0` and implement request/response with selectors.
- Use GraphSync for directory/DAG traversals; leave block transfer to Bitswap.

4) Policy & Performance
- Implement rarest-first piece selection and provider scoring (latency, reliability, relay cost).
- Enforce presence caching in Bitswap; throttle peers with poor reciprocity.

REST Compatibility

- Inputs: accept `?cid=` and `/content/{cid}` paths. If `hash` is provided, convert to CID (Raw) behind the scenes.
- Outputs: include both `hash` and `cid` for a deprecation window. Announce timeline to remove plain hash-only paths.

Operational Notes

- DHT Provide/FindProviders for CIDs only. If hex hash appears, derive corresponding CID and advertise that CID.
- Pinning and GC should operate on CID roots. Backwards pins translated to CIDs internally.

