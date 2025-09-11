# OpenHashDB

OpenHashDB is a blazing‑fast, content‑addressable, distributed database with a friendly CLI, a REST API, and an embedded web UI. It combines efficient chunking, verifiable Merkle structures, optional erasure coding, and libp2p networking with a custom Bitswap implementation for peer‑to‑peer distribution.

## Highlights

- Content addressing: SHA‑256 hashes for chunks and objects, integrity by default.
- Fast chunking: FastCDC with tunable sizes; balanced for throughput and dedupe.
- Verifiable trees: Ternary Mesh Tree (BLAKE3) roots for files/dirs and proofs.
- Optional erasure coding: Reed‑Solomon (data/parity shards) for resilience.
- Local storage: Sharded on‑disk blocks + LevelDB metadata (efficient scans).
- P2P networking: libp2p (TCP/QUIC), mDNS discovery, DHT content announcements.
- Bitswap: Lightweight engine with wantlist/presence, sessions, and backoff.
- Replication: Pinning, background fetch, DHT announces, periodic GC of blocks.
- Interfaces: CLI, REST API endpoints, and an embedded single‑page web UI.

## Repository Structure

- `cmd/oh`: CLI entrypoint (`openhash`) and commands.
- `api/rest`: REST server, routes, streaming, range requests, uploads, UI.
- `core`: Blocks, blockstore (LevelDB + sharded files), chunker, hasher, TMT tree, sharder (RS), utils.
- `network`: libp2p node, routing/DHT, relayer, heartbeat, Bitswap engine, replicator, stream manager.
- `protobuf`: `.proto` schemas and generated code for API, core, bitswap, network, relay.
- `openhashdb-ui`: Embedded web UI served by the REST server (built artifacts in `build/`).
- `version`: Build‑time metadata (Version, Author, Branch via `-ldflags`).

## Requirements

- Go 1.23+ (module targets `go 1.23.8`).
- Git (for building with version metadata via Makefile).

## Build

- Makefile (recommended, sets version metadata):
  - `make build` → outputs `bin/openhash` (or `bin/openhash.exe` on Windows)

- Direct Go build:
  - `go build -o bin/openhash ./cmd/oh`
  - Optional ldflags for versioning:
    - `go build -o bin/openhash -ldflags="-X 'openhashdb/version.Version=vX.Y.Z' -X 'openhashdb/version.Author=Your Name' -X 'openhashdb/version.Branch=main'" ./cmd/oh`

Verify:

```bash
bin/openhash --help
bin/openhash version
```

## Storage Layout

- Default database path: `./.openhashdb` (override via `--db`).
- Blocks: sharded under `<db>/shards/<hh>/<hh>/<hash>` (fast directory fan‑out).
- Metadata: LevelDB at `<db>/leveldb` with key prefix `content:<root-hash>`.

## CLI

- Global flags:
  - `--db string` (default `./.openhashdb`): database root path
  - `--key-path string` (default `<db>/peer.key`): libp2p private key path
  - `--api-port int` (default `8080`): REST port
  - `--p2p-port int` (default `0` → random): P2P listen port
  - `--bootnode string` (comma‑separated multiaddrs): DHT/bootstrap peers
  - `--api string` (e.g., `http://host:port`): use REST API instead of local DB
  - `--verbose`: verbose logs

- Commands:
  - `add [file|folder]`: add a file or directory
  - `get <hash>`: retrieve and print info (local or network)
  - `view <hash>`: detailed metadata view
  - `list`: list all stored content
  - `daemon`: start P2P node and REST server (with embedded UI)
  - `version`: build metadata

Examples:

```bash
# Local direct mode (no daemon required)
bin/openhash add ./README.md
bin/openhash list
bin/openhash view <root-hash>

# Start daemon with REST API and P2P
bin/openhash daemon --api-port 8080 --p2p-port 4001

# Use API mode from another terminal/machine
bin/openhash add ./somefile --api http://localhost:8080
bin/openhash list --api http://localhost:8080
```

Behavior notes:

- Direct mode writes blocks/metadata locally.
- API mode sends operations to the REST server; the daemon handles storage/networking.

## REST API

Base URL: `http://<host>:<port>` (default `http://localhost:8080`). The UI is served at `/`.

- Health: `GET /health`
- Upload:
  - `POST /upload/file` (multipart `file=@...`; optional `?ec=true` to use erasure coding)
  - `POST /upload/folder` (multipart with many `files`, keeps folder structure)
- Download/Inline:
  - `GET /download/{hash}` (supports `Range` requests; sets `Content-Disposition: attachment`)
  - `GET /view/{hash}` (renders inline if MIME is renderable; directory listing otherwise)
- Metadata:
  - `GET /info/{hash}` → full metadata; if missing locally, attempts network fetch and starts background replication
  - `GET /list` → list all content metadata
  - `GET /stats` → storage and replication stats
  - `GET /network` → peer ID, addresses, connected peers, DHT info, recent peer events
- Pinning:
  - `POST /pin/{hash}` → pin content (kept during GC; triggers fetch if missing)
  - `DELETE /unpin/{hash}` → unpin content
  - `GET /pins` → list pinned hashes

Response shapes align with `protobuf/api.proto`; see `API.md` for detailed schemas and examples.

## Networking

- libp2p transports: TCP and QUIC; Noise security; hole punching enabled.
- Discovery: mDNS on LAN and Kademlia DHT (server mode) with optional bootnodes.
- DHT announcements: content roots are announced; peers can discover providers.
- Bitswap: custom protocol `/openhashdb/bitswap/1.2.0` with wantlist/presence and concurrent fetch.
- Relaying/Reservations: relayer support for NAT traversal (see `network/libp2p/relayer.go`).

Bootnodes:

```go
// network/libp2p/bootnodes.go
var DefaultBootnodes = []string{
    // add multiaddrs here, e.g.: 
    // "/ip4/1.2.3.4/tcp/4001/p2p/12D3K...",
}
```

Provide bootnodes via `--bootnode` (comma‑separated) or bake them into the default list.

## Data Model & Verification

- Files: chunked via FastCDC; each chunk hashed with SHA‑256.
- Trees: Ternary Mesh Tree (TMT) built over chunk hashes using BLAKE3; the TMT root is the content root hash stored in metadata and used for addressing.
- Directories: stored as links (name, hash, size, type) and a directory metadata object rooted by its own TMT hash; directory view renders listings and deep links.
- Integrity: chunk verification (hash), tree verification (root), and optional proof generation (`core/tmt`).
- Erasure coding: when `?ec=true`, files are split into data/parity shards; metadata records shard hashes and shard counts; streaming reconstructs on the fly if enough shards are retrievable.

## Replication, Pinning, and GC

- Pins are in‑memory (in the daemon) and keep content reachable; pinned content is periodically re‑announced.
- Background fetch: when metadata is discovered via network, chunks are fetched in background to satisfy the replication factor.
- Default replication factor: 3 (see `replicator.DefaultReplicationFactor`).
- GC: runs periodically; sweeps unreferenced blocks by walking from pinned roots (`blockstore.GC`).

## Embedded Web UI

- The REST server serves an embedded UI (from `openhashdb-ui/build`) at `/`.
- Use it to upload files/folders, browse content, and view metadata.

## Development

- Formatting/testing:
  - `go test ./...`
- Protobufs:
  - Schemas in `protobuf/*.proto`; generated code in `protobuf/pb/`.
- Versioning:
  - `version.Version`, `version.Author`, `version.Branch` are set at build time (Makefile `-ldflags`).

## Notes & Limitations

- Default bootnodes list is empty; for WAN discovery, provide bootnodes via `--bootnode`.
- CLI `add` via API mode currently uploads without toggling erasure coding; use the REST endpoint directly with `?ec=true` to enable.
- `get` in direct mode will attempt Bitswap for a single block if not available locally; full metadata‑driven fetch is handled by the daemon via REST endpoints and the replicator.

## Additional Docs

- `INSTALL.md`: OS‑level setup, service deployment, and operational guidance.
- `API.md`: Detailed REST endpoint specs and example requests/responses.

