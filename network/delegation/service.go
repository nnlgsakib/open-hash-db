package delegation

import (
    "bufio"
    "context"
    "encoding/binary"
    "io"
    "log"
    "time"

    "openhashdb/core/block"
    "openhashdb/core/hasher"

    "github.com/google/uuid"
    "github.com/libp2p/go-libp2p/core/host"
    "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/libp2p/go-libp2p/core/protocol"
    "google.golang.org/protobuf/proto"
    "openhashdb/protobuf/pb"
)

const ProtocolDelegation = protocol.ID("/openhashdb/delegation/1.0.0")

type Service struct {
    ctx     context.Context
    host    host.Host
    fetcher BlockFetcher
}

// BlockFetcher is the minimal interface required from a content exchange engine.
type BlockFetcher interface {
    GetBlock(ctx context.Context, h hasher.Hash) (block.Block, error)
}

func NewService(ctx context.Context, h host.Host, fetcher BlockFetcher) *Service {
    s := &Service{ctx: ctx, host: h, fetcher: fetcher}
    h.SetStreamHandler(ProtocolDelegation, s.handleDelegationStream)
    return s
}

func (s *Service) handleDelegationStream(stream network.Stream) {
    defer stream.Close()
    peerID := stream.Conn().RemotePeer()
    reader := bufio.NewReader(stream)
    msgLen, err := binary.ReadUvarint(reader)
    if err != nil {
        log.Printf("[Delegation] Failed reading length from %s: %v", peerID, err)
        stream.Reset()
        return
    }
    buf := make([]byte, msgLen)
    if _, err := io.ReadFull(reader, buf); err != nil {
        log.Printf("[Delegation] Failed reading body from %s: %v", peerID, err)
        stream.Reset()
        return
    }
    var req pb.DelegatedFetchRequest
    if err := proto.Unmarshal(buf, &req); err != nil {
        log.Printf("[Delegation] Bad protobuf from %s: %v", peerID, err)
        stream.Reset()
        return
    }
    log.Printf("[Delegation] Received delegated fetch request %s for %d hashes from %s", req.GetRequestId(), len(req.GetHashes()), peerID)

    // Ack immediately
    resp := pb.DelegatedFetchResponse{RequestId: req.GetRequestId(), Accepted: true}
    if err := writeProto(stream, &resp); err != nil {
        log.Printf("[Delegation] Failed writing ack to %s: %v", peerID, err)
        return
    }

    // Start background fetch; the engine's normal wantlist will deliver data back to requester
    go func() {
        for _, hs := range req.GetHashes() {
            h, err := hasher.HashFromString(hs)
            if err != nil {
                continue
            }
            ctx, cancel := context.WithTimeout(s.ctx, 3*time.Minute)
            _, _ = s.fetcher.GetBlock(ctx, h)
            cancel()
        }
    }()
}

// RequestDelegatedFetchToPeers sends a delegation request to the given peers in parallel.
func (s *Service) RequestDelegatedFetchToPeers(peers []peer.ID, hashes []string) {
    if len(peers) == 0 || len(hashes) == 0 {
        return
    }
    req := pb.DelegatedFetchRequest{RequestId: uuid.New().String(), Hashes: hashes}
    data, _ := proto.Marshal(&req)
    for _, p := range peers {
        go func(pid peer.ID) {
            ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
            defer cancel()
            stream, err := s.host.NewStream(network.WithAllowLimitedConn(ctx, "delegation"), pid, ProtocolDelegation)
            if err != nil {
                return
            }
            defer stream.Close()
            if err := writeSized(stream, data); err != nil {
                return
            }
            // Read ack (optional)
            _ = stream.SetReadDeadline(time.Now().Add(10 * time.Second))
            if ack, err := readSized(stream); err == nil {
                var resp pb.DelegatedFetchResponse
                _ = proto.Unmarshal(ack, &resp)
            }
        }(p)
    }
}

func writeSized(w io.Writer, data []byte) error {
    lenBuf := make([]byte, binary.MaxVarintLen64)
    n := binary.PutUvarint(lenBuf, uint64(len(data)))
    if _, err := w.Write(lenBuf[:n]); err != nil {
        return err
    }
    _, err := w.Write(data)
    return err
}

func readSized(r io.Reader) ([]byte, error) {
    br := bufio.NewReader(r)
    n, err := binary.ReadUvarint(br)
    if err != nil {
        return nil, err
    }
    buf := make([]byte, n)
    _, err = io.ReadFull(br, buf)
    return buf, err
}

func writeProto(w io.Writer, m proto.Message) error {
    b, err := proto.Marshal(m)
    if err != nil { return err }
    return writeSized(w, b)
}
