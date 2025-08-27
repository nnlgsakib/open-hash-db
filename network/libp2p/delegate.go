package libp2p

import (
    "bufio"
    "context"
    "encoding/binary"
    "io"
    "log"
    "time"

    "openhashdb/core/hasher"
    "openhashdb/network/protocols"
    "google.golang.org/protobuf/proto"

    "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/libp2p/go-libp2p/core/protocol"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var ProtocolDelegate = protocol.ID(protocols.DelegateProtocolID)

var (
    delegationRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
        Name: "openhashdb_delegate_requests_total",
        Help: "Total delegation requests served by this node",
    })
    delegationRequestErrors = promauto.NewCounter(prometheus.CounterOpts{
        Name: "openhashdb_delegate_request_errors_total",
        Help: "Total failed delegation requests served by this node",
    })
)

// handleDelegateStream serves delegation requests: fetch requested block via local bitswap and return bytes
func (n *Node) handleDelegateStream(s network.Stream) {
    defer s.Close()
    reader := bufio.NewReader(s)

    msgLen, err := binary.ReadUvarint(reader)
    if err != nil {
        if err != io.EOF && err != network.ErrReset {
            log.Printf("[delegate] Failed to read message length from %s: %v", s.Conn().RemotePeer(), err)
        }
        return
    }
    buf := make([]byte, msgLen)
    if _, err := io.ReadFull(reader, buf); err != nil {
        log.Printf("[delegate] Failed to read request from %s: %v", s.Conn().RemotePeer(), err)
        return
    }
    req, err := protocols.DecodeDelegationRequest(buf)
    if err != nil {
        log.Printf("[delegate] Failed to decode request from %s: %v", s.Conn().RemotePeer(), err)
        return
    }

    var resp protocols.DelegationResponse
    resp.Hash = req.Hash
    delegationRequestsTotal.Inc()
    h, err := hasher.HashFromBytes(req.Hash)
    if err != nil {
        resp.Error = "invalid hash"
        n.writeDelegateResponse(s, &resp)
        return
    }
    // Mode 1 = metadata
    if req.Mode == 1 {
        if n.blockstore == nil || !n.blockstore.HasContent(h) {
            resp.Error = "metadata not found"
            delegationRequestErrors.Inc()
            n.writeDelegateResponse(s, &resp)
            return
        }
        meta, err := n.blockstore.GetContent(h)
        if err != nil {
            resp.Error = err.Error()
            delegationRequestErrors.Inc()
            n.writeDelegateResponse(s, &resp)
            return
        }
        // marshal using existing pb definitions
        data, err := proto.Marshal(meta)
        if err != nil {
            resp.Error = err.Error()
            delegationRequestErrors.Inc()
            n.writeDelegateResponse(s, &resp)
            return
        }
        resp.Data = data
        n.writeDelegateResponse(s, &resp)
        return
    }
    // Default mode 0: raw block
    if n.bitswap == nil {
        resp.Error = "bitswap not available"
        n.writeDelegateResponse(s, &resp)
        return
    }
    ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
    defer cancel()
    blk, err := n.bitswap.GetBlock(ctx, h)
    if err != nil {
        resp.Error = err.Error()
        delegationRequestErrors.Inc()
        n.writeDelegateResponse(s, &resp)
        return
    }
    resp.Data = blk.RawData()
    n.writeDelegateResponse(s, &resp)
}

func (n *Node) writeDelegateResponse(s network.Stream, resp *protocols.DelegationResponse) {
    data := resp.Encode()
    writer := bufio.NewWriter(s)
    lenBuf := make([]byte, binary.MaxVarintLen64)
    nbytes := binary.PutUvarint(lenBuf, uint64(len(data)))
    var err error
    if _, err = writer.Write(lenBuf[:nbytes]); err == nil {
        _, err = writer.Write(data)
    }
    if err == nil {
        _ = writer.Flush()
    }
}

// RequestDelegatedBlock asks a direct peer to fetch a block on our behalf and return bytes
func (n *Node) RequestDelegatedBlock(ctx context.Context, to peer.ID, h hasher.Hash) ([]byte, error) {
    req := &protocols.DelegationRequest{Mode: 0, Hash: h[:]}
    payload := req.Encode()
    stream, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "delegate"), to, ProtocolDelegate)
    if err != nil { return nil, err }
    defer stream.Close()
    writer := bufio.NewWriter(stream)
    lenBuf := make([]byte, binary.MaxVarintLen64)
    nbytes := binary.PutUvarint(lenBuf, uint64(len(payload)))
    if _, err := writer.Write(lenBuf[:nbytes]); err == nil {
        _, err = writer.Write(payload)
    }
    if err == nil {
        if err = writer.Flush(); err != nil { return nil, err }
    } else { return nil, err }

    // Read response
    reader := bufio.NewReader(stream)
    msgLen, err := binary.ReadUvarint(reader)
    if err != nil { return nil, err }
    buf := make([]byte, msgLen)
    if _, err := io.ReadFull(reader, buf); err != nil { return nil, err }
    resp, err := protocols.DecodeDelegationResponse(buf)
    if err != nil { return nil, err }
    if resp.Error != "" { return nil, io.ErrUnexpectedEOF }
    return resp.Data, nil
}

// RequestDelegatedMetadata attempts to obtain metadata bytes for a content root from a peer
func (n *Node) RequestDelegatedMetadata(ctx context.Context, to peer.ID, h hasher.Hash) ([]byte, error) {
    req := &protocols.DelegationRequest{Mode: 1, Hash: h[:]}
    payload := req.Encode()
    stream, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "delegate"), to, ProtocolDelegate)
    if err != nil { return nil, err }
    defer stream.Close()
    writer := bufio.NewWriter(stream)
    lenBuf := make([]byte, binary.MaxVarintLen64)
    nbytes := binary.PutUvarint(lenBuf, uint64(len(payload)))
    if _, err := writer.Write(lenBuf[:nbytes]); err == nil {
        _, err = writer.Write(payload)
    }
    if err == nil {
        if err = writer.Flush(); err != nil { return nil, err }
    } else { return nil, err }
    reader := bufio.NewReader(stream)
    msgLen, err := binary.ReadUvarint(reader)
    if err != nil { return nil, err }
    buf := make([]byte, msgLen)
    if _, err := io.ReadFull(reader, buf); err != nil { return nil, err }
    resp, err := protocols.DecodeDelegationResponse(buf)
    if err != nil { return nil, err }
    if resp.Error != "" { return nil, io.ErrUnexpectedEOF }
    return resp.Data, nil
}
