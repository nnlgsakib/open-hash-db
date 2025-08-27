package protocols

import (
    "encoding/binary"
    "fmt"
)

// DelegationRequest is the request to fetch a block by its hash
type DelegationRequest struct {
    // Mode: 0 = raw block bytes, 1 = metadata bytes
    Mode uint64
    Hash []byte
}

// DelegationResponse contains either Data or an Error
type DelegationResponse struct {
    Hash  []byte
    Data  []byte
    Error string
}

// Encode encodes DelegationRequest with a simple wire format:
// [varint hashLen][hash]
func (r *DelegationRequest) Encode() []byte {
    hb := r.Hash
    lenBuf := make([]byte, binary.MaxVarintLen64*2)
    n1 := binary.PutUvarint(lenBuf, r.Mode)
    n2 := binary.PutUvarint(lenBuf[n1:], uint64(len(hb)))
    out := make([]byte, 0, n1+n2+len(hb))
    out = append(out, lenBuf[:n1]...)
    out = append(out, lenBuf[n1:n1+n2]...)
    out = append(out, hb...)
    return out
}

// DecodeDelegationRequest decodes bytes into DelegationRequest
func DecodeDelegationRequest(b []byte) (*DelegationRequest, error) {
    var off int
    mode, n := binary.Uvarint(b)
    if n <= 0 { return nil, fmt.Errorf("invalid varint mode") }
    off += n
    l, n2 := binary.Uvarint(b[off:])
    if n2 <= 0 { return nil, fmt.Errorf("invalid varint len") }
    off += n2
    if uint64(len(b[off:])) < l { return nil, fmt.Errorf("short buffer") }
    hb := make([]byte, int(l))
    copy(hb, b[off:off+int(l)])
    return &DelegationRequest{Mode: mode, Hash: hb}, nil
}

// Encode encodes DelegationResponse:
// [varint hashLen][hash][varint errLen][errBytes][varint dataLen][data]
func (r *DelegationResponse) Encode() []byte {
    // hash
    lenBuf := make([]byte, binary.MaxVarintLen64)
    n1 := binary.PutUvarint(lenBuf, uint64(len(r.Hash)))
    // error
    eb := []byte(r.Error)
    n2 := binary.PutUvarint(lenBuf[n1:], uint64(len(eb)))
    // data
    n3 := binary.PutUvarint(lenBuf[n1+n2:], uint64(len(r.Data)))
    out := make([]byte, 0, n1+n2+n3+len(r.Hash)+len(eb)+len(r.Data))
    out = append(out, lenBuf[:n1]...)
    out = append(out, r.Hash...)
    out = append(out, lenBuf[n1:n1+n2]...)
    out = append(out, eb...)
    out = append(out, lenBuf[n1+n2:n1+n2+n3]...)
    out = append(out, r.Data...)
    return out
}

// DecodeDelegationResponse decodes bytes into DelegationResponse
func DecodeDelegationResponse(b []byte) (*DelegationResponse, error) {
    var off int
    readVar := func() (uint64, error) {
        v, n := binary.Uvarint(b[off:])
        if n <= 0 { return 0, fmt.Errorf("invalid varint") }
        off += n
        return v, nil
    }
    // hash
    hl, err := readVar()
    if err != nil { return nil, err }
    if len(b[off:]) < int(hl) { return nil, fmt.Errorf("short hash") }
    hash := make([]byte, int(hl))
    copy(hash, b[off:off+int(hl)])
    off += int(hl)
    // error
    el, err := readVar()
    if err != nil { return nil, err }
    if len(b[off:]) < int(el) { return nil, fmt.Errorf("short error") }
    errStr := string(b[off:off+int(el)])
    off += int(el)
    // data
    dl, err := readVar()
    if err != nil { return nil, err }
    if len(b[off:]) < int(dl) { return nil, fmt.Errorf("short data") }
    data := make([]byte, int(dl))
    copy(data, b[off:off+int(dl)])
    return &DelegationResponse{Hash: hash, Error: errStr, Data: data}, nil
}
