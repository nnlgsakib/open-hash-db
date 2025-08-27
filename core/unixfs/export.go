package unixfs

import (
    "bytes"
    "time"

    dagcbor "github.com/ipld/go-ipld-prime/codec/dagcbor"
    "github.com/ipld/go-ipld-prime/node/basicnode"
    "openhashdb/protobuf/pb"
)

// EncodeMetadataDagCBOR encodes selected fields of ContentMetadata into a dag-cbor IPLD node.
// This aims for basic interop with IPFS tooling for metadata inspection.
func EncodeMetadataDagCBOR(meta *pb.ContentMetadata) ([]byte, error) {
    mb := basicnode.Prototype__Map{}.NewBuilder()
    ma, _ := mb.BeginMap(10)
    // minimal fields for interop
    if ent, err := ma.AssembleEntry("filename"); err == nil { ent.AssignString(meta.Filename) }
    if ent, err := ma.AssembleEntry("mime"); err == nil { ent.AssignString(meta.MimeType) }
    if ent, err := ma.AssembleEntry("size"); err == nil { ent.AssignInt(meta.Size) }
    if ent, err := ma.AssembleEntry("isDir"); err == nil { ent.AssignBool(meta.IsDirectory) }
    if meta.ModTime != nil {
        if ent, err := ma.AssembleEntry("mod"); err == nil { ent.AssignString(meta.ModTime.AsTime().Format(time.RFC3339)) }
    }
    if meta.CreatedAt != nil {
        if ent, err := ma.AssembleEntry("created"); err == nil { ent.AssignString(meta.CreatedAt.AsTime().Format(time.RFC3339)) }
    }
    if ent, err := ma.AssembleEntry("refCount"); err == nil { ent.AssignInt(int64(meta.RefCount)) }
    // basic chunks list (hashes as hex bytes)
    if len(meta.Chunks) > 0 {
        if ent, err := ma.AssembleEntry("chunks"); err == nil {
            la, _ := ent.BeginList(int64(len(meta.Chunks)))
            for _, ch := range meta.Chunks {
                la.AssembleValue().AssignBytes(ch.Hash)
            }
            la.Finish()
        }
    }
    // links: store name + hash bytes + size
    if len(meta.Links) > 0 {
        if ent, err := ma.AssembleEntry("links"); err == nil {
            la, _ := ent.BeginList(int64(len(meta.Links)))
            for _, l := range meta.Links {
                // each link is a small map
                lmb := basicnode.Prototype__Map{}.NewBuilder()
                lma, _ := lmb.BeginMap(3)
                if e2, er := lma.AssembleEntry("name"); er == nil { e2.AssignString(l.Name) }
                if e2, er := lma.AssembleEntry("hash"); er == nil { e2.AssignBytes(l.Hash) }
                if e2, er := lma.AssembleEntry("size"); er == nil { e2.AssignInt(l.Size) }
                lma.Finish()
                la.AssembleValue().AssignNode(lmb.Build())
            }
            la.Finish()
        }
    }
    ma.Finish()
    n := mb.Build()
    // encode as dag-cbor
    var buf bytes.Buffer
    if err := dagcbor.Encode(n, &buf); err != nil {
        return nil, err
    }
    return buf.Bytes(), nil
}
