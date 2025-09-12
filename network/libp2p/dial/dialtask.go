package dial

import "github.com/libp2p/go-libp2p/core/peer"

type DialTask struct {
    index int

    addrInfo *peer.AddrInfo
    priority uint64
}

func (dt *DialTask) GetAddrInfo() *peer.AddrInfo { return dt.addrInfo }

