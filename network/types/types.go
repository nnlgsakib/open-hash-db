package types

import "github.com/libp2p/go-libp2p/core/peer"

// ConnectionType classifies how a peer is connected.
type ConnectionType int

const (
	// ConnectionTypeUnknown means the connection type is undetermined.
	ConnectionTypeUnknown ConnectionType = 0
	// ConnectionTypeDirect means a direct TCP or QUIC connection.
	ConnectionTypeDirect  ConnectionType = 1
	// ConnectionTypeRelayed means the connection is routed through a circuit relay.
	ConnectionTypeRelayed ConnectionType = 2
)

// NodeConnector is an interface that Bitswap uses to get information
// about the network node without creating an import cycle.
type NodeConnector interface {
	GetPeerConnectionType(p peer.ID) ConnectionType
	ConnectedPeers() []peer.ID
}
