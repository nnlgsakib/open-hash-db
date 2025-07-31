package mtr

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NetworkMessagesTotal counts the total number of network messages
var NetworkMessagesTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "openhashdb_network_messages_total",
		Help: "Total number of network messages",
	},
	[]string{"type"},
)

// NetworkErrorsTotal counts the total number of network errors
var NetworkErrorsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "openhashdb_network_errors_total",
		Help: "Total number of network errors",
	},
	[]string{"type"},
)

// NetworkRetriesTotal counts the total number of network operation retries
var NetworkRetriesTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "openhashdb_network_retries_total",
		Help: "Total number of network operation retries",
	},
)

// PeerConnectionsTotal counts the total number of peer connections
var PeerConnectionsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "openhashdb_peer_connections_total",
		Help: "Total number of peer connections",
	},
)

// PeerDisconnectionsTotal counts the total number of peer disconnections
var PeerDisconnectionsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "openhashdb_peer_disconnections_total",
		Help: "Total number of peer disconnections",
	},
)

// ReplicationRequestsTotal counts the total number of replication requests
var ReplicationRequestsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "openhashdb_replication_requests_total",
		Help: "Total number of replication requests",
	},
	[]string{"type"},
)

// ReplicationSuccessTotal counts the total number of successful replications
var ReplicationSuccessTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "openhashdb_replication_success_total",
		Help: "Total number of successful replications",
	},
)

// ReplicationFailuresTotal counts the total number of failed replications
var ReplicationFailuresTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "openhashdb_replication_failures_total",
		Help: "Total number of failed replications",
	},
)

// var (
// 	networkMessagesTotal = promauto.NewCounterVec(
// 		prometheus.CounterOpts{
// 			Name: "openhashdb_network_messages_total",
// 			Help: "Total number of network messages",
// 		},
// 		[]string{"type"},
// 	)
// 	networkErrorsTotal = promauto.NewCounterVec(
// 		prometheus.CounterOpts{
// 			Name: "openhashdb_network_errors_total",
// 			Help: "Total number of network errors",
// 		},
// 		[]string{"type"},
// 	)
// 	networkRetriesTotal = promauto.NewCounter(
// 		prometheus.CounterOpts{
// 			Name: "openhashdb_network_retries_total",
// 			Help: "Total number of network operation retries",
// 		},
// 	)
// 	peerConnectionsTotal = promauto.NewCounter(
// 		prometheus.CounterOpts{
// 			Name: "openhashdb_peer_connections_total",
// 			Help: "Total number of peer connections",
// 		},
// 	)
// 	peerDisconnectionsTotal = promauto.NewCounter(
// 		prometheus.CounterOpts{
// 			Name: "openhashdb_peer_disconnections_total",
// 			Help: "Total number of peer disconnections",
// 		},
// 	)
// )
