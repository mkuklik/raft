package raft

import (
	"context"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/stats"
)

// Build stats handler
type serverStats struct {
	node *RaftNode
	name string
}

func newServerStats(node *RaftNode, name string) *serverStats {
	return &serverStats{node, name}
}

func (h *serverStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	// fmt.Printf("%s: TagRPC, %#v\n", h.name, info)
	return ctx
}

func (h *serverStats) HandleRPC(ctx context.Context, s stats.RPCStats) {
	// log.Infof("%s: HandleRPC, %#v\n", h.name, s)
}

func (h *serverStats) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	// fmt.Printf("%s: TagConn, %#v\n", h.name, info)
	id, ok := h.node.revPeers[info.RemoteAddr.String()]
	if !ok {
		ctx = context.WithValue(ctx, NodeIDKey, id)
	}
	return context.WithValue(ctx, AddressKey, info.RemoteAddr)

}

func (h *serverStats) HandleConn(ctx context.Context, s stats.ConnStats) {
	// log.Infof("HandleConn Addresskey: %#v", ctx.Value(AddressKey)) // Returns nil, can't access the value
	addr := ctx.Value(AddressKey).(net.Addr).String()

	switch s.(type) {
	case *stats.ConnBegin:
		log.Infof("%s: client connected, %s\n", h.name, addr)
	case *stats.ConnEnd:
		log.Infof("%s: client disconnected, %s\n", h.name, addr)
	default:
		log.Infof("%s: stats message, %#v\n", h.name, s)
	}
}
