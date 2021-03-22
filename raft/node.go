package raft

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

var ContextKey string

const AddressKey string = "address"

type ClinetNMessage struct {
	Client  *client
	Message interface{}
}

type RaftNode struct {
	pb.UnimplementedRaftServer

	config *Config
	nodeID uint32

	nodeStatus NodeStatus
	state      State
	sm         StateMachine
	clog       *CommandLog
	leader     *client

	conns   map[string]*grpc.ClientConn
	clients map[string]pb.RaftClient

	// nextIndex
	// for each server, index of the next log entry to send to that
	// server (initialized to leader last log index + 1)
	nextIndex map[string]uint32

	// matchIndex
	// for each server, index of highest log entry known to be replicated
	// on server (initialized to 0, increases monotonically)
	matchIndex map[string]uint32

	command chan interface{} // ?? Payloader

	switchChan chan NodeStatus

	// resetTimer chan is used to reset election timer
	electionTimeoutTimer *time.Timer

	outboundLogEntriesLock sync.Mutex
	outboundLogEntries     []LogEntry
}

func NewRaftNode(config *Config, nodeID uint32, sm StateMachine) RaftNode {
	log := NewCommandLog()
	return RaftNode{
		pb.UnimplementedRaftServer{},
		config,
		nodeID,
		Follower,
		NewState(),
		sm,
		&log,
		nil, // leader
		make(map[string]*grpc.ClientConn),
		make(map[string]pb.RaftClient),
		make(map[string]uint32),
		make(map[string]uint32),
		make(chan interface{}),
		make(chan NodeStatus),
		time.NewTimer(config.ElectionTimeout),
		sync.Mutex{},
		make([]LogEntry, 0, 100),
	}
}

func (node *RaftNode) SwitchTo(to NodeStatus) {
	node.switchChan <- to
}

func (node *RaftNode) ResetElectionTimer() {
	tmp := node.config.ElectionTimeout + time.Duration(rand.Intn(150))*time.Millisecond
	node.electionTimeoutTimer.Reset(tmp)
	log.Infof("Election timer set to %s", tmp.String())
}

func (node *RaftNode) mainLoop(parentCtx context.Context) {

	var ctx context.Context
	var cancel context.CancelFunc

	for {
		select {
		case <-parentCtx.Done():
			break
		case s := <-node.switchChan:
			if cancel != nil {
				cancel()
			}
			node.nodeStatus = s
			ctx, cancel = context.WithCancel(parentCtx)
			switch s {
			case Leader:
				go node.RunLeader(ctx)
			case Candidate:
				go node.RunCandidate(ctx)
			case Follower:
				go node.RunFollower(ctx)
			default:
				log.Fatal("unsupported node status, %d", s)
			}
		default:
		}
	}
}

// func (m *RaftNode) handleConnection(c *client) {
// 	for {
// 		msg, err := c.recv()
// 		if err == io.EOF {
// 			// TOOD close connection or try to reconnect
// 			log.Infof("Closing connection with %s, EOF", c.addr)
// 			break
// 		}
// 		m.inbound <- ClinetNMessage{c, msg}
// 	}
// 	m.DeregisterClinet(c)
// }

// func (m *RaftNode) connectToLeader(address string) (*client, error) {
// 	var err error
// 	var conn net.Conn
// 	// Find leader
// 	var n uint32
// 	for n < m.config.MaxConnectionAttempts {
// 		log.Infof("Dialing %s", address)
// 		conn, err = net.Dial("tcp", address)
// 		if err != nil {
// 			log.Errorf("Dialing failed, %s", err.Error())
// 			n++
// 			time.Sleep(time.Duration(10*n) * time.Millisecond)
// 			continue
// 		}
// 		c := newClient(conn, "")

// 		// send registration request
// 		c.send(RegistrationRequest{"Jon"})

// 		// check response
// 		msg, err := c.recv()
// 		if err != nil {
// 			log.Errorf("Invalid response from %s, %s", address, err.Error())
// 		}
// 		reply := msg.(RegistrationReply)
// 		if reply.Success {
// 			log.Infof("Connected successfully to a leader at %s", address)
// 			return &c, nil
// 		} else if reply.Address != "" {
// 			address = reply.Address
// 			log.Infof("forwarded to a leader at %s", address)
// 		} else {
// 			time.Sleep(time.Duration(10*n) * time.Millisecond)
// 		}
// 	}
// 	n++
// 	log.Errorf("Failed finding a leader after %d attempts", n)
// 	return nil, fmt.Errorf("Failed finding a leader after %d attempts", n)
// }

// Bootstrap start with connection the the leader
// func (m *RaftNode) Bootstrap(address string) {
// 	leader, err := m.connectToLeader(address)
// 	if err != nil {
// 		panic(err) // TODO graceful shotdown
// 	}
// 	m.RegisterClient(leader)
// 	m.leader = leader
// 	m.state.broadcastTime = time.Now()
// 	go m.handleConnection(leader)
// }

// runListener run listener to incoming traffic
func (n *RaftNode) runListener(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s, %s", addr, err.Error())
		return
	}
	defer l.Close()

	server := grpc.NewServer(grpc.StatsHandler(&serverStats{n, "server"}))
	pb.RegisterRaftServer(server, n)
	err = server.Serve(l)
	if err != nil {
		log.Fatal()
	}
}

func (node *RaftNode) connectToPeer(serverAddr string) {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithStatsHandler(&serverStats{node, "dialer"}),
	}
	conn, err := grpc.DialContext(
		context.WithValue(context.Background(), AddressKey, serverAddr),
		serverAddr,
		opts...)
	if err != nil {
		log.Errorf("Dial failed, %s", err.Error())
	}
	node.conns[serverAddr] = conn
	node.clients[serverAddr] = pb.NewRaftClient(conn)
}

func (node *RaftNode) connectToPeers(selfAddr string) {
	for id, addr := range node.config.Peers {
		if id != int(node.nodeID) {
			node.connectToPeer(addr)
		}
	}
}

func (node *RaftNode) Run(ctx context.Context, addr string) {
	go node.runListener(addr)
	node.connectToPeers(addr)
	go node.mainLoop(ctx)
	node.SwitchTo(Follower)
}

// func (m *RaftNode) RegisterClient(c *client) {
// 	if _, exists := m.clients[c.addr]; !exists {
// 		m.clients[c.addr] = c
// 	}
// 	log.Debugf("Registered client, %s at %s", c.name, c.addr)
// }

// func (m *RaftNode) DeregisterClinet(c *client) {
// 	if _, exists := m.clients[c.addr]; exists {
// 		delete(m.clients, c.addr)
// 	}
// 	c.close()
// 	log.Infof("Deregistered client, %s at %s", c.name, c.addr)
// }

// Build stats handler
type serverStats struct {
	node *RaftNode
	name string
}

func (h *serverStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	// fmt.Printf("%s: TagRPC, %#v\n", h.name, info)
	return ctx
}

func (h *serverStats) HandleRPC(ctx context.Context, s stats.RPCStats) {
	log.Infof("%s: HandleRPC, %#v\n", h.name, s)
}

func (h *serverStats) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	// fmt.Printf("%s: TagConn, %#v\n", h.name, info)
	return context.WithValue(ctx, AddressKey, info.RemoteAddr)
}

func (h *serverStats) HandleConn(ctx context.Context, s stats.ConnStats) {
	log.Infof("HandleConn Addresskey: %#v", ctx.Value(AddressKey)) // Returns nil, can't access the value
	addr := ctx.Value(AddressKey).(net.Addr).String()

	switch s.(type) {
	case *stats.ConnBegin:
		fmt.Printf("%s: client connected, %s\n", h.name, addr)
	case *stats.ConnEnd:
		fmt.Printf("%s: client disconnected, %s\n", h.name, addr)
	default:
		fmt.Printf("%s: stats message, %#v\n", h.name, s)
	}
}
