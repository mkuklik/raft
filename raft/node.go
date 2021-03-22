package raft

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var ContextKey string

const AddressKey string = "address"

type ClinetNMessage struct {
	Client  *client
	Message interface{}
}

type RaftNode struct {
	raftpb.UnimplementedRaftServer

	config *Config
	nodeID uint32

	nodeStatus NodeStatus
	state      State
	sm         StateMachine
	clog       *CommandLog
	leader     *client

	conns   []*grpc.ClientConn
	clients []*raftpb.RaftClient

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
	NPeers := len(config.Peers)
	rand.Seed(time.Now().Unix())
	log := NewCommandLog()
	return RaftNode{
		raftpb.UnimplementedRaftServer{},
		config,
		nodeID,
		Follower,
		NewState(),
		sm,
		&log,
		nil, // leader
		make([]*grpc.ClientConn, NPeers),
		make([]*raftpb.RaftClient, NPeers),
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

func (node *RaftNode) StopElectionTimer() {
	node.electionTimeoutTimer.Stop()
	log.Infof("Election timer stopped")
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

func (node *RaftNode) connectToPeer(id uint32, serverAddr string) {
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
	node.conns[id] = conn
	client := raftpb.NewRaftClient(conn)
	node.clients[id] = &client
}

func (node *RaftNode) connectToPeers(selfAddr string) {
	for id, addr := range node.config.Peers {
		if id != int(node.nodeID) { // skip candidate/leader
			node.connectToPeer(uint32(id), addr)
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

// runListener run listener to incoming traffic
func (n *RaftNode) runListener(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s, %s", addr, err.Error())
		return
	}
	defer l.Close()

	server := grpc.NewServer(grpc.StatsHandler(&serverStats{n, "server"}))
	raftpb.RegisterRaftServer(server, n)
	err = server.Serve(l)
	if err != nil {
		log.Fatal()
	}
}
