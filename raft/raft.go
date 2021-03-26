package raft

import (
	"context"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/mkuklik/raft/raftpb"
	pb "github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Signal int

const (
	MatchIndexUpdate Signal = iota
	CommitIndexUpdate
	SwitchToFollower
	SwitchToCandidate
	SwitchToLeader
)

type RaftNode struct {
	raftpb.UnimplementedRaftServer

	Logger *log.Entry

	config   *Config
	nodeID   uint32
	revPeers map[string]uint32

	nodeStatus NodeStatus
	state      State
	sm         StateMachine
	clog       *CommandLog

	conns   []*grpc.ClientConn
	clients []*raftpb.RaftClient

	signals chan Signal

	// resetTimer chan is used to reset election timer
	electionTimeoutTimer *time.Timer
}

func NewRaftNode(config *Config, nodeID uint32, sm StateMachine, file *os.File) RaftNode {

	log.Infof("Raft NodeID %d", nodeID)

	nPeers := len(config.Peers)
	rand.Seed(time.Now().Unix())

	commandLog := NewCommandLog(file)

	revPeers := make(map[string]uint32, nPeers)
	for i, p := range config.Peers {
		revPeers[p] = uint32(i)
	}

	return RaftNode{
		raftpb.UnimplementedRaftServer{},
		log.NewEntry(log.StandardLogger()),
		config,
		nodeID,
		revPeers,
		Follower,
		NewState(nPeers),
		sm,
		&commandLog,
		make([]*grpc.ClientConn, nPeers),
		make([]*raftpb.RaftClient, nPeers),
		make(chan Signal),
		time.NewTimer(config.ElectionTimeout),
	}
}

// checkCommittedEntries checks if there are new commited entries to be applied to the sate machine
// 	If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
func (node *RaftNode) checkCommittedEntries() {

	// TODO lock
	for node.state.CommitIndex > node.state.LastApplied {
		node.state.LastApplied++
		node.sm.Apply(node.state.LastApplied)
	}

}

func (node *RaftNode) SwitchTo(to NodeStatus) {
	switch to {
	case Follower:
		node.signals <- SwitchToFollower
	case Leader:
		node.signals <- SwitchToLeader
	case Candidate:
		node.signals <- SwitchToCandidate
	}
}

func (node *RaftNode) NewElectionTimer() *time.Timer {
	tmp := node.config.ElectionTimeout + time.Duration(rand.Intn(150))*time.Millisecond
	return time.NewTimer(tmp)
}

func (node *RaftNode) ResetElectionTimer() {
	tmp := node.config.ElectionTimeout + time.Duration(rand.Intn(150))*time.Millisecond
	node.electionTimeoutTimer.Reset(tmp)
	node.Logger.Infof("Election timer set to %s", tmp.String())
}

func (node *RaftNode) StopElectionTimer() {
	node.electionTimeoutTimer.Stop()
	node.Logger.Infof("Election timer stopped")
}

func (node *RaftNode) mainLoop(parentCtx context.Context) {

	var ctx context.Context
	var cancel context.CancelFunc

	for {
		select {
		case <-parentCtx.Done():
			break
		case s := <-node.signals:
			switch s {
			case CommitIndexUpdate:
				go node.checkCommittedEntries()

			case MatchIndexUpdate:
				go node.checkCommitIndex()

			case SwitchToCandidate, SwitchToFollower, SwitchToLeader:
				if cancel != nil {
					cancel()
				}
				ctx, cancel = context.WithCancel(parentCtx)

				switch s {
				case SwitchToLeader:
					node.nodeStatus = Leader
					go node.RunLeader(ctx)

				case SwitchToCandidate:
					node.nodeStatus = Candidate
					go node.RunCandidate(ctx)

				case SwitchToFollower:
					node.nodeStatus = Follower
					go node.RunFollower(ctx)
				}
				node.Logger = log.WithFields(log.Fields{"S": NodeStatusMap[node.nodeStatus], "T": node.state.CurrentTerm})
			}

		// case s := <-node.switchChan:

		// 	if cancel != nil {
		// 		cancel()
		// 	}
		// 	node.nodeStatus = s
		// 	ctx, cancel = context.WithCancel(parentCtx)

		// 	node.Logger = log.WithFields(log.Fields{"S": NodeStatusMap[s], "T": node.state.CurrentTerm})

		// 	switch s {
		// 	case Leader:
		// 		go node.RunLeader(ctx)

		// 	case Candidate:
		// 		go node.RunCandidate(ctx)

		// 	case Follower:
		// 		go node.RunFollower(ctx)
		// 	default:
		// 		log.Fatal("unsupported node status, %d", s)
		// 	}
		default:
		}
	}
}

func (node *RaftNode) connectToPeer(ctx context.Context, id uint32, serverAddr string) {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithStatsHandler(&serverStats{node, "dialer"}),
	}
	conn, err := grpc.DialContext(
		context.WithValue(context.Background(), AddressKey, serverAddr),
		serverAddr,
		opts...)
	if err != nil {
		node.Logger.Errorf("Dial failed, %s", err.Error())
	}
	node.conns[id] = conn
	client := raftpb.NewRaftClient(conn)
	node.clients[id] = &client
}

func (node *RaftNode) connectToPeers(ctx context.Context, selfAddr string) {
	for id, addr := range node.config.Peers {
		if id != int(node.nodeID) { // skip candidate/leader
			node.connectToPeer(ctx, uint32(id), addr)
		}
	}
}

func (node *RaftNode) Run(ctx context.Context, addr string) {
	go node.runListener(ctx, addr)
	node.connectToPeers(ctx, addr)
	go node.mainLoop(ctx)
	node.SwitchTo(Follower)
}

// runListener run listener to incoming traffic
func (n *RaftNode) runListener(ctx context.Context, addr string) {
	// l, err := net.Listen("tcp", addr)
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", addr)

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

func (node *RaftNode) AppendEntries(ctx context.Context, msg *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	switch node.nodeStatus {
	case Follower:
		return node.AppendEntriesFollower(ctx, msg)
	case Candidate:
		return node.AppendEntriesCandidate(ctx, msg)
	case Leader:
		return node.AppendEntriesLeader(ctx, msg)
	}
	return nil, grpc.Errorf(codes.Internal, "invalid nodeStatus, %v", node.nodeStatus)
}

func (node *RaftNode) RequestVote(ctx context.Context, msg *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	switch node.nodeStatus {
	case Follower:
		return node.RequestVoteFollower(ctx, msg)
	case Candidate:
		return node.RequestVoteCandidate(ctx, msg)
	case Leader:
		return node.RequestVoteLeader(ctx, msg)
	}
	return nil, grpc.Errorf(codes.Internal, "invalid nodeStatus, %v", node.nodeStatus)
}

func (node *RaftNode) InstallSnapshot(ctx context.Context, msg *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	switch node.nodeStatus {
	case Follower:
		return node.InstallSnapshotFollower(ctx, msg)
	case Candidate:
		return node.InstallSnapshotCandidate(ctx, msg)
	case Leader:
		return node.InstallSnapshotLeader(ctx, msg)
	}
	return nil, grpc.Errorf(codes.Internal, "invalid nodeStatus, %v", node.nodeStatus)
}
