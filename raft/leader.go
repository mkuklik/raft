package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/mkuklik/raft/raftpb"
	pb "github.com/mkuklik/raft/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// leader maintains nextIndex for each follower; it is initiated at index leader is at,
// and in case of conflict, leader drops nextIndex for the follower and reties AppendEntries

/*
Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries)
to all followers in order to maintain their authority. If a follower receives
no communication over a period of time called the election timeout, then it
assumes there is no viable leader and begins an election to choose a new leader.
*/

/*
• Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
• If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
• If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
• If successful: update nextIndex and matchIndex for
follower (§5.3)
• If AppendEntries fails because of log inconsistency:
decrement nextIndex and retry (§5.3)
• If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
*/

/*
 Each client request contains a command to be executed by the replicated state machines.
 The leader appends the command to its log as a new entry, then is- sues AppendEntries
 RPCs in parallel to each of the other servers to replicate the entry. When the entry
 has been safely replicated (as described below), the leader applies the entry to its
 state machine and returns the result of that execution to the client. If followers crash
 or run slowly, or if network packets are lost, the leader retries Append- Entries RPCs
 indefinitely (even after it has responded to the client) until all followers eventually
 store all log entries.
*/

func (node *RaftNode) prepareLog(ctx context.Context, id int) *raftpb.AppendEntriesRequest {
	// • If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	// 		• If successful: update nextIndex and matchIndex for follower (§5.3)
	entries := []*raftpb.LogEntry{}
	var prevLogIndex uint32
	var prevLogTerm uint32

	if node.state.CommitIndex >= node.state.NextIndex[id] {
		tmp, err := node.clog.GetRange(node.state.NextIndex[id], node.state.CommitIndex)
		if err != nil {
			node.Logger.Fatalf("can't find index in the log") // TODO ???
		}
		for _, e := range tmp {
			tmp := raftpb.LogEntry{
				Term:    e.Term,
				Index:   e.Index,
				Payload: e.Payload,
			}
			entries = append(entries, &tmp)
		}
		prev := node.clog.Get(node.state.NextIndex[id] - 1)
		prevLogIndex = prev.Index
		prevLogTerm = prev.Term
	}

	req := &raftpb.AppendEntriesRequest{
		Term:         node.state.CurrentTerm,
		LeaderId:     node.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: node.state.CommitIndex,
	}

	return req
}

func (node *RaftNode) sendAppendEntries(ctx context.Context, id int, req *raftpb.AppendEntriesRequest) {
	client := node.clients[id]
	node.Logger.Debugf("sending AppendEntries to %d, %s", id, req.String())
	reply, err := (*client).AppendEntries(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			node.Logger.Errorf("failed sending AppendEntries to %s due to, %s", node.config.Peers[id], st.Message())
		}
	} else {
		node.Logger.Debugf("reply to AppendEntries from %d, (term %d, success %v", id, reply.Term, reply.Success)
		if reply.Term > node.state.CurrentTerm {
			// switch to follower
			node.state.CurrentTerm = reply.Term
			node.SwitchTo(Follower)
		}

		if reply.Success && len(req.Entries) > 0 {
			// If successful: update nextIndex and matchIndex for follower (§5.3)
			// TODO lock
			node.state.MatchIndex[id] = req.Entries[len(req.Entries)-1].Index
			node.state.NextIndex[id] = node.state.MatchIndex[id] + 1

			node.signals <- MatchIndexUpdate
		} else if !reply.Success && reply.Term < node.state.CurrentTerm {
			// Append failed because other node had lower term than currentTerm
			// node is behind and has to switch to follower to
			// TODO show we delay
			go node.sendAppendEntries(ctx, id, node.prepareLog(ctx, id))

		} else if !reply.Success && len(req.Entries) > 0 {
			// If AppendEntries fails because of log inconsistency:
			// decrement nextIndex and retry (§5.3)
			if node.state.NextIndex[id] > 0 {
				node.state.NextIndex[id]-- // TODO lock
			}
			// do we repeat it indefinitely ???
			go node.sendAppendEntries(ctx, id, node.prepareLog(ctx, id))
		}
	}
}

func (node *RaftNode) sendEmptyHeartBeat(ctx context.Context) {

	emptyReq := &raftpb.AppendEntriesRequest{
		Term:         node.state.CurrentTerm,
		LeaderId:     node.nodeID,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raftpb.LogEntry{},
		LeaderCommit: node.state.CommitIndex,
	}

	for id := range node.clients {
		if id != int(node.nodeID) { // skip candidate/leader
			tx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			go func(id int) {
				node.sendAppendEntries(tx, id, emptyReq)
			}(id)
		}
	}
}

// checkCommitIndex checks whether to increase CommitIndex
//  If there exists an N such that N > commitIndex, a majority
//  of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (node *RaftNode) checkCommitIndex() {
	T := len(node.clients)
	majority := (T + 1) / 2

	ci := node.state.CommitIndex + 1
	for {
		// check for majority
		count := 0
		for j := range node.clients {
			if node.state.MatchIndex[j] >= ci {
				count++
			}
		}
		if count < majority {
			ci--
			break
		}
		ci++
	}
	// TODO LOCK
	if ci > node.state.CommitIndex {
		node.state.CommitIndex = ci
		node.signals <- CommitIndexUpdate
	}
}

// Once a leader has been elected, it begins servicing client requests. Each client
// request contains a command to be executed by the replicated state machines.
// The leader appends the command to its log as a new entry, then issues AppendEntries
// RPCs in parallel to each of the other servers to replicate the entry. When the entry
// has been safely replicated (as described below), the leader applies the entry to its
// state machine and returns the result of that execution to the client.
//
// The leader decides when it is safe to apply a log entry to the state machines;
// such an entry is called committed. Raft guarantees that committed entries are
// durable and will eventually be executed by all of the available state machines.
// A log entry is committed once the leader that created the entry has
// replicated it on a majority of the servers (e.g., entry 7 in Figure 6).

// AddLogEntry used by client to propagate log entry
func (node *RaftNode) AddCommand(payload []byte) error {

	if node.nodeStatus != Leader {
		return fmt.Errorf("commands are not accepted: node %d is not a leader", node.nodeID)
	}

	N := len(node.clients)

	// Add entry to local log and persist

	prevLogEntry, newLogEntry := node.clog.Append(node.state.CurrentTerm, payload)

	// replicate

	ballotbox := make(chan bool, N)
	defer close(ballotbox)

	for i, client := range node.clients {
		// skip yourself
		if i == int(node.nodeID) {
			continue
		}

		go func(c *raftpb.RaftClient) {
			ctx, cancelfunc := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancelfunc()

			node.Logger.Infof("prevLogEntry=%v ; newLogEntry=%#v", prevLogEntry, *newLogEntry)
			var prevLogIndex uint32
			var prevLogTerm uint32
			if prevLogEntry != nil {
				prevLogIndex = prevLogEntry.Index
				prevLogTerm = prevLogEntry.Term
			}

			logEntry := raftpb.LogEntry{
				Term:    newLogEntry.Term,
				Index:   newLogEntry.Index,
				Payload: newLogEntry.Payload,
			}
			msg := raftpb.AppendEntriesRequest{
				Term:         node.state.CurrentTerm,
				LeaderId:     node.nodeID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []*raftpb.LogEntry{&logEntry},
				LeaderCommit: node.state.CommitIndex, // double check
			}
			reply, err := (*c).AppendEntries(ctx, &msg)
			if err != nil {
				node.Logger.Errorf("AppendEntries request failed, %s", err.Error())
				ballotbox <- false
			} else {
				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
				if reply.Term > node.state.CurrentTerm {
					node.state.CurrentTerm = reply.Term
					node.SwitchTo(Follower)
					return
				}
				ballotbox <- reply.Success
			}
		}(client)
	}

	yeas := 0
	count := 0
	passed := false
	for {
		select {
		case v := <-ballotbox:
			count++
			if v {
				yeas++
			}
			if yeas >= (N+1)/2 {
				passed = true
				break
			}
			// if count >= N {
			// 	break
			// }
		}
	}

	if !passed {
		node.Logger.Debugf("replication failed to majority")
		return fmt.Errorf("failed to process command")
	}

	node.Logger.Debugf("replication succeeded to majority")

	node.sm.Apply(payload)

	return nil
}

func (node *RaftNode) RunLeader(ctx context.Context) {
	node.Logger.Infof("Starting as Leader")

	// INITIALIZATION
	// The leader maintains a nextIndex for each follower, which is the
	// index of the next log entry the leader will send to that follower.
	// When a leader first comes to power, it initializes all nextIndex values
	// to the index just after the last one in its log (11 in Figure 7).
	// TODO lock
	for i := range node.state.NextIndex {
		// (Reinitialized after election)
		node.state.NextIndex[i] = node.state.CommitIndex + 1
		node.state.MatchIndex[i] = 0
	}

	// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
	// repeat during idle periods to prevent election timeouts (§5.2)
	node.sendEmptyHeartBeat(ctx)

	heartBeatTimer := time.NewTimer(node.config.HeartBeat)

	for {
		select {
		case <-ctx.Done():
			heartBeatTimer.Stop()
			node.Logger.Infof("cancelled RunLeader")
			return
		case <-heartBeatTimer.C:
			node.Logger.Debugf("Leader timer")
			// node.outboundLogEntriesLock.Lock()
			for id := range node.clients {
				if id != int(node.nodeID) { // skip candidate/leader

					tx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()

					go func(id int) {
						node.Logger.Debugf("sending heartbeat to %d", id)
						node.sendAppendEntries(tx, id, node.prepareLog(ctx, id))
					}(id)
				}
			}

			heartBeatTimer.Reset(node.config.HeartBeat)
			// default:
		}
	}
}

func (node *RaftNode) AppendEntriesLeader(ctx context.Context, msg *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
	}

	return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil // ????
}

func (node *RaftNode) RequestVoteLeader(ctx context.Context, msg *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
	}

	return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil // ????
}

func (node *RaftNode) InstallSnapshotLeader(ctx context.Context, msg *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
	}

	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
}
