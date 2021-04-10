package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/mkuklik/raft/raftpb"
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
		if prev != nil {
			prevLogIndex = prev.Index
			prevLogTerm = prev.Term
		}
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

func (node *RaftNode) sendAppendEntries(ctx context.Context, id int, req *raftpb.AppendEntriesRequest, success chan bool) {

	node.Logger.Debugf("sending AppendEntries to %d, %s", id, req.String())

	client := node.clients[id]
	if client == nil {
		node.Logger.Errorf("node %d client is nil", id)
		if success != nil {
			success <- false
		}
		return
	}

	reply, err := (*client).AppendEntries(ctx, req)

	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			node.Logger.Errorf("failed sending AppendEntries to %s due to, %s", node.config.Peers[id], st.Message())
		}
		if success != nil {
			success <- false
		}
	} else {
		node.Logger.Debugf("reply to AppendEntries from %d, (term %d, success %v", id, reply.Term, reply.Success)
		if reply.Success {

			if success != nil {
				success <- true
			}

			if len(req.Entries) > 0 {
				// If successful: update nextIndex and matchIndex for follower (§5.3)
				node.lock.Lock()
				if req.Entries[len(req.Entries)-1].Index > node.state.MatchIndex[id] {
					node.state.MatchIndex[id] = req.Entries[len(req.Entries)-1].Index
					node.state.NextIndex[id] = node.state.MatchIndex[id] + 1
					// node.signals <- MatchIndexUpdate
					go node.updateCommitIndex()
				}
				node.lock.Unlock()
			}

		} else {

			if success != nil {
				success <- false
			}

			if reply.Term > node.state.CurrentTerm {
				// switch to follower
				node.state.CurrentTerm = reply.Term
				node.SwitchTo(Follower)
				node.saveState()
			} else if reply.Term < node.state.CurrentTerm {
				// Append failed because other node had lower term than currentTerm.
				// the node is behind and has to switch to follower to
				// TODO should we delay resend ???
				go node.sendAppendEntries(ctx, id, node.prepareLog(ctx, id), nil)

			} else if len(req.Entries) > 0 {
				// If AppendEntries fails because of log inconsistency:
				// decrease nextIndex and retry (§5.3)
				node.lock.Lock()
				if node.state.NextIndex[id] > 0 {
					node.state.NextIndex[id]--
				}
				node.lock.Unlock()
				// do we repeat it indefinitely ???
				go node.sendAppendEntries(ctx, id, node.prepareLog(ctx, id), nil)
			}
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
				node.sendAppendEntries(tx, id, emptyReq, nil)
			}(id)
		}
	}
}

// updateCommitIndex checks whether to increase CommitIndex
//  If there exists an N such that N > commitIndex, a majority
//  of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (node *RaftNode) updateCommitIndex() {
	node.lock.Lock()
	defer node.lock.Unlock()

	majority := len(node.clients) / 2

	ci := node.state.CommitIndex + 1
	for {
		// check for majority
		count := 1 // voting for herself
		for j := range node.clients {
			if j != int(node.nodeID) && node.state.MatchIndex[j] >= ci {
				count++
			}
		}
		if count <= majority {
			// need 2 out of 3, 3 out of 4 nodes, etc
			ci-- // backtrack by one
			break
		}
		ci++
	}

	if ci > node.state.CommitIndex {
		node.state.CommitIndex = ci
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
func (node *RaftNode) AddCommand(ctx context.Context, payload []byte) error {
	node.logLock.Lock()
	defer node.logLock.Unlock()

	if node.nodeStatus != Leader {
		return fmt.Errorf("commands are not accepted: node %d is not a leader", node.nodeID)
	}

	N := len(node.clients) - 1
	majority := len(node.clients) / 2

	// Add entry to local log and persist

	prevLogEntry, newLogEntry := node.clog.Append(node.state.CurrentTerm, payload)

	// replicate

	ballotbox := make(chan bool, N)
	defer close(ballotbox)

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

	for i := range node.clients {
		// skip yourself
		if i != int(node.nodeID) {
			go func(clinetID int) {
				ctx, cancelfunc := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancelfunc()

				node.sendAppendEntries(ctx, i, &msg, ballotbox)

			}(i)
		}
	}

	yeas := 1 // voting for herself
	count := 0
	for count < N {
		v := <-ballotbox
		count++
		if v {
			yeas++
		}
		if yeas > majority {
			// need 2 out of 2, need 2 out of 3, 3 out of 4 nodes, etc
			// log is applied by checkCommittedEntries upon update to matchIndex
			if err := (*node.sm).Apply(newLogEntry.Payload); err != nil {
				node.Logger.Errorf("failed to apply log entry %d, %s", newLogEntry.Index, err.Error())
				return err
			} else {
				node.state.LastApplied++
			}
			node.Logger.Debugf("replication succeeded, logInx %d", newLogEntry.Index)
			return nil
		}
	}

	node.Logger.Debugf("replication failed to majority")
	return fmt.Errorf("failed to process command")
}

func (node *RaftNode) AppendEntriesLeader(ctx context.Context, msg *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
		node.saveState()
	}

	return &raftpb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil // ????
}

func (node *RaftNode) RequestVoteLeader(ctx context.Context, msg *raftpb.RequestVoteRequest) (*raftpb.RequestVoteReply, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
		node.saveState()
	}

	return &raftpb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil // ????
}

func (node *RaftNode) InstallSnapshotLeader(ctx context.Context, msg *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotReply, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
		node.saveState()
	}

	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
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
						node.sendAppendEntries(tx, id, node.prepareLog(ctx, id), nil)
					}(id)
				}
			}
			heartBeatTimer.Reset(node.config.HeartBeat)
		}
	}
}
