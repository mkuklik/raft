package raft

import (
	"context"
	"net"

	pb "github.com/mkuklik/raft/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// leader maintains nextIndex for each follower; it is initiated at index leader is at,
// and in case of conflict, leader drops nextIndex for the follower and reties AppendEntries

// Followers (§5.2):
// • Respond to RPCs from candidates and leaders
// • If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate: convert to candidate

func (node *RaftNode) AppendEntriesFollower(ctx context.Context, msg *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	node.Logger.Infof("received AppendEntries from %d", ctx.Value(NodeIDKey))

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
		return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil
	}

	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	if msg.Term < node.state.CurrentTerm {
		node.Logger.Infof("replied false to AppendEntries from %d; msg.Term %d, currentTerm %d", ctx.Value(NodeIDKey), msg.Term, node.state.CurrentTerm)
		return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil
	}

	// if len(msg.Entries) == 0, AppendEntries is a heartbeat check only

	if len(msg.Entries) > 0 {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		if msg.PrevLogIndex != 0 && !node.clog.Has(msg.PrevLogTerm, msg.PrevLogIndex) {
			node.Logger.Infof("replied false to AppendEntries from %d; 2.", ctx.Value(NodeIDKey))
			return &pb.AppendEntriesReply{
				Term:    node.state.CurrentTerm,
				Success: false,
				// LastLogIndex: node.clog.Last().Index,
			}, nil
		}

		// 3. If an existing entry conflicts with a new one (same index
		// 	but different terms), delete the existing entry and all that
		// 	follow it (§5.3)
		for _, entry := range msg.Entries {
			e := node.clog.Get(entry.Index)
			if e != nil && e.Term != entry.Term {
				node.Logger.Debugf("Term mismatch in log entry, index=%d, term local %d, wire %d",
					entry.Index, e.Term, entry.Term)
			}
		}

		// 4. Append any new entries not already in the log
		tmp := make([]LogEntry, len(msg.Entries))
		for i, entry := range msg.Entries {
			tmp[i] = LogEntry{entry.Term, entry.Index, entry.Payload}
		}

		if err := node.clog.AddEntries(&tmp); err != nil {
			node.Logger.Infof("replied false to AppendEntries from %d; 4.", ctx.Value(NodeIDKey))
			return &pb.AppendEntriesReply{
				Term:    node.state.CurrentTerm,
				Success: false,
			}, nil
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// 	min(leaderCommit, index of last new entry)
	if msg.LeaderCommit > node.state.CommitIndex {
		if last := node.clog.Last(); msg.LeaderCommit < last.Index {
			node.state.CommitIndex = msg.LeaderCommit
		} else {
			node.state.CommitIndex = last.Index
		}
	}

	node.ResetElectionTimer()

	return &pb.AppendEntriesReply{
		Term:    node.state.CurrentTerm,
		Success: true,
	}, nil
}

func (node *RaftNode) RequestVoteFollower(ctx context.Context, msg *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	node.Logger.Infof("recieved vote request from %s", ctx.Value(AddressKey).(net.Addr).String())

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if msg.Term < node.state.CurrentTerm {
		node.Logger.Debugf("voted NO (1)")
		return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (node.state.VotedFor < 0 || node.state.VotedFor == int(msg.CandidateId)) &&
		msg.LastLogIndex >= node.state.CommitIndex { // ??? Doube check
		node.Logger.Debugf("voted YES")
		node.state.VotedFor = int(msg.CandidateId)
		return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: true}, nil
	}
	node.Logger.Debugf("voted NO (3)")
	return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil
}

// InstallSnapshot install snapshot
func (node *RaftNode) InstallSnapshotFollower(ctx context.Context, msg *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if msg.Term > node.state.CurrentTerm {
		node.state.CurrentTerm = msg.Term
		node.SwitchTo(Follower)
	}

	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
}

func (node *RaftNode) RunFollower(ctx context.Context) {
	node.Logger.Infof("Starting as Follower")

	node.state.VotedFor = -1

	node.ResetElectionTimer()

	for {
		select {
		case <-ctx.Done():
			node.Logger.Debugf("cancelled RunFollower")
			return

		// reset election timer
		case <-node.electionTimeoutTimer.C:
			node.Logger.Debugf("Follower timer")
			node.SwitchTo(Candidate)
			return
		}
	}

}
