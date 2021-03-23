package raft

import (
	"context"
	"net"

	pb "github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
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
	log.Infof("received AppendEntries from %d", ctx.Value(NodeIDKey))

	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	if msg.Term < node.state.CurrentTerm {
		log.Infof("replied false to AppendEntries from %d; msg.Term %d, currentTerm %d", ctx.Value(NodeIDKey), msg.Term, node.state.CurrentTerm)
		return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil
	}

	// if len(msg.Entries) == 0, AppendEntries is a heartbeat check only

	if len(msg.Entries) > 0 {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		if !node.clog.Has(msg.PrevLogTerm, msg.PrevLogIndex) {
			log.Infof("replied false to AppendEntries from %d; 2.", ctx.Value(NodeIDKey))
			return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil
		}

		// 3. If an existing entry conflicts with a new one (same index
		// 	but different terms), delete the existing entry and all that
		// 	follow it (§5.3)

		// TODO check conflict !!!

		// 4. Append any new entries not already in the log
		tmp := make([]LogEntry, len(msg.Entries))
		for i, entry := range msg.Entries {
			tmp[i] = LogEntry{entry.Term, entry.Index, entry.Payload}
		}
		if !node.clog.AddEntries(&tmp) {
			log.Infof("replied false to AppendEntries from %d; 4.", ctx.Value(NodeIDKey))
			return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil
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

	return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: true}, nil
}
func (node *RaftNode) RequestVoteFollower(ctx context.Context, msg *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	log.Infof("recieved vote request from %s", ctx.Value(AddressKey).(net.Addr).String())

	// 1. Reply false if term < currentTerm (§5.1)
	if msg.Term < node.state.CurrentTerm {
		log.Infof("voted NO (1)")
		return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (node.state.VotedFor < 0 || node.state.VotedFor == int(msg.CandidateId)) &&
		logIndex(msg.LastLogTerm, msg.LastLogIndex) >= LogIndex(node.state.CommitIndex) { // ??? Doube check
		log.Infof("voted YES")
		return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: true}, nil
	}
	log.Infof("voted NO (3)")
	return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil
}

// InstallSnapshot install snapshot
func (node *RaftNode) InstallSnapshotFollower(context.Context, *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
}

func (node *RaftNode) RunFollower(ctx context.Context) {
	log.Infof("Starting as Follower")

	node.ResetElectionTimer()

	for {
		select {
		case <-ctx.Done():
			log.Infof("cancelled RunFollower")
			return
		// reset election timer
		case <-node.electionTimeoutTimer.C:
			log.Infof("Follower timer")
			node.SwitchTo(Candidate)
			return
			// default:
		}
	}

}
