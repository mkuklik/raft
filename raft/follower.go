package raft

import (
	"context"

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

func (node *RaftNode) AppendEntries(ctx context.Context, msg *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	if node.nodeStatus == Candidate {
		// If AppendEntries RPC received from new leader: convert to follower
		// TODO lock here
		node.state.LeaderID = msg.LeaderId
		node.state.LeaderID = msg.LeaderId
		node.SwitchTo(Follower)
	}

	// FOLLOWER

	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	if msg.Term < node.state.CurrentTerm {
		return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if _, ok := node.state.Log[logIndex(msg.PrevLogTerm, msg.PrevLogIndex)]; !ok {
		return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: false}, nil
	}

	// 3. If an existing entry conflicts with a new one (same index
	// 	but different terms), delete the existing entry and all that
	// 	follow it (§5.3)

	// TODO check conflict

	// 4. Append any new entries not already in the log
	var indexLastNewEntry uint32
	for _, entry := range msg.Entries {
		inx := logIndex(entry.Term, entry.Index)
		if _, ok := node.state.Log[inx]; !ok {
			node.state.Log[inx] = LogEntry{entry.Term, entry.Index, entry.Payload}
			indexLastNewEntry = entry.Index
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// 	min(leaderCommit, index of last new entry)
	if msg.LeaderCommit > node.state.CommitIndex {
		if msg.LeaderCommit < indexLastNewEntry {
			node.state.CommitIndex = msg.LeaderCommit
		} else {
			node.state.CommitIndex = indexLastNewEntry
		}
	}

	node.ResetElectionTimer()

	return &pb.AppendEntriesReply{Term: node.state.CurrentTerm, Success: true}, nil
}
func (node *RaftNode) RequestVote(ctx context.Context, msg *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	// 1. Reply false if term < currentTerm (§5.1)
	if msg.Term < node.state.CurrentTerm {
		return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (node.state.VotedFor == 0 || node.state.VotedFor == msg.CandidateId) &&
		logIndex(msg.LastLogTerm, msg.LastLogIndex) > LogIndex(node.state.CommitIndex) { // ??? Doube check
		return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: true}, nil
	}
	return &pb.RequestVoteReply{Term: node.state.CurrentTerm, VoteGranted: false}, nil
}

// InstallSnapshot install snapshot
func (node *RaftNode) InstallSnapshot(context.Context, *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
}

func (node *RaftNode) RunFollower(ctx context.Context) {
	log.Infof("Starting as Follower")

	node.ResetElectionTimer()

	for {
		select {
		case <-ctx.Done():
			log.Infof("Exiting Follower")
			return
		// reset election timer
		case <-node.electionTimeoutTimer.C:
			node.SwitchTo(Candidate)
		default:
		}
	}

}
