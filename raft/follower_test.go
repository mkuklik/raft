package raft

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/mkuklik/raft/raftpb"
)

type StateM struct{}

func (sm StateM) Apply(event []byte) error {
	return fmt.Errorf("Not implemented yet")
}

func (sm StateM) Snapshot() ([]byte, error) {
	return nil, fmt.Errorf("Not implemented yet")
}

func create(t *testing.T, term uint32) RaftNode {

	config := NewConfig()
	config.Peers = []string{
		"localhost:1234",
		"localhost:1235",
	}

	file, err := ioutil.TempFile("/tmp/", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// file.Close()
		err := os.Remove(file.Name())
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	sm := StateM{} // Some state machine
	r := NewRaftNode(&config, 0, sm, file)
	r.state.CurrentTerm = term

	return r
}

func TestFollowerAppendEntry(t *testing.T) {

	r := create(t, 1)

	ctx := context.WithValue(context.Background(), NodeIDKey, 1)

	newLogEntry := LogEntry{1, 1, []byte("adsdas")}

	var prevLogIndex uint32 = 0
	var prevLogTerm uint32 = 0

	logEntry := raftpb.LogEntry{
		Term:    newLogEntry.Term,
		Index:   newLogEntry.Index,
		Payload: newLogEntry.Payload,
	}
	msg := raftpb.AppendEntriesRequest{
		Term:         r.state.CurrentTerm,
		LeaderId:     r.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []*raftpb.LogEntry{&logEntry},
		LeaderCommit: r.state.CommitIndex, // double check
	}

	t.Run("first message, SUCCESS", func(t *testing.T) {

		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if !reply.Success {
			t.Errorf("AppendEntriesFollower reply to first logentry is failure")
		}
		if len(r.clog.data) != 1 {
			t.Fatal("AppendEntriesFollower failed to write first logEntry to LogJournal")
		}
		if r.clog.data[0].Index != 1 {
			t.Errorf("AppendEntriesFollower  logEntry index is wrong")
		}
	})

	t.Run("second message, wrong PrevIndex, 2., ERROR", func(t *testing.T) {
		msg.PrevLogTerm = 1
		msg.PrevLogIndex = 4
		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if reply.Success {
			t.Errorf("AppendEntriesFollower reply to first logentry is failure")
		}
	})

	t.Run("send second second message, SUCCESS", func(t *testing.T) {
		msg.PrevLogTerm = 1
		msg.PrevLogIndex = 1
		msg.Entries[0].Index++
		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if !reply.Success {
			t.Errorf("AppendEntriesFollower reply to first logentry is failure")
		}
	})

	t.Run("sending message with the same index but different term, 3., FAIL", func(t *testing.T) {
		msg.PrevLogTerm = 1
		msg.PrevLogIndex = 2
		msg.Entries[0].Term = 0
		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if !reply.Success {
			t.Errorf("AppendEntriesFollower reply to first logentry is failure")
		}
	})

	t.Run("second message,  msg.Term < node.state.CurrentTerm, ERROR", func(t *testing.T) {
		msg.PrevLogTerm = 1
		msg.PrevLogIndex = 1
		msg.Term = 0
		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if reply.Success {
			t.Errorf("AppendEntriesFollower reply to message with lower term failed")
		}
	})

	t.Run(" msg.Term > node.state.CurrentTerm, ERROR", func(t *testing.T) {
		msg.Term = 5
		go func() { <-r.signals }()
		reply, _ := r.AppendEntriesFollower(ctx, &msg)
		if reply.Success {
			t.Errorf("AppendEntriesFollower reply to first logentry is failure")
		}

		if r.state.CurrentTerm != msg.Term {
			t.Errorf("AppendEntriesFollower didn't update currentTerm when received message with higher term")
		}
	})

	t.Run("Step 5 LeaderCommit < last.Index", func(t *testing.T) {
		r.state.CommitIndex = 1
		msg.LeaderCommit = 2 // > r.state.CommitIndex, but < msg.Entries[0].Index = 3
		msg.PrevLogTerm = 1
		msg.PrevLogIndex = 2
		msg.Entries[0].Term = 1
		msg.Entries[0].Index = 3
		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if !reply.Success {
			t.Errorf("AppendEntries Step 5 failed")
		}
		if r.state.CommitIndex != msg.LeaderCommit {
			t.Errorf("AppendEntries Step 5 failed")
		}
	})

	t.Run("Step 5, LeaderCommit > last.Index", func(t *testing.T) {
		r.state.CommitIndex = 1
		msg.LeaderCommit = 5 // > r.state.CommitIndex, but > msg.Entries[0].Index = 4
		msg.PrevLogTerm = 1
		msg.PrevLogIndex = 3
		msg.Entries[0].Term = 1
		msg.Entries[0].Index = 4
		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if !reply.Success {
			t.Errorf("AppendEntries Step 5 failed")
		}
		if r.state.CommitIndex != msg.Entries[0].Index {
			t.Errorf("AppendEntries Step 5 failed")
		}
	})

	t.Run("Step 4, AddEntries error returned", func(t *testing.T) {
		r.state.CommitIndex = 1
		msg.LeaderCommit = 5 // > r.state.CommitIndex, but > msg.Entries[0].Index = 4
		msg.PrevLogTerm = 1
		msg.PrevLogIndex = 4
		msg.Entries[0].Term = 1
		msg.Entries[0].Index = 5
		logEntry2 := raftpb.LogEntry{
			Term:    1,
			Index:   8, // commandlog can't append entry with index > last.Index+1
			Payload: []byte("asdas"),
		}
		msg.Entries = append(msg.Entries, &logEntry2)

		reply, _ := r.AppendEntriesFollower(ctx, &msg)

		if reply.Success {
			t.Errorf("AppendEntries error not thrown")
		}
	})

}

type netAddr struct{}

func (x netAddr) Network() string { return "tcp" }
func (x netAddr) String() string  { return "some.address" }

func TestRequestVoteFollower(t *testing.T) {

	r := create(t, 3)

	ctx := context.WithValue(context.WithValue(context.Background(), NodeIDKey, 1), AddressKey, netAddr{})

	t.Run("vote, SUCCESS", func(t *testing.T) {

		msg := raftpb.RequestVoteRequest{
			Term:         3,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		}

		reply, _ := r.RequestVoteFollower(ctx, &msg)

		if !reply.VoteGranted {
			t.Errorf("Vote should be granted")
		}
	})

	t.Run("vote, term < 3 ", func(t *testing.T) {

		msg := raftpb.RequestVoteRequest{
			Term:         2,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		}

		reply, _ := r.RequestVoteFollower(ctx, &msg)

		if reply.VoteGranted {
			t.Errorf("Vote shouldn't be granted to candidate that is behind")
		}
		if reply.Term != r.state.CurrentTerm {
			t.Errorf("reply should be CurrentTerm")
		}
	})

	t.Run("vote, SUCCESS", func(t *testing.T) {

		msg := raftpb.RequestVoteRequest{
			Term:         3,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		}

		reply, _ := r.RequestVoteFollower(ctx, &msg)

		if !reply.VoteGranted {
			t.Errorf("Vote should be granted ")
		}
	})

	t.Run("term > currentTerm, switch to follower, SUCCESS", func(t *testing.T) {
		want_term := uint32(6)
		msg := raftpb.RequestVoteRequest{
			Term:         want_term,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		}

		go func() { <-r.signals }()
		reply, _ := r.RequestVoteFollower(ctx, &msg)

		if !reply.VoteGranted {
			t.Errorf("vote should be granted")
		}
		if r.state.CurrentTerm != want_term {
			t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
		}
	})

	t.Run("voted for another candidate already", func(t *testing.T) {

		msg := raftpb.RequestVoteRequest{
			Term:         3,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		}

		r.state.CurrentTerm = 3
		r.state.VotedFor = 10

		reply, _ := r.RequestVoteFollower(ctx, &msg)

		if reply.VoteGranted {
			t.Errorf("vote shouldn't be granted to another candidate when already voted for someone else")
		}
	})
}
