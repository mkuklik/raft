package raft

import (
	"context"
	"testing"

	"github.com/mkuklik/raft/raftpb"
)

func TestRunCandidate(t *testing.T) {

	// r := create(t, 3)

	// ctx := context.WithValue(context.Background(), NodeIDKey, 1)

	// TODO
	// r.RunCandidate(ctx)

	// t.Run("first message, SUCCESS", func(t *testing.T) {
	// })

}

func TestRequestVoteCandidate(t *testing.T) {

	r := createRaftTestNode(t, 3, 2, 1220)

	ctx := context.WithValue(context.WithValue(context.Background(), NodeIDKey, 1), AddressKey, netAddr{})

	t.Run("vote, SUCCESS", func(t *testing.T) {

		msg := raftpb.RequestVoteRequest{
			Term:         3,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		}

		reply, _ := r.RequestVoteCandidate(ctx, &msg)

		if reply.VoteGranted {
			t.Errorf("Vote should be granted")
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
		reply, _ := r.RequestVoteCandidate(ctx, &msg)

		if reply.VoteGranted {
			t.Errorf("vote shouldn't be granted")
		}
		if r.state.CurrentTerm != want_term {
			t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
		}
	})
}

func TestCandidateAppendEntriesLeader(t *testing.T) {

	r := createRaftTestNode(t, 2, 2, 1300)
	r.nodeID = 0
	r.nodeStatus = Candidate
	r.state.CurrentTerm = 2

	want_term := uint32(3)

	t.Run("AppendEntries to Candidate with higher term", func(t *testing.T) {
		msg := raftpb.AppendEntriesRequest{
			Term:         want_term,
			LeaderId:     r.nodeID,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []*raftpb.LogEntry{},
			LeaderCommit: 0,
		}

		ctx, cancelfunc := context.WithCancel(context.Background())

		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				t.Errorf("didn't received a signal")
			case s := <-r.signals:
				if s != SwitchToFollower {
					t.Errorf("wrong signal received, wanted %d, got %d", SwitchToFollower, s)
				}
			}
		}(ctx)

		reply, err := r.AppendEntries(context.Background(), &msg)

		if err != nil {
			t.Errorf("AppendEntries returned error ")
		}

		if !reply.Success {
			t.Errorf("AppendEntries shouldn't return success")
		}
		if r.state.CurrentTerm != want_term {
			t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
		}

		cancelfunc()
	})

	t.Run("AppendEntries to Candidate with same term", func(t *testing.T) {
		msg := raftpb.AppendEntriesRequest{
			Term:         r.state.CurrentTerm,
			LeaderId:     r.nodeID,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []*raftpb.LogEntry{},
			LeaderCommit: 0,
		}

		ctx, cancelfunc := context.WithCancel(context.Background())

		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				t.Errorf("didn't received a signal")
			case s := <-r.signals:
				if s != SwitchToFollower {
					t.Errorf("wrong signal received, wanted %d, got %d", SwitchToFollower, s)
				}
			}
		}(ctx)

		reply, err := r.AppendEntries(context.Background(), &msg)

		if err != nil {
			t.Errorf("AppendEntries returned error ")
		}

		if !reply.Success {
			t.Errorf("AppendEntries shouldn't return success")
		}
		if r.state.CurrentTerm != want_term {
			t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
		}

		cancelfunc()
	})

	t.Run("AppendEntries to Candidate with lower term", func(t *testing.T) {
		msg := raftpb.AppendEntriesRequest{
			Term:         want_term - 1,
			LeaderId:     r.nodeID,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []*raftpb.LogEntry{},
			LeaderCommit: 0,
		}

		ctx, cancelfunc := context.WithCancel(context.Background())

		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				// t.Errorf("didn't received a signal")
				break
			case s := <-r.signals:
				t.Errorf("received a signal, %d", s)
			}
		}(ctx)

		reply, err := r.AppendEntries(context.Background(), &msg)

		if err != nil {
			t.Errorf("AppendEntries returned error ")
		}

		if reply.Success {
			t.Errorf("AppendEntries shouldn't return success")
		}
		if r.state.CurrentTerm != want_term {
			t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
		}

		cancelfunc()
	})
}
