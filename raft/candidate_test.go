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

	r := create(t, 3)

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
