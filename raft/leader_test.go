package raft

import (
	"context"
	"testing"

	"github.com/mkuklik/raft/raftpb"
	"google.golang.org/grpc"
)

type raftClientMock struct {
	check func(in *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error)
}

func NewRaftClientMock(check func(in *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error)) raftpb.RaftClient {
	return &raftClientMock{check}
}
func (r *raftClientMock) AppendEntries(ctx context.Context, in *raftpb.AppendEntriesRequest, opts ...grpc.CallOption) (*raftpb.AppendEntriesReply, error) {
	return r.check(in)
}

func (r *raftClientMock) RequestVote(ctx context.Context, in *raftpb.RequestVoteRequest, opts ...grpc.CallOption) (*raftpb.RequestVoteReply, error) {
	return nil, nil
}

func (r *raftClientMock) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest, opts ...grpc.CallOption) (*raftpb.InstallSnapshotReply, error) {
	return nil, nil
}

func TestLeaderAddCommand(t *testing.T) {

	r := create(t, 3)
	r.nodeStatus = Leader
	r.state.CurrentTerm = 1

	ctx := context.WithValue(context.WithValue(context.Background(), NodeIDKey, 1), AddressKey, netAddr{})

	t.Run("vote, SUCCESS", func(t *testing.T) {
		want := []byte("abcd")

		tmp := NewRaftClientMock(func(in *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error) {
			tmp := raftpb.AppendEntriesReply{
				Term:    1,
				Success: true,
			}
			return &tmp, nil
		})
		r.clients[1] = &tmp

		err := r.AddCommand(ctx, want)

		if err != nil {
			t.Errorf("unwanted error, %s", err.Error())
		}
	})

	// 	msg := raftpb.RequestVoteRequest{
	// 		Term:         3,
	// 		CandidateId:  2,
	// 		LastLogIndex: 2,
	// 		LastLogTerm:  1,
	// 	}

	// 	reply, _ := r.RequestVoteCandidate(ctx, &msg)

	// 	if reply.VoteGranted {
	// 		t.Errorf("Vote should be granted")
	// 	}
	// })

	// t.Run("term > currentTerm, switch to follower, SUCCESS", func(t *testing.T) {
	// 	want_term := uint32(6)
	// 	msg := raftpb.RequestVoteRequest{
	// 		Term:         want_term,
	// 		CandidateId:  2,
	// 		LastLogIndex: 2,
	// 		LastLogTerm:  1,
	// 	}

	// 	go func() { <-r.signals }()
	// 	reply, _ := r.RequestVoteCandidate(ctx, &msg)

	// 	if reply.VoteGranted {
	// 		t.Errorf("vote shouldn't be granted")
	// 	}
	// 	if r.state.CurrentTerm != want_term {
	// 		t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
	// 	}
	// })
}
