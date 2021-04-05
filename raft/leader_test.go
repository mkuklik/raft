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

	t.Run("Add command 2 nodes, SUCCESS", func(t *testing.T) {

		r := create(t, 3, 2)
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		ctx := context.WithValue(context.WithValue(context.Background(), NodeIDKey, 1), AddressKey, netAddr{})

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

	t.Run("Add command 2 nodes, Failed", func(t *testing.T) {

		r := create(t, 3, 2)
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		ctx := context.WithValue(context.WithValue(context.Background(), NodeIDKey, 1), AddressKey, netAddr{})

		want := []byte("abcd")

		tmp := NewRaftClientMock(func(in *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error) {
			tmp := raftpb.AppendEntriesReply{
				Term:    1,
				Success: false,
			}
			return &tmp, nil
		})
		r.clients[1] = &tmp

		err := r.AddCommand(ctx, want)

		if err == nil {
			t.Errorf("didn't get error")
		}
	})
}

func TestCheckCommitIndex(t *testing.T) {

	t.Run("increate CommitIndex on replication 2", func(t *testing.T) {
		r := create(t, 3, 2)
		r.nodeID = 0
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		r.state.CommitIndex = 100
		r.state.MatchIndex[0] = 0
		r.state.MatchIndex[1] = 101

		want := uint32(101)

		ctx, cancelfunc := context.WithCancel(context.Background())

		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				t.Errorf("didn't received a signal")
			case s := <-r.signals:
				if s != CommitIndexUpdate {
					t.Errorf("wrong signal received, wanted %d, got %d", CommitIndexUpdate, s)
				}
			}
		}(ctx)

		r.checkCommitIndex()

		got := r.state.CommitIndex

		if got != want {
			t.Errorf("checking commitIndex failed, wanted %v, got %v", want, got)
		}

		cancelfunc()
	})

	t.Run("increate CommitIndex on replication 4", func(t *testing.T) {
		r := create(t, 3, 4)
		r.nodeID = 0
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		r.state.CommitIndex = 100
		r.state.MatchIndex[0] = 0
		r.state.MatchIndex[1] = 101
		r.state.MatchIndex[2] = 101
		r.state.MatchIndex[3] = 101

		want := uint32(101)

		ctx, cancelfunc := context.WithCancel(context.Background())

		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				t.Errorf("didn't received a signal")
			case s := <-r.signals:
				if s != CommitIndexUpdate {
					t.Errorf("wrong signal received, wanted %d, got %d", CommitIndexUpdate, s)
				}
			}
		}(ctx)

		r.checkCommitIndex()

		got := r.state.CommitIndex

		if got != want {
			t.Errorf("checking commitIndex failed, wanted %v, got %v", want, got)
		}

		cancelfunc()
	})

	t.Run("replication failed, need 3 out of 4", func(t *testing.T) {
		r := create(t, 3, 4)
		r.nodeID = 0
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		r.state.CommitIndex = 100
		r.state.MatchIndex[0] = 0
		r.state.MatchIndex[1] = 101
		r.state.MatchIndex[2] = 100
		r.state.MatchIndex[3] = 100

		want := uint32(100)

		ctx, cancelfunc := context.WithCancel(context.Background())

		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				break
			case s := <-r.signals:
				t.Errorf("shouldn't receive a signal, got %d", s)
			}
		}(ctx)

		r.checkCommitIndex()

		got := r.state.CommitIndex

		if got != want {
			t.Errorf("checking commitIndex failed, wanted %v, got %v", want, got)
		}

		cancelfunc()
	})

}
