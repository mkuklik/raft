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

		r := createRaftTestNode(t, 3, 2, 1250)
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

		r := createRaftTestNode(t, 3, 2, 1260)
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
		r := createRaftTestNode(t, 3, 2, 1270)
		r.nodeID = 0
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		r.state.CommitIndex = 100
		r.state.MatchIndex[0] = 0
		r.state.MatchIndex[1] = 101

		want := uint32(101)

		r.updateCommitIndex()

		got := r.state.CommitIndex

		if got != want {
			t.Errorf("checking commitIndex failed, wanted %v, got %v", want, got)
		}
	})

	t.Run("increate CommitIndex on replication 4", func(t *testing.T) {
		r := createRaftTestNode(t, 3, 4, 1280)
		r.nodeID = 0
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		r.state.CommitIndex = 100
		r.state.MatchIndex[0] = 0
		r.state.MatchIndex[1] = 101
		r.state.MatchIndex[2] = 101
		r.state.MatchIndex[3] = 101

		want := uint32(101)

		r.updateCommitIndex()

		got := r.state.CommitIndex

		if got != want {
			t.Errorf("checking commitIndex failed, wanted %v, got %v", want, got)
		}
	})

	t.Run("replication failed, need 3 out of 4", func(t *testing.T) {
		r := createRaftTestNode(t, 3, 4, 1290)
		r.nodeID = 0
		r.nodeStatus = Leader
		r.state.CurrentTerm = 1

		r.state.CommitIndex = 100
		r.state.MatchIndex[0] = 0
		r.state.MatchIndex[1] = 101
		r.state.MatchIndex[2] = 100
		r.state.MatchIndex[3] = 100

		want := uint32(100)

		r.updateCommitIndex()

		got := r.state.CommitIndex

		if got != want {
			t.Errorf("checking commitIndex failed, wanted %v, got %v", want, got)
		}
	})

}

func TestLeaderAppendEntriesLeader(t *testing.T) {

	r := createRaftTestNode(t, 1, 2, 1300)
	r.nodeID = 0
	r.nodeStatus = Leader
	r.state.CurrentTerm = 1

	want_term := uint32(3)

	t.Run("AppendEntries to Leader with higher term", func(t *testing.T) {
		msg := raftpb.AppendEntriesRequest{
			Term:         want_term,
			LeaderId:     r.nodeID,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []*raftpb.LogEntry{},
			LeaderCommit: 0,
		}

		ctx, cancelfunc := context.WithCancel(context.Background())
		defer cancelfunc()

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

		if reply.Success {
			t.Errorf("AppendEntries shouldn't return success")
		}
		if r.state.CurrentTerm != want_term {
			t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
		}
	})

	t.Run("AppendEntries to Leader with same term", func(t *testing.T) {
		msg := raftpb.AppendEntriesRequest{
			Term:         want_term,
			LeaderId:     r.nodeID,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []*raftpb.LogEntry{},
			LeaderCommit: 0,
		}

		ctx, cancelfunc := context.WithCancel(context.Background())
		defer cancelfunc()

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

	})
}

func TestRequestVoteLeader(t *testing.T) {

	r := createRaftTestNode(t, 2, 2, 1300)
	r.nodeID = 0
	r.nodeStatus = Leader
	r.state.CurrentTerm = 2

	ctx := context.WithValue(context.WithValue(context.Background(), NodeIDKey, 1), AddressKey, netAddr{})

	t.Run("term == currentTerm, reply false", func(t *testing.T) {

		msg := raftpb.RequestVoteRequest{
			Term:         r.state.CurrentTerm,
			CandidateId:  2,
			LastLogIndex: 2,
			LastLogTerm:  1,
		}

		reply, err := r.RequestVote(ctx, &msg)

		if err != nil {
			t.Errorf("AppendEntries returned error ")
		}

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

		ctx2, cancelfunc := context.WithCancel(context.Background())
		defer cancelfunc()

		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				t.Errorf("didn't received a signal")
			case s := <-r.signals:
				if s != SwitchToFollower {
					t.Errorf("wrong signal received, wanted %d, got %d", SwitchToFollower, s)
				}
			}
		}(ctx2)

		reply, _ := r.RequestVoteCandidate(ctx, &msg)

		if reply.VoteGranted {
			t.Errorf("vote shouldn't be granted")
		}
		if r.state.CurrentTerm != want_term {
			t.Errorf("invalid term, got %d, want %d", r.state.CurrentTerm, want_term)
		}
	})
}
