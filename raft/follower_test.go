package raft

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/mkuklik/raft/raftpb"
)

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

	// first message

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

	// ERROR second message, wrong PrevIndex, 2.
	msg.PrevLogTerm = 1
	msg.PrevLogIndex = 4
	reply, _ = r.AppendEntriesFollower(ctx, &msg)

	if reply.Success {
		t.Errorf("AppendEntriesFollower reply to first logentry is failure")
	}

	// SUCCESS, send second second message
	msg.PrevLogTerm = 1
	msg.PrevLogIndex = 1
	msg.Entries[0].Index++
	reply, _ = r.AppendEntriesFollower(ctx, &msg)

	if !reply.Success {
		t.Errorf("AppendEntriesFollower reply to first logentry is failure")
	}

	// FAIL, sending message with the same index but different term, 3.
	msg.PrevLogTerm = 1
	msg.PrevLogIndex = 2
	msg.Entries[0].Term = 0
	reply, _ = r.AppendEntriesFollower(ctx, &msg)

	if !reply.Success {
		t.Errorf("AppendEntriesFollower reply to first logentry is failure")
	}

	// ERROR second message,  msg.Term < node.state.CurrentTerm
	msg.PrevLogTerm = 1
	msg.PrevLogIndex = 1
	msg.Term = 0
	reply, _ = r.AppendEntriesFollower(ctx, &msg)

	if reply.Success {
		t.Errorf("AppendEntriesFollower reply to message with lower term failed")
	}

	// ERROR msg.Term > node.state.CurrentTerm
	msg.Term = 5
	go func() { <-r.signals }()
	reply, _ = r.AppendEntriesFollower(ctx, &msg)
	if reply.Success {
		t.Errorf("AppendEntriesFollower reply to first logentry is failure")
	}

	if r.state.CurrentTerm != msg.Term {
		t.Errorf("AppendEntriesFollower didn't update currentTerm when received message with higher term")
	}
}

// 3. If an existing entry conflicts with a new one (same index
// 	but different terms), delete the existing entry and all that
// 	follow it (§5.3)

func TestFollowerStep3(t *testing.T) {

	r := create(t, 1)

	ctx := context.WithValue(context.Background(), NodeIDKey, 1)

	fmt.Println(r, ctx)

}

func TestFollowerStep4(t *testing.T) {

	r := create(t, 1)

	ctx := context.WithValue(context.Background(), NodeIDKey, 1)

	fmt.Println(r, ctx)

}

func TestFollowerStep5(t *testing.T) {

	r := create(t, 1)

	ctx := context.WithValue(context.Background(), NodeIDKey, 1)

	fmt.Println(r, ctx)

}
