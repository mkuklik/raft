package raft

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"
)

type StateMachineMock struct {
	AppliedEvents [][]byte
}

func NewStateMachineMock() StateMachine {
	tmp := StateMachineMock{make([][]byte, 0)}
	return &tmp
}

func (sm *StateMachineMock) Apply(event []byte) error {
	sm.AppliedEvents = append(sm.AppliedEvents, event)
	return nil
}

func (sm StateMachineMock) Snapshot() ([]byte, error) {
	return nil, fmt.Errorf("not implemented yet")
}

func start(t *testing.T, ctx context.Context, nodeID uint32, startPort int) *RaftNode {

	config := NewConfig()
	config.Peers = []string{
		"localhost:" + strconv.Itoa(startPort),
		"localhost:" + strconv.Itoa(startPort+1),
	}

	// logfile
	logFile, err := ioutil.TempFile("/tmp/", "*.logfile")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// file.Close()
		err := os.Remove(logFile.Name())
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	// state file
	stateFile, err := ioutil.TempFile("/tmp/", "*.statefile")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// file.Close()
		err := os.Remove(stateFile.Name())
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	sm := NewStateMachineMock() // Some state machine
	r := NewRaftNode(&config, uint32(nodeID), &sm, logFile, stateFile)

	r.Run(ctx, config.Peers[nodeID])

	return r
}

func TestReplication(t *testing.T) {

	ctx, cancelFunc := context.WithCancel(context.Background())

	// create two in
	r1 := start(t, ctx, 0, 1210)
	r2 := start(t, ctx, 1, 1210)

	time.Sleep(25 * time.Second)

	//
	want := []byte("fdkslflkdsj")
	if r1.nodeStatus == Leader {
		r1.AddCommand(ctx, want)
	} else if r2.nodeStatus == Leader {
		r2.AddCommand(ctx, want)
	} else {
		t.Fatalf("there is no leader")
	}

	time.Sleep(1 * time.Second)

	var got []byte
	var tmp *LogEntry
	if r1.nodeStatus == Leader {
		tmp = r2.clog.Last()
	} else {
		tmp = r1.clog.Last()
	}

	if tmp == nil {
		t.Fatalf("command didn't replicate")
	} else {
		got = tmp.Payload
	}

	if !bytes.Equal(want, got) {
		t.Errorf("wrong index: want %v, got: %v", want, got)
	}

	cancelFunc()
}
