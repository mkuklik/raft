package raft

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func start(t *testing.T, ctx context.Context, nodeID uint32) *RaftNode {

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
	r := NewRaftNode(&config, uint32(nodeID), sm, file)

	r.Run(ctx, config.Peers[nodeID])

	return &r
}

func TestReplication(t *testing.T) {

	ctx, cancelFunc := context.WithCancel(context.Background())

	// create two in
	r1 := start(t, ctx, 0)
	r2 := start(t, ctx, 1)

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
