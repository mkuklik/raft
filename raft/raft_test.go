package raft

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

type StateM struct{}

func (sm StateM) Apply(event []byte) error {
	return fmt.Errorf("Not implemented yet")
}

func (sm StateM) Snapshot() ([]byte, error) {
	return nil, fmt.Errorf("Not implemented yet")
}

func start(t *testing.T, ctx context.Context, nodeID uint32) RaftNode {

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

	return r
	// <-ctx.Done()

}

func TestReplication(t *testing.T) {

	ctx, cancelFunc := context.WithCancel(context.Background())

	// create two in
	r1 := start(t, ctx, 0)
	r2 := start(t, ctx, 1)

	time.Sleep(10 * time.Second)

	//
	want := []byte("fdkslflkdsj")
	if r1.nodeStatus == Leader {
		r1.AddCommand(want)
	} else {
		r2.AddCommand(want)
	}

	time.Sleep(10 * time.Second)

	var got []byte
	if r1.nodeStatus == Leader {
		got = r2.clog.Last().Payload
	} else {
		got = r1.clog.Last().Payload
	}

	if !bytes.Equal(want, got) {
		t.Errorf("wrong index: want %v, got: %v", want, got)
	}

	cancelFunc()
}
