package raft

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

type StateM struct{}

func NewStateM() StateMachine {
	return &StateM{}
}
func (sm StateM) Apply(event []byte) error {
	return nil
	// return fmt.Errorf("not implemented yet")
}

func (sm StateM) Snapshot() ([]byte, error) {
	return nil, fmt.Errorf("not implemented yet")
}

func createRaftTestNode(t *testing.T, term uint32, nclients int, startPort int) *RaftNode {

	config := NewConfig()
	config.Peers = []string{}
	for i := 0; i < nclients; i++ {
		config.Peers = append(config.Peers, "localhost:"+strconv.Itoa(startPort+i))
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

	sm := NewStateM() // Some state machine
	r := NewRaftNode(&config, 0, &sm, logFile, stateFile)
	r.state.CurrentTerm = term

	return r
}
