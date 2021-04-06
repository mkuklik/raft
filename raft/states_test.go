package raft

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestSaveLoadState(t *testing.T) {

	// state file
	logFile, err := ioutil.TempFile("/tmp/", "*.logfile")
	if err != nil {
		t.Fatal(err)
	}
	// state file
	stateFile, err := ioutil.TempFile("/tmp/", "*.statefile")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// file.Close()
		err := os.Remove(logFile.Name())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = os.Remove(stateFile.Name())
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	config := NewConfig()
	sm := NewStateM() // Some state machine
	r := NewRaftNode(&config, 0, &sm, logFile, stateFile)

	t.Run("round trip", func(t *testing.T) {
		WantCurrentTerm := uint32(10)
		WantVotedFor := 1
		r.state.CurrentTerm = WantCurrentTerm
		r.state.VotedFor = WantVotedFor

		err := r.saveState()
		if err != nil {
			t.Fatalf("failed saving state, %s", err.Error())
		}

		r.state.CurrentTerm = 0
		r.state.VotedFor = 0

		err = r.loadState()
		if err != nil {
			t.Errorf("failed loading state, %s", err.Error())
		}

		if r.state.CurrentTerm != WantCurrentTerm {
			t.Errorf("r.state.CurrentTerm error; got %v, want %v", r.state.CurrentTerm, WantCurrentTerm)
		}
		if r.state.CurrentTerm != WantCurrentTerm {
			t.Errorf("r.state.VotedFor error; got %v, want %v", r.state.VotedFor, WantVotedFor)
		}
	})

	t.Run("consecutive save", func(t *testing.T) {
		WantCurrentTerm := uint32(100)
		WantVotedFor := -1
		r.state.CurrentTerm = WantCurrentTerm
		r.state.VotedFor = WantVotedFor

		err := r.saveState()
		if err != nil {
			t.Fatalf("failed saving state, %s", err.Error())
		}

		r.state.CurrentTerm = 0
		r.state.VotedFor = 0

		err = r.loadState()
		if err != nil {
			t.Errorf("failed loading state, %s", err.Error())
		}

		if r.state.CurrentTerm != WantCurrentTerm {
			t.Errorf("r.state.CurrentTerm error; got %v, want %v", r.state.CurrentTerm, WantCurrentTerm)
		}
		if r.state.CurrentTerm != WantCurrentTerm {
			t.Errorf("r.state.VotedFor error; got %v, want %v", r.state.VotedFor, WantVotedFor)
		}
	})

}
