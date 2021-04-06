package raft

import (
	"encoding/gob"
	"io"
)

type NodeStatus uint

const (
	Follower NodeStatus = iota
	Candidate
	Leader
)

var NodeStatusMap = map[NodeStatus]string{
	Follower:  "F",
	Candidate: "C",
	Leader:    "L",
}

type LogIndex uint32

// Persistent state on all servers:
type PersistantState struct {
	// (Updated on stable storage before responding to RPCs)
	CurrentTerm uint32 // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int    // candidateId that received vote in current term (or null if none)
	// Log         map[LogIndex]LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
}

// VolatileState Volatile state on all servers:
type VolatileState struct {
	CommitIndex uint32 // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied uint32 // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
}

// LeaderState Volatile state on leaders:
type LeaderState struct {
	// (Reinitialized after election)
	NextIndex  []uint32 // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex []uint32 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type State struct {
	PersistantState
	VolatileState
	LeaderState

	LeaderID int
}

func NewState(nPeers int) State {
	return State{
		PersistantState{
			CurrentTerm: 0,
			VotedFor:    -1,
			// Log:         make(map[LogIndex]LogEntry),
		},
		VolatileState{},
		LeaderState{
			make([]uint32, nPeers), // TODO initialized to leader last log index + 1
			make([]uint32, nPeers), // TODO initialized to 0, increases monotonically
		},
		-1, // LeaderID
	}
}

func (node *RaftNode) saveState() error {
	node.stateFile.Truncate(0)
	node.stateFile.Seek(0, io.SeekStart)
	enc := gob.NewEncoder(node.stateFile)
	err := enc.Encode(PersistantState{
		CurrentTerm: node.state.CurrentTerm,
		VotedFor:    node.state.VotedFor,
	})
	if err != nil {
		node.Logger.Errorf("failed to save persistant state to %s, %s", node.stateFile.Name(), err.Error())
		return err
	}
	err = node.stateFile.Sync()
	if err != nil {
		node.Logger.Errorf("failed to file sync on savng persistant state to %s, %s", node.stateFile.Name(), err.Error())
		return err
	}
	return nil
}

func (node *RaftNode) loadState() error {
	node.stateFile.Seek(0, io.SeekStart)
	dec := gob.NewDecoder(node.stateFile)
	tmp := PersistantState{}
	err := dec.Decode(&tmp)
	if err == io.EOF {
		node.Logger.Errorf("can't find a persistant state in %s; starting fresh", node.stateFile.Name())
	} else if err != nil {
		node.Logger.Errorf("failed to load persistant state to %s, %s", node.stateFile.Name(), err.Error())
		return err
	} else {
		node.Logger.Errorf("loaded persistant state from %s", node.stateFile.Name())
		node.state.CurrentTerm = tmp.CurrentTerm
		node.state.VotedFor = tmp.VotedFor
	}
	return nil
}
