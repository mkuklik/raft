package raft

import (
	"bytes"
	"encoding"
	"encoding/gob"
	"errors"
	"io"
)

func init() {
	gob.Register(RegistrationRequest{})
	gob.Register(RegistrationReply{})
	gob.Register(VoteRequest{})
	gob.Register(VoteReply{})
	gob.Register(AppendEntriesRequest{})
	gob.Register(AppendEntriesReply{})
	gob.Register(LogEntry{})
}

type Packet struct {
	Message interface{}
}

// RegistrationRequest is sent by a new node trying to join the cluster
type RegistrationRequest struct {
	Name string // unique name
}

// RegistrationReply confirms registration or redirects to  new joiner to a leader
type RegistrationReply struct {
	Success bool   // true if leader registered this follower
	Address string // address to a leader
}

type VoteRequest struct {
	// Invoked by candidates to gather votes (§5.2).
	Term         uint32 // candidate’s term
	CandidateId  uint32 // candidate requesting vote
	LastLogIndex uint32 // index of candidate’s last log entry (§5.4)
	LastLogTerm  uint32 // term of candidate’s last log entry (§5.4)
}

// func (x *VoteRequest) Abc() string {
// 	return "fdf"
// }

// VoteReply results of VoteRequest
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
type VoteReply struct {
	Term        uint32 // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
}

// func (x *VoteReply) Abc() string {
// 	return "fdf"
// }

type Payloader interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// LogEntry each entry contains command for state machine, and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Term  uint32 // leader's term
	Index uint32 // log index
	// plus:
	// command to the state machine
	Payload interface{} //Payloader
}

type AppendEntriesRequest struct {
	Term         uint32     // leader’s term
	LeaderId     uint32     // so follower can redirect clients
	PrevLogIndex uint32     // index of log entry immediately preceding new ones
	PrevLogTerm  uint32     // term of prevLogIndex entry
	LeaderCommit uint32     // currentTerm, for leader to update itself true if follower contained entry matching prevLogIndex and prevLogTerm
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency) leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    uint32 // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (r AppendEntriesRequest) Encode() ([]byte, error) {
	var network bytes.Buffer        // Stand-in for a network connection
	enc := gob.NewEncoder(&network) // Will write to network.
	err := enc.Encode(r)
	if err != nil {
		// todo
		return nil, errors.New("failed encoding")
	}
	return network.Bytes(), nil
}

func (r AppendEntriesReply) Encode() ([]byte, error) {
	var network bytes.Buffer        // Stand-in for a network connection
	enc := gob.NewEncoder(&network) // Will write to network.
	err := enc.Encode(r)
	if err != nil {
		// todo
		return nil, errors.New("failed encoding")
	}
	return network.Bytes(), nil
}

func DecodeAppendEntriesReply(r io.Reader) (*AppendEntriesReply, error) {
	dec := gob.NewDecoder(r) // Will read from network.
	// Encode (send) the value.
	var x AppendEntriesReply
	err := dec.Decode(&x)
	if err != nil {
		// todo
		return nil, errors.New("decode error")
	}
	return &x, nil
}

func appendEntriesReplyHandle(r *AppendEntriesReply) {
	/*
		Receiver implementation:
		1. Reply false if term < currentTerm (§5.1)
		2. Reply false if log doesn’t contain an entry at prevLogIndex
		whose term matches prevLogTerm (§5.3)
		3. If an existing entry conflicts with a new one (same index
		but different terms), delete the existing entry and all that
		follow it (§5.3)
		4. Append any new entries not already in the log
		5. If leaderCommit > commitIndex, set commitIndex =
		min(leaderCommit, index of last new entry)
	*/

}
