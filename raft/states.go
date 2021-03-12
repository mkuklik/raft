package raft

import "time"

type NodeState uint

const (
	Follower NodeState = iota
	Candidate
	Leader
)

/*
Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries)
to all followers in order to maintain their authority. If a follower receives
no communication over a period of time called the election timeout, then it
assumes there is no viable leader and begins an election to choose a new leader.
*/

type Config struct {

	// ElectionTimeout
	//  If a follower receives no communication over a period of time called the election
	// timeout, then it assumes there is no viable leader and begins an election to choose
	// a new leader.
	ElectionTimeout time.Duration
}
