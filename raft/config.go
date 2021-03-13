package raft

import "time"

type Config struct {

	// ElectionTimeout
	//  If a follower receives no communication over a period of time called the election
	// timeout, then it assumes there is no viable leader and begins an election to choose
	// a new leader.
	ElectionTimeout time.Duration
}
