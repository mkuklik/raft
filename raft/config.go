package raft

import "time"

type Config struct {

	// ElectionTimeout
	//  If a follower receives no communication over a period of time called the election
	// timeout, then it assumes there is no viable leader and begins an election to choose
	// a new leader.
	ElectionTimeout time.Duration // the election timeout is likely to be somewhere between 10ms and 500ms.

	// MTBF is the average time between failures for a single server.
	// Typical server MTBFs are several months or more, which easily satisfies the timing requirement.
	MTBF time.Duration
}

func NewConfig() Config {
	return Config{ElectionTimeout: 50 * time.Millisecond, MTBF: 24 * 30 * 3 * time.Hour}
}
