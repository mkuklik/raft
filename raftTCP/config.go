package rafttcp

import "time"

type Config struct {
	// Bootstrap leader address to connect to
	Bootstrap string

	// MaxConnectionAttempts maximum attempts to find a leader
	MaxConnectionAttempts uint32

	// ElectionTimeout
	//  If a follower receives no communication over a period of time called the election
	// timeout, then it assumes there is no viable leader and begins an election to choose
	// a new leader.
	ElectionTimeout time.Duration // the election timeout is likely to be somewhere between 10ms and 500ms.

	// HeartBeat
	// time between Heartbeats by a leader
	HeartBeat time.Duration

	// MTBF is the average time between failures for a single server.
	// Typical server MTBFs are several months or more, which easily satisfies the timing requirement.
	MTBF time.Duration
}

func NewConfig() Config {
	return Config{
		Bootstrap:             "",
		MaxConnectionAttempts: 10,
		ElectionTimeout:       50 * time.Millisecond,
		HeartBeat:             10 * time.Millisecond,
		MTBF:                  24 * 30 * 3 * time.Hour,
	}
}
