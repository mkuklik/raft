package raft

import "fmt"

// leader maintains nextIndex for each follower; it is initiated at index leader is at,
// and in case of conflict, leader drops nextIndex for the follower and reties AppendEntries

/*
Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries)
to all followers in order to maintain their authority. If a follower receives
no communication over a period of time called the election timeout, then it
assumes there is no viable leader and begins an election to choose a new leader.
*/

func (state *State) LeaderHandle(msg interface{}) interface{} {

	switch m := msg.(type) {
	case AppendEntriesReply:
		fmt.Println("TODO", m)
	case VoteRequest:
	}
	return nil // TODO
}
