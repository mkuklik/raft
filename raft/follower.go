package raft

import "fmt"

// leader maintains nextIndex for each follower; it is initiated at index leader is at,
// and in case of conflict, leader drops nextIndex for the follower and reties AppendEntries

// Followers (§5.2):
// • Respond to RPCs from candidates and leaders
// • If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate: convert to candidate

type FollowerInterface interface{}

func (f *FollowerInterface) FollowerHandle(msg interface{}) {

	switch t := msg.(type) {
	case AppendEntriesRequest:
		fmt.Println(t, f)
		return
	case VoteRequest:
		// todo
		return
	default:
		//todo
	}

}
