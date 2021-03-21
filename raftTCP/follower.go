package rafttcp

import "time"

// leader maintains nextIndex for each follower; it is initiated at index leader is at,
// and in case of conflict, leader drops nextIndex for the follower and reties AppendEntries

// Followers (§5.2):
// • Respond to RPCs from candidates and leaders
// • If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate: convert to candidate

func (n *RaftNode) FollowerHandle(inb ClinetNMessage) interface{} {
	state := &n.state
	switch msg := inb.Message.(type) {
	case AppendEntriesRequest:
		// Receiver implementation:
		// 1. Reply false if term < currentTerm (§5.1)
		if msg.Term < state.CurrentTerm {
			return AppendEntriesReply{state.CurrentTerm, false}
		}
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		if _, ok := state.Log[logIndex(msg.PrevLogTerm, msg.PrevLogIndex)]; !ok {
			return AppendEntriesReply{state.CurrentTerm, false}
		}

		// 3. If an existing entry conflicts with a new one (same index
		// 	but different terms), delete the existing entry and all that
		// 	follow it (§5.3)

		// TODO check conflict

		// 4. Append any new entries not already in the log
		var indexLastNewEntry uint32
		for _, entry := range msg.Entries {
			inx := logIndex(entry.Term, entry.Index)
			if _, ok := state.Log[inx]; !ok {
				state.Log[inx] = entry
				indexLastNewEntry = entry.Index
			}
		}

		// 5. If leaderCommit > commitIndex, set commitIndex =
		// 	min(leaderCommit, index of last new entry)
		if msg.LeaderCommit > state.CommitIndex {
			if msg.LeaderCommit < indexLastNewEntry {
				state.CommitIndex = msg.LeaderCommit
			} else {
				state.CommitIndex = indexLastNewEntry
			}
		}

		state.broadcastTime = time.Now()

		return AppendEntriesReply{state.CurrentTerm, true}

	case VoteRequest:
		// 1. Reply false if term < currentTerm (§5.1)
		if msg.Term < state.CurrentTerm {
			return VoteReply{state.CurrentTerm, false}
		}
		// 2. If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if (state.VotedFor == 0 || state.VotedFor == msg.CandidateId) &&
			logIndex(msg.LastLogTerm, msg.LastLogIndex) > LogIndex(state.CommitIndex) { // ??? Doube check
			return VoteReply{state.CurrentTerm, true}
		}
		return VoteReply{state.CurrentTerm, false}

	default:
		return nil
	}
}
