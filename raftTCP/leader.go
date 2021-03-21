package rafttcp

// leader maintains nextIndex for each follower; it is initiated at index leader is at,
// and in case of conflict, leader drops nextIndex for the follower and reties AppendEntries

/*
Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries)
to all followers in order to maintain their authority. If a follower receives
no communication over a period of time called the election timeout, then it
assumes there is no viable leader and begins an election to choose a new leader.
*/

/*
• Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
• If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
• If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
• If successful: update nextIndex and matchIndex for
follower (§5.3)
• If AppendEntries fails because of log inconsistency:
decrement nextIndex and retry (§5.3)
• If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
*/

func (m *RaftNode) LeaderHandle(inb ClinetNMessage) interface{} {

	switch msg := inb.Message.(type) {

	case AppendEntriesReply:
		if msg.Success {
			// If successful: update nextIndex and matchIndex for follower (§5.3)

			//msg.
		} else {
			m.nextIndex[inb.Client.addr]--
			inb.Client.send(AppendEntriesRequest{
				m.state.CurrentTerm,
				0, // ??
				0, // ??
				0, // ??
				m.state.CommitIndex,
				[]LogEntry{}, // ??
			})
		}
	case VoteRequest:

	}
	return nil // TODO
}

/*
 Each client request contains a command to be executed by the replicated state machines.
 The leader appends the command to its log as a new entry, then is- sues AppendEntries
 RPCs in parallel to each of the other servers to replicate the entry. When the entry
 has been safely replicated (as described below), the leader applies the entry to its
 state machine and returns the result of that execution to the client. If followers crash
 or run slowly, or if network packets are lost, the leader retries Append- Entries RPCs
 indefinitely (even after it has responded to the client) until all followers eventually
 store all log en- tries.
*/

// SubmitCommand client is using it to submit next command to state machine
func (m *RaftNode) SubmitCommand(payload interface{}) {

}
