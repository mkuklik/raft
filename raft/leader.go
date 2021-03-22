package raft

import (
	"context"
	"fmt"
	"time"

	pb "github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
)

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

func (node *RaftNode) RunLeader(ctx context.Context) {
	log.Infof("Starting as Leader")

	heartBeatTimer := time.NewTicker(node.config.HeartBeat)

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartBeatTimer.C:
			node.outboundLogEntriesLock.Lock()
			for _, client := range node.clients {
				ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				go func() {
					reply, err := client.AppendEntries(ctx2, &pb.AppendEntriesRequest{
						Term:         node.state.CurrentTerm,
						LeaderId:     0,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      []*pb.LogEntry{},
						LeaderCommit: 0,
					})
					if err != nil {
						// todo
					}
					if reply.Term > node.state.CurrentTerm {
						// switch to follower
						node.state.CurrentTerm = reply.Term
						node.SwitchTo(Follower)
					}
					fmt.Println(reply) // todo
				}()
			}
		default:
		}
	}
}

// AddLogEntry used by client to propagate log entry
func (node *RaftNode) AddCommand(payload []byte) error {

	prevLogEntry, newLogEntry := node.clog.Append(payload)
	count := make(chan int, len(node.clients))
	for _, client := range node.clients {
		go func() {
			ctx, cancelfunc := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancelfunc()

			reply, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
				Term:         node.state.CurrentTerm,
				LeaderId:     0,
				PrevLogIndex: prevLogEntry.Index,
				PrevLogTerm:  prevLogEntry.Term,
				Entries: []*pb.LogEntry{
					{
						Term:    newLogEntry.Term,
						Index:   newLogEntry.Index,
						Payload: newLogEntry.Payload,
					},
				},
				LeaderCommit: 0,
			})
			if err != nil {
				// todo
			}
			if reply.Term != node.state.CurrentTerm {
				// todo switch to follower
			}
			if reply.Success == true {
				count <- 1
			}
			fmt.Println(reply) // todo
		}()
	}

	majority := true
	if !majority {
		return fmt.Errorf("failed to apply command")
	}
	// if majority apply entry
	node.sm.Apply(payload)
	return nil
}
