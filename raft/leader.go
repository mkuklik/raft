package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
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

func (node *RaftNode) prepareLog(ctx context.Context, id int) *raftpb.AppendEntriesRequest {
	// TODO
	req := &raftpb.AppendEntriesRequest{
		Term:         node.state.CurrentTerm,
		LeaderId:     node.nodeID,
		PrevLogIndex: 0,                    // ???
		PrevLogTerm:  0,                    // ???
		Entries:      []*raftpb.LogEntry{}, // ????
		LeaderCommit: node.state.CommitIndex,
	}
	return req
}

func (node *RaftNode) sendAppendEntries(ctx context.Context, id int, req *raftpb.AppendEntriesRequest) {
	client := node.clients[id]
	reply, err := (*client).AppendEntries(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			log.Errorf("Heartbeat to %s due to, %s", node.config.Peers[id], st.Message())
		}
	} else {
		if reply.Term > node.state.CurrentTerm {
			// switch to follower
			node.state.CurrentTerm = reply.Term
			node.SwitchTo(Follower)
		}

		if !reply.Success {
			// • If AppendEntries fails because of log inconsistency:
			// decrement nextIndex and retry (§5.3)
			node.state.NextIndex[id]-- // TODO lock
			go node.sendAppendEntries(ctx, id, node.prepareLog(ctx, id))
		}

	}
}

func (node *RaftNode) sendEmptyHeartBeat(ctx context.Context) {

	emptyReq := &raftpb.AppendEntriesRequest{
		Term:         node.state.CurrentTerm,
		LeaderId:     node.nodeID,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raftpb.LogEntry{},
		LeaderCommit: node.state.CommitIndex,
	}

	for id := range node.clients {
		if id != int(node.nodeID) { // skip candidate/leader
			tx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			go func(id int) {
				node.sendAppendEntries(tx, id, emptyReq)
			}(id)
		}
	}
}

// AddLogEntry used by client to propagate log entry
func (node *RaftNode) AddCommand(payload []byte) error {

	prevLogEntry, newLogEntry := node.clog.Append(payload)
	count := make(chan int, len(node.clients))
	for _, client := range node.clients {
		go func(c *raftpb.RaftClient) {
			ctx, cancelfunc := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancelfunc()

			reply, err := (*c).AppendEntries(ctx, &raftpb.AppendEntriesRequest{
				Term:         node.state.CurrentTerm,
				LeaderId:     0,
				PrevLogIndex: prevLogEntry.Index,
				PrevLogTerm:  prevLogEntry.Term,
				Entries: []*raftpb.LogEntry{
					{
						Term:    newLogEntry.Term,
						Index:   newLogEntry.Index,
						Payload: newLogEntry.Payload,
					},
				},
				LeaderCommit: node.state.CommitIndex, // double check
			})
			if err != nil {
				// todo
			} else {
				if reply.Term != node.state.CurrentTerm {
					// todo switch to follower
				}
				if reply.Success == true {
					count <- 1
				}
				fmt.Println(reply) // todo
			}
		}(client)
	}

	majority := true
	if !majority {
		return fmt.Errorf("failed to apply command")
	}
	// if majority apply entry
	node.sm.Apply(payload)
	return nil
}

func (node *RaftNode) RunLeader(ctx context.Context) {
	log.Infof("Starting as Leader")

	// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
	// repeat during idle periods to prevent election timeouts (§5.2)
	node.sendEmptyHeartBeat(ctx)

	heartBeatTimer := time.NewTimer(node.config.HeartBeat)

	for {
		select {
		case <-ctx.Done():
			heartBeatTimer.Stop()
			return
		case <-heartBeatTimer.C:
			// node.outboundLogEntriesLock.Lock()
			for id := range node.clients {
				if id != int(node.nodeID) { // skip candidate/leader

					tx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()

					go func(id int) {
						node.sendAppendEntries(tx, id, node.prepareLog(ctx, id))
					}(id)
				}
			}

			heartBeatTimer.Reset(node.config.HeartBeat)
		default:
		}
	}
}
