package raft

import (
	"context"
	"fmt"
	"sync"

	"github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
)

func (m *RaftNode) CandidateHandle(inb ClinetNMessage) interface{} {

	switch m := inb.Message.(type) {
	case AppendEntriesRequest:
		fmt.Println("TODO", m.LeaderId)
	}
	return nil
}

func (node *RaftNode) RunCandidate(ctx context.Context) {
	log.Infof("Starting as Candidate")

	node.ResetElectionTimer()

	// vote for yourself
	node.state.CurrentTerm++
	node.state.VotedFor = node.nodeID
	req := &raftpb.RequestVoteRequest{
		Term:         node.state.CurrentTerm,
		CandidateId:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	// send out VoteRequests
	lock := sync.Mutex{}
	yea := 0
	for _, client := range node.clients {
		tx, cancel := context.WithTimeout(ctx, node.config.ElectionTimeout)
		defer cancel()
		go func() {
			resp, err := client.RequestVote(tx, req)
			if err != nil {
				log.Errorf("RequestVote err, %s", err.Error())
			} else {
				if resp.VoteGranted {
					lock.Lock()
					yea++
					lock.Unlock()
				}
			}
			if yea > len(node.config.Peers)/2 {
				// won by majority
				return
			}
		}()
	}

	if yea > len(node.config.Peers)/2 {

	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-node.electionTimeoutTimer.C:
			// if time.Now().After(node.state.broadcastTime.Add(node.config.ElectionTimeout)) {
			// 	node.SwitchTo(Candidate)
			// }
		default:
		}
	}
}
