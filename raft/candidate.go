package raft

import (
	"context"
	"sync"

	"github.com/mkuklik/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
)

func (node *RaftNode) RunCandidate(ctx context.Context) {
	log.Infof("Switching to Candidate and starting election")

	node.ResetElectionTimer()

	// vote for yourself
	node.state.CurrentTerm++
	node.state.VotedFor = int(node.nodeID)
	req := &raftpb.RequestVoteRequest{
		Term:         node.state.CurrentTerm,
		CandidateId:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	// send out VoteRequests
	lock := sync.Mutex{}
	vote := make(chan bool)
	for id, client := range node.clients {
		if id != int(node.nodeID) { // skip candidate/leader

			tx, cancel := context.WithTimeout(ctx, node.config.ElectionTimeout)
			defer cancel()

			go func(c *raftpb.RaftClient) {
				log.Infof("requesting vote from %d", id)
				resp, err := (*c).RequestVote(tx, req)
				if err != nil {
					st, ok := status.FromError(err)
					if ok {
						log.Errorf("RequestVote to %s due to, %s", node.config.Peers[id], st.Message())
					}
				} else {
					if resp.VoteGranted {
						log.Infof("server %d voted YES", id)
						lock.Lock()
						vote <- true
						lock.Unlock()
					} else {
						log.Infof("server %d voted NO", id)
					}
				}
			}(client)
		}
	}

	yea := 0
	for {
		select {
		case <-ctx.Done():
			node.StopElectionTimer()
			return
		case <-vote:
			yea++
			log.Infof("Checking majority, %d >= %d", yea, len(node.config.Peers)/2)
			if yea >= len(node.config.Peers)/2 {
				node.SwitchTo(Leader)
				return
			}
		case <-node.electionTimeoutTimer.C:
			// start new election
			node.SwitchTo(Candidate)
			return
		default:
		}
	}
}
