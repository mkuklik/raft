package raft

import (
	"context"
	"fmt"
	"time"

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
	electionTimeoutTimer := time.NewTicker(node.config.ElectionTimeout)
	for {
		select {
		case <-ctx.Done():
			return
		case <-electionTimeoutTimer.C:
			// if time.Now().After(node.state.broadcastTime.Add(node.config.ElectionTimeout)) {
			// 	node.SwitchTo(Candidate)
			// }
		default:
		}
	}
}
