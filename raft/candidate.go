package raft

import "fmt"

func (m *Manager) CandidateHandle(inb InboundType) interface{} {

	switch m := inb.Message.(type) {
	case AppendEntriesRequest:
		fmt.Println("TODO", m.LeaderId)
	}
	return nil
}
