package raft

import "fmt"

func (state *State) CandidateHandle(msg interface{}) interface{} {

	switch m := msg.(type) {
	case AppendEntriesRequest:
		fmt.Println("TODO", m.LeaderId)
	}
	return nil
}
