package rafttcp

import "fmt"

func (m *RaftNode) CandidateHandle(inb ClinetNMessage) interface{} {

	switch m := inb.Message.(type) {
	case AppendEntriesRequest:
		fmt.Println("TODO", m.LeaderId)
	}
	return nil
}
