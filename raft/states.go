package raft

type NodeStatus uint

const (
	Follower NodeStatus = iota
	Candidate
	Leader
)

LeaderInterface interface {}
CandidateInterface interface {}

// Persistent state on all servers:
type PersistantState struct {
	// (Updated on stable storage before responding to RPCs)
	CurrentTerm uint32     // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor               // candidateId that received vote in current term (or null if none)
	Log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
}

// VolatileState Volatile state on all servers:
type VolatileState struct {
	CommitIndex uint32 //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied uint32 // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
}

// LeaderState Volatile state on leaders:
type LeaderState struct {
	// (Reinitialized after election)
	NextIndex  []uint32 // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex []uint32 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}
