package raft

import (
	"encoding"
	"sync"
)

type Payloader interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// LogEntry each entry contains command for state machine, and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Term  uint32 // leader's term
	Index uint32 // log index
	// plus:
	// command to the state machine
	Payload interface{} //Payloader
}

type CommandLog struct {
	log   []LogEntry
	Term  uint32
	Index uint32
	lock  sync.Mutex
}

func NewCommandLog(term uint32) CommandLog {
	return CommandLog{
		make([]LogEntry, 0, 100),
		term,
		0,
		sync.Mutex{},
	}
}

func (l *CommandLog) AddCommand(payload interface{}) uint32 {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.Index++
	l.log = append(l.log, LogEntry{l.Term, l.Index, payload})
	return l.Index
}
