package raft

import (
	"sync"
)

// type Payloader interface {
// 	encoding.BinaryMarshaler
// 	encoding.BinaryUnmarshaler
// }

// LogEntry each entry contains command for state machine, and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Term  uint32 // leader's term
	Index uint32 // log index
	// plus:
	// command to the state machine
	Payload []byte
}

type CommandLog struct {
	log   []LogEntry
	Term  uint32
	Index uint32
	lock  sync.Mutex
}

func NewCommandLog() CommandLog {
	return CommandLog{
		make([]LogEntry, 0, 100),
		0,
		0,
		sync.Mutex{},
	}
}

func (l *CommandLog) Append(payload []byte) (*LogEntry, *LogEntry) {
	l.lock.Lock()
	defer l.lock.Unlock()
	prevLogEntry := l.log[len(l.log)-1]
	l.Index++
	e := LogEntry{l.Term, l.Index, payload}
	l.log = append(l.log, e)
	return &prevLogEntry, &e
}
