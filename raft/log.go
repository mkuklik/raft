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

// Last Entry in the Log
func (l *CommandLog) Last() *LogEntry {
	l.lock.Lock()
	defer l.lock.Unlock()

	if n := len(l.log); n > 0 {
		return &l.log[n-1]
	}
	return nil
}

func (l *CommandLog) Has(term uint32, index uint32) bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	// change to map lookup
	for i := len(l.log); i >= 0; i++ {
		if e := l.log[i]; e.Index == index && e.Term == term {
			return true
		}
	}
	return false
}

// AddEntries add new entries to the log
func (l *CommandLog) AddEntries(entries *[]LogEntry) bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	// TODO add entries that don't exist and skip oen already there
	return false
}

// Append add new payload as entry; index is automatically generated
func (l *CommandLog) Append(payload []byte) (*LogEntry, *LogEntry) {
	l.lock.Lock()
	defer l.lock.Unlock()

	prevLogEntry := l.log[len(l.log)-1]
	l.Index++
	e := LogEntry{l.Term, l.Index, payload}
	l.log = append(l.log, e)
	return &prevLogEntry, &e
}
