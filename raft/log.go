package raft

import (
	"bufio"
	"encoding/gob"
	"io"
	"os"
	"sync"

	"github.com/elliotchance/orderedmap"
	log "github.com/sirupsen/logrus"
)

func init() {
	gob.Register(LogEntry{})

}

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
	// log   []LogEntry
	log     *orderedmap.OrderedMap
	Term    uint32
	Index   uint32
	lock    sync.Mutex
	file    io.ReadWriter
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func NewCommandLog(file *os.File) CommandLog {

	data := orderedmap.NewOrderedMap()

	// load
	buf := bufio.NewReader(file)
	// load entries
	dec := gob.NewDecoder(buf)
	for {
		entry := LogEntry{}
		e := dec.Decode(&entry)
		if e == io.EOF {
			break
		}
		data.Set(entry.Index, entry)
	}

	return CommandLog{
		// make([]LogEntry, 0, 100),
		data,
		0,
		0,
		sync.Mutex{},
		file,
		gob.NewEncoder(file),
		dec,
	}
}

// Last Entry in the Log
func (this *CommandLog) Last() *LogEntry {
	this.lock.Lock()
	defer this.lock.Unlock()

	if this.log.Len() > 0 {
		return this.log.Back().Value.(*LogEntry)
	}
	// if n := len(l.log); n > 0 {
	// 	return &l.log[n-1]
	// }
	return nil
}

func (this *CommandLog) Has(term uint32, index uint32) bool {
	this.lock.Lock()
	defer this.lock.Unlock()

	_, ok := this.log.Get(index)
	return ok
	// // change to map lookup
	// for i := len(l.log); i >= 0; i++ {
	// 	if e := l.log[i]; e.Index == index && e.Term == term {
	// 		return true
	// 	}
	// }
	// return false
}

// AddEntries add new entries to the log
func (this *CommandLog) AddEntries(entries *[]LogEntry) bool {
	this.lock.Lock()
	defer this.lock.Unlock()

	// TODO add entries that don't exist and skip oen already there
	return false
}

// Append add new payload as entry; index is automatically generated
func (this *CommandLog) Append(payload []byte) (*LogEntry, *LogEntry) {
	this.lock.Lock()
	defer this.lock.Unlock()

	prevLogEntry := this.log.Back().Value.(LogEntry)
	newLogEntry := LogEntry{this.Term, this.Index, payload}

	this.log.Set(this.Index, newLogEntry)
	this.Index++
	last := this.log.Back().Value.(LogEntry)

	// persist
	if this.file != nil {
		// append entry to file
		err := this.encoder.Encode(newLogEntry)
		if err != nil {
			log.Fatal("failed to persist log entry,", err)
		}
		// todo flash file
	}

	return &prevLogEntry, &last

	// prevLogEntry := l.log[len(l.log)-1]
	// l.Index++
	// e := LogEntry{l.Term, l.Index, payload}
	// l.log = append(l.log, e)
	// return &prevLogEntry, &e
}
func (this *CommandLog) Get(inx uint32) *LogEntry {
	return nil
}

func (this *CommandLog) GetRange(start, end uint32) []*LogEntry {
	return []*LogEntry{}
}
