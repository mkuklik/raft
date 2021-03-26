package raft

import (
	"bufio"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sort"
	"sync"

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
	data []LogEntry
	// log     *orderedmap.OrderedMap
	Term    uint32
	Index   uint32
	lock    sync.Mutex
	file    *os.File
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func NewCommandLog(file *os.File) CommandLog {

	data := make([]LogEntry, 0, 100)
	// data := orderedmap.NewOrderedMap()

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
		// data.Set(entry.Index, entry)
		data = append(data, entry)
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

	// if this.log.Len() > 0 {
	// 	return this.log.Back().Value.(*LogEntry)
	// }
	if n := len(this.data); n > 0 {
		return &this.data[n-1]
	}
	return nil
}

func (this *CommandLog) Has(term uint32, index uint32) bool {
	this.lock.Lock()
	defer this.lock.Unlock()

	// _, ok := this.log.Get(index)
	// return ok
	// change to map lookup
	for i := len(this.data); i >= 0; i++ {
		if e := this.data[i]; e.Index == index && e.Term == term {
			return true
		}
	}
	return false
}

// AddEntries add new entries to the log
func (this *CommandLog) AddEntries(entries *[]LogEntry) bool {
	this.lock.Lock()
	defer this.lock.Unlock()

	// this.log.

	// TODO add entries that don't exist and skip oen already there
	return false
}

// Append add new payload as entry; index is automatically generated, log is persisted.
func (this *CommandLog) Append(payload []byte) (prev *LogEntry, curr *LogEntry) {
	this.lock.Lock()
	defer this.lock.Unlock()

	// prevLogEntry := this.log.Back().Value.(LogEntry)
	// newLogEntry := LogEntry{this.Term, this.Index, payload}

	// this.log.Set(this.Index, newLogEntry)
	// this.Index++
	// last := this.log.Back().Value.(LogEntry)

	if len(this.data) != 0 {
		prev = &this.data[len(this.data)-1]
	}
	newLogEntry := LogEntry{this.Term, this.Index, payload}

	this.data = append(this.data, newLogEntry)
	this.Index++
	curr = &this.data[len(this.data)-1]

	// persist
	if this.file != nil {
		// append entry to file
		err := this.encoder.Encode(newLogEntry)
		if err != nil {
			log.Fatal("failed to persist log entry,", err)
		}
		this.file.Sync()
		// todo flash file
	}

	return prev, curr
}

func (this *CommandLog) Size() int {
	return len(this.data)
}

// Get get single log
func (this *CommandLog) Get(inx uint32) *LogEntry {
	if len(this.data) > 0 && (inx < this.data[0].Index || inx > this.data[len(this.data)-1].Index) {
		return nil
	}
	i := sort.Search(len(this.data), func(i int) bool { return this.data[i].Index >= inx })
	if i < 0 {
		return nil
	}
	return &this.data[i]
}

// GetRange get slice with range of data
func (this *CommandLog) GetRange(start, end uint32) ([]*LogEntry, error) {
	if start > end {
		return nil, errors.New("invalid input, start > end index")
	}

	iStart := sort.Search(len(this.data), func(i int) bool { return this.data[i].Index >= start })
	if iStart < 0 {
		return nil, errors.New("index out of range")
	}
	iEnd := sort.Search(len(this.data), func(i int) bool { return this.data[i].Index >= end })
	if iEnd == len(this.data) {
		return nil, errors.New("index out of range")
	}
	out := make([]*LogEntry, iEnd-iStart+1)
	for i := iStart; i <= iEnd; i++ {
		out[i-iStart] = &this.data[i]
	}
	return out, nil
}

func (this *CommandLog) DropFrom(inx uint32) error {
	return nil
}

// GetRange get slice with range of data
func (this *CommandLog) DropLast(n uint) error {
	return nil
	// func Truncate(name string, size int64) error

	// this.file.Truncate()

	// if start > end {
	// 	return nil, errors.New("invalid input, start > end index")
	// }

	// iStart := sort.Search(len(this.data), func(i int) bool { return this.data[i].Index >= start })
	// if iStart < 0 {
	// 	return nil, errors.New("index out of range")
	// }
	// iEnd := sort.Search(len(this.data), func(i int) bool { return this.data[i].Index >= end })
	// if iEnd == len(this.data) {
	// 	return nil, errors.New("index out of range")
	// }
	// out := make([]*LogEntry, iEnd-iStart+1)
	// for i := iStart; i <= iEnd; i++ {
	// 	out[i-iStart] = &this.data[i]
	// }
	// return out, nil
}
