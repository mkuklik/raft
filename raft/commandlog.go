package raft

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	log "github.com/sirupsen/logrus"
)

func init() {
	gob.Register(LogEntry{})

}

// LogEntry each entry contains command for state machine, and term when
//   entry was received by leader (first index is 1)
type LogEntry struct {
	Term  uint32 // leader's term
	Index uint32 // log index
	// plus:
	// command to the state machine
	Payload []byte
}

type CommandLog struct {
	data    []LogEntry
	Index   uint32
	lock    sync.Mutex
	file    *os.File
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func NewCommandLog(file *os.File) CommandLog {

	data := make([]LogEntry, 0, 100)

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
		data = append(data, entry)
	}

	return CommandLog{
		data,
		1,
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

	if n := len(this.data); n > 0 {
		return &this.data[n-1]
	}
	return nil
}

func (this *CommandLog) Has(term uint32, index uint32) bool {
	this.lock.Lock()
	defer this.lock.Unlock()

	n := len(this.data)

	if n == 0 {
		return false
	}

	for i := n - 1; i >= 0; i-- {
		if e := this.data[i]; e.Index == index && e.Term == term {
			return true
		}
	}
	return false
}

// AddEntries add new entries to the log
func (this *CommandLog) AddEntries(entries *[]LogEntry) error {
	if len(*entries) == 0 {
		return nil
	}

	this.lock.Lock()
	defer this.lock.Unlock()

	if this.file == nil { // ??? do I need it
		log.Fatal("file descriptor is missing")
	}

	var index uint32
	if this.Size() > 0 {
		index = this.data[len(this.data)-1].Index + 1
	}

	for i, entry := range *entries {
		if i == 0 {
			index = entry.Index
		} else if index != entry.Index {
			return fmt.Errorf("inconsistent log index, previous %d, current %d", index-1, entry.Index)
		}

		this.data = append(this.data, entry)
		err := this.encoder.Encode(entry)
		if err != nil {
			log.Fatal("failed to persist log entry,", err)
		}
		index++
	}
	// persist
	err := this.file.Sync()
	if err != nil {
		log.Fatalf("failed flashing log file, %s", err.Error())
	}
	return nil
}

// Append add new payload as entry; index is automatically generated, log is persisted.
func (this *CommandLog) Append(term uint32, payload []byte) (prev *LogEntry, curr *LogEntry) {

	n := len(this.data)

	this.lock.Lock()
	defer this.lock.Unlock()

	if n != 0 {
		prev = &this.data[n-1]
	}
	newLogEntry := LogEntry{term, this.Index, payload}

	this.data = append(this.data, newLogEntry)
	this.Index++
	curr = &this.data[n-1]

	// persist
	if this.file != nil {
		// append entry to file
		err := this.encoder.Encode(newLogEntry)
		if err != nil {
			log.Fatal("failed to persist log entry,", err)
		}
		err = this.file.Sync()
		if err != nil {
			log.Fatalf("failed flashing log file, %s", err.Error())
		}
	}

	return prev, curr
}

func (this *CommandLog) Size() int {
	return len(this.data)
}

func (this *CommandLog) Reload() {

	this.file.Seek(0, 0)
	this.data = this.data[:0]

	buf := bufio.NewReader(this.file)
	dec := gob.NewDecoder(buf)

	for {
		entry := LogEntry{}
		e := dec.Decode(&entry)
		if e == io.EOF {
			break
		}
		// data.Set(entry.Index, entry)
		this.data = append(this.data, entry)
	}

}

// Get get single log
func (this *CommandLog) Get(inx uint32) *LogEntry {
	n := len(this.data)
	if n == 0 || (n > 0 && (inx < this.data[0].Index || inx > this.data[n-1].Index)) {
		return nil
	}
	i := sort.Search(n, func(i int) bool { return this.data[i].Index >= inx })
	if i < 0 {
		return nil
	}
	return &this.data[i]
}

// GetRange get slice with range of data
func (this *CommandLog) GetRange(start, end uint32) ([]*LogEntry, error) {
	n := len(this.data)

	if n == 0 {
		return nil, errors.New("index out of range")
	}

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

// DropLast drop last n entries and persist
func (this *CommandLog) DropLast(n int) error {

	// get initial file size
	stats, err := this.file.Stat()
	if err != nil {
		return err
	}

	// create
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	enc := gob.NewEncoder(w)

	// write empty to figure out starting point; gob creates header
	enc.Encode(LogEntry{})
	w.Flush()
	start := buf.Len()

	// write actual data
	for i := len(this.data) - 1; i >= len(this.data)-n; i-- {
		enc.Encode(this.data[i])
	}
	// flush and figure out new size
	w.Flush()
	size := stats.Size() - int64(buf.Len()-start)

	// truncate and set new offset
	err = this.file.Truncate(size)
	this.file.Seek(size, 0)

	// drop entires from data slice
	this.data = this.data[:len(this.data)-n]

	return err
}
