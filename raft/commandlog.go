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

	if file == nil {
		panic("command log file is nil")
	}

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
func (cl *CommandLog) Last() *LogEntry {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	if n := len(cl.data); n > 0 {
		return &cl.data[n-1]
	}
	return nil
}

func (cl *CommandLog) Has(term uint32, index uint32) bool {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	n := len(cl.data)

	if n == 0 {
		return false
	}

	for i := n - 1; i >= 0; i-- {
		if e := cl.data[i]; e.Index == index && e.Term == term {
			return true
		}
	}
	return false
}

// AddEntries add new entries to the log
func (cl *CommandLog) AddEntries(entries *[]LogEntry) error {
	if len(*entries) == 0 {
		return nil
	}

	cl.lock.Lock()
	defer cl.lock.Unlock()

	if cl.file == nil { // ??? do I need it
		log.Fatal("file descriptor is missing")
	}

	var index uint32
	if cl.Size() > 0 {
		index = cl.data[len(cl.data)-1].Index + 1
	}

	for i, entry := range *entries {
		if i == 0 {
			index = entry.Index
		} else if index != entry.Index {
			return fmt.Errorf("inconsistent log index, previous %d, current %d", index-1, entry.Index)
		}

		cl.data = append(cl.data, entry)
		err := cl.encoder.Encode(entry)
		if err != nil {
			log.Fatal("failed to persist log entry,", err)
		}
		index++
	}
	// persist
	err := cl.file.Sync()
	if err != nil {
		return fmt.Errorf("failed flashing log file, %s", err.Error())
	}
	return nil
}

// Append add new payload as entry; index is automatically generated, log is persisted.
func (cl *CommandLog) Append(term uint32, payload []byte) (prev *LogEntry, curr *LogEntry) {

	n := len(cl.data)

	cl.lock.Lock()
	defer cl.lock.Unlock()

	if n != 0 {
		prev = &cl.data[n-1]
	}
	newLogEntry := LogEntry{term, cl.Index, payload}

	cl.data = append(cl.data, newLogEntry)
	cl.Index++
	curr = &cl.data[len(cl.data)-1]

	// persist
	if cl.file != nil {
		// append entry to file
		err := cl.encoder.Encode(newLogEntry)
		if err != nil {
			log.Fatal("failed to persist log entry,", err)
		}
		err = cl.file.Sync()
		if err != nil {
			log.Fatalf("failed flashing log file, %s", err.Error())
		}
	}

	return prev, curr
}

func (cl *CommandLog) Size() int {
	return len(cl.data)
}

func (cl *CommandLog) Reload() {

	cl.file.Seek(0, 0)
	cl.data = cl.data[:0]

	buf := bufio.NewReader(cl.file)
	dec := gob.NewDecoder(buf)

	for {
		entry := LogEntry{}
		e := dec.Decode(&entry)
		if e == io.EOF {
			break
		}
		// data.Set(entry.Index, entry)
		cl.data = append(cl.data, entry)
	}

}

// Get get single log
func (cl *CommandLog) Get(inx uint32) *LogEntry {
	n := len(cl.data)
	if n == 0 || (n > 0 && (inx < cl.data[0].Index || inx > cl.data[n-1].Index)) {
		return nil
	}
	i := sort.Search(n, func(i int) bool { return cl.data[i].Index >= inx })
	if i < 0 {
		return nil
	}
	return &cl.data[i]
}

// GetRange get slice with range of data
func (cl *CommandLog) GetRange(start, end uint32) ([]*LogEntry, error) {
	n := len(cl.data)

	if n == 0 {
		return nil, errors.New("index out of range")
	}

	if start > end {
		return nil, errors.New("invalid input, start > end index")
	}

	iStart := sort.Search(len(cl.data), func(i int) bool { return cl.data[i].Index >= start })
	if iStart < 0 {
		return nil, errors.New("index out of range")
	}
	iEnd := sort.Search(len(cl.data), func(i int) bool { return cl.data[i].Index >= end })
	if iEnd == len(cl.data) {
		return nil, errors.New("index out of range")
	}
	out := make([]*LogEntry, iEnd-iStart+1)
	for i := iStart; i <= iEnd; i++ {
		out[i-iStart] = &cl.data[i]
	}
	return out, nil
}

func (cl *CommandLog) DropFrom(inx uint32) error {
	return nil
}

// DropLast drop last n entries and persist
func (cl *CommandLog) DropLast(n int) error {

	// get initial file size
	stats, err := cl.file.Stat()
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
	for i := len(cl.data) - 1; i >= len(cl.data)-n; i-- {
		enc.Encode(cl.data[i])
	}
	// flush and figure out new size
	w.Flush()
	size := stats.Size() - int64(buf.Len()-start)

	// truncate and set new offset
	err = cl.file.Truncate(size)
	cl.file.Seek(size, 0)

	// drop entires from data slice
	cl.data = cl.data[:len(cl.data)-n]

	return err
}
