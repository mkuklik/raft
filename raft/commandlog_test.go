package raft

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestCommandLog(t *testing.T) {

	file, err := ioutil.TempFile(".", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	clog := NewCommandLog(file)

	payload1 := []byte("111")
	payload2 := []byte("222")
	payload3 := []byte("333")
	payload4 := []byte("444")

	t.Run("append and sync", func(t *testing.T) {

		tmp, _ := file.Stat()
		before := tmp.Size()

		clog.Append(payload1)

		tmp, _ = file.Stat()
		after := tmp.Size()

		if before == after {
			t.Errorf("size shouldnt be the same")
		}

		prev, curr := clog.Append(payload2)
		if prev.Index != curr.Index-1 {
			t.Errorf("wrong index: prev %v, curr: %v", prev.Index, curr.Index)

		}
		prev, curr = clog.Append(payload3)
		if prev.Index != curr.Index-1 {
			t.Errorf("wrong index: prev %v, curr: %v", prev.Index, curr.Index)

		}
		clog.Append(payload4)
	})

	t.Run("check loading", func(t *testing.T) {

		file.Seek(0, 0)

		clog := NewCommandLog(file)

		if clog.Size() != 4 {
			t.Errorf("failed loading data")
		}

		if clog.Last().Index != 3 {
			t.Errorf("wrong index: got %v, want: %v", clog.Last().Index, 1)
		}

		if bytes.Equal(clog.Last().Payload, payload2) {
			t.Errorf("wrong payload: got %v, want: %v", clog.Last().Payload, payload2)
		}

		got := clog.Get(0).Payload
		want := payload1
		if !bytes.Equal(got, want) {
			t.Errorf("wrong payload: got %v, want: %v", got, want)
		}

	})

	t.Run("check get", func(t *testing.T) {
		got := clog.Get(0)
		if got.Index != 0 {
			t.Errorf("wrong index, got %q, want %q", got.Index, 0)
		}
		if !bytes.Equal(got.Payload, payload1) {
			t.Errorf("wrong payload: got %v, want: %v", got.Payload, payload1)
		}
		got = clog.Get(1000)
		if got != nil {
			t.Errorf("get should return nil, instead got %#v", got)
		}
	})

	t.Run("check range", func(t *testing.T) {
		got, _ := clog.GetRange(1, 2)
		if len(got) != 2 {
			t.Errorf("wrong length of range , got %q, want %q", len(got), 2)
		}
		if got[0].Index != 1 {
			t.Errorf("wrong index, got %q, want %q", got[0].Index, 0)
		}
		if !bytes.Equal(got[0].Payload, payload2) {
			t.Errorf("wrong payload: got %v, want: %v", got[0].Payload, payload1)
		}

		_, err = clog.GetRange(1, 2000)
		if err == nil {
			t.Errorf("range should return error")
		}

		_, err = clog.GetRange(1000, 2000)
		if err == nil {
			t.Errorf("range should return error")
		}

	})

	// got := Hello("Chris")
	// want := "Hello, Chris"

	// if got != want {
	//     t.Errorf("got %q want %q", got, want)
	// }
}
