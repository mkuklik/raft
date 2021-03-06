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
	t.Cleanup(func() {
		// file.Close()
		err := os.Remove(file.Name())
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	clog := NewCommandLog(file)
	clogEmpty := NewCommandLog(file)

	payload1 := []byte("111")
	payload2 := []byte("22222")
	payload3 := []byte("33")
	payload4 := []byte("4444444444")

	t.Run("append and sync", func(t *testing.T) {

		tmp, _ := file.Stat()
		before := tmp.Size()
		var term uint32 = 1

		clog.Append(term, payload1)

		tmp, _ = file.Stat()
		after := tmp.Size()

		if before == after {
			t.Errorf("size shouldnt be the same")
		}

		prev, curr := clog.Append(term, payload2)
		if prev.Index != curr.Index-1 {
			t.Errorf("wrong index: prev %v, curr: %v", prev.Index, curr.Index)

		}
		prev, curr = clog.Append(term, payload3)
		if prev.Index != curr.Index-1 {
			t.Errorf("wrong index: prev %v, curr: %v", prev.Index, curr.Index)

		}
		clog.Append(term, payload4)

	})

	t.Run("check loading", func(t *testing.T) {

		file.Seek(0, 0)

		clog := NewCommandLog(file)

		if clog.Size() != 4 {
			t.Errorf("failed loading data")
		}

		if clog.Last().Index != 4 {
			t.Errorf("wrong index: got %v, want: %v", clog.Last().Index, 4)
		}

		if bytes.Equal(clog.Last().Payload, payload2) {
			t.Errorf("wrong payload: got %v, want: %v", clog.Last().Payload, payload2)
		}

		got := clog.Get(1).Payload
		want := payload1
		if !bytes.Equal(got, want) {
			t.Errorf("wrong payload: got %v, want: %v", got, want)
		}

	})

	t.Run("check get", func(t *testing.T) {
		got := clog.Get(1)
		if got.Index != 1 {
			t.Errorf("wrong index, got %q, want %q", got.Index, 1)
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
			t.Errorf("wrong index, got %q, want %q", got[0].Index, 1)
		}
		if !bytes.Equal(got[0].Payload, payload1) {
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

	t.Run("check range, errors", func(t *testing.T) {
		_, err := clogEmpty.GetRange(1, 3)
		if err == nil {
			t.Error("didn't get index error")
		}

		_, err = clog.GetRange(3, 1)
		if err == nil {
			t.Error("didn't get index error")
		}
		_, err = clog.GetRange(0, uint32(len(clog.data)+10))
		if err == nil {
			t.Error("didn't get index error")
		}
	})

	t.Run("check DropLast", func(t *testing.T) {
		n := clog.Size()

		clog.DropLast(1)

		if clog.Size() != n-1 {
			t.Errorf("faield to drop last entry, got %q, want %q", clog.Size(), n-1)
		}

		clog.Reload()

		if clog.Size() != n-1 {
			t.Errorf("faield to drop last entry, got %q, want %q", clog.Size(), n-1)
		}
		last := clog.Last()
		if !bytes.Equal(last.Payload, payload3) {
			t.Errorf("failed Dropping last")
		}
	})

	t.Run("check DropLast 2", func(t *testing.T) {
		n := clog.Size()

		clog.DropLast(2)

		if clog.Size() != n-2 {
			t.Errorf("faield to drop last entry, got %q, want %q", clog.Size(), n-2)
		}

		clog.Reload()

		if clog.Size() != n-2 {
			t.Errorf("faield to drop last entry, got %d, want %d", clog.Size(), n-2)
		}
		last := clog.Last()
		if !bytes.Equal(last.Payload, payload1) {
			t.Errorf("failed Dropping last")
		}
	})
}
