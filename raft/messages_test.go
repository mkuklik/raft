package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
)

type TestPayload struct {
	A string
	B int
}

func init() {
	gob.Register(TestPayload{})
	gob.Register(Packet{})
	gob.Register(VoteRequest{})
	gob.Register(VoteReply{})
	gob.Register(AppendEntriesRequest{})
	gob.Register(AppendEntriesReply{})
}

func TestMessageEncodingDecoding(t *testing.T) {

	t.Run("VoteRequest", func(t *testing.T) {
		want := VoteRequest{1, 2, 3, 4}

		var network bytes.Buffer        // Stand-in for a network connection
		enc := gob.NewEncoder(&network) // Will write to network.
		dec := gob.NewDecoder(&network) // Will read from network.
		// Encode (send) the value.
		err := enc.Encode(want)
		if err != nil {
			t.Fatal("encode error:", err)
		}
		// Decode (receive) the value.
		var got VoteRequest
		err = dec.Decode(&got)
		if err != nil {
			t.Fatal("decode error:", err)
		}

		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	})

	t.Run("VoteRequest2", func(t *testing.T) {
		want := VoteRequest{1, 2, 3, 4}
		want2 := VoteRequest{11, 12, 13, 14}

		var network bytes.Buffer        // Stand-in for a network connection
		enc := gob.NewEncoder(&network) // Will write to network.
		dec := gob.NewDecoder(&network) // Will read from network.
		// Encode (send) the value.
		err := enc.Encode(want)
		err = enc.Encode(want2)
		if err != nil {
			t.Fatal("encode error:", err)
		}
		// Decode (receive) the value.
		var got VoteRequest
		err = dec.Decode(&got)
		if err != nil {
			t.Fatal("decode error:", err)
		}
		var got2 VoteRequest
		err = dec.Decode(&got2)
		if err != nil {
			t.Fatal("decode error:", err)
		}

		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
		if got2 != want2 {
			t.Errorf("got %q want %q", got2, want2)
		}
	})

	t.Run("VoteReply", func(t *testing.T) {
		want := VoteReply{10, true}

		var network bytes.Buffer        // Stand-in for a network connection
		enc := gob.NewEncoder(&network) // Will write to network.
		dec := gob.NewDecoder(&network) // Will read from network.
		// Encode (send) the value.
		err := enc.Encode(want)
		if err != nil {
			t.Fatal("encode error:", err)
		}
		// Decode (receive) the value.
		var got VoteReply
		err = dec.Decode(&got)
		if err != nil {
			t.Fatal("decode error:", err)
		}
		if got != want {
			t.Errorf("got %#v want %#v", got, want)
		}
	})

	t.Run("AppendEntriesRequest", func(t *testing.T) {

		log1 := LogEntry{1, 2, TestPayload{"y", 55}}
		log2 := LogEntry{11, 21, TestPayload{"x", 66}}
		want := AppendEntriesRequest{1, 2, 3, 4, 5, []LogEntry{log1, log2}}

		var network bytes.Buffer        // Stand-in for a network connection
		enc := gob.NewEncoder(&network) // Will write to network.
		dec := gob.NewDecoder(&network) // Will read from network.
		// Encode (send) the value.
		err := enc.Encode(Packet{want})
		if err != nil {
			t.Fatal("encode error:", err)
		}
		// Decode (receive) the value.
		tmp := Packet{}
		var got AppendEntriesRequest
		err = dec.Decode(&tmp)
		if err != nil {
			t.Fatal("decode error:", err)
		}
		got = tmp.Message.(AppendEntriesRequest)

		if got.Entries[0] != want.Entries[0] || got.Entries[1] != want.Entries[1] ||
			got.LeaderCommit != want.LeaderCommit || got.LeaderId != want.LeaderId ||
			got.PrevLogIndex != want.PrevLogIndex || got.PrevLogTerm != want.PrevLogTerm {
			t.Errorf("got %v want %v", got, want)
		}
	})

	t.Run("AppendEntriesReply", func(t *testing.T) {
		want := AppendEntriesReply{10, true}

		var network bytes.Buffer        // Stand-in for a network connection
		enc := gob.NewEncoder(&network) // Will write to network.
		dec := gob.NewDecoder(&network) // Will read from network.
		// Encode (send) the value.
		err := enc.Encode(want)
		if err != nil {
			t.Fatal("encode error:", err)
		}
		// Decode (receive) the value.
		var got AppendEntriesReply
		err = dec.Decode(&got)
		if err != nil {
			t.Fatal("decode error:", err)
		}

		if got != want {
			t.Errorf("got %v want %v", got, want)
		}
	})
}

func TestStream(t *testing.T) {

	t.Run("Stream1", func(t *testing.T) {
		want := VoteRequest{1, 2, 3, 4}
		want2 := VoteReply{11, true}

		var network bytes.Buffer // Stand-in for a network connection

		enc := gob.NewEncoder(&network) // Will write to network.
		// Encode (send) the value.
		err := enc.Encode(Packet{want})
		// err := enc.Encode(want)
		if err != nil {
			t.Fatal("encode error:", err)
		}
		err = enc.Encode(Packet{want2})
		// err = enc.Encode(want2)
		if err != nil {
			t.Fatal("encode error:", err)
		}

		dec := gob.NewDecoder(&network) // Will read from network.

		msg1 := Packet{}
		err = dec.Decode(&msg1)
		if err != nil {
			t.Fatal("decoding error:", err)
		}
		got := msg1.Message
		if got != want {
			t.Errorf("got %v want %v", got, want)
		}

		msg2 := Packet{}
		err = dec.Decode(&msg2)
		var got2 VoteReply
		got2 = msg2.Message.(VoteReply)

		if got2 != want2 {
			t.Errorf("got %q want %q", got, want)
		}
	})

}
