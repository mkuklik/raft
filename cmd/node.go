package main

import (
	"fmt"

	raft "github.com/mkuklik/raft/raft"
)

type StateM struct{}
type StateEvent struct{}

func (sm StateM) Apply(event raft.StateMachineEvent) bool {
	return false
}

func (sm StateM) Current() interface{} {
	return StateM{}
}

func main() {
	sm := StateM{}
	config := raft.NewConfig()
	mgr := raft.NewManager(&config, sm)
	fmt.Println(mgr)
}
