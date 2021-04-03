package raft

import "fmt"

type StateMachine interface {
	Apply(event []byte) error
	Snapshot() ([]byte, error)
}

type BudgerStateMachine struct{}

func (b *BudgerStateMachine) Apply(event interface{}) error {
	return fmt.Errorf("not implented yet")
}

func (b *BudgerStateMachine) Snapshot() ([]byte, error) {
	return nil, fmt.Errorf("not implented yet")
}
