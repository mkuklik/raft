package raft

type StateMachineEvent interface{}

type StateMachine interface {
	Apply(event StateMachineEvent) bool
	Current() interface{}
}
