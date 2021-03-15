package raft

import "time"

type Manager struct {
	config *Config
	state  State
	sm     StateMachine
}

func NewManager(config *Config, sm StateMachine) Manager {
	return Manager{config, State{}, sm}
}

func (m *Manager) Loop(chan int) {
	timer := time.NewTimer(2 * time.Second)
	for {
		select {
		case <-timer.C:
			if time.Now().After(m.state.broadcastTime.Add(m.config.ElectionTimeout)) {
				m.SwitchToCandidate()
			}

			// case
			// TODO
		}
	}
}

func (m *Manager) SwitchToCandidate() {

	return
}
