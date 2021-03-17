package raft

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

type Manager struct {
	config *Config

	nodeStatus NodeStatus
	state      State
	sm         StateMachine

	connections map[string]net.Conn
	inbound     chan Message
}

func NewManager(config *Config, sm StateMachine) Manager {
	return Manager{
		config,
		Follower,
		State{},
		sm,
		make(map[string]net.Conn),
		make(chan Message)}
}

func (m *Manager) Loop(chan int) {
	timer := time.NewTimer(2 * time.Second)
	for {
		select {
		case <-timer.C:
			if time.Now().After(m.state.broadcastTime.Add(m.config.ElectionTimeout)) {
				m.SwitchToCandidate()
			}
		case msg := <-m.inbound:

			switch m.nodeStatus {
			case Follower:
				m.state.FollowerHandle(msg)
			case Leader:
				m.state.LeaderHandle(msg)
			case Candidate:
				m.state.CandidateHandle(msg)
			}
			// case
			// TODO
		}
	}
}

func (m *Manager) SwitchToCandidate() {
	m.nodeStatus = Candidate
}

func (m *Manager) broadcast(msg Message) error {
	// for name, c := range m.clients {
	// 	c.conn
	// }
	return nil
}

var count = 0

func (m *Manager) handleConnection(c net.Conn) {
	dec := gob.NewDecoder(c)
	for {
		msg := Message{}
		err := dec.Decode(&msg)
		if err == io.EOF {
			// TOOD close connection or try to reconnect
			log.Errorf("Decode failed, %s", err.Error())
			break
		}
		m.inbound <- msg
	}
	// TODO regegister connection
	// m.deregister(c)
	c.Close()
}

func connectToLeader(address string) (net.Conn, error) {
	var err error
	var c net.Conn
	// Find leader
	for {
		log.Infof("Dialing %s", address)
		c, err = net.Dial("tcp", address)
		if err != nil {
			log.Errorf("Dialing failed")
			fmt.Println(err)
			return nil, err
		}
		// send registration request
		enc := gob.NewEncoder(c)
		enc.Encode(RegistrationRequest{"Jon"})

		dec := gob.NewDecoder(c)
		replyMsg := Message{}
		err = dec.Decode(&replyMsg)
		if err != nil {
			log.Errorf("No response failed")
		}
		reply := replyMsg.Message.(RegistrationReply)
		if reply.Success {
			// TODO
		} else if reply.Address != "" {
			address = reply.Address
		}
	}
	return c, nil
}

func (m *Manager) run(address string) {

	leaderConn, err := connectToLeader(address)
	if err != nil {
		log.Fatalf("failed to find a leader")
	}
	m.RegisterConn("", leaderConn)
	go m.handleConnection(leaderConn)

	port := 1234
	// add := address
	add := ":" + strconv.Itoa(port)
	l, err := net.Listen("tcp", add)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Errorf("conn Accept failed, %s", err.Error())
		}
		go m.handleConnection(c)
	}
}

type client struct {
	conn net.Conn
	name string
}

func (c *client) send(msg Message) error {
	enc := gob.NewEncoder(c.conn)
	return enc.Encode(msg)
}

func (m *Manager) RegisterConn(name string, c net.Conn) {
	if _, exists := m.connections[name]; !exists {
		m.connections[name] = c
	}
}
