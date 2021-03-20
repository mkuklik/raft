package raft

import (
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

type Manager struct {
	config *Config

	nodeStatus NodeStatus
	state      State
	sm         StateMachine

	leader *client

	clients  map[string]*client
	inbound  chan interface{}
	outbound chan interface{}
}

func NewManager(config *Config, sm StateMachine) Manager {
	return Manager{
		config,
		Follower,
		State{},
		sm,
		nil, // leader
		make(map[string]*client),
		make(chan interface{}),
		make(chan interface{}),
	}
}

func (m *Manager) Loop() {
	electionTimeoutTimer := time.NewTicker(m.config.ElectionTimeout)
	heartBeatTimer := time.NewTicker(m.config.HeartBeat)

	for {
		switch m.nodeStatus {
		case Follower:
			select {
			case <-electionTimeoutTimer.C:
				if time.Now().After(m.state.broadcastTime.Add(m.config.ElectionTimeout)) {
					m.SwitchTo(Candidate)
				}
			case msg := <-m.inbound:
				m.state.FollowerHandle(msg)
			}
		case Leader:
			select {
			case <-heartBeatTimer.C:
				m.outbound <- AppendEntriesRequest{m.state.CurrentTerm, 0, 0, 0, 0, []LogEntry{}}
			case msg := <-m.inbound:
				m.state.LeaderHandle(msg)
			default:
			}

		case Candidate:
			select {
			// ????
			// case <-electionTimeoutTimer.C:
			// 	if time.Now().After(m.state.broadcastTime.Add(m.config.ElectionTimeout)) {
			// 		m.SwitchTo(Candidate)
			// 	}
			case msg := <-m.inbound:
				m.state.CandidateHandle(msg)
			}
		}
	}
}

func (m *Manager) SwitchTo(to NodeStatus) {
	if to == Leader {
		m.nodeStatus = Leader
		log.Infof("Becoming a leader")
	} else if to == Follower {
		m.nodeStatus = Follower
		log.Infof("Becoming a follower")
	} else if to == Candidate {
		m.nodeStatus = Candidate
		log.Infof("Becoming a candidate")
	} else {
		log.Fatalf("SwitchTo: invalid nodeStatus")
	}
}

var count = 0

func (m *Manager) handleConnection(c *client) {
	for {
		msg, err := c.recv()
		if err == io.EOF {
			// TOOD close connection or try to reconnect
			log.Infof("Closing connection with %s, EOF", c.addr)
			break
		}
		m.inbound <- msg
	}
	// TODO regegister connection
	m.DeregisterClinet(c)
}

func (m *Manager) connectToLeader(address string) (*client, error) {
	var err error
	var conn net.Conn
	// Find leader
	var n uint32
	for n < m.config.MaxConnectionAttempts {
		log.Infof("Dialing %s", address)
		conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Errorf("Dialing failed, %s", err.Error())
			n++
			time.Sleep(time.Duration(10*n) * time.Millisecond)
			continue
		}
		c := newClient(conn, "")

		// send registration request
		c.send(RegistrationRequest{"Jon"})

		// check response
		msg, err := c.recv()
		if err != nil {
			log.Errorf("Invalid response from %s, %s", address, err.Error())
		}
		reply := msg.(RegistrationReply)
		if reply.Success {
			log.Infof("Connected successfully to a leader at %s", address)
			return &c, nil
		} else if reply.Address != "" {
			address = reply.Address
			log.Infof("forwarded to a leader at %s", address)
		} else {
			time.Sleep(time.Duration(10*n) * time.Millisecond)
		}
	}
	n++
	log.Errorf("Failed finding a leader after %d attempts", n)
	return nil, fmt.Errorf("Failed finding a leader after %d attempts", n)
}

func (m *Manager) Bootstrap(address string) {
	leader, err := m.connectToLeader(address)
	if err != nil {
		panic(err) // TODO graceful shotdown
	}
	m.RegisterClient(leader)
	m.leader = leader
	m.state.broadcastTime = time.Now()
	go m.handleConnection(leader)
}

// Run Raft
func (m *Manager) RunListener(addr string) {

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s, %s", addr, err.Error())
		return
	}
	defer l.Close()

	for {
		log.Infof("Listining on %s", addr)
		conn, err := l.Accept()
		log.Infof("connection from %s", conn.RemoteAddr().String())
		if err != nil {
			log.Errorf("connection accept failed, %s", err.Error())
		}
		c := newClient(conn, conn.RemoteAddr().String())
		// read registration request
		msg, err := c.recv()
		if err != nil {
			log.Infof("invalid registration request from %s, %s", c.addr, err.Error())
			c.close()
			continue
		}
		req, ok := msg.(RegistrationRequest)
		if !ok {
			log.Infof("invalid registration request from %s, %s", c.addr, err.Error())
			c.close()
			continue
		}

		if m.nodeStatus == Leader {
			log.Infof("registration request from %s at %s", req.Name, c.addr)
			m.RegisterClient(&c)
			c.send(RegistrationReply{true, ""})
			go m.handleConnection(&c)
		} else {
			// Candidate or follower
			c.send(RegistrationReply{false, m.leader.addr})
		}
	}
}

func (m *Manager) RunBroadcast() {
	for {
		select {
		case msg := <-m.outbound:
			log.Infof("broadcast %#v", msg)
			for _, c := range m.clients {
				c.send(msg)
			}
		}
	}
}

func (m *Manager) Run(addr string) {
	go m.RunListener(addr)
	go m.RunBroadcast()
	m.Loop()
}

func (m *Manager) RegisterClient(c *client) {
	if _, exists := m.clients[c.addr]; !exists {
		m.clients[c.addr] = c
	}
	log.Debugf("Registered client, %s at %s", c.name, c.addr)
}

func (m *Manager) DeregisterClinet(c *client) {
	if _, exists := m.clients[c.addr]; exists {
		delete(m.clients, c.addr)
	}
	c.close()
	log.Infof("Deregistered client, %s at %s", c.name, c.addr)
}
