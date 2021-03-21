package rafttcp

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type ClinetNMessage struct {
	Client  *client
	Message interface{}
}

type RaftNode struct {
	config *Config

	nodeStatus NodeStatus
	state      State
	sm         StateMachine
	clog       *CommandLog
	leader     *client

	clients map[string]*client

	// nextIndex
	// for each server, index of the next log entry to send to that
	// server (initialized to leader last log index + 1)
	nextIndex map[string]uint32

	// matchIndex
	// for each server, index of highest log entry known to be replicated
	// on server (initialized to 0, increases monotonically)
	matchIndex map[string]uint32

	inbound  chan ClinetNMessage
	outbound chan interface{}

	outboundLogEntriesLock sync.Mutex
	outboundLogEntries     []LogEntry
}

func NewManager(config *Config, sm StateMachine) RaftNode {
	log := NewCommandLog()
	return RaftNode{
		config,
		Follower,
		NewState(),
		sm,
		&log,
		nil, // leader
		make(map[string]*client),
		make(map[string]uint32),
		make(map[string]uint32),
		make(chan ClinetNMessage),
		make(chan interface{}),
		sync.Mutex{},
		make([]LogEntry, 0, 100),
	}
}

func (m *RaftNode) Loop() {
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
			case inb := <-m.inbound:
				m.FollowerHandle(inb)
			}
		case Leader:
			select {
			case <-heartBeatTimer.C:
				m.outboundLogEntriesLock.Lock()
				m.outbound <- AppendEntriesRequest{m.state.CurrentTerm, 0, 0, 0, 0, m.outboundLogEntries}
				m.outboundLogEntries = m.outboundLogEntries[:0]
				m.outboundLogEntriesLock.Unlock()
			case msg := <-m.inbound:
				m.LeaderHandle(msg)
			default:
			}

		case Candidate:
			select {
			// ????
			// case <-electionTimeoutTimer.C:
			// 	if time.Now().After(m.state.broadcastTime.Add(m.config.ElectionTimeout)) {
			// 		m.SwitchTo(Candidate)
			// 	}
			case inb := <-m.inbound:
				m.CandidateHandle(inb)
			}
		}
	}
}

func (m *RaftNode) SwitchTo(to NodeStatus) {
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

func (m *RaftNode) handleConnection(c *client) {
	for {
		msg, err := c.recv()
		if err == io.EOF {
			// TOOD close connection or try to reconnect
			log.Infof("Closing connection with %s, EOF", c.addr)
			break
		}
		m.inbound <- ClinetNMessage{c, msg}
	}
	m.DeregisterClinet(c)
}

func (m *RaftNode) connectToLeader(address string) (*client, error) {
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

// Bootstrap start with connection the the leader
func (m *RaftNode) Bootstrap(address string) {
	leader, err := m.connectToLeader(address)
	if err != nil {
		panic(err) // TODO graceful shotdown
	}
	m.RegisterClient(leader)
	m.leader = leader
	m.state.broadcastTime = time.Now()
	go m.handleConnection(leader)
}

// runListener run listener to incoming traffic
func (m *RaftNode) runListener(addr string) {

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

func (m *RaftNode) runBroadcast() {
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

func (m *RaftNode) Run(addr string) {
	go m.runListener(addr)
	go m.runBroadcast()
	m.Loop()
}

func (m *RaftNode) RegisterClient(c *client) {
	if _, exists := m.clients[c.addr]; !exists {
		m.clients[c.addr] = c
	}
	log.Debugf("Registered client, %s at %s", c.name, c.addr)
}

func (m *RaftNode) DeregisterClinet(c *client) {
	if _, exists := m.clients[c.addr]; exists {
		delete(m.clients, c.addr)
	}
	c.close()
	log.Infof("Deregistered client, %s at %s", c.name, c.addr)
}

// AddLogEntry used by client to propagate log entry
func (m *RaftNode) AddLogEntry(payload interface{}) {
	prevLogEntry, newLogEntry := m.clog.AddCommand(payload)
	// replicate log entry
	m.outbound <- AppendEntriesRequest{
		m.state.CurrentTerm,
		0, // ?? leaderID
		prevLogEntry.Index,
		prevLogEntry.Term,
		m.state.CommitIndex, // ??
		[]LogEntry{*newLogEntry},
	}
}
