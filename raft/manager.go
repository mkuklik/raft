package raft

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

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

var count = 0

func handleConnection(c net.Conn) {
	fmt.Print(".")
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		temp := strings.TrimSpace(string(netData))
		if temp == "STOP" {
			break
		}
		fmt.Println(temp)
		counter := strconv.Itoa(count) + "\n"
		c.Write([]byte(string(counter)))
	}
	c.Close()
}

func (m *Manager) run(address string) {
	port := 1234
	// add := address
	add := ":" + strconv.Itoa(port)
	l, err := net.Listen("tcp4", add)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c)
		count++
	}
}
