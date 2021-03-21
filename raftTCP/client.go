package rafttcp

import (
	"encoding/gob"
	"net"

	log "github.com/sirupsen/logrus"
)

type client struct {
	conn net.Conn
	addr string
	name string
	enc  *gob.Encoder
	dec  *gob.Decoder
}

func newClient(conn net.Conn, name string) client {
	log.Infof("%s, %s", conn.RemoteAddr().String(), conn.RemoteAddr().Network())
	return client{
		conn,
		conn.RemoteAddr().String(),
		name,
		gob.NewEncoder(conn),
		gob.NewDecoder(conn),
	}
}

func (c *client) send(msg interface{}) error {
	return c.enc.Encode(Packet{msg})
}

func (c *client) recv() (interface{}, error) {
	msg := Packet{}
	err := c.dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return msg.Message, nil
}

func (c *client) close() error {
	err := c.conn.Close()
	if err != nil {
		log.Errorf("closing connection with %s failed, %s", c.conn.RemoteAddr().String(), err.Error())
		return err
	}
	return nil
}
