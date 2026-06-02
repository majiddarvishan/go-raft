package transport

import (
	"net"
)

type Client struct {
	conn net.Conn
}

func Dial(addr string) (*Client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: c}, nil
}

func (c *Client) Send(msgType byte, payload []byte) error {
	return WriteFrame(c.conn, msgType, payload)
}

func (c *Client) Conn() net.Conn {
	return c.conn
}