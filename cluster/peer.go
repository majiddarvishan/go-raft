package cluster

import (
	"net"
)

type Peer struct {
	ID   string
	Addr string
	Conn net.Conn
}