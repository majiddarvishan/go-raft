package transport

import (
	"net"
)

type Handler func(Frame, net.Conn)

type Server struct {
	addr    string
	handler Handler
}

func NewServer(addr string, h Handler) *Server {
	return &Server{
		addr:    addr,
		handler: h,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		frame, err := ReadFrame(conn)
		if err != nil {
			return
		}

		s.handler(frame, conn)
	}
}