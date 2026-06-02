package dist

import (
	"dist/cluster"
	"dist/transport"
)

type Node struct {
	cfg Config

	state *State

	server *transport.Server
	peers  *cluster.PeerManager
}

func New(cfg Config) *Node {
	return &Node{
		cfg:   cfg,
		state: NewState(),
		peers: cluster.NewPeerManager(),
	}
}

func (n *Node) handleMessage(frame transport.Frame, conn any) {
	// placeholder for next phase (consensus)
}

func (n *Node) Start() error {
	n.server = transport.NewServer(n.cfg.Listen, func(f transport.Frame, c net.Conn) {
		n.consensusHandler.Handle(f, c)
	})

	go n.election.RunElectionLoop()

	return n.server.Start()
}

