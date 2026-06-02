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

func (n *Node) Start() error {
	n.server = transport.NewServer(n.cfg.Listen, n.handleMessage)
	return n.server.Start()
}

func (n *Node) handleMessage(frame transport.Frame, conn any) {
	// placeholder for next phase (consensus)
}