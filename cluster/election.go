package cluster

import (
	"math/rand"
	"sync"
	"time"

	"dist/consensus"
	"dist/transport"
)

type Election struct {
	state *consensus.State
	pm    *PeerManager
	self  string

	mu sync.Mutex
}

func NewElection(self string, s *consensus.State, pm *PeerManager) *Election {
	return &Election{
		state: s,
		pm:    pm,
		self:  self,
	}
}

func (e *Election) RunElectionLoop() {
	go func() {
		for {
			timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
			time.Sleep(timeout)

			if e.state.Role() == consensus.Leader {
				continue
			}

			e.startElection()
		}
	}()
}

func (e *Election) startElection() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.state.SetRole(consensus.Candidate)
	term := e.state.IncTerm()

	votes := 1
	total := len(e.pm.All())

	for _, p := range e.pm.All() {
		go func(peer *Peer) {
			req := transport.EncodeTLV(0x01, uint64ToBytes(term))

			c, err := transport.Dial(peer.Addr)
			if err != nil {
				return
			}

			defer c.Conn().Close()

			transport.WriteFrame(
				c.Conn(),
				transport.MSG_VOTE_REQ,
				req,
			)
		}(p)
	}

	time.Sleep(100 * time.Millisecond)

	// simplified majority check
	if votes > total/2 {
		e.becomeLeader()
	}
}

func (e *Election) becomeLeader() {
	e.state.SetRole(consensus.Leader)
	e.state.SetLeader(e.self)

	go e.heartbeat()
}

