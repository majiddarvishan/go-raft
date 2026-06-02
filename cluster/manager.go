package cluster

import (
	"sync"
)

type PeerManager struct {
	mu    sync.RWMutex
	peers map[string]*Peer
}

func NewPeerManager() *PeerManager {
	return &PeerManager{
		peers: make(map[string]*Peer),
	}
}

func (pm *PeerManager) Add(p *Peer) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.peers[p.ID] = p
}

func (pm *PeerManager) Get(id string) *Peer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.peers[id]
}

func (pm *PeerManager) All() []*Peer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	out := make([]*Peer, 0, len(pm.peers))

	for _, p := range pm.peers {
		out = append(out, p)
	}

	return out
}