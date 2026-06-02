package dist

import "sync"

type State struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewState() *State {
	return &State{
		m: make(map[string][]byte),
	}
}

func (s *State) Put(k string, v []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[k] = v
}

func (s *State) Get(k string) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.m[k]
}