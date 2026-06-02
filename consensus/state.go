package consensus

import "sync/atomic"

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type State struct {
	term      uint64
	votedFor  string
	role      atomic.Value // Role
	leaderID  atomic.Value
}

func NewState() *State {
	s := &State{}
	s.role.Store(Follower)
	s.leaderID.Store("")
	return s
}

func (s *State) Role() Role {
	return s.role.Load().(Role)
}

func (s *State) SetRole(r Role) {
	s.role.Store(r)
}

func (s *State) Term() uint64 {
	return atomic.LoadUint64(&s.term)
}

func (s *State) IncTerm() uint64 {
	return atomic.AddUint64(&s.term, 1)
}

func (s *State) SetLeader(id string) {
	s.leaderID.Store(id)
}

func (s *State) Leader() string {
	v := s.leaderID.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}