package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"authenticator/internal/fsm"
)

var ErrNotLeader = errors.New("node is not the leader")
var ErrNoLeader  = errors.New("no leader elected yet")

// Config تنظیمات راه‌اندازی یک نود
type Config struct {
	NodeID   string
	RaftBind string // آدرس bind برای TCP listener:  0.0.0.0:7001
	RaftPeer string // آدرس advertise به cluster:     node1:7001
	GRPCAddr string // آدرس gRPC (برای forwarding):  node1:9001
	DataDir  string
}

// Store لایه‌ی میانی بین gRPC server و Raft
type Store struct {
	mu       sync.RWMutex
	raft     *raft.Raft
	FSM      *fsm.FSM
	nodeID   string
	grpcAddr string
	nodeGRPC map[string]string // raftPeer → grpcAddr
}

func New(cfg Config) (*Store, error) {
	sm := fsm.New()

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)

	// advertise address — این را به cluster اعلام می‌کنیم
	peerAddr, err := net.ResolveTCPAddr("tcp", cfg.RaftPeer)
	if err != nil {
		return nil, fmt.Errorf("resolve peer %q: %w", cfg.RaftPeer, err)
	}

	// bind روی همه interface‌ها، ولی peerAddr را advertise می‌کنیم
	transport, err := raft.NewTCPTransport(cfg.RaftBind, peerAddr, 5, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.bolt"))
	if err != nil {
		return nil, fmt.Errorf("boltdb: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshots: %w", err)
	}

	r, err := raft.NewRaft(raftCfg, sm, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	s := &Store{
		raft:     r,
		FSM:      sm,
		nodeID:   cfg.NodeID,
		grpcAddr: cfg.GRPCAddr,
		nodeGRPC: make(map[string]string),
	}
	// ثبت آدرس gRPC خودمان برای forwarding
	s.nodeGRPC[cfg.RaftPeer] = cfg.GRPCAddr
	return s, nil
}

// Bootstrap فقط یک‌بار برای نود اول صدا زده می‌شود
func (s *Store) Bootstrap(peers []raft.Server) error {
	f := s.raft.BootstrapCluster(raft.Configuration{Servers: peers})
	if err := f.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return err
	}
	return nil
}

// Apply یک command را به Raft log می‌نویسد (فقط از Leader)
func (s *Store) Apply(cmdType fsm.CommandType, payload interface{}) (interface{}, error) {
	if !s.IsLeader() {
		return nil, ErrNotLeader
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	cmdBytes, err := json.Marshal(fsm.Command{Type: cmdType, Payload: payloadBytes})
	if err != nil {
		return nil, err
	}
	future := s.raft.Apply(cmdBytes, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply: %w", err)
	}
	return future.Response(), nil
}

// AddVoter نود جدیدی را به cluster اضافه می‌کند (فقط Leader)
func (s *Store) AddVoter(nodeID, raftAddr, grpcAddr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	s.RegisterNodeGRPC(raftAddr, grpcAddr)
	return nil
}

// RemoveServer نود را از cluster حذف می‌کند (فقط Leader)
func (s *Store) RemoveServer(nodeID string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	return s.raft.RemoveServer(raft.ServerID(nodeID), 0, 10*time.Second).Error()
}

func (s *Store) IsLeader() bool   { return s.raft.State() == raft.Leader }
func (s *Store) NodeID() string   { return s.nodeID }
func (s *Store) GRPCAddr() string { return s.grpcAddr }

// LeaderGRPCAddr آدرس gRPC نود leader را برمی‌گرداند
func (s *Store) LeaderGRPCAddr() (string, error) {
	leaderRaft, _ := s.raft.LeaderWithID()
	if leaderRaft == "" {
		return "", ErrNoLeader
	}
	s.mu.RLock()
	addr, ok := s.nodeGRPC[string(leaderRaft)]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("grpc addr unknown for leader raft=%s", leaderRaft)
	}
	return addr, nil
}

// RegisterNodeGRPC آدرس gRPC یک نود را ذخیره می‌کند
func (s *Store) RegisterNodeGRPC(raftAddr, grpcAddr string) {
	s.mu.Lock()
	s.nodeGRPC[raftAddr] = grpcAddr
	s.mu.Unlock()
}

// NodeStatus اطلاعات یک نود در cluster
type NodeStatus struct {
	NodeID   string
	RaftAddr string
	GRPCAddr string
	IsLeader bool
	State    string
}

// ClusterStatus وضعیت کل cluster را برمی‌گرداند
func (s *Store) ClusterStatus() ([]NodeStatus, error) {
	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		return nil, err
	}
	leaderAddr, _ := s.raft.LeaderWithID()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var nodes []NodeStatus
	for _, srv := range cf.Configuration().Servers {
		ra := string(srv.Address)
		nodes = append(nodes, NodeStatus{
			NodeID:   string(srv.ID),
			RaftAddr: ra,
			GRPCAddr: s.nodeGRPC[ra],
			IsLeader: srv.Address == leaderAddr,
			State:    s.raft.State().String(),
		})
	}
	return nodes, nil
}
