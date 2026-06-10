package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM پیاده‌سازی raft.FSM است.
// دو نوع state نگه می‌دارد:
//   - clientConfigs: تنظیمات کلاینت‌ها (جایگزین DB replication)
//   - connections:   اتصالات فعال (ephemeral state)
type FSM struct {
	mu             sync.RWMutex
	clientConfigs  map[string]*ClientConfigEntry    // keyed by system_id
	connections    map[int64]*connectionEntry        // keyed by connection_id
	systemConns    map[string]map[int64]struct{}     // system_id → active conn ids
	requesterConns map[string]map[int64]struct{}     // requester_id → active conn ids
}

func New() *FSM {
	return &FSM{
		clientConfigs:  make(map[string]*ClientConfigEntry),
		connections:    make(map[int64]*connectionEntry),
		systemConns:    make(map[string]map[int64]struct{}),
		requesterConns: make(map[string]map[int64]struct{}),
	}
}

// =============================================================
// Apply — تنها نقطه‌ی ورود برای تغییر state
// توسط Raft روی تمام نودها به‌صورت sequential صدا زده می‌شود
// =============================================================

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return &RegisterResult{Code: ResultInternal, Message: fmt.Sprintf("unmarshal: %v", err)}
	}

	switch cmd.Type {
	case CmdUpsertClientConfig:
		var p UpsertClientConfigPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return &UpsertClientConfigResult{Code: ResultInternal, Message: err.Error()}
		}
		return f.applyUpsertConfig(&p)

	case CmdDeleteClientConfig:
		var p DeleteClientConfigPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return &DeleteClientConfigResult{Code: ResultInternal, Message: err.Error()}
		}
		return f.applyDeleteConfig(&p)

	case CmdRegisterConnection:
		var p RegisterConnectionPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return &RegisterResult{Code: ResultInternal, Message: err.Error()}
		}
		return f.applyRegister(log.Index, &p)

	case CmdUnregisterConnection:
		var p UnregisterConnectionPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return &UnregisterResult{Code: ResultInternal, Message: err.Error()}
		}
		return f.applyUnregister(&p)

	case CmdUnregisterAllConnections:
		var p UnregisterAllConnectionsPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return &UnregisterAllResult{Code: ResultInternal, Message: err.Error()}
		}
		return f.applyUnregisterAll(&p)

	default:
		return fmt.Errorf("unknown command: %s", cmd.Type)
	}
}

// =============================================================
// ClientConfig — Apply
// =============================================================

func (f *FSM) applyUpsertConfig(p *UpsertClientConfigPayload) *UpsertClientConfigResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg := p.Config
	f.clientConfigs[cfg.SystemID] = &cfg
	return &UpsertClientConfigResult{Code: ResultOK}
}

func (f *FSM) applyDeleteConfig(p *DeleteClientConfigPayload) *DeleteClientConfigResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.clientConfigs[p.SystemID]; !ok {
		return &DeleteClientConfigResult{
			Code:    ResultConfigNotFound,
			Message: fmt.Sprintf("system_id=%s not found", p.SystemID),
		}
	}
	delete(f.clientConfigs, p.SystemID)
	return &DeleteClientConfigResult{Code: ResultOK}
}

// =============================================================
// Connection — Apply
// =============================================================

func (f *FSM) applyRegister(logIndex uint64, p *RegisterConnectionPayload) *RegisterResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	active := int32(len(f.systemConns[p.SystemID]))
	if active >= p.MaxConnections {
		return &RegisterResult{
			Code:              ResultMaxConnectionsReached,
			Message:           fmt.Sprintf("max connections %d/%d for %s", active, p.MaxConnections, p.SystemID),
			ActiveConnections: active,
		}
	}

	// Raft log index به عنوان connection_id — unique و monotonic
	connID := int64(logIndex)
	f.connections[connID] = &connectionEntry{
		ID:              connID,
		ClientConfigID:  p.ClientConfigID,
		SourceIP:        p.SourceIP,
		BindType:        p.BindType,
		SystemID:        p.SystemID,
		SystemType:      p.SystemType,
		ConnectedAt:     p.ConnectedAt,
		AuthenticatorID: p.AuthenticatorID,
		RequesterID:     p.RequesterID,
		MaxConnections:  p.MaxConnections,
	}

	if f.systemConns[p.SystemID] == nil {
		f.systemConns[p.SystemID] = make(map[int64]struct{})
	}
	f.systemConns[p.SystemID][connID] = struct{}{}

	if f.requesterConns[p.RequesterID] == nil {
		f.requesterConns[p.RequesterID] = make(map[int64]struct{})
	}
	f.requesterConns[p.RequesterID][connID] = struct{}{}

	return &RegisterResult{
		ConnectionID:      connID,
		ActiveConnections: int32(len(f.systemConns[p.SystemID])),
		Code:              ResultOK,
	}
}

func (f *FSM) applyUnregister(p *UnregisterConnectionPayload) *UnregisterResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.connections[p.ConnectionID]
	if !ok {
		return &UnregisterResult{Code: ResultConnectionNotFound,
			Message: fmt.Sprintf("connection_id=%d not found", p.ConnectionID)}
	}
	if entry.DisconnectedAt != 0 {
		return &UnregisterResult{Code: ResultConnectionAlreadyClosed,
			Message: fmt.Sprintf("connection_id=%d already closed", p.ConnectionID)}
	}

	entry.DisconnectedAt = p.DisconnectedAt
	delete(f.systemConns[entry.SystemID], p.ConnectionID)
	delete(f.requesterConns[entry.RequesterID], p.ConnectionID)

	return &UnregisterResult{
		ActiveConnections: int32(len(f.systemConns[entry.SystemID])),
		Code:              ResultOK,
	}
}

func (f *FSM) applyUnregisterAll(p *UnregisterAllConnectionsPayload) *UnregisterAllResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	connIDs, ok := f.requesterConns[p.RequesterID]
	if !ok || len(connIDs) == 0 {
		return &UnregisterAllResult{Code: ResultOK}
	}

	removed := int32(0)
	for connID := range connIDs {
		entry, exists := f.connections[connID]
		if !exists || entry.DisconnectedAt != 0 {
			continue
		}
		entry.DisconnectedAt = p.DisconnectedAt
		delete(f.systemConns[entry.SystemID], connID)
		removed++
	}
	delete(f.requesterConns, p.RequesterID)
	return &UnregisterAllResult{RemovedCount: removed, Code: ResultOK}
}

// =============================================================
// Read Methods — مستقیم از state، بدون Raft
// =============================================================

func (f *FSM) GetClientConfig(systemID string) (*ClientConfigEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cfg, ok := f.clientConfigs[systemID]
	if !ok {
		return nil, false
	}
	copy := *cfg
	return &copy, true
}

func (f *FSM) ListClientConfigs() []ClientConfigEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make([]ClientConfigEntry, 0, len(f.clientConfigs))
	for _, cfg := range f.clientConfigs {
		result = append(result, *cfg)
	}
	return result
}

func (f *FSM) HasClientConfigs() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.clientConfigs) > 0
}

func (f *FSM) ListConnections(systemID, systemType string) []ConnectionView {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var result []ConnectionView
	for _, e := range f.connections {
		if systemID != "" && e.SystemID != systemID {
			continue
		}
		if systemType != "" && e.SystemType != systemType {
			continue
		}
		result = append(result, ConnectionView{
			ID: e.ID, ClientConfigID: e.ClientConfigID,
			SourceIP: e.SourceIP, BindType: e.BindType,
			ConnectedAt: e.ConnectedAt, DisconnectedAt: e.DisconnectedAt,
			AuthenticatorID: e.AuthenticatorID, RequesterID: e.RequesterID,
		})
	}
	return result
}

// =============================================================
// Snapshot & Restore
// =============================================================

type snapshotState struct {
	ClientConfigs map[string]*ClientConfigEntry `json:"client_configs"`
	Connections   map[int64]*connectionEntry    `json:"connections"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	cfgs := make(map[string]*ClientConfigEntry, len(f.clientConfigs))
	for k, v := range f.clientConfigs {
		c := *v; cfgs[k] = &c
	}
	conns := make(map[int64]*connectionEntry, len(f.connections))
	for k, v := range f.connections {
		c := *v; conns[k] = &c
	}
	return &fsmSnapshot{&snapshotState{cfgs, conns}}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s snapshotState
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if s.ClientConfigs != nil {
		f.clientConfigs = s.ClientConfigs
	} else {
		f.clientConfigs = make(map[string]*ClientConfigEntry)
	}
	if s.Connections != nil {
		f.connections = s.Connections
	} else {
		f.connections = make(map[int64]*connectionEntry)
	}

	f.systemConns = make(map[string]map[int64]struct{})
	f.requesterConns = make(map[string]map[int64]struct{})
	for id, e := range f.connections {
		if e.DisconnectedAt != 0 {
			continue
		}
		if f.systemConns[e.SystemID] == nil {
			f.systemConns[e.SystemID] = make(map[int64]struct{})
		}
		f.systemConns[e.SystemID][id] = struct{}{}
		if f.requesterConns[e.RequesterID] == nil {
			f.requesterConns[e.RequesterID] = make(map[int64]struct{})
		}
		f.requesterConns[e.RequesterID][id] = struct{}{}
	}
	return nil
}

type fsmSnapshot struct{ state *snapshotState }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *fsmSnapshot) Release() {}
