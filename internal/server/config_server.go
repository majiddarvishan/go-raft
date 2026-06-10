package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "authenticator/api/proto/auth"
	"authenticator/internal/fsm"
	"authenticator/internal/store"
)

type ConfigServer struct {
	pb.UnimplementedConfigServiceServer
	store *store.Store
}

func NewConfigServer(s *store.Store) *ConfigServer {
	return &ConfigServer{store: s}
}

// UpsertClientConfig — ایجاد یا به‌روزرسانی ClientConfig از طریق Raft
func (s *ConfigServer) UpsertClientConfig(
	ctx context.Context, req *pb.UpsertClientConfigRequest,
) (*pb.UpsertClientConfigResponse, error) {

	if req.Config == nil || req.Config.SystemId == "" {
		return nil, status.Error(codes.InvalidArgument, "config.system_id is required")
	}

	raw, err := s.store.Apply(fsm.CmdUpsertClientConfig,
		fsm.UpsertClientConfigPayload{Config: protoToEntry(req.Config)})
	if err != nil {
		if errors.Is(err, store.ErrNotLeader) {
			return s.fwdUpsert(ctx, req)
		}
		return &pb.UpsertClientConfigResponse{
			ErrorCode: pb.ErrorCode_ERROR_CODE_RAFT_APPLY_FAILED, ErrorMessage: err.Error(),
		}, nil
	}

	r := raw.(*fsm.UpsertClientConfigResult)
	return &pb.UpsertClientConfigResponse{ErrorCode: toCode(r.Code), ErrorMessage: r.Message}, nil
}

// DeleteClientConfig — حذف ClientConfig از طریق Raft
func (s *ConfigServer) DeleteClientConfig(
	ctx context.Context, req *pb.DeleteClientConfigRequest,
) (*pb.DeleteClientConfigResponse, error) {

	if req.SystemId == "" {
		return nil, status.Error(codes.InvalidArgument, "system_id is required")
	}

	raw, err := s.store.Apply(fsm.CmdDeleteClientConfig,
		fsm.DeleteClientConfigPayload{SystemID: req.SystemId})
	if err != nil {
		if errors.Is(err, store.ErrNotLeader) {
			return s.fwdDelete(ctx, req)
		}
		return &pb.DeleteClientConfigResponse{
			ErrorCode: pb.ErrorCode_ERROR_CODE_RAFT_APPLY_FAILED, ErrorMessage: err.Error(),
		}, nil
	}

	r := raw.(*fsm.DeleteClientConfigResult)
	return &pb.DeleteClientConfigResponse{ErrorCode: toCode(r.Code), ErrorMessage: r.Message}, nil
}

// GetClientConfig — خواندن از هر نود (بدون Raft)
func (s *ConfigServer) GetClientConfig(
	ctx context.Context, req *pb.GetClientConfigRequest,
) (*pb.GetClientConfigResponse, error) {

	cfg, ok := s.store.FSM.GetClientConfig(req.SystemId)
	if !ok {
		return &pb.GetClientConfigResponse{
			ErrorCode:    pb.ErrorCode_ERROR_CODE_CONFIG_NOT_FOUND,
			ErrorMessage: fmt.Sprintf("system_id=%s not found", req.SystemId),
		}, nil
	}
	return &pb.GetClientConfigResponse{
		ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS, Config: entryToProto(cfg),
	}, nil
}

// ListClientConfigs — خواندن از هر نود (بدون Raft)
func (s *ConfigServer) ListClientConfigs(
	ctx context.Context, req *pb.ListClientConfigsRequest,
) (*pb.ListClientConfigsResponse, error) {

	entries := s.store.FSM.ListClientConfigs()
	cfgs := make([]*pb.ClientConfig, 0, len(entries))
	for _, e := range entries {
		e := e
		cfgs = append(cfgs, entryToProto(&e))
	}
	return &pb.ListClientConfigsResponse{
		ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS, Configs: cfgs,
	}, nil
}

// BootstrapConfigs — بارگذاری اولیه از DB به FSM (فقط یک‌بار روی Leader)
func (s *ConfigServer) BootstrapConfigs(
	ctx context.Context, req *pb.BootstrapConfigsRequest,
) (*pb.BootstrapConfigsResponse, error) {

	if !s.store.IsLeader() {
		return &pb.BootstrapConfigsResponse{
			ErrorCode: pb.ErrorCode_ERROR_CODE_NOT_LEADER,
			ErrorMessage: "must be called on leader",
		}, nil
	}
	if s.store.FSM.HasClientConfigs() {
		return &pb.BootstrapConfigsResponse{
			ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS,
			ErrorMessage: "already bootstrapped", LoadedCount: 0,
		}, nil
	}

	loaded := int32(0)
	for _, pbCfg := range req.Configs {
		_, err := s.store.Apply(fsm.CmdUpsertClientConfig,
			fsm.UpsertClientConfigPayload{Config: protoToEntry(pbCfg)})
		if err != nil {
			return &pb.BootstrapConfigsResponse{
				ErrorCode:    pb.ErrorCode_ERROR_CODE_RAFT_APPLY_FAILED,
				ErrorMessage: fmt.Sprintf("failed at %s: %v", pbCfg.SystemId, err),
				LoadedCount:  loaded,
			}, nil
		}
		loaded++
	}
	return &pb.BootstrapConfigsResponse{
		ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS, LoadedCount: loaded,
	}, nil
}

// ── Leader Forwarding ─────────────────────────────────────────────────────────

func (s *ConfigServer) fwdUpsert(ctx context.Context, req *pb.UpsertClientConfigRequest) (*pb.UpsertClientConfigResponse, error) {
	c, conn, err := s.leaderClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.UpsertClientConfig(ctx, req)
}

func (s *ConfigServer) fwdDelete(ctx context.Context, req *pb.DeleteClientConfigRequest) (*pb.DeleteClientConfigResponse, error) {
	c, conn, err := s.leaderClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.DeleteClientConfig(ctx, req)
}

func (s *ConfigServer) leaderClient() (pb.ConfigServiceClient, *grpc.ClientConn, error) {
	addr, err := s.store.LeaderGRPCAddr()
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "leader: %v", err)
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "dial leader: %v", err)
	}
	return pb.NewConfigServiceClient(conn), conn, nil
}

// ── Proto ↔ FSM conversion ────────────────────────────────────────────────────

func protoToEntry(p *pb.ClientConfig) fsm.ClientConfigEntry {
	return fsm.ClientConfigEntry{
		ID:                          p.Id,
		SystemID:                    p.SystemId,
		PasswordHash:                p.PasswordHash,
		MaxConnections:              p.MaxConnections,
		TPSLimit:                    p.TpsLimit,
		SubmitRespMessageIDType:     p.SubmitRespMessageIdType,
		DeliveryReportMessageIDType: p.DeliveryReportMessageIdType,
	}
}

func entryToProto(e *fsm.ClientConfigEntry) *pb.ClientConfig {
	return &pb.ClientConfig{
		Id:                          e.ID,
		SystemId:                    e.SystemID,
		PasswordHash:                e.PasswordHash,
		MaxConnections:              e.MaxConnections,
		TpsLimit:                    e.TPSLimit,
		SubmitRespMessageIdType:     e.SubmitRespMessageIDType,
		DeliveryReportMessageIdType: e.DeliveryReportMessageIDType,
	}
}
