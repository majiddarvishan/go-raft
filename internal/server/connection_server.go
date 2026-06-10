package server

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "authenticator/api/proto/auth"
	"authenticator/internal/fsm"
	"authenticator/internal/store"
)

type ConnectionServer struct {
	pb.UnimplementedConnectionServiceServer
	store *store.Store
}

func NewConnectionServer(s *store.Store) *ConnectionServer {
	return &ConnectionServer{store: s}
}

// RegisterConnection — ثبت اتصال جدید
func (s *ConnectionServer) RegisterConnection(
	ctx context.Context, req *pb.RegisterConnectionRequest,
) (*pb.RegisterConnectionResponse, error) {

	if ve := validateRegister(req); ve != nil {
		return &pb.RegisterConnectionResponse{
			AuthenticatorId: s.store.NodeID(),
			ErrorCode: ve.code, ErrorMessage: ve.msg,
		}, nil
	}

	// ClientConfig مستقیم از FSM خوانده می‌شود — بدون DB query
	cfg, ok := s.store.FSM.GetClientConfig(req.SystemId)
	if !ok {
		return &pb.RegisterConnectionResponse{
			AuthenticatorId: s.store.NodeID(),
			ErrorCode:       pb.ErrorCode_ERROR_CODE_CONFIG_NOT_FOUND,
			ErrorMessage:    fmt.Sprintf("no config for system_id=%s", req.SystemId),
		}, nil
	}

	payload := fsm.RegisterConnectionPayload{
		RequesterID:     req.RequesterId,
		SourceIP:        req.SourceIp,
		BindType:        int32(req.BindType),
		SystemID:        req.SystemId,
		SystemType:      req.SystemType,
		AuthenticatorID: s.store.NodeID(),
		ClientConfigID:  cfg.ID,
		MaxConnections:  cfg.MaxConnections,
		ConnectedAt:     time.Now().UnixNano(),
	}

	raw, err := s.store.Apply(fsm.CmdRegisterConnection, payload)
	if err != nil {
		if err == store.ErrNotLeader {
			return s.fwdRegister(ctx, req)
		}
		return &pb.RegisterConnectionResponse{
			AuthenticatorId: s.store.NodeID(),
			ErrorCode: pb.ErrorCode_ERROR_CODE_RAFT_APPLY_FAILED, ErrorMessage: err.Error(),
		}, nil
	}

	r := raw.(*fsm.RegisterResult)
	return &pb.RegisterConnectionResponse{
		AuthenticatorId:       s.store.NodeID(),
		ErrorCode:             toCode(r.Code),
		ErrorMessage:          r.Message,
		ConnectionId:          r.ConnectionID,
		ActiveConnections:     r.ActiveConnections,
		MaxConnectionsAllowed: cfg.MaxConnections,
	}, nil
}

// UnregisterConnection — بستن یک اتصال
func (s *ConnectionServer) UnregisterConnection(
	ctx context.Context, req *pb.UnregisterConnectionRequest,
) (*pb.UnregisterConnectionResponse, error) {

	if req.ConnectionId == 0 {
		return &pb.UnregisterConnectionResponse{
			AuthenticatorId: s.store.NodeID(),
			ErrorCode:       pb.ErrorCode_ERROR_CODE_INVALID_CONNECTION_ID,
		}, nil
	}

	raw, err := s.store.Apply(fsm.CmdUnregisterConnection,
		fsm.UnregisterConnectionPayload{ConnectionID: req.ConnectionId, DisconnectedAt: time.Now().UnixNano()})
	if err != nil {
		if err == store.ErrNotLeader {
			return s.fwdUnregister(ctx, req)
		}
		return &pb.UnregisterConnectionResponse{
			AuthenticatorId: s.store.NodeID(),
			ErrorCode: pb.ErrorCode_ERROR_CODE_RAFT_APPLY_FAILED, ErrorMessage: err.Error(),
		}, nil
	}

	r := raw.(*fsm.UnregisterResult)
	return &pb.UnregisterConnectionResponse{
		AuthenticatorId: s.store.NodeID(),
		ErrorCode: toCode(r.Code), ErrorMessage: r.Message,
		ActiveConnections: r.ActiveConnections,
	}, nil
}

// UnregisterAllConnections — بستن همه اتصالات یک requester
func (s *ConnectionServer) UnregisterAllConnections(
	ctx context.Context, req *pb.UnregisterAllConnectionsRequest,
) (*pb.UnregisterAllConnectionsResponse, error) {

	if req.RequesterId == "" {
		return &pb.UnregisterAllConnectionsResponse{
			AuthenticatorId: s.store.NodeID(),
			ErrorCode:       pb.ErrorCode_ERROR_CODE_MISSING_REQUESTER_ID,
		}, nil
	}

	_, err := s.store.Apply(fsm.CmdUnregisterAllConnections,
		fsm.UnregisterAllConnectionsPayload{RequesterID: req.RequesterId, DisconnectedAt: time.Now().UnixNano()})
	if err != nil {
		if err == store.ErrNotLeader {
			return s.fwdUnregisterAll(ctx, req)
		}
		return &pb.UnregisterAllConnectionsResponse{
			AuthenticatorId: s.store.NodeID(),
			ErrorCode: pb.ErrorCode_ERROR_CODE_RAFT_APPLY_FAILED, ErrorMessage: err.Error(),
		}, nil
	}
	return &pb.UnregisterAllConnectionsResponse{
		AuthenticatorId: s.store.NodeID(), ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS,
	}, nil
}

// ListConnections — خواندن اتصالات از هر نود (بدون Raft)
func (s *ConnectionServer) ListConnections(
	ctx context.Context, req *pb.ListConnectionsRequest,
) (*pb.ListConnectionsResponse, error) {

	views := s.store.FSM.ListConnections(req.SystemId, "")
	conns := make([]*pb.ConnectionInfo, 0, len(views))
	for _, v := range views {
		conns = append(conns, &pb.ConnectionInfo{
			Id: v.ID, ClientConfigId: v.ClientConfigID,
			SourceIp: v.SourceIP, BindType: pb.BindType(v.BindType),
			ConnectedAt: v.ConnectedAt, DisconnectedAt: v.DisconnectedAt,
			AuthenticatorId: v.AuthenticatorID, RequesterId: v.RequesterID,
		})
	}
	return &pb.ListConnectionsResponse{
		AuthenticatorId: s.store.NodeID(),
		ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS, Connections: conns,
	}, nil
}

// ── Leader Forwarding ─────────────────────────────────────────────────────────

func (s *ConnectionServer) fwdRegister(ctx context.Context, req *pb.RegisterConnectionRequest) (*pb.RegisterConnectionResponse, error) {
	c, conn, err := s.leaderClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.RegisterConnection(ctx, req)
}

func (s *ConnectionServer) fwdUnregister(ctx context.Context, req *pb.UnregisterConnectionRequest) (*pb.UnregisterConnectionResponse, error) {
	c, conn, err := s.leaderClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.UnregisterConnection(ctx, req)
}

func (s *ConnectionServer) fwdUnregisterAll(ctx context.Context, req *pb.UnregisterAllConnectionsRequest) (*pb.UnregisterAllConnectionsResponse, error) {
	c, conn, err := s.leaderClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.UnregisterAllConnections(ctx, req)
}

func (s *ConnectionServer) leaderClient() (pb.ConnectionServiceClient, *grpc.ClientConn, error) {
	addr, err := s.store.LeaderGRPCAddr()
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "leader: %v", err)
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "dial leader: %v", err)
	}
	return pb.NewConnectionServiceClient(conn), conn, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

type validErr struct{ code pb.ErrorCode; msg string }

func validateRegister(req *pb.RegisterConnectionRequest) *validErr {
	if req.RequesterId == "" {
		return &validErr{pb.ErrorCode_ERROR_CODE_MISSING_REQUESTER_ID, "requester_id required"}
	}
	if req.SystemId == "" {
		return &validErr{pb.ErrorCode_ERROR_CODE_MISSING_SYSTEM_ID, "system_id required"}
	}
	if req.SourceIp == "" {
		return &validErr{pb.ErrorCode_ERROR_CODE_INVALID_SOURCE_IP, "source_ip required"}
	}
	if req.BindType == pb.BindType_BIND_TYPE_UNKNOWN {
		return &validErr{pb.ErrorCode_ERROR_CODE_INVALID_BIND_TYPE, "bind_type required"}
	}
	return nil
}

func toCode(c fsm.ResultCode) pb.ErrorCode {
	switch c {
	case fsm.ResultOK:
		return pb.ErrorCode_ERROR_CODE_SUCCESS
	case fsm.ResultMaxConnectionsReached:
		return pb.ErrorCode_ERROR_CODE_MAX_CONNECTIONS_REACHED
	case fsm.ResultConnectionNotFound:
		return pb.ErrorCode_ERROR_CODE_CONNECTION_NOT_FOUND
	case fsm.ResultConnectionAlreadyClosed:
		return pb.ErrorCode_ERROR_CODE_CONNECTION_ALREADY_CLOSED
	case fsm.ResultConfigNotFound:
		return pb.ErrorCode_ERROR_CODE_CONFIG_NOT_FOUND
	default:
		return pb.ErrorCode_ERROR_CODE_INTERNAL
	}
}
