package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "authenticator/api/proto/auth"
	"authenticator/internal/store"
)

type ClusterServer struct {
	pb.UnimplementedClusterServiceServer
	store *store.Store
}

func NewClusterServer(s *store.Store) *ClusterServer {
	return &ClusterServer{store: s}
}

// Join — نود جدید برای پیوستن، این را روی Leader صدا می‌زند
func (s *ClusterServer) Join(
	ctx context.Context, req *pb.JoinRequest,
) (*pb.JoinResponse, error) {

	if req.NodeId == "" || req.RaftAddr == "" || req.GrpcAddr == "" {
		return nil, status.Error(codes.InvalidArgument,
			"node_id, raft_addr, grpc_addr are required")
	}

	if err := s.store.AddVoter(req.NodeId, req.RaftAddr, req.GrpcAddr); err != nil {
		if err == store.ErrNotLeader {
			return &pb.JoinResponse{
				ErrorCode:    pb.ErrorCode_ERROR_CODE_NOT_LEADER,
				ErrorMessage: "this node is not the leader",
			}, nil
		}
		return &pb.JoinResponse{
			ErrorCode:    pb.ErrorCode_ERROR_CODE_INTERNAL,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.JoinResponse{ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS}, nil
}

// Leave — نود برای خروج graceful، این را روی Leader صدا می‌زند
func (s *ClusterServer) Leave(
	ctx context.Context, req *pb.LeaveRequest,
) (*pb.LeaveResponse, error) {

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	if err := s.store.RemoveServer(req.NodeId); err != nil {
		if err == store.ErrNotLeader {
			return &pb.LeaveResponse{
				ErrorCode:    pb.ErrorCode_ERROR_CODE_NOT_LEADER,
				ErrorMessage: "this node is not the leader",
			}, nil
		}
		return &pb.LeaveResponse{
			ErrorCode:    pb.ErrorCode_ERROR_CODE_INTERNAL,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.LeaveResponse{ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS}, nil
}

// GetClusterStatus — وضعیت cluster (قابل فراخوانی روی هر نود)
func (s *ClusterServer) GetClusterStatus(
	ctx context.Context, req *pb.ClusterStatusRequest,
) (*pb.ClusterStatusResponse, error) {

	nodes, err := s.store.ClusterStatus()
	if err != nil {
		return &pb.ClusterStatusResponse{
			ErrorCode:    pb.ErrorCode_ERROR_CODE_INTERNAL,
			ErrorMessage: err.Error(),
		}, nil
	}

	var pbNodes []*pb.NodeInfo
	var leaderID, leaderGRPC string

	for _, n := range nodes {
		pbNodes = append(pbNodes, &pb.NodeInfo{
			NodeId:   n.NodeID,
			RaftAddr: n.RaftAddr,
			GrpcAddr: n.GRPCAddr,
			IsLeader: n.IsLeader,
			State:    n.State,
		})
		if n.IsLeader {
			leaderID   = n.NodeID
			leaderGRPC = n.GRPCAddr
		}
	}

	return &pb.ClusterStatusResponse{
		LeaderId:     leaderID,
		LeaderGrpc:   leaderGRPC,
		Nodes:        pbNodes,
		ErrorCode:    pb.ErrorCode_ERROR_CODE_SUCCESS,
	}, nil
}
