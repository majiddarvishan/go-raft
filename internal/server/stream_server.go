package server

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "authenticator/api/proto/auth"
	"authenticator/internal/stream"
)

// StreamServer پیاده‌سازی pb.CommandStreamServiceServer است.
//
// جریان کار:
//   کلاینت Subscribe صدا می‌زند → Hub کانالی می‌سازد
//   Admin/Server Broadcast صدا می‌زند → Hub به کانال‌ها push می‌کند
//   کلاینت دستورات را از stream.Recv دریافت می‌کند
type StreamServer struct {
	pb.UnimplementedCommandStreamServiceServer
	hub *stream.Hub
}

func NewStreamServer(hub *stream.Hub) *StreamServer {
	return &StreamServer{hub: hub}
}

// Subscribe — کلاینت subscribe می‌کند، سرور دستور push می‌کند (server-side streaming)
func (s *StreamServer) Subscribe(
	req *pb.SubscribeRequest,
	srv pb.CommandStreamService_SubscribeServer,
) error {
	if req.RequesterId == "" {
		return status.Error(codes.InvalidArgument, "requester_id is required")
	}

	ch, unsubscribe := s.hub.Subscribe(req.RequesterId)
	defer unsubscribe()

	// ping اولیه — تأیید اتصال موفق
	if err := srv.Send(&pb.ServerCommand{
		CommandId: stream.NewCommandID(),
		Type:      pb.ServerCommandType_SERVER_COMMAND_PING,
		Payload:   `{"message":"subscribed","requester_id":"` + req.RequesterId + `"}`,
		Timestamp: time.Now().UnixNano(),
	}); err != nil {
		return err
	}

	ctx := srv.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cmd, ok := <-ch:
			if !ok {
				return nil
			}
			if err := srv.Send(&pb.ServerCommand{
				CommandId: cmd.CommandID,
				Type:      pb.ServerCommandType(cmd.Type),
				Payload:   cmd.Payload,
				Timestamp: cmd.Timestamp,
			}); err != nil {
				return err
			}
		}
	}
}

// Broadcast — دستور را به یک یا همه کلاینت‌های subscribed ارسال می‌کند
func (s *StreamServer) Broadcast(
	ctx context.Context, req *pb.BroadcastRequest,
) (*pb.BroadcastResponse, error) {

	cmd := stream.Command{
		CommandID: stream.NewCommandID(),
		Type:      int32(req.Type),
		Payload:   req.Payload,
		Timestamp: time.Now().UnixNano(),
	}

	var count int
	if req.TargetId != "" {
		// ارسال به کلاینت مشخص
		if s.hub.Send(req.TargetId, cmd) {
			count = 1
		}
	} else {
		// broadcast به همه کلاینت‌های subscribed روی این نود
		count = s.hub.Broadcast(cmd)
	}

	return &pb.BroadcastResponse{
		ErrorCode: pb.ErrorCode_ERROR_CODE_SUCCESS,
		SentCount: int32(count),
	}, nil
}
