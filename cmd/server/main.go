package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "authenticator/api/proto/auth"
	"authenticator/internal/db"
	"authenticator/internal/server"
	"authenticator/internal/store"
	"authenticator/internal/stream"
)

func main() {
	// ── تنظیمات از environment variables ─────────────────────────────────────
	cfg := store.Config{
		NodeID:   env("NODE_ID",   "node1"),
		RaftBind: env("RAFT_BIND", "0.0.0.0:7001"), // bind روی همه interface‌ها
		RaftPeer: env("RAFT_ADDR", "node1:7001"),    // advertise به cluster
		GRPCAddr: env("GRPC_ADDR", "0.0.0.0:9001"),
		DataDir:  env("DATA_DIR",  "/data"),
	}
	joinAddr    := env("JOIN_ADDR",  "")      // خالی = نود اول (bootstrap)
	dbDSN       := env("DB_DSN",     "postgres://auth:secret@localhost:5432/authenticator?sslmode=disable")
	isBootstrap := env("BOOTSTRAP",  "") == "true"

	log.Printf("[%s] starting | raft_peer=%s grpc=%s", cfg.NodeID, cfg.RaftPeer, cfg.GRPCAddr)

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	pg := mustConnectDB(dbDSN)
	defer pg.Close()

	// ── Hub برای streaming سرور → کلاینت ─────────────────────────────────────
	hub := stream.NewHub()

	// ── Raft Store ────────────────────────────────────────────────────────────
	s, err := store.New(cfg)
	if err != nil {
		log.Fatalf("store: %v", err)
	}

	// ── Bootstrap یا Join ────────────────────────────────────────────────────
	if joinAddr == "" || isBootstrap {
		if err := s.Bootstrap([]raft.Server{{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(cfg.RaftPeer),
		}}); err != nil {
			log.Fatalf("bootstrap: %v", err)
		}
		log.Printf("[%s] bootstrapped — waiting to become leader...", cfg.NodeID)
		// بارگذاری ClientConfigها از DB پس از انتخاب leader
		go bootstrapConfigs(s, pg, cfg.GRPCAddr)
	} else {
		if err := joinCluster(joinAddr, s, cfg.NodeID, cfg.RaftPeer, cfg.GRPCAddr); err != nil {
			log.Fatalf("join: %v", err)
		}
		log.Printf("[%s] joined cluster via %s", cfg.NodeID, joinAddr)
	}

	// ── gRPC Server ───────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.GRPCAddr, err)
	}

	srv := grpc.NewServer()
	pb.RegisterConnectionServiceServer(srv, server.NewConnectionServer(s))
	pb.RegisterConfigServiceServer(srv, server.NewConfigServer(s))
	pb.RegisterClusterServiceServer(srv, server.NewClusterServer(s))
	pb.RegisterCommandStreamServiceServer(srv, server.NewStreamServer(hub))
	reflection.Register(srv) // برای grpcurl و testing

	log.Printf("[%s] gRPC listening on %s", cfg.NodeID, cfg.GRPCAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// =============================================================
// Bootstrap — بارگذاری ClientConfig از DB به FSM
// فقط یک‌بار و فقط روی leader اجرا می‌شود
// =============================================================

func bootstrapConfigs(s *store.Store, pg *db.Postgres, grpcAddr string) {
	// صبر می‌کنیم leader شود (حداکثر ۲۰ ثانیه)
	for i := 0; i < 40; i++ {
		if s.IsLeader() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !s.IsLeader() {
		log.Println("[bootstrap] not leader, skipping")
		return
	}
	if s.FSM.HasClientConfigs() {
		log.Println("[bootstrap] FSM already has configs, skipping")
		return
	}

	rows, err := pg.LoadAll()
	if err != nil {
		log.Printf("[bootstrap] DB error: %v", err)
		return
	}
	if len(rows) == 0 {
		log.Println("[bootstrap] no configs in DB")
		return
	}

	// ارسال از طریق gRPC به خودمان
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		log.Printf("[bootstrap] dial self: %v", err)
		return
	}
	defer conn.Close()

	cfgs := make([]*pb.ClientConfig, len(rows))
	for i, r := range rows {
		cfgs[i] = &pb.ClientConfig{
			Id:                          r.ID,
			SystemId:                    r.SystemID,
			PasswordHash:                r.PasswordHash,
			MaxConnections:              r.MaxConnections,
			TpsLimit:                    r.TPSLimit,
			SubmitRespMessageIdType:     r.SubmitRespMessageIDType,
			DeliveryReportMessageIdType: r.DeliveryReportMessageIDType,
		}
	}

	resp, err := pb.NewConfigServiceClient(conn).BootstrapConfigs(
		context.Background(),
		&pb.BootstrapConfigsRequest{Configs: cfgs},
	)
	if err != nil {
		log.Printf("[bootstrap] error: %v", err)
		return
	}
	log.Printf("[bootstrap] loaded %d configs from DB into Raft FSM", resp.LoadedCount)
}

// =============================================================
// Join — پیوستن به cluster موجود (با retry)
// =============================================================

func joinCluster(leaderGRPC string, s *store.Store, nodeID, raftPeer, grpcAddr string) error {
	for attempt := 1; attempt <= 15; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, leaderGRPC, grpc.WithInsecure(), grpc.WithBlock())
		cancel()
		if err != nil {
			log.Printf("[join] attempt %d: cannot dial %s: %v", attempt, leaderGRPC, err)
			time.Sleep(3 * time.Second)
			continue
		}

		resp, err := pb.NewClusterServiceClient(conn).Join(
			context.Background(),
			&pb.JoinRequest{NodeId: nodeID, RaftAddr: raftPeer, GrpcAddr: grpcAddr},
		)
		conn.Close()

		if err != nil {
			log.Printf("[join] attempt %d: rpc error: %v", attempt, err)
			time.Sleep(3 * time.Second)
			continue
		}
		if resp.ErrorCode != pb.ErrorCode_ERROR_CODE_SUCCESS {
			return fmt.Errorf("join rejected: %s", resp.ErrorMessage)
		}

		// ثبت آدرس gRPC leader برای forwarding آینده
		s.RegisterNodeGRPC(leaderGRPC, leaderGRPC) // placeholder — status call زیر این را اصلاح می‌کند

		// دریافت وضعیت کامل cluster برای ثبت آدرس‌های همه نودها
		go populateNodeGRPC(leaderGRPC, s)
		return nil
	}
	return fmt.Errorf("could not join %s after 15 attempts", leaderGRPC)
}

// populateNodeGRPC وضعیت cluster را از leader می‌خواند و آدرس‌های gRPC را ثبت می‌کند
func populateNodeGRPC(leaderGRPC string, s *store.Store) {
	time.Sleep(2 * time.Second) // صبر می‌کنیم Raft آماده شود
	conn, err := grpc.Dial(leaderGRPC, grpc.WithInsecure(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		return
	}
	defer conn.Close()

	resp, err := pb.NewClusterServiceClient(conn).GetClusterStatus(
		context.Background(), &pb.ClusterStatusRequest{})
	if err != nil {
		return
	}
	for _, n := range resp.Nodes {
		if n.RaftAddr != "" && n.GrpcAddr != "" {
			s.RegisterNodeGRPC(n.RaftAddr, n.GrpcAddr)
		}
	}
	log.Printf("[%s] populated %d node addresses from cluster status", s.NodeID(), len(resp.Nodes))
}

// =============================================================
// Helpers
// =============================================================

func mustConnectDB(dsn string) *db.Postgres {
	for i := 1; i <= 20; i++ {
		pg, err := db.New(dsn)
		if err == nil {
			log.Println("[db] connected to PostgreSQL")
			return pg
		}
		log.Printf("[db] attempt %d/20: %v", i, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatal("[db] could not connect to PostgreSQL after 20 attempts")
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
