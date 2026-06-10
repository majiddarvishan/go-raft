// کلاینت CLI برای authenticator cluster
// راه‌اندازی: ./client --server=localhost:9001
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	pb "authenticator/api/proto/auth"
)

const banner = `
╔══════════════════════════════════════════════╗
║   Authenticator CLI — type "help" for cmds   ║
╚══════════════════════════════════════════════╝`

func main() {
	addr := flag.String("server", "localhost:9001", "آدرس gRPC سرور")
	flag.Parse()

	conn, err := grpc.Dial(*addr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("connected to %s\n", *addr)
	fmt.Println(banner)

	c := &cli{
		addr:    *addr,
		connSvc: pb.NewConnectionServiceClient(conn),
		cfgSvc:  pb.NewConfigServiceClient(conn),
		cluSvc:  pb.NewClusterServiceClient(conn),
		strmSvc: pb.NewCommandStreamServiceClient(conn),
	}
	c.repl()
}

type cli struct {
	addr    string
	connSvc pb.ConnectionServiceClient
	cfgSvc  pb.ConfigServiceClient
	cluSvc  pb.ClusterServiceClient
	strmSvc pb.CommandStreamServiceClient
}

// ─────────────────────────────────────────────────────────────
// REPL
// ─────────────────────────────────────────────────────────────

func (c *cli) repl() {
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("\n[%s]> ", c.addr)
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		args := strings.Fields(line)
		if err := c.dispatch(args); err != nil {
			fmt.Printf("✗ %v\n", err)
		}
	}
}

func (c *cli) dispatch(args []string) error {
	switch args[0] {
	// Connection Service
	case "register":
		return c.register(args[1:])
	case "unregister":
		return c.unregister(args[1:])
	case "unregister-all":
		return c.unregisterAll(args[1:])
	case "list":
		return c.listConns(args[1:])
	// Config Service
	case "list-configs":
		return c.listConfigs()
	case "get-config":
		return c.getConfig(args[1:])
	case "upsert-config":
		return c.upsertConfig(args[1:])
	// Cluster
	case "status":
		return c.clusterStatus()
	// Streaming  سرور → کلاینت
	case "watch":
		return c.watch(args[1:])
	case "broadcast":
		return c.broadcast(args[1:])
	case "send":
		return c.sendTo(args[1:])
	// General
	case "help", "?":
		printHelp()
	case "exit", "quit":
		fmt.Println("bye")
		os.Exit(0)
	default:
		fmt.Printf("unknown: %q — type 'help'\n", args[0])
	}
	return nil
}

// ─────────────────────────────────────────────────────────────
// Connection Service
// ─────────────────────────────────────────────────────────────

// register <system_id> <source_ip> [requester_id]
func (c *cli) register(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: register <system_id> <source_ip> [requester_id]")
	}
	requester := "cli-client"
	if len(args) >= 3 {
		requester = args[2]
	}
	resp, err := c.connSvc.RegisterConnection(context.Background(),
		&pb.RegisterConnectionRequest{
			RequesterId: requester,
			SystemId:    args[0],
			SourceIp:    args[1],
			BindType:    pb.BindType_BIND_TYPE_TRANSCEIVER,
			Timestamp:   time.Now().Unix(),
		})
	if err != nil {
		return err
	}
	fmt.Printf("authenticator : %s\n", resp.AuthenticatorId)
	fmt.Printf("error_code    : %s\n", resp.ErrorCode)
	if resp.ErrorCode == pb.ErrorCode_ERROR_CODE_SUCCESS {
		fmt.Printf("connection_id : %d\n", resp.ConnectionId)
		fmt.Printf("active / max  : %d / %d\n", resp.ActiveConnections, resp.MaxConnectionsAllowed)
	} else {
		fmt.Printf("message       : %s\n", resp.ErrorMessage)
	}
	return nil
}

// unregister <connection_id>
func (c *cli) unregister(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: unregister <connection_id>")
	}
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid id: %v", err)
	}
	resp, err := c.connSvc.UnregisterConnection(context.Background(),
		&pb.UnregisterConnectionRequest{
			RequesterId:  "cli-client",
			ConnectionId: id,
			Timestamp:    time.Now().Unix(),
		})
	if err != nil {
		return err
	}
	fmt.Printf("result: %s | active=%d\n", resp.ErrorCode, resp.ActiveConnections)
	return nil
}

// unregister-all <requester_id>
func (c *cli) unregisterAll(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: unregister-all <requester_id>")
	}
	resp, err := c.connSvc.UnregisterAllConnections(context.Background(),
		&pb.UnregisterAllConnectionsRequest{
			RequesterId: args[0],
			Timestamp:   time.Now().Unix(),
		})
	if err != nil {
		return err
	}
	fmt.Printf("result: %s\n", resp.ErrorCode)
	return nil
}

// list [system_id]
func (c *cli) listConns(args []string) error {
	systemID := ""
	if len(args) > 0 {
		systemID = args[0]
	}
	resp, err := c.connSvc.ListConnections(context.Background(),
		&pb.ListConnectionsRequest{RequesterId: "cli-client", SystemId: systemID})
	if err != nil {
		return err
	}
	if len(resp.Connections) == 0 {
		fmt.Println("no active connections")
		return nil
	}
	fmt.Printf("%-14s %-18s %-12s %-14s %s\n",
		"conn_id", "source_ip", "bind_type", "connected_at", "requester_id")
	fmt.Println(strings.Repeat("─", 75))
	for _, cn := range resp.Connections {
		t := time.Unix(0, cn.ConnectedAt).Format("15:04:05.000")
		fmt.Printf("%-14d %-18s %-12s %-14s %s\n",
			cn.Id, cn.SourceIp, cn.BindType, t, cn.RequesterId)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────
// Config Service
// ─────────────────────────────────────────────────────────────

func (c *cli) listConfigs() error {
	resp, err := c.cfgSvc.ListClientConfigs(context.Background(), &pb.ListClientConfigsRequest{})
	if err != nil {
		return err
	}
	if len(resp.Configs) == 0 {
		fmt.Println("no configs in FSM")
		return nil
	}
	fmt.Printf("%-6s %-22s %-8s %-8s\n", "id", "system_id", "max_conn", "tps")
	fmt.Println(strings.Repeat("─", 48))
	for _, cfg := range resp.Configs {
		fmt.Printf("%-6d %-22s %-8d %-8d\n",
			cfg.Id, cfg.SystemId, cfg.MaxConnections, cfg.TpsLimit)
	}
	return nil
}

// get-config <system_id>
func (c *cli) getConfig(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: get-config <system_id>")
	}
	resp, err := c.cfgSvc.GetClientConfig(context.Background(),
		&pb.GetClientConfigRequest{SystemId: args[0]})
	if err != nil {
		return err
	}
	if resp.ErrorCode != pb.ErrorCode_ERROR_CODE_SUCCESS {
		fmt.Printf("error: %s — %s\n", resp.ErrorCode, resp.ErrorMessage)
		return nil
	}
	cfg := resp.Config
	fmt.Printf("id              : %d\n", cfg.Id)
	fmt.Printf("system_id       : %s\n", cfg.SystemId)
	fmt.Printf("max_connections : %d\n", cfg.MaxConnections)
	fmt.Printf("tps_limit       : %d\n", cfg.TpsLimit)
	fmt.Printf("password_hash   : %s\n", cfg.PasswordHash)
	return nil
}

// upsert-config <system_id> <password_hash> <max_conn> <tps>
func (c *cli) upsertConfig(args []string) error {
	if len(args) < 4 {
		return fmt.Errorf("usage: upsert-config <system_id> <password_hash> <max_conn> <tps>")
	}
	maxConn, _ := strconv.Atoi(args[2])
	tps, _     := strconv.Atoi(args[3])

	resp, err := c.cfgSvc.UpsertClientConfig(context.Background(),
		&pb.UpsertClientConfigRequest{Config: &pb.ClientConfig{
			SystemId:       args[0],
			PasswordHash:   args[1],
			MaxConnections: int32(maxConn),
			TpsLimit:       int32(tps),
		}})
	if err != nil {
		return err
	}
	fmt.Printf("result: %s %s\n", resp.ErrorCode, resp.ErrorMessage)
	return nil
}

// ─────────────────────────────────────────────────────────────
// Cluster
// ─────────────────────────────────────────────────────────────

func (c *cli) clusterStatus() error {
	resp, err := c.cluSvc.GetClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("leader: %s (%s)\n\n", resp.LeaderId, resp.LeaderGrpc)
	fmt.Printf("%-10s %-22s %-22s %-10s\n", "node_id", "raft_addr", "grpc_addr", "state")
	fmt.Println(strings.Repeat("─", 68))
	for _, n := range resp.Nodes {
		star := ""
		if n.IsLeader {
			star = " ★"
		}
		fmt.Printf("%-10s %-22s %-22s %-10s%s\n",
			n.NodeId, n.RaftAddr, n.GrpcAddr, n.State, star)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────
// Streaming — سرور → کلاینت
// ─────────────────────────────────────────────────────────────

// watch [requester_id]
// Subscribe به دستورات سرور — blocking تا Ctrl+C
func (c *cli) watch(args []string) error {
	requester := "cli-client"
	if len(args) > 0 {
		requester = args[0]
	}

	strm, err := c.strmSvc.Subscribe(context.Background(),
		&pb.SubscribeRequest{RequesterId: requester})
	if err != nil {
		return fmt.Errorf("subscribe: %v", err)
	}

	fmt.Printf("▶ watching commands for requester_id=%q  (Ctrl+C to stop)\n", requester)
	fmt.Println(strings.Repeat("─", 60))

	for {
		cmd, err := strm.Recv()
		if err == io.EOF {
			fmt.Println("\n◼ stream closed by server")
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream error: %v", err)
		}
		ts := time.Unix(0, cmd.Timestamp).Format("15:04:05.000")
		fmt.Printf("[%s] %-28s id=%-22s payload=%s\n",
			ts, cmd.Type, cmd.CommandId, cmd.Payload)
	}
}

// broadcast <type> [payload]
// سرور دستور را به همه کلاینت‌های در حال watch می‌فرستد
// types: ping | disconnect | throttle | reload | custom
func (c *cli) broadcast(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: broadcast <type> [payload]\n  types: ping disconnect throttle reload custom")
	}
	payload := ""
	if len(args) > 1 {
		payload = strings.Join(args[1:], " ")
	}
	resp, err := c.strmSvc.Broadcast(context.Background(), &pb.BroadcastRequest{
		Type:    parseCmdType(args[0]),
		Payload: payload,
	})
	if err != nil {
		return err
	}
	fmt.Printf("✓ broadcast to %d client(s)\n", resp.SentCount)
	return nil
}

// send <requester_id> <type> [payload]
// دستور به یک کلاینت مشخص
func (c *cli) sendTo(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: send <requester_id> <type> [payload]")
	}
	payload := ""
	if len(args) > 2 {
		payload = strings.Join(args[2:], " ")
	}
	resp, err := c.strmSvc.Broadcast(context.Background(), &pb.BroadcastRequest{
		TargetId: args[0],
		Type:     parseCmdType(args[1]),
		Payload:  payload,
	})
	if err != nil {
		return err
	}
	if resp.SentCount > 0 {
		fmt.Printf("✓ sent to %q\n", args[0])
	} else {
		fmt.Printf("✗ %q not subscribed on this node\n", args[0])
	}
	return nil
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func parseCmdType(s string) pb.ServerCommandType {
	switch strings.ToLower(s) {
	case "ping":
		return pb.ServerCommandType_SERVER_COMMAND_PING
	case "disconnect":
		return pb.ServerCommandType_SERVER_COMMAND_DISCONNECT
	case "throttle":
		return pb.ServerCommandType_SERVER_COMMAND_THROTTLE
	case "reload":
		return pb.ServerCommandType_SERVER_COMMAND_RELOAD_CONFIG
	default:
		return pb.ServerCommandType_SERVER_COMMAND_CUSTOM
	}
}

func printHelp() {
	fmt.Print(`
── Connection Service ─────────────────────────────────────────────────────────
  register <system_id> <source_ip> [requester_id]
  unregister <connection_id>
  unregister-all <requester_id>
  list [system_id]

── Config Service (Raft-backed) ───────────────────────────────────────────────
  list-configs
  get-config <system_id>
  upsert-config <system_id> <password_hash> <max_conn> <tps>

── Cluster ────────────────────────────────────────────────────────────────────
  status

── Streaming — سرور → کلاینت ─────────────────────────────────────────────────
  watch [requester_id]               subscribe (blocking)
  broadcast <type> [payload]         به همه کلاینت‌های subscribed
  send <requester_id> <type> [payload]  به یک کلاینت مشخص

  انواع دستور: ping | disconnect | throttle | reload | custom

── General ────────────────────────────────────────────────────────────────────
  help   exit
`)
}
