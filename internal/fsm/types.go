package fsm

// ===================== Command Types =====================

type CommandType string

const (
	CmdRegisterConnection       CommandType = "register_connection"
	CmdUnregisterConnection     CommandType = "unregister_connection"
	CmdUnregisterAllConnections CommandType = "unregister_all_connections"
	CmdUpsertClientConfig       CommandType = "upsert_client_config"
	CmdDeleteClientConfig       CommandType = "delete_client_config"
)

type Command struct {
	Type    CommandType `json:"type"`
	Payload []byte      `json:"payload"`
}

// ===================== Payloads =====================

type RegisterConnectionPayload struct {
	RequesterID     string `json:"requester_id"`
	SourceIP        string `json:"source_ip"`
	BindType        int32  `json:"bind_type"`
	SystemID        string `json:"system_id"`
	SystemType      string `json:"system_type"`
	AuthenticatorID string `json:"authenticator_id"`
	ClientConfigID  int64  `json:"client_config_id"`
	MaxConnections  int32  `json:"max_connections"`
	ConnectedAt     int64  `json:"connected_at"`
}

type UnregisterConnectionPayload struct {
	ConnectionID   int64 `json:"connection_id"`
	DisconnectedAt int64 `json:"disconnected_at"`
}

type UnregisterAllConnectionsPayload struct {
	RequesterID    string `json:"requester_id"`
	DisconnectedAt int64  `json:"disconnected_at"`
}

type UpsertClientConfigPayload struct {
	Config ClientConfigEntry `json:"config"`
}

type DeleteClientConfigPayload struct {
	SystemID string `json:"system_id"`
}

// ===================== Result Codes =====================

type ResultCode int32

const (
	ResultOK                      ResultCode = 1
	ResultMaxConnectionsReached   ResultCode = 400
	ResultConnectionNotFound      ResultCode = 500
	ResultConnectionAlreadyClosed ResultCode = 501
	ResultConfigNotFound          ResultCode = 700
	ResultInternal                ResultCode = 100
)

// ===================== Results =====================

type RegisterResult struct {
	ConnectionID      int64
	ActiveConnections int32
	Code              ResultCode
	Message           string
}

type UnregisterResult struct {
	ActiveConnections int32
	Code              ResultCode
	Message           string
}

type UnregisterAllResult struct {
	RemovedCount int32
	Code         ResultCode
	Message      string
}

type UpsertClientConfigResult struct {
	Code    ResultCode
	Message string
}

type DeleteClientConfigResult struct {
	Code    ResultCode
	Message string
}

// ===================== ClientConfig Entry =====================

type ClientConfigEntry struct {
	ID                          int64    `json:"id"`
	SystemID                    string   `json:"system_id"`
	PasswordHash                string   `json:"password_hash"`
	MaxConnections              int32    `json:"max_connections"`
	TPSLimit                    int32    `json:"tps_limit"`
	SubmitRespMessageIDType     string   `json:"submit_resp_message_id_type"`
	DeliveryReportMessageIDType string   `json:"delivery_report_message_id_type"`
	SystemTypes                 []string `json:"system_types"`
	PermittedBindTypes          []int32  `json:"permitted_bind_types"`
}

// ===================== Internal Connection State =====================

type connectionEntry struct {
	ID              int64
	ClientConfigID  int64
	SourceIP        string
	BindType        int32
	SystemID        string
	SystemType      string
	ConnectedAt     int64
	DisconnectedAt  int64
	AuthenticatorID string
	RequesterID     string
	MaxConnections  int32
}

// ConnectionView — view خواندنی برای لایه‌ی gRPC
type ConnectionView struct {
	ID              int64
	ClientConfigID  int64
	SourceIP        string
	BindType        int32
	ConnectedAt     int64
	DisconnectedAt  int64
	AuthenticatorID string
	RequesterID     string
}
