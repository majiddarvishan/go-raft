package transport

const (
	Magic   uint16 = 0xCAFE
	Version byte   = 1
)

// Message types
const (
	MSG_HEARTBEAT = 0x01
	MSG_VOTE_REQ  = 0x02
	MSG_VOTE_RES  = 0x03
	MSG_APPEND    = 0x04
	MSG_ACK       = 0x05
	MSG_CLIENT    = 0x06
	MSG_RESPONSE  = 0x07
)

// TLV Tags
const (
	TAG_TERM       = 0x01
	TAG_NODE_ID    = 0x02
	TAG_REQUEST_ID = 0x03
	TAG_KEY        = 0x04
	TAG_VALUE      = 0x05
	TAG_INDEX      = 0x06
	TAG_COMMIT     = 0x07
    TAG_VOTE_GRANTED = 0x08
)
