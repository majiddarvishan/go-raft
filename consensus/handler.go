package consensus

import (
	"dist/transport"
	"net"
)

type Handler struct {
	state *State
}

func NewHandler(s *State) *Handler {
	return &Handler{state: s}
}

func (h *Handler) Handle(frame transport.Frame, conn net.Conn) {
	switch frame.Type {

	case transport.MSG_VOTE_REQ:
		h.handleVoteReq(frame, conn)

	case transport.MSG_HEARTBEAT:
		h.handleHeartbeat(frame)

	}
}

func (h *Handler) handleVoteReq(f transport.Frame, conn net.Conn) {
	tlv := transport.DecodeTLV(f.Payload)

	term := bytesToUint64(tlv[0x01])

	granted := false

	if term >= h.state.Term() {
		h.state.SetRole(Follower)
		granted = true
	}

	resp := transport.EncodeTLV(0x08, boolToBytes(granted))

	transport.WriteFrame(conn, transport.MSG_VOTE_RES, resp)
}

func (h *Handler) handleHeartbeat(f transport.Frame) {
	h.state.SetRole(Follower)
}