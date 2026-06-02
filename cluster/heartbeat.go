package cluster

import (
	"time"
)

type Heartbeat struct {
	interval time.Duration
	stop     chan struct{}
}

func NewHeartbeat(interval time.Duration) *Heartbeat {
	return &Heartbeat{
		interval: interval,
		stop:     make(chan struct{}),
	}
}

func (h *Heartbeat) Start(fn func()) {
	ticker := time.NewTicker(h.interval)

	go func() {
		for {
			select {
			case <-ticker.C:
				fn()
			case <-h.stop:
				return
			}
		}
	}()
}

func (h *Heartbeat) Stop() {
	close(h.stop)
}

func (e *Election) heartbeat() {
	ticker := time.NewTicker(50 * time.Millisecond)

	for range ticker.C {
		if e.state.Role() != consensus.Leader {
			return
		}

		for _, p := range e.pm.All() {
			go func(peer *Peer) {
				c, err := transport.Dial(peer.Addr)
				if err != nil {
					return
				}
				defer c.Conn().Close()

				payload := transport.EncodeTLV(0x01, []byte("hb"))

				transport.WriteFrame(
					c.Conn(),
					transport.MSG_HEARTBEAT,
					payload,
				)
			}(p)
		}
	}
}