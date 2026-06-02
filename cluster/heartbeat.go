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