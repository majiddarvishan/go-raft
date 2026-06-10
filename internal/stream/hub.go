// Package stream مدیریت اتصالات streaming کلاینت‌ها روی این نود.
// هر نود یک Hub مستقل دارد. Broadcast فقط روی همین نود کار می‌کند.
package stream

import (
	"fmt"
	"sync"
	"time"
)

// Command دستوری که سرور برای کلاینت ارسال می‌کند.
type Command struct {
	CommandID string
	Type      int32
	Payload   string
	Timestamp int64
}

// Hub registry اتصالات streaming فعال.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]chan Command // requester_id → channel
}

func NewHub() *Hub {
	return &Hub{clients: make(map[string]chan Command)}
}

// Subscribe کانالی برای کلاینت ایجاد می‌کند.
// تابع برگشتی را حتماً defer کنید.
func (h *Hub) Subscribe(requesterID string) (<-chan Command, func()) {
	ch := make(chan Command, 16)
	h.mu.Lock()
	h.clients[requesterID] = ch
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.clients, requesterID)
		close(ch)
		h.mu.Unlock()
	}
}

// Send دستور را به کلاینت مشخص ارسال می‌کند.
// false = کلاینت subscribed نیست یا channel پر است.
func (h *Hub) Send(requesterID string, cmd Command) bool {
	h.mu.RLock()
	ch, ok := h.clients[requesterID]
	h.mu.RUnlock()
	if !ok {
		return false
	}
	select {
	case ch <- cmd:
		return true
	default:
		return false
	}
}

// Broadcast دستور را به همه کلاینت‌های متصل ارسال می‌کند.
// تعداد کلاینت‌هایی که دستور را دریافت کردند برمی‌گرداند.
func (h *Hub) Broadcast(cmd Command) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	sent := 0
	for _, ch := range h.clients {
		select {
		case ch <- cmd:
			sent++
		default:
		}
	}
	return sent
}

// ConnectedClients لیست requester_id کلاینت‌های subscribed را برمی‌گرداند.
func (h *Hub) ConnectedClients() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]string, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	return ids
}

// NewCommandID یک ID یکتا برای دستور تولید می‌کند.
func NewCommandID() string {
	return fmt.Sprintf("cmd-%d", time.Now().UnixNano())
}
