package veritystream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	// ErrHubClosed is returned or indicated when the hub has been shut down.
	ErrHubClosed = errors.New("veritystream: hub is closed")
)

// Client represents a connected user/agent session in a room.
type Client struct {
	ID         string
	RoomID     string
	SenderName string
	hub        *Hub
	ws         *websocket.Conn
	isSSE      bool
	send       chan Message
	closed     chan struct{}
	closeOnce  sync.Once
	isClosed   atomic.Bool
}

// IsClosed returns whether the client has been closed.
func (c *Client) IsClosed() bool {
	return c.isClosed.Load()
}

// Hub manages active rooms, participant connections, and message dispatch.
type Hub struct {
	mu           sync.RWMutex
	rooms        map[string]map[*Client]bool
	broadcast    chan Message
	register     chan *Client
	unregister   chan *Client
	storage      StorageBackend
	historyHooks []func(msg Message)
	config       HubConfig
	upgrader     websocket.Upgrader

	closed   chan struct{}
	shutdown atomic.Bool
	wg       sync.WaitGroup
}

// NewHub creates a new streaming Hub with optional configurations.
func NewHub(storage StorageBackend, opts ...HubOption) *Hub {
	cfg := DefaultHubConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if storage == nil {
		ls, _ := NewLocalStorage("./data/veritystream")
		storage = ls
	}

	h := &Hub{
		rooms:      make(map[string]map[*Client]bool),
		broadcast:  make(chan Message, cfg.BroadcastBufferSize),
		register:   make(chan *Client, 256),
		unregister: make(chan *Client, 256),
		storage:    storage,
		config:     cfg,
		closed:     make(chan struct{}),
	}

	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     cfg.CheckOrigin,
	}

	return h
}

// Config returns the active HubConfig.
func (h *Hub) Config() HubConfig {
	return h.config
}

// Storage returns the configured storage backend.
func (h *Hub) Storage() StorageBackend {
	return h.storage
}

// OnMessage appends a hook callback whenever any message is broadcasted (useful for DB persistence).
func (h *Hub) OnMessage(fn func(msg Message)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.historyHooks = append(h.historyHooks, fn)
}

// Run executes the core event routing loop until ctx is cancelled or Shutdown is called.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			_ = h.Shutdown(context.Background())
			return
		case <-h.closed:
			return
		case client, ok := <-h.register:
			if !ok {
				return
			}
			h.mu.Lock()
			clients := h.rooms[client.RoomID]
			if clients == nil {
				clients = make(map[*Client]bool)
				h.rooms[client.RoomID] = clients
			}
			clients[client] = true
			h.mu.Unlock()

			// Broadcast presence join
			_ = h.BroadcastPresence(client.RoomID, client.ID, "online")

		case client, ok := <-h.unregister:
			if !ok {
				return
			}
			h.mu.Lock()
			if clients, exists := h.rooms[client.RoomID]; exists {
				if _, ok := clients[client]; ok {
					delete(clients, client)
					client.Close()
					if len(clients) == 0 {
						delete(h.rooms, client.RoomID)
					}
				}
			}
			h.mu.Unlock()

			// Broadcast presence leave
			_ = h.BroadcastPresence(client.RoomID, client.ID, "offline")

		case msg, ok := <-h.broadcast:
			if !ok {
				return
			}
			h.mu.RLock()
			clients := h.rooms[msg.RoomID]
			for client := range clients {
				if client.IsClosed() {
					continue
				}
				select {
				case client.send <- msg:
				default:
					// Backpressure: client send buffer is full
					if h.config.OnDrop != nil {
						h.config.OnDrop(client, msg)
					}
				}
			}
			hooks := append([]func(msg Message){}, h.historyHooks...)
			h.mu.RUnlock()

			for _, hook := range hooks {
				hook(msg)
			}
		}
	}
}

// Shutdown gracefully terminates the hub, unregistering and closing all connections.
func (h *Hub) Shutdown(ctx context.Context) error {
	if !h.shutdown.CompareAndSwap(false, true) {
		return nil // already shut down
	}

	close(h.closed)

	// Collect and close all connected clients
	h.mu.Lock()
	var allClients []*Client
	for _, clients := range h.rooms {
		for client := range clients {
			allClients = append(allClients, client)
		}
	}
	h.rooms = make(map[string]map[*Client]bool)
	h.mu.Unlock()

	for _, client := range allClients {
		client.Close()
	}

	// Wait for goroutines with timeout
	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// Publish enqueues a message to be broadcasted to its room.
func (h *Hub) Publish(msg Message) {
	if h.shutdown.Load() {
		return
	}
	if msg.ID == "" {
		msg.ID = uuid.NewString()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}

	select {
	case <-h.closed:
		return
	case h.broadcast <- msg:
	default:
		// Broadcast channel full; drop or log
	}
}

// StreamToken sends a single token or log chunk to a room.
func (h *Hub) StreamToken(roomID, streamID, senderID string, index int, chunk string, done bool) {
	payload, _ := json.Marshal(StreamTokenPayload{
		StreamID: streamID,
		Index:    index,
		Chunk:    chunk,
		Done:     done,
	})
	mType := MsgStreamToken
	if done {
		mType = MsgStreamEnd
	}
	h.Publish(Message{
		RoomID:    roomID,
		SenderID:  senderID,
		Type:      mType,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	})
}

// BroadcastPresence sends an online/offline or custom status update.
func (h *Hub) BroadcastPresence(roomID, userID, status string) error {
	payload, _ := json.Marshal(PresencePayload{
		UserID: userID,
		Status: status,
	})
	h.Publish(Message{
		RoomID:    roomID,
		SenderID:  userID,
		Type:      MsgPresence,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	})
	return nil
}

// Close cleanly stops the client connection and resources.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.isClosed.Store(true)
		close(c.closed)
		if c.ws != nil {
			// Send standard normal closure frame
			_ = c.ws.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "connection closed"),
				time.Now().Add(time.Second),
			)
			_ = c.ws.Close()
		}
	})
}

// HandleWebSocket upgrades and attaches a WebSocket client to a room.
func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request, roomID, clientID, senderName string) error {
	if h.shutdown.Load() {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return ErrHubClosed
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}

	client := &Client{
		ID:         clientID,
		RoomID:     roomID,
		SenderName: senderName,
		hub:        h,
		ws:         conn,
		send:       make(chan Message, h.config.ClientSendBufferSize),
		closed:     make(chan struct{}),
	}

	select {
	case <-h.closed:
		client.Close()
		return ErrHubClosed
	case h.register <- client:
	}

	h.wg.Add(2)
	go func() {
		defer h.wg.Done()
		client.wsWritePump()
	}()
	go func() {
		defer h.wg.Done()
		client.wsReadPump()
	}()

	return nil
}

// HandleSSE attaches a client receiving server-sent events for a room.
func (h *Hub) HandleSSE(w http.ResponseWriter, r *http.Request, roomID, clientID string) {
	if h.shutdown.Load() {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	client := &Client{
		ID:     clientID,
		RoomID: roomID,
		hub:    h,
		isSSE:  true,
		send:   make(chan Message, h.config.ClientSendBufferSize),
		closed: make(chan struct{}),
	}

	select {
	case <-h.closed:
		return
	case h.register <- client:
	}

	defer func() {
		select {
		case <-h.closed:
		case h.unregister <- client:
		}
		client.Close()
	}()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case <-h.closed:
			return
		case <-client.closed:
			return
		case msg, ok := <-client.send:
			if !ok {
				return
			}
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.Type, data)
			flusher.Flush()
		}
	}
}

func (c *Client) wsWritePump() {
	ticker := time.NewTicker(c.hub.config.PingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(c.hub.config.WriteWait))
			if !ok {
				_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.ws.WriteJSON(msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(c.hub.config.WriteWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Client) wsReadPump() {
	defer func() {
		select {
		case <-c.hub.closed:
		case c.hub.unregister <- c:
		}
		c.Close()
	}()

	c.ws.SetReadLimit(c.hub.config.MaxMessageSize)
	_ = c.ws.SetReadDeadline(time.Now().Add(c.hub.config.PongWait))
	c.ws.SetPongHandler(func(string) error {
		_ = c.ws.SetReadDeadline(time.Now().Add(c.hub.config.PongWait))
		return nil
	})

	for {
		var incoming Message
		if err := c.ws.ReadJSON(&incoming); err != nil {
			break
		}
		incoming.RoomID = c.RoomID
		incoming.SenderID = c.ID
		incoming.SenderName = c.SenderName
		c.hub.Publish(incoming)
	}
}
