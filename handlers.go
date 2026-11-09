package veritystream

import (
	"context"
	"net/http"
)

// Identity represents the authenticated client context extracted from an incoming request.
type Identity struct {
	ClientID   string
	SenderName string
	RoomID     string
	Metadata   map[string]string
}

// AuthorizerFunc inspects the incoming HTTP handshake request and determines
// if the connection is permitted, returning the resolved identity.
// If allowed is false, the connection is rejected with 401/403.
type AuthorizerFunc func(r *http.Request, targetRoom string) (identity Identity, allowed bool, err error)

// DefaultAuthorizer provides a simple fallback reading "room", "user", and "name" from query parameters.
func DefaultAuthorizer(r *http.Request, targetRoom string) (Identity, bool, error) {
	q := r.URL.Query()
	room := targetRoom
	if room == "" {
		room = q.Get("room")
	}
	user := q.Get("user")
	if user == "" {
		user = "anonymous"
	}
	name := q.Get("name")
	if name == "" {
		name = user
	}

	return Identity{
		ClientID:   user,
		SenderName: name,
		RoomID:     room,
	}, true, nil
}

// WebSocketHandler returns an http.Handler that authenticates and attaches WebSocket clients.
// Can be mounted directly to any router (chi, gin, echo, gorilla/mux, http.DefaultServeMux).
func (h *Hub) WebSocketHandler(auth AuthorizerFunc) http.Handler {
	if auth == nil {
		auth = DefaultAuthorizer
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		ident, allowed, err := auth(r, room)
		if err != nil || !allowed {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if ident.RoomID == "" {
			http.Error(w, "room required", http.StatusBadRequest)
			return
		}

		_ = h.HandleWebSocket(w, r, ident.RoomID, ident.ClientID, ident.SenderName)
	})
}

// SSEHandler returns an http.Handler that authenticates and streams SSE events to clients.
func (h *Hub) SSEHandler(auth AuthorizerFunc) http.Handler {
	if auth == nil {
		auth = DefaultAuthorizer
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		ident, allowed, err := auth(r, room)
		if err != nil || !allowed {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if ident.RoomID == "" {
			http.Error(w, "room required", http.StatusBadRequest)
			return
		}

		h.HandleSSE(w, r, ident.RoomID, ident.ClientID)
	})
}

// Mount mounts all file server endpoints into a standard http.Handler sub-router.
// This allows host services to mount it under any prefix, e.g. router.Mount("/files", fs.Handler()).
func (fs *FileServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/chunk", fs.HandleUploadChunk)
	mux.HandleFunc("/complete", fs.HandleCompleteChunkUpload)
	mux.HandleFunc("/upload", fs.HandleDirectUpload)
	mux.HandleFunc("/download", fs.HandleDownload)
	return mux
}

// RoomStats holds snapshot statistics for active rooms.
type RoomStats struct {
	TotalRooms   int            `json:"total_rooms"`
	TotalClients int            `json:"total_clients"`
	RoomClients  map[string]int `json:"room_clients"`
}

// Stats returns a safe read-only snapshot of active room counts and connection counts.
func (h *Hub) Stats(ctx context.Context) RoomStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := RoomStats{
		TotalRooms:  len(h.rooms),
		RoomClients: make(map[string]int, len(h.rooms)),
	}

	for roomID, clients := range h.rooms {
		count := len(clients)
		stats.RoomClients[roomID] = count
		stats.TotalClients += count
	}

	return stats
}
