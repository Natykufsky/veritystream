package veritystream

import (
	"encoding/json"
	"time"
)

// MessageType distinguishes normal chat messages, token streams (AI / logs), presence, and file notifications.
type MessageType string

const (
	// MsgText represents a standard text message.
	MsgText MessageType = "text"
	// MsgStreamToken represents an incremental token for LLM answers or deployment stdout/stderr lines.
	MsgStreamToken MessageType = "stream_token"
	// MsgStreamEnd marks the completion of a stream sequence.
	MsgStreamEnd MessageType = "stream_end"
	// MsgFileShare notifies participants of an uploaded or available file.
	MsgFileShare MessageType = "file_share"
	// MsgPresence indicates user join/leave/status events.
	MsgPresence MessageType = "presence"
	// MsgTyping represents active typing indicators.
	MsgTyping MessageType = "typing"
	// MsgSystem represents internal room/system notices.
	MsgSystem MessageType = "system"
)

// Message is the universal real-time payload flowing through VerityStream.
type Message struct {
	ID         string          `json:"id"`
	RoomID     string          `json:"room_id"`
	SenderID   string          `json:"sender_id"`
	SenderName string          `json:"sender_name,omitempty"`
	Type       MessageType     `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
}

// StreamTokenPayload defines the payload for live token or log line streams.
type StreamTokenPayload struct {
	StreamID string `json:"stream_id"`
	Index    int    `json:"index"`
	Chunk    string `json:"chunk"`
	Done     bool   `json:"done"`
}

// PresencePayload defines user presence changes.
type PresencePayload struct {
	UserID string `json:"user_id"`
	Status string `json:"status"` // e.g. "online", "offline", "idle"
}

// TypingPayload defines typing indicator events.
type TypingPayload struct {
	UserID   string `json:"user_id"`
	IsTyping bool   `json:"is_typing"`
}
