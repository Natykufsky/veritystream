package veritystream

import (
	"net/http"
	"time"
)

// HubConfig contains all runtime tuning and security parameters for Hub and WebSockets.
type HubConfig struct {
	// WriteWait is the time allowed to write a message to the peer.
	WriteWait time.Duration
	// PongWait is the time allowed to read the next pong message from the peer.
	PongWait time.Duration
	// PingPeriod is the interval to send pings to peer. Must be less than PongWait.
	PingPeriod time.Duration
	// MaxMessageSize is the maximum message size in bytes allowed from peer.
	MaxMessageSize int64
	// BroadcastBufferSize is the buffer capacity for the hub's incoming message queue.
	BroadcastBufferSize int
	// ClientSendBufferSize is the buffer capacity for per-client outbound channels.
	ClientSendBufferSize int
	// CheckOrigin checks the request origin for WebSocket connections.
	CheckOrigin func(r *http.Request) bool
	// OnDrop is invoked when a client's send buffer is saturated and a message is dropped.
	OnDrop func(client *Client, msg Message)
	// MaxChunkSize is the maximum allowed size in bytes for a single uploaded chunk.
	MaxChunkSize int64
}

// DefaultHubConfig returns a production-ready baseline configuration.
func DefaultHubConfig() HubConfig {
	pongWait := 60 * time.Second
	return HubConfig{
		WriteWait:            10 * time.Second,
		PongWait:             pongWait,
		PingPeriod:           (pongWait * 9) / 10,
		MaxMessageSize:       1024 * 1024, // 1MB
		BroadcastBufferSize:  2048,
		ClientSendBufferSize: 256,
		CheckOrigin: func(r *http.Request) bool {
			return true // Can be overridden via WithCheckOrigin or WithAllowedOrigins
		},
		OnDrop:       nil,
		MaxChunkSize: 32 * 1024 * 1024, // 32MB
	}
}

// HubOption configures a Hub during instantiation.
type HubOption func(*HubConfig)

// WithWriteWait sets the network write deadline.
func WithWriteWait(d time.Duration) HubOption {
	return func(c *HubConfig) {
		if d > 0 {
			c.WriteWait = d
		}
	}
}

// WithHeartbeat sets the PongWait and calculates PingPeriod accordingly.
func WithHeartbeat(pongWait time.Duration) HubOption {
	return func(c *HubConfig) {
		if pongWait > 0 {
			c.PongWait = pongWait
			c.PingPeriod = (pongWait * 9) / 10
		}
	}
}

// WithMaxMessageSize sets the maximum inbound WebSocket message size.
func WithMaxMessageSize(bytes int64) HubOption {
	return func(c *HubConfig) {
		if bytes > 0 {
			c.MaxMessageSize = bytes
		}
	}
}

// WithBroadcastBufferSize sets the capacity of the hub's central broadcast channel.
func WithBroadcastBufferSize(size int) HubOption {
	return func(c *HubConfig) {
		if size > 0 {
			c.BroadcastBufferSize = size
		}
	}
}

// WithClientSendBufferSize sets the capacity of per-client outbound channels.
func WithClientSendBufferSize(size int) HubOption {
	return func(c *HubConfig) {
		if size > 0 {
			c.ClientSendBufferSize = size
		}
	}
}

// WithCheckOrigin configures a custom origin validation function for WebSocket upgrades.
func WithCheckOrigin(check func(r *http.Request) bool) HubOption {
	return func(c *HubConfig) {
		if check != nil {
			c.CheckOrigin = check
		}
	}
}

// WithOnDrop registers a callback for telemetry/metrics when a slow consumer drops a message.
func WithOnDrop(fn func(client *Client, msg Message)) HubOption {
	return func(c *HubConfig) {
		c.OnDrop = fn
	}
}

// WithMaxChunkSize sets the maximum size allowed for file chunk uploads.
func WithMaxChunkSize(bytes int64) HubOption {
	return func(c *HubConfig) {
		if bytes > 0 {
			c.MaxChunkSize = bytes
		}
	}
}

