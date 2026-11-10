# veritystream

A high-performance, modular Go library for **real-time chat**, **log & LLM token streaming (SSE / WebSockets)**, and **chunked file sharing** with zero runtime bloat.

Designed to be embedded into any Go backend service across the **Verity ecosystem** (`verity-cloud`, `go-apidocs`) or standalone microservices.

---

## Features

- ⚡ **Dual Protocols**:
  - **WebSockets**: Bi-directional messages, active typing indicators, presence tracking.
  - **Server-Sent Events (SSE)**: HTTP/2-friendly event streaming for process logs and AI tokens.
- 🤖 **AI Token & Process Output Streaming**: Dedicated `StreamToken` API tailored for LLM chat responses and command stdout/stderr pipelines.
- 📦 **Chunked File Streaming**:
  - Resumable chunk upload handler with bounded memory footprint.
  - Multi-part file reassembly with SHA-256 integrity validation.
  - Built-in HTTP Range streaming for audio, video, and logs.
  - Pluggable storage abstraction (`LocalStorage` included; extensible to S3/MinIO).
- 🛡️ **Production-Hardened**:
  - Strict path traversal prevention (`validateID`) rejecting directory escalations.
  - Atomic metadata persistence (`fsync` + temp file renames) to prevent file corruption on power failure.
  - Configurable heartbeats, write deadlines, and buffer sizes with slow-consumer drop hooks (`WithOnDrop`).
  - Graceful shutdown (`hub.Shutdown(ctx)`) with normal closure frames (`CloseNormalClosure`).
- 🎨 **Embedded Web Console**:
  - Built-in dark mode web console with room switcher, token streamer, and chunked uploader embedded directly into the Go binary (`//go:embed`).

---

## Installation

```bash
go get github.com/Natykufsky/veritystream@v0.1.1
```

---

## Quick Start

### 1. Initialize Hub and Mount Routes

```go
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/Natykufsky/veritystream"
)

func main() {
	ctx := context.Background()

	// 1. Initialize local file storage
	storage, err := veritystream.NewLocalStorage("./data/streams")
	if err != nil {
		panic(err)
	}

	// 2. Initialize Hub with optional tuning
	hub := veritystream.NewHub(
		storage,
		veritystream.WithHeartbeat(30*time.Second),
		veritystream.WithClientSendBufferSize(512),
	)
	go hub.Run(ctx)

	fileServer := veritystream.NewFileServer(hub)

	// 3. Mount directly to your router (chi, gin, standard mux, etc.)
	mux := http.NewServeMux()
	mux.Handle("/ws", hub.WebSocketHandler(nil))
	mux.Handle("/sse", hub.SSEHandler(nil))
	mux.Handle("/api/files/", http.StripPrefix("/api/files", fileServer.Handler()))

	// Mount the embedded web console UI
	mux.Handle("/console", veritystream.ConsoleHandler())

	http.ListenAndServe(":8080", mux)
}
```

---

### 2. Stream AI Tokens or Process Logs

Stream step logs or LLM token generations to any room:

```go
// Stream step logs or LLM generation chunk by chunk
hub.StreamToken("deployer:job-42", "stream-01", "daemon", 0, "Pulling container image...\n", false)
hub.StreamToken("deployer:job-42", "stream-01", "daemon", 1, "Compiling Go binary...\n", false)
hub.StreamToken("deployer:job-42", "stream-01", "daemon", 2, "Deploy finished successfully.", true)
```

---

### 3. Custom Authentication / Room Authorization

Pass a custom `AuthorizerFunc` to inspect JWT tokens, sessions, or request headers:

```go
auth := func(r *http.Request, targetRoom string) (veritystream.Identity, bool, error) {
	token := r.Header.Get("Authorization")
	// Verify token with your auth service...
	if token == "" {
		return veritystream.Identity{}, false, nil
	}

	return veritystream.Identity{
		ClientID:   "user_123",
		SenderName: "Alice",
		RoomID:     targetRoom,
	}, true, nil
}

mux.Handle("/ws", hub.WebSocketHandler(auth))
mux.Handle("/sse", hub.SSEHandler(auth))
```

---

### 4. Run the Example Application

```bash
git clone https://github.com/Natykufsky/veritystream.git
cd veritystream
go run ./examples/chat_and_logs/main.go
```

Open your browser at `http://localhost:8080/` to explore the interactive dark-mode console.

---

## Testing

Run unit tests and the race detector:

```bash
go test -v -race ./...
```

---

## License

MIT © [Kufre N Moses](https://github.com/Natykufsky)
