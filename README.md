# veritystream

A modular, high-performance Go library for **real-time chat streaming**, **log & LLM token streaming**, and **chunked file sharing**.

Designed for seamless sharing across the **Verity ecosystem** (`deployer-service`, `go-apidocs`, `verity-cloud`, `monakbanker-api`).

---

## Features

- **Multi-room Hub**: Topic-based multiplexing with non-blocking channels and backpressure safety.
- **Dual-Protocol**:
  - **WebSockets**: Bi-directional chat messages, typing status, user presence.
  - **Server-Sent Events (SSE)**: Lightweight HTTP/2-friendly event streaming for live deploy logs and AI tokens.
- **Token Streaming**: Specialized `StreamToken` API tailored for LLM chat responses and real-time process output (stdout/stderr).
- **Chunked File Streaming**:
  - Resumable chunk upload handler.
  - Multi-part file reassembly with SHA-256 integrity validation.
  - HTTP Range streaming support for logs, images, and audio/video previews.
  - Pluggable storage abstraction (`LocalStorage` built-in, extensible to S3/MinIO).
- **Zero Heavy Dependencies**: Built on standard library with clean `gorilla/websocket` integration.

---

## Installation

```bash
go get github.com/Natykufsky/veritystream
```

---

## Quick Start

### 1. Initialize Hub and File Server

```go
package main

import (
	"context"
	"net/http"

	"github.com/Natykufsky/veritystream"
)

func main() {
	storage, err := veritystream.NewLocalStorage("./data/streams")
	if err != nil {
		panic(err)
	}

	hub := veritystream.NewHub(storage)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)

	fileServer := veritystream.NewFileServer(hub)

	// Attach WebSocket endpoint
	http.HandleFunc("/ws/chat", func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		user := r.URL.Query().Get("user")
		_ = hub.HandleWebSocket(w, r, room, user, user)
	})

	// Attach SSE endpoint
	http.HandleFunc("/sse/events", func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		user := r.URL.Query().Get("user")
		hub.HandleSSE(w, r, room, user)
	})

	// Attach File Streaming endpoints
	http.HandleFunc("/api/files/chunk", fileServer.HandleUploadChunk)
	http.HandleFunc("/api/files/complete", fileServer.HandleCompleteChunkUpload)
	http.HandleFunc("/api/files/upload", fileServer.HandleDirectUpload)
	http.HandleFunc("/api/files/download", fileServer.HandleDownload)

	http.ListenAndServe(":8080", nil)
}
```

### 2. Stream AI Tokens or Deploy Logs to a Room

```go
// Stream step logs or LLM generation chunk by chunk
hub.StreamToken("deployer:job-42", "stream-01", "daemon", 0, "Pulling container image...\n", false)
hub.StreamToken("deployer:job-42", "stream-01", "daemon", 1, "Deploy finished successfully.", true)
```

---

## License

MIT
