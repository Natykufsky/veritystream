package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Natykufsky/veritystream"
)

func main() {
	// 1. Initialize local file storage
	storage, err := veritystream.NewLocalStorage("./tmp/stream-storage")
	if err != nil {
		log.Fatalf("Storage init failed: %v", err)
	}

	// 2. Initialize Hub with custom options
	hub := veritystream.NewHub(
		storage,
		veritystream.WithHeartbeat(30*time.Second),
		veritystream.WithClientSendBufferSize(512),
		veritystream.WithOnDrop(func(client *veritystream.Client, msg veritystream.Message) {
			log.Printf("[DROP] Dropped msg %s for client %s in room %s", msg.ID, client.ID, client.RoomID)
		}),
	)

	// Background message logger hook (example of persistence or auditing)
	hub.OnMessage(func(msg veritystream.Message) {
		log.Printf("[EVENT] Room: %s | Type: %s | From: %s", msg.RoomID, msg.Type, msg.SenderID)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	// 3. File Server
	fileServer := veritystream.NewFileServer(hub)

	// 4. Router setup
	mux := http.NewServeMux()

	// Direct plug-and-play WebSocket & SSE Handlers with custom auth
	customAuth := func(r *http.Request, room string) (veritystream.Identity, bool, error) {
		// Host application can check Bearer tokens, cookies, or session headers here
		token := r.Header.Get("Authorization")
		_ = token // validate token...

		q := r.URL.Query()
		return veritystream.Identity{
			ClientID:   q.Get("user"),
			SenderName: q.Get("name"),
			RoomID:     q.Get("room"),
		}, true, nil
	}

	mux.Handle("/ws", hub.WebSocketHandler(customAuth))
	mux.Handle("/sse", hub.SSEHandler(customAuth))

	// Mount chunked file streaming endpoints under /api/files/
	mux.Handle("/api/files/", http.StripPrefix("/api/files", fileServer.Handler()))

	// Room metrics endpoint
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := hub.Stats(r.Context())
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"total_rooms":%d,"total_clients":%d}`+"\n", stats.TotalRooms, stats.TotalClients)
	})

	// Minimal embedded HTML client for quick testing
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>VerityStream Demo</title></head>
<body style="font-family: sans-serif; padding: 20px;">
  <h2>VerityStream Testbed</h2>
  <p>Connected to room: <code>general</code></p>
  <div id="logs" style="border: 1px solid #ccc; height: 300px; overflow-y: scroll; padding: 8px; background: #fafafa;"></div>
  <input type="text" id="msg" placeholder="Type a message..." style="width: 300px; padding: 6px; margin-top: 8px;"/>
  <button onclick="send()">Send</button>

  <script>
    const logBox = document.getElementById("logs");
    const ws = new WebSocket("ws://" + location.host + "/ws?room=general&user=guest_" + Math.floor(Math.random()*1000));
    ws.onmessage = (e) => {
      const p = document.createElement("div");
      p.textContent = e.data;
      logBox.appendChild(p);
      logBox.scrollTop = logBox.scrollHeight;
    };
    function send() {
      const input = document.getElementById("msg");
      if (input.value) {
        ws.send(JSON.stringify({ type: "text", payload: input.value }));
        input.value = "";
      }
    }
  </script>
</body>
</html>`))
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// 5. Graceful shutdown handler
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down VerityStream server...")

		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()

		_ = srv.Shutdown(shutCtx)
		_ = hub.Shutdown(shutCtx)
		log.Println("Server stopped cleanly.")
	}()

	log.Println("VerityStream server running on http://localhost:8080")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
