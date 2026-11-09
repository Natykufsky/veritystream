package veritystream_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Natykufsky/veritystream"
	"github.com/gorilla/websocket"
)

func TestHub_MessageBroadcast(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "veritystream_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := veritystream.NewLocalStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	hub := veritystream.NewHub(storage)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		user := r.URL.Query().Get("user")
		_ = hub.HandleWebSocket(w, r, room, user, user)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?room=dev-chat&user=alice"
	wsAlice, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Alice dial failed: %v", err)
	}
	defer wsAlice.Close()

	wsURLBob := "ws" + strings.TrimPrefix(server.URL, "http") + "?room=dev-chat&user=bob"
	wsBob, _, err := websocket.DefaultDialer.Dial(wsURLBob, nil)
	if err != nil {
		t.Fatalf("Bob dial failed: %v", err)
	}
	defer wsBob.Close()

	// Wait briefly for join events
	time.Sleep(50 * time.Millisecond)

	// Alice sends a message
	testMsg := veritystream.Message{
		Type:    veritystream.MsgText,
		Payload: json.RawMessage(`"Hello from deployer dev room!"`),
	}
	if err := wsAlice.WriteJSON(testMsg); err != nil {
		t.Fatalf("Alice write failed: %v", err)
	}

	// Bob should receive Alice's message
	_ = wsBob.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		var received veritystream.Message
		if err := wsBob.ReadJSON(&received); err != nil {
			t.Fatalf("Bob read failed: %v", err)
		}
		if received.Type == veritystream.MsgText {
			if string(received.Payload) != `"Hello from deployer dev room!"` {
				t.Fatalf("Unexpected payload: %s", string(received.Payload))
			}
			break
		}
	}
}

func TestHub_TokenStreaming(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "veritystream_token_*")
	defer os.RemoveAll(tmpDir)

	storage, _ := veritystream.NewLocalStorage(tmpDir)
	hub := veritystream.NewHub(storage)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = hub.HandleWebSocket(w, r, "qa-room", "qa-bot", "QABot")
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	// Stream 3 incremental tokens
	hub.StreamToken("qa-room", "job-1", "runner", 0, "Building binary...", false)
	hub.StreamToken("qa-room", "job-1", "runner", 1, "Running unit tests...", false)
	hub.StreamToken("qa-room", "job-1", "runner", 2, "Done.", true)

	receivedCount := 0
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for receivedCount < 3 {
		var msg veritystream.Message
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatalf("Token read error: %v", err)
		}
		if msg.Type == veritystream.MsgStreamToken || msg.Type == veritystream.MsgStreamEnd {
			receivedCount++
		}
	}
}

func TestHub_GracefulShutdown(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "veritystream_shutdown_*")
	defer os.RemoveAll(tmpDir)

	storage, _ := veritystream.NewLocalStorage(tmpDir)
	hub := veritystream.NewHub(storage)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = hub.HandleWebSocket(w, r, "room-x", "user-1", "User One")
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Shutdown hub
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()

	if err := hub.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	// Connection should receive close error or message
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			// Expected normal close or EOF
			break
		}
	}
}

func TestStorage_PathTraversalProtection(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "veritystream_security_*")
	defer os.RemoveAll(tmpDir)

	storage, err := veritystream.NewLocalStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Malicious upload IDs
	maliciousIDs := []string{
		"../../etc/passwd",
		"../hack",
		"/root/owned",
		"foo/bar",
		"test;rm -rf",
		"id\x00null",
	}

	for _, malID := range maliciousIDs {
		err := storage.SaveChunk(ctx, malID, 0, strings.NewReader("bad data"))
		if err != veritystream.ErrInvalidIdentifier {
			t.Errorf("Expected ErrInvalidIdentifier for uploadID '%s', got: %v", malID, err)
		}

		_, _, err = storage.Assemble(ctx, malID, 1, veritystream.FileMetadata{})
		if err != veritystream.ErrInvalidIdentifier {
			t.Errorf("Expected ErrInvalidIdentifier for Assemble uploadID '%s', got: %v", malID, err)
		}

		_, _, err = storage.OpenStream(ctx, malID)
		if err != veritystream.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound for malicious fileID '%s', got: %v", malID, err)
		}
	}
}

func TestStorage_ConcurrentMetadataSave(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "veritystream_atomic_*")
	defer os.RemoveAll(tmpDir)

	storage, err := veritystream.NewLocalStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	workers := 10
	ctx := context.Background()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			meta := veritystream.FileMetadata{
				ID:       fmt.Sprintf("file-worker-%d", workerID),
				FileName: fmt.Sprintf("test-%d.txt", workerID),
			}
			_, _, saveErr := storage.SaveDirect(ctx, meta, strings.NewReader("worker payload data"))
			if saveErr != nil {
				t.Errorf("Worker %d failed: %v", workerID, saveErr)
			}
		}(i)
	}

	wg.Wait()

	// Verify all items are retrievable
	for i := 0; i < workers; i++ {
		rc, meta, err := storage.OpenStream(ctx, fmt.Sprintf("file-worker-%d", i))
		if err != nil {
			t.Fatalf("Failed to open file %d: %v", i, err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()
		if string(data) != "worker payload data" {
			t.Errorf("Corrupted data in worker %d", i)
		}
		if meta.FileName != fmt.Sprintf("test-%d.txt", i) {
			t.Errorf("Unexpected metadata for worker %d: %+v", i, meta)
		}
	}
}

func TestHub_BackpressureDropHook(t *testing.T) {
	var droppedCount atomic.Int32

	hub := veritystream.NewHub(nil,
		veritystream.WithClientSendBufferSize(2),
		veritystream.WithOnDrop(func(client *veritystream.Client, msg veritystream.Message) {
			droppedCount.Add(1)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = hub.HandleWebSocket(w, r, "overflow-room", "slow-consumer", "Slow")
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	// Publish more messages than the buffer size (2) without reading
	for i := 0; i < 20; i++ {
		hub.Publish(veritystream.Message{
			RoomID:  "overflow-room",
			Type:    veritystream.MsgText,
			Payload: json.RawMessage(fmt.Sprintf(`"message-%d"`, i)),
		})
	}

	time.Sleep(100 * time.Millisecond)

	if droppedCount.Load() == 0 {
		t.Fatalf("Expected dropped messages for saturated client buffer, got 0")
	}
}

func TestFileServer_ChunkedUploadAndAssemble(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "veritystream_chunk_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, _ := veritystream.NewLocalStorage(tmpDir)
	hub := veritystream.NewHub(storage)
	fs := veritystream.NewFileServer(hub)

	chunk1 := []byte("Hello, ")
	chunk2 := []byte("VerityStream ")
	chunk3 := []byte("world!")
	fullData := append(append(chunk1, chunk2...), chunk3...)

	hasher := sha256.New()
	hasher.Write(fullData)
	expectedSHA := hex.EncodeToString(hasher.Sum(nil))

	uploadID := "upload-test-123"

	// Upload Chunk 0
	req0 := httptest.NewRequest(http.MethodPost, "/api/files/chunk?upload_id="+uploadID+"&chunk_index=0", bytes.NewReader(chunk1))
	w0 := httptest.NewRecorder()
	fs.HandleUploadChunk(w0, req0)
	if w0.Code != http.StatusOK {
		t.Fatalf("Upload chunk 0 failed: %d: %s", w0.Code, w0.Body.String())
	}

	// Upload Chunk 1
	req1 := httptest.NewRequest(http.MethodPost, "/api/files/chunk?upload_id="+uploadID+"&chunk_index=1", bytes.NewReader(chunk2))
	w1 := httptest.NewRecorder()
	fs.HandleUploadChunk(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("Upload chunk 1 failed: %d", w1.Code)
	}

	// Upload Chunk 2
	req2 := httptest.NewRequest(http.MethodPost, "/api/files/chunk?upload_id="+uploadID+"&chunk_index=2", bytes.NewReader(chunk3))
	w2 := httptest.NewRecorder()
	fs.HandleUploadChunk(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("Upload chunk 2 failed: %d", w2.Code)
	}

	// Complete & Assemble
	completeReq := veritystream.CompleteUploadRequest{
		UploadID:    uploadID,
		RoomID:      "dev-room",
		UploaderID:  "agent-smith",
		FileName:    "greeting.txt",
		ContentType: "text/plain",
		TotalChunks: 3,
		ExpectedSHA: expectedSHA,
	}
	body, _ := json.Marshal(completeReq)
	reqComp := httptest.NewRequest(http.MethodPost, "/api/files/complete", bytes.NewReader(body))
	wComp := httptest.NewRecorder()
	fs.HandleCompleteChunkUpload(wComp, reqComp)

	if wComp.Code != http.StatusCreated {
		t.Fatalf("Complete upload failed: %d body: %s", wComp.Code, wComp.Body.String())
	}

	var meta veritystream.FileMetadata
	if err := json.Unmarshal(wComp.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}

	if meta.SHA256 != expectedSHA {
		t.Fatalf("Expected SHA %s, got %s", expectedSHA, meta.SHA256)
	}
	if meta.Size != int64(len(fullData)) {
		t.Fatalf("Expected size %d, got %d", len(fullData), meta.Size)
	}

	// Download & verify content
	reqDown := httptest.NewRequest(http.MethodGet, "/api/files/download?file_id="+meta.ID, nil)
	wDown := httptest.NewRecorder()
	fs.HandleDownload(wDown, reqDown)

	if wDown.Code != http.StatusOK {
		t.Fatalf("Download failed: %d", wDown.Code)
	}
	if !bytes.Equal(wDown.Body.Bytes(), fullData) {
		t.Fatalf("Downloaded content mismatch: got %s, expected %s", wDown.Body.String(), string(fullData))
	}
}

func TestFileServer_DirectUpload(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "veritystream_direct_*")
	defer os.RemoveAll(tmpDir)

	storage, _ := veritystream.NewLocalStorage(tmpDir)
	hub := veritystream.NewHub(storage)
	fs := veritystream.NewFileServer(hub)

	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	_ = w.WriteField("room_id", "direct-room")
	_ = w.WriteField("uploader_id", "direct-user")

	fw, err := w.CreateFormFile("file", "direct.txt\r\nInjected: True")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fw.Write([]byte("Direct upload test content"))
	w.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/files/upload", &b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()

	fs.HandleDirectUpload(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("Direct upload failed: %d body: %s", rec.Code, rec.Body.String())
	}

	var meta veritystream.FileMetadata
	_ = json.Unmarshal(rec.Body.Bytes(), &meta)

	// Ensure header injection was sanitized
	if strings.Contains(meta.FileName, "\r") || strings.Contains(meta.FileName, "\n") {
		t.Fatalf("Unsanitized filename: %s", meta.FileName)
	}
}
