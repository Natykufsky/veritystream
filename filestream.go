package veritystream

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// FileServer coordinates chunk uploads, single-pass uploads, downloads, and room notifications.
type FileServer struct {
	hub     *Hub
	storage StorageBackend
}

// NewFileServer returns a FileServer instance.
func NewFileServer(hub *Hub) *FileServer {
	return &FileServer{
		hub:     hub,
		storage: hub.Storage(),
	}
}

// sanitizeHeaderString cleans string to avoid HTTP response splitting or header injection.
func sanitizeHeaderString(val string) string {
	val = filepath.Base(val)
	val = strings.ReplaceAll(val, "\r", "")
	val = strings.ReplaceAll(val, "\n", "")
	val = strings.ReplaceAll(val, `"`, "")
	if val == "" || val == "." {
		return "file"
	}
	return val
}

// writeJSONError responds with a uniform JSON error payload.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
	})
}

// HandleUploadChunk receives a single chunk of a multi-part file upload.
// Query or header params: upload_id, chunk_index.
func (fs *FileServer) HandleUploadChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	uploadID := r.URL.Query().Get("upload_id")
	indexStr := r.URL.Query().Get("chunk_index")
	if uploadID == "" || indexStr == "" {
		writeJSONError(w, http.StatusBadRequest, "upload_id and chunk_index required")
		return
	}

	index, err := strconv.Atoi(indexStr)
	if err != nil || index < 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid chunk_index")
		return
	}

	maxChunkSize := fs.hub.Config().MaxChunkSize
	if maxChunkSize <= 0 {
		maxChunkSize = 32 * 1024 * 1024
	}

	lr := io.LimitReader(r.Body, maxChunkSize+1)
	buf := new(strings.Builder)
	_ = buf

	// Stream chunk directly to storage backend
	if err := fs.storage.SaveChunk(r.Context(), uploadID, index, lr); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to save chunk: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"chunk_received"}`))
}

// CompleteUploadRequest contains metadata required to assemble chunks into final file.
type CompleteUploadRequest struct {
	UploadID    string `json:"upload_id"`
	RoomID      string `json:"room_id"`
	UploaderID  string `json:"uploader_id"`
	FileName    string `json:"file_name"`
	ContentType string `json:"content_type"`
	TotalChunks int    `json:"total_chunks"`
	ExpectedSHA string `json:"expected_sha256,omitempty"`
}

// HandleCompleteChunkUpload finalizes the chunk assembly and broadcasts a MsgFileShare event.
func (fs *FileServer) HandleCompleteChunkUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req CompleteUploadRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1024*64)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if req.UploadID == "" || req.TotalChunks <= 0 {
		writeJSONError(w, http.StatusBadRequest, "upload_id and total_chunks > 0 required")
		return
	}

	meta := FileMetadata{
		ID:          uuid.NewString(),
		RoomID:      req.RoomID,
		UploaderID:  req.UploaderID,
		FileName:    sanitizeHeaderString(req.FileName),
		ContentType: req.ContentType,
		SHA256:      req.ExpectedSHA,
		CreatedAt:   time.Now().UTC(),
	}

	savedMeta, _, err := fs.storage.Assemble(r.Context(), req.UploadID, req.TotalChunks, meta)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to assemble chunks: %v", err))
		return
	}

	// Broadcast file shared event to room
	fs.broadcastFileEvent(savedMeta)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(savedMeta)
}

// HandleDirectUpload handles single-pass multipart form file uploads.
func (fs *FileServer) HandleDirectUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// 64MB memory limit for multipart parse
	if err := r.ParseMultipartForm(64 * 1024 * 1024); err != nil {
		writeJSONError(w, http.StatusBadRequest, "failed to parse multipart form")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "missing 'file' field")
		return
	}
	defer file.Close()

	roomID := r.FormValue("room_id")
	uploaderID := r.FormValue("uploader_id")

	meta := FileMetadata{
		ID:          uuid.NewString(),
		RoomID:      roomID,
		UploaderID:  uploaderID,
		FileName:    sanitizeHeaderString(header.Filename),
		ContentType: header.Header.Get("Content-Type"),
		Size:        header.Size,
		CreatedAt:   time.Now().UTC(),
	}

	hasher := sha256.New()
	tr := io.TeeReader(file, hasher)

	savedMeta, _, err := fs.storage.SaveDirect(r.Context(), meta, tr)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("save failed: %v", err))
		return
	}

	fs.broadcastFileEvent(savedMeta)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(savedMeta)
}

// HandleDownload streams the file back with HTTP Range support.
func (fs *FileServer) HandleDownload(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("file_id")
	if fileID == "" {
		writeJSONError(w, http.StatusBadRequest, "file_id required")
		return
	}

	if ls, ok := fs.storage.(*LocalStorage); ok {
		ls.ServeHTTPStream(w, r, fileID)
		return
	}

	rc, meta, err := fs.storage.OpenStream(r.Context(), fileID)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "file not found")
		return
	}
	defer rc.Close()

	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
	}
	safeFileName := sanitizeHeaderString(meta.FileName)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, safeFileName))
	_, _ = io.Copy(w, rc)
}

func (fs *FileServer) broadcastFileEvent(meta FileMetadata) {
	if meta.RoomID == "" {
		return
	}
	payload, _ := json.Marshal(meta)
	fs.hub.Publish(Message{
		RoomID:    meta.RoomID,
		SenderID:  meta.UploaderID,
		Type:      MsgFileShare,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	})
}
