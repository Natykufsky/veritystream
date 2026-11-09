package veritystream

import (
	"context"
	"io"
	"time"
)

// FileMetadata describes an uploaded or streaming file.
type FileMetadata struct {
	ID          string            `json:"id"`
	RoomID      string            `json:"room_id"`
	UploaderID  string            `json:"uploader_id"`
	FileName    string            `json:"file_name"`
	ContentType string            `json:"content_type"`
	Size        int64             `json:"size"`
	SHA256      string            `json:"sha256"`
	CreatedAt   time.Time         `json:"created_at"`
	CustomMeta  map[string]string `json:"custom_meta,omitempty"`
}

// StorageBackend defines the interface for persisting and reading files and chunks.
type StorageBackend interface {
	// SaveChunk writes a specific chunk slice for an active multi-part upload.
	SaveChunk(ctx context.Context, uploadID string, chunkIndex int, r io.Reader) error

	// Assemble stitches together received chunks into the final stored file, returning updated FileMetadata and final path.
	Assemble(ctx context.Context, uploadID string, totalChunks int, meta FileMetadata) (FileMetadata, string, error)

	// SaveDirect persists an entire file in a single pass, returning updated FileMetadata and storage path.
	SaveDirect(ctx context.Context, meta FileMetadata, r io.Reader) (FileMetadata, string, error)

	// OpenStream opens a reader to retrieve the file by ID.
	OpenStream(ctx context.Context, fileID string) (io.ReadSeekCloser, FileMetadata, error)

	// Delete removes the file by ID.
	Delete(ctx context.Context, fileID string) error
}
