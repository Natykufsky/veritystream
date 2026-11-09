package veritystream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrChunkOutOfBounds is returned when a chunk index is invalid.
	ErrChunkOutOfBounds = errors.New("chunk index out of bounds")
	// ErrChecksumMismatch is returned when the sha256 checksum does not match expectations.
	ErrChecksumMismatch = errors.New("calculated sha256 does not match provided checksum")
	// ErrFileNotFound is returned when the requested file does not exist.
	ErrFileNotFound = errors.New("file not found")
	// ErrInvalidIdentifier is returned when an upload_id or file_id contains invalid characters.
	ErrInvalidIdentifier = errors.New("invalid identifier: path traversal detected or invalid characters")
)

// validIDRegex ensures safe alphanumeric + hyphen + underscore identifiers (uuid, nanoid, hash).
var validIDRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)

// validateID validates that an identifier contains no path traversal sequences or illegal characters.
func validateID(id string) error {
	if len(id) == 0 || len(id) > 128 || !validIDRegex.MatchString(id) {
		return ErrInvalidIdentifier
	}
	return nil
}

// LocalStorage implements StorageBackend storing files on the local filesystem with atomic metadata persistence.
type LocalStorage struct {
	baseDir  string
	metaMu   sync.RWMutex
	metaFile string
	metadata map[string]FileMetadata
}

// NewLocalStorage creates a new LocalStorage backend under the specified directory.
func NewLocalStorage(baseDir string) (*LocalStorage, error) {
	cleanDir := filepath.Clean(baseDir)
	if err := os.MkdirAll(filepath.Join(cleanDir, "chunks"), 0o750); err != nil {
		return nil, fmt.Errorf("failed to create chunks dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cleanDir, "files"), 0o750); err != nil {
		return nil, fmt.Errorf("failed to create files dir: %w", err)
	}

	ls := &LocalStorage{
		baseDir:  cleanDir,
		metaFile: filepath.Join(cleanDir, "metadata.json"),
		metadata: make(map[string]FileMetadata),
	}

	_ = ls.loadMetadata()
	return ls, nil
}

func (ls *LocalStorage) loadMetadata() error {
	data, err := os.ReadFile(ls.metaFile)
	if err != nil {
		return err
	}
	ls.metaMu.Lock()
	defer ls.metaMu.Unlock()
	return json.Unmarshal(data, &ls.metadata)
}

// saveMetadata atomically writes the metadata map to avoid corruption on crashes.
func (ls *LocalStorage) saveMetadata() error {
	data, err := json.MarshalIndent(ls.metadata, "", "  ")
	if err != nil {
		return err
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d", ls.metaFile, time.Now().UnixNano())
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile)
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile)
		return err
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpFile)
		return err
	}

	return os.Rename(tmpFile, ls.metaFile)
}

func (ls *LocalStorage) SaveChunk(ctx context.Context, uploadID string, chunkIndex int, r io.Reader) error {
	if err := validateID(uploadID); err != nil {
		return err
	}
	if chunkIndex < 0 || chunkIndex > 100000 {
		return ErrChunkOutOfBounds
	}

	chunkDir := filepath.Join(ls.baseDir, "chunks", uploadID)
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		return err
	}

	chunkPath := filepath.Join(chunkDir, fmt.Sprintf("%08d.part", chunkIndex))
	tmpChunkPath := fmt.Sprintf("%s.tmp.%d", chunkPath, time.Now().UnixNano())

	f, err := os.OpenFile(tmpChunkPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	_, copyErr := io.Copy(f, r)
	_ = f.Sync()
	_ = f.Close()

	if copyErr != nil {
		_ = os.Remove(tmpChunkPath)
		return copyErr
	}

	return os.Rename(tmpChunkPath, chunkPath)
}

func (ls *LocalStorage) Assemble(ctx context.Context, uploadID string, totalChunks int, meta FileMetadata) (FileMetadata, string, error) {
	if err := validateID(uploadID); err != nil {
		return meta, "", err
	}
	if totalChunks <= 0 || totalChunks > 100000 {
		return meta, "", ErrChunkOutOfBounds
	}

	if meta.ID == "" {
		meta.ID = uuid.NewString()
	}
	if err := validateID(meta.ID); err != nil {
		return meta, "", err
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}

	chunkDir := filepath.Join(ls.baseDir, "chunks", uploadID)
	destPath := filepath.Join(ls.baseDir, "files", meta.ID)
	tmpDestPath := fmt.Sprintf("%s.tmp.%d", destPath, time.Now().UnixNano())

	out, err := os.OpenFile(tmpDestPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return meta, "", err
	}

	hasher := sha256.New()
	multiWriter := io.MultiWriter(out, hasher)

	for i := 0; i < totalChunks; i++ {
		chunkPath := filepath.Join(chunkDir, fmt.Sprintf("%08d.part", i))
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmpDestPath)
			return meta, "", fmt.Errorf("missing chunk %d: %w", i, err)
		}

		_, err = io.Copy(multiWriter, chunkFile)
		_ = chunkFile.Close()
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmpDestPath)
			return meta, "", err
		}
	}

	_ = out.Sync()
	_ = out.Close()

	calculatedHash := hex.EncodeToString(hasher.Sum(nil))
	if meta.SHA256 != "" && meta.SHA256 != calculatedHash {
		_ = os.Remove(tmpDestPath)
		return meta, "", ErrChecksumMismatch
	}

	if err := os.Rename(tmpDestPath, destPath); err != nil {
		_ = os.Remove(tmpDestPath)
		return meta, "", err
	}

	meta.SHA256 = calculatedHash
	if fi, err := os.Stat(destPath); err == nil {
		meta.Size = fi.Size()
	}

	// Clean up chunk temporary directory
	_ = os.RemoveAll(chunkDir)

	ls.metaMu.Lock()
	ls.metadata[meta.ID] = meta
	_ = ls.saveMetadata()
	ls.metaMu.Unlock()

	return meta, destPath, nil
}

func (ls *LocalStorage) SaveDirect(ctx context.Context, meta FileMetadata, r io.Reader) (FileMetadata, string, error) {
	if meta.ID == "" {
		meta.ID = uuid.NewString()
	}
	if err := validateID(meta.ID); err != nil {
		return meta, "", err
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}

	destPath := filepath.Join(ls.baseDir, "files", meta.ID)
	tmpDestPath := fmt.Sprintf("%s.tmp.%d", destPath, time.Now().UnixNano())

	out, err := os.OpenFile(tmpDestPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return meta, "", err
	}

	hasher := sha256.New()
	multiWriter := io.MultiWriter(out, hasher)

	written, err := io.Copy(multiWriter, r)
	_ = out.Sync()
	_ = out.Close()

	if err != nil {
		_ = os.Remove(tmpDestPath)
		return meta, "", err
	}

	if err := os.Rename(tmpDestPath, destPath); err != nil {
		_ = os.Remove(tmpDestPath)
		return meta, "", err
	}

	meta.Size = written
	meta.SHA256 = hex.EncodeToString(hasher.Sum(nil))

	ls.metaMu.Lock()
	ls.metadata[meta.ID] = meta
	_ = ls.saveMetadata()
	ls.metaMu.Unlock()

	return meta, destPath, nil
}

func (ls *LocalStorage) OpenStream(ctx context.Context, fileID string) (io.ReadSeekCloser, FileMetadata, error) {
	if err := validateID(fileID); err != nil {
		return nil, FileMetadata{}, ErrFileNotFound
	}

	ls.metaMu.RLock()
	meta, exists := ls.metadata[fileID]
	ls.metaMu.RUnlock()

	if !exists {
		return nil, FileMetadata{}, ErrFileNotFound
	}

	filePath := filepath.Join(ls.baseDir, "files", fileID)
	f, err := os.Open(filePath)
	if err != nil {
		return nil, FileMetadata{}, ErrFileNotFound
	}

	return f, meta, nil
}

func (ls *LocalStorage) Delete(ctx context.Context, fileID string) error {
	if err := validateID(fileID); err != nil {
		return ErrFileNotFound
	}

	ls.metaMu.Lock()
	delete(ls.metadata, fileID)
	_ = ls.saveMetadata()
	ls.metaMu.Unlock()

	filePath := filepath.Join(ls.baseDir, "files", fileID)
	return os.Remove(filePath)
}

// ServeHTTPStream handles HTTP Range requests for video/audio previews, downloads, and logs.
func (ls *LocalStorage) ServeHTTPStream(w http.ResponseWriter, r *http.Request, fileID string) {
	f, meta, err := ls.OpenStream(r.Context(), fileID)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
	}
	safeFileName := filepath.Base(meta.FileName)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, safeFileName))
	http.ServeContent(w, r, safeFileName, meta.CreatedAt, f)
}
