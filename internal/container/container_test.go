package container

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func createTestContainerFile(t *testing.T, path string, maxSize int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create container file: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := writeNewContainerHeader(f, maxSize); err != nil {
		t.Fatalf("write container header: %v", err)
	}
}

func openWritableTestContainer(t *testing.T, maxSize int64) *FileContainer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "container.bin")
	createTestContainerFile(t, path, maxSize)
	c, err := OpenWritableContainer(path, maxSize)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	return c
}

func TestFileContainerMethodsFailWhenClosed(t *testing.T) {
	c := openWritableTestContainer(t, ContainerHdrLen+32)
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := c.Append([]byte("x")); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected append closed-container contract, got: %v", err)
	}
	if _, err := c.ReadAt(ContainerHdrLen, 1); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected read closed-container contract, got: %v", err)
	}
	if err := c.Truncate(ContainerHdrLen); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected truncate closed-container contract, got: %v", err)
	}
	if err := c.Sync(); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected sync closed-container contract, got: %v", err)
	}
}

func TestFileContainerAppendFailsWhenFull(t *testing.T) {
	// Start at ContainerHdrLen and allow only one extra byte.
	c := openWritableTestContainer(t, ContainerHdrLen+1)
	defer func() { _ = c.Close() }()

	_, err := c.Append([]byte("xx"))
	if !errors.Is(err, ErrContainerFull) {
		t.Fatalf("expected ErrContainerFull, got: %v", err)
	}
}

func TestFileContainerReadAtFailsOnShortRead(t *testing.T) {
	c := openWritableTestContainer(t, ContainerHdrLen+32)
	defer func() { _ = c.Close() }()

	// No payload has been written, so reads from the payload region are short.
	_, err := c.ReadAt(ContainerHdrLen, 1)
	if err == nil || !strings.Contains(err.Error(), "short read") {
		t.Fatalf("expected short-read error contract, got: %v", err)
	}
}

func TestFileContainerBuffersSmallWritesUntilSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffered.bin")
	createTestContainerFile(t, path, ContainerHdrLen+128)

	c, err := OpenWritableContainer(path, ContainerHdrLen+128)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Append([]byte("hello")); err != nil {
		t.Fatalf("append payload: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat container file: %v", err)
	}
	if info.Size() != ContainerHdrLen {
		t.Fatalf("expected physical size to remain at header length before sync, got %d", info.Size())
	}

	if err := c.Sync(); err != nil {
		t.Fatalf("sync payload: %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat container file after sync: %v", err)
	}
	if info.Size() != ContainerHdrLen+5 {
		t.Fatalf("expected physical size %d after sync, got %d", ContainerHdrLen+5, info.Size())
	}
	if got := c.Size(); got != ContainerHdrLen+5 {
		t.Fatalf("expected logical size %d, got %d", ContainerHdrLen+5, got)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read container file: %v", err)
	}
	if string(payload[ContainerHdrLen:]) != "hello" {
		t.Fatalf("unexpected payload bytes: %q", payload[ContainerHdrLen:])
	}
}

func TestFileContainerReadAtFlushesPendingWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read-flush.bin")
	createTestContainerFile(t, path, ContainerHdrLen+128)

	c, err := OpenWritableContainer(path, ContainerHdrLen+128)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Append([]byte("abc")); err != nil {
		t.Fatalf("append payload: %v", err)
	}

	got, err := c.ReadAt(ContainerHdrLen, 3)
	if err != nil {
		t.Fatalf("read payload after buffered append: %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("payload mismatch: got %q want %q", got, "abc")
	}
}

func TestFileContainerTruncateDiscardsPendingWritesWithoutSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncate-buffer.bin")
	createTestContainerFile(t, path, ContainerHdrLen+128)

	c, err := OpenWritableContainer(path, ContainerHdrLen+128)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Append([]byte("pending")); err != nil {
		t.Fatalf("append payload: %v", err)
	}
	if err := c.Truncate(ContainerHdrLen); err != nil {
		t.Fatalf("truncate pending append: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat container file: %v", err)
	}
	if info.Size() != ContainerHdrLen {
		t.Fatalf("expected physical size %d after truncate, got %d", ContainerHdrLen, info.Size())
	}
	if got := c.Size(); got != ContainerHdrLen {
		t.Fatalf("expected logical size reset to %d, got %d", ContainerHdrLen, got)
	}
	_, err = c.ReadAt(ContainerHdrLen, 1)
	if err == nil || !strings.Contains(err.Error(), "short read") {
		t.Fatalf("expected no payload after truncate, got: %v", err)
	}
}

func TestFileContainerSyncSkipsRedundantFsyncWithoutNewWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "redundant-sync.bin")
	createTestContainerFile(t, path, ContainerHdrLen+128)

	c, err := OpenWritableContainer(path, ContainerHdrLen+128)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Append([]byte("abc")); err != nil {
		t.Fatalf("append payload: %v", err)
	}
	if c.pending.Len() != 3 {
		t.Fatalf("expected small append to stay buffered before sync, got pending=%d", c.pending.Len())
	}
	if err := c.Sync(); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if c.dirty {
		t.Fatalf("expected first sync to clear dirty state")
	}
	if c.pending.Len() != 0 {
		t.Fatalf("expected first sync to flush pending bytes, got pending=%d", c.pending.Len())
	}
	if err := c.Sync(); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if c.dirty {
		t.Fatalf("expected second sync without writes to remain a no-op")
	}

	if err := c.Truncate(ContainerHdrLen); err != nil {
		t.Fatalf("truncate payload: %v", err)
	}
	if !c.dirty {
		t.Fatalf("expected truncate to mark container dirty")
	}
	if err := c.Sync(); err != nil {
		t.Fatalf("sync after truncate: %v", err)
	}
	if c.dirty {
		t.Fatalf("expected sync after truncate to clear dirty state")
	}
}

func TestOpenWritableContainerFailsOnInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-header.bin")
	if err := os.WriteFile(path, []byte("not-a-valid-container-header"), 0o644); err != nil {
		t.Fatalf("write invalid container file: %v", err)
	}

	_, err := OpenWritableContainer(path, ContainerHdrLen+32)
	if err == nil || !strings.Contains(err.Error(), "validate container header") {
		t.Fatalf("expected wrapped header-validation contract, got: %v", err)
	}
}

func TestOpenReadOnlyContainerFailsOnInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-header-readonly.bin")
	if err := os.WriteFile(path, []byte("still-not-a-valid-container-header"), 0o644); err != nil {
		t.Fatalf("write invalid container file: %v", err)
	}

	_, err := OpenReadOnlyContainer(path, ContainerHdrLen+32)
	if err == nil || !strings.Contains(err.Error(), "validate container header") {
		t.Fatalf("expected wrapped readonly header-validation contract, got: %v", err)
	}
}

func TestBrokenOpenContainerErrorFormatsAndUnwraps(t *testing.T) {
	inner := errors.New("open failed")
	err := &BrokenOpenContainerError{ContainerID: 77, Err: inner}

	if got := err.Error(); !strings.Contains(got, "open container 77") || !strings.Contains(got, "open failed") {
		t.Fatalf("unexpected formatted error: %q", got)
	}
	if !errors.Is(err, inner) {
		t.Fatalf("expected errors.Is to match wrapped inner error")
	}
	if err.Unwrap() != inner {
		t.Fatalf("expected Unwrap to return inner error")
	}
}

func TestBrokenOpenContainerErrorNilReceiverBehavior(t *testing.T) {
	var err *BrokenOpenContainerError

	if got := err.Error(); got != "broken open container" {
		t.Fatalf("unexpected nil-receiver error string: %q", got)
	}
	if err.Unwrap() != nil {
		t.Fatalf("expected nil-receiver Unwrap to return nil")
	}
}

func TestQuarantineContainerInDirUpdatesSizesToPhysicalFile(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	filename := "quarantine-size-sync.bin"
	path := filepath.Join(containersDir, filename)
	createTestContainerFile(t, path, ContainerHdrLen+128)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		t.Fatalf("open container file for append: %v", err)
	}
	if _, err := f.Write([]byte("payload-expands-physical-size")); err != nil {
		_ = f.Close()
		t.Fatalf("append payload: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close container file: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat container file: %v", err)
	}

	res, err := dbconn.Exec(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES (?, ?, ?, FALSE, TRUE, FALSE)
	`, filename, ContainerHdrLen, ContainerHdrLen+128)
	if err != nil {
		t.Fatalf("insert container row: %v", err)
	}
	containerID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	if err := QuarantineContainerInDir(dbconn, containerID, containersDir); err != nil {
		t.Fatalf("quarantine container in dir: %v", err)
	}

	var quarantine int
	var sealing int
	var currentSize int64
	var maxSize int64
	if err := dbconn.QueryRow(`SELECT quarantine, sealing, current_size, max_size FROM container WHERE id = ?`, containerID).Scan(&quarantine, &sealing, &currentSize, &maxSize); err != nil {
		t.Fatalf("query quarantined container row: %v", err)
	}
	if quarantine != 1 {
		t.Fatalf("expected container to be quarantined, got %d", quarantine)
	}
	if sealing != 0 {
		t.Fatalf("expected quarantine to clear sealing flag, got %d", sealing)
	}
	if currentSize != info.Size() {
		t.Fatalf("expected current_size=%d, got %d", info.Size(), currentSize)
	}
	if maxSize != info.Size() {
		t.Fatalf("expected max_size=%d, got %d", info.Size(), maxSize)
	}
}
