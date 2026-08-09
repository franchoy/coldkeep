package container

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
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

func TestFileContainerReadAtFailsClosedWhenFileShrinksAfterOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shrinking.bin")
	createTestContainerFile(t, path, ContainerHdrLen+32)
	c, err := OpenWritableContainer(path, ContainerHdrLen+32)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Append([]byte("x")); err != nil {
		t.Fatalf("append payload: %v", err)
	}
	if err := c.Sync(); err != nil {
		t.Fatalf("sync payload: %v", err)
	}
	if err := os.Truncate(path, ContainerHdrLen); err != nil {
		t.Fatalf("truncate behind open container: %v", err)
	}

	// The open handle still records the pre-truncation logical size, so the
	// lower-level short-read check remains the fail-closed fallback.
	_, err = c.ReadAt(ContainerHdrLen, 1)
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
	if err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("expected no payload after truncate, got: %v", err)
	}
}

func TestValidateContainerRangeBoundaries(t *testing.T) {
	tests := []struct {
		name           string
		offset, length int64
		limit          int64
		wantErr        bool
	}{
		{name: "zero at start", offset: 0, length: 0, limit: 10},
		{name: "zero at end", offset: 10, length: 0, limit: 10},
		{name: "exact end", offset: 4, length: 6, limit: 10},
		{name: "negative offset", offset: -1, length: 1, limit: 10, wantErr: true},
		{name: "negative length", offset: 0, length: -1, limit: 10, wantErr: true},
		{name: "negative limit", offset: 0, length: 0, limit: -1, wantErr: true},
		{name: "offset past limit", offset: 11, length: 0, limit: 10, wantErr: true},
		{name: "length past limit", offset: 4, length: 7, limit: 10, wantErr: true},
		{name: "overflow shape", offset: math.MaxInt64 - 1, length: 2, limit: math.MaxInt64, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateContainerRange("test range", tc.offset, tc.length, tc.limit)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateContainerRange(%d, %d, %d) error=%v wantErr=%v", tc.offset, tc.length, tc.limit, err, tc.wantErr)
			}
		})
	}
}

func TestFileContainerReadAtRejectsInvalidRangeBeforeAllocation(t *testing.T) {
	c := openWritableTestContainer(t, ContainerHdrLen+32)
	defer func() { _ = c.Close() }()

	tests := []struct {
		name         string
		offset, size int64
	}{
		{name: "negative offset", offset: -1, size: 1},
		{name: "negative size", offset: ContainerHdrLen, size: -1},
		{name: "offset past eof", offset: ContainerHdrLen + 1, size: 0},
		{name: "range past eof", offset: ContainerHdrLen, size: 1},
		{name: "huge size", offset: ContainerHdrLen, size: math.MaxInt64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.ReadAt(tc.offset, tc.size); err == nil {
				t.Fatalf("expected invalid range offset=%d size=%d to fail", tc.offset, tc.size)
			}
		})
	}
}

func TestFileContainerReadAtAllowsZeroLengthAndExactEOF(t *testing.T) {
	c := openWritableTestContainer(t, ContainerHdrLen+32)
	defer func() { _ = c.Close() }()

	if _, err := c.Append([]byte("data")); err != nil {
		t.Fatalf("append payload: %v", err)
	}
	if got, err := c.ReadAt(ContainerHdrLen+4, 0); err != nil || len(got) != 0 {
		t.Fatalf("zero-length EOF read got=%v err=%v", got, err)
	}
	got, err := c.ReadAt(ContainerHdrLen, 4)
	if err != nil || string(got) != "data" {
		t.Fatalf("exact-EOF read got=%q err=%v", got, err)
	}
}

func TestOpenExistingContainerRejectsHeaderCatalogMaxSizeMismatch(t *testing.T) {
	const headerMax = ContainerHdrLen + 128
	path := filepath.Join(t.TempDir(), "mismatch.bin")
	createTestContainerFile(t, path, headerMax)

	for _, readonly := range []bool{true, false} {
		_, err := openExistingContainer(readonly, path, headerMax+1, fsx.Default())
		if err == nil || !strings.Contains(err.Error(), "container max size mismatch") {
			t.Fatalf("readonly=%v expected max-size mismatch, got %v", readonly, err)
		}
	}
}

func TestOpenExistingContainerRejectsPhysicalSizeBeyondDeclaredMaximum(t *testing.T) {
	const maxSize = ContainerHdrLen + 1
	path := filepath.Join(t.TempDir(), "oversized.bin")
	createTestContainerFile(t, path, maxSize)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open container for append: %v", err)
	}
	if _, err := f.Write([]byte("xx")); err != nil {
		_ = f.Close()
		t.Fatalf("append oversized bytes: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close oversized container: %v", err)
	}

	_, err = OpenReadOnlyContainer(path, maxSize)
	if err == nil || !strings.Contains(err.Error(), "container size exceeds maximum") {
		t.Fatalf("expected oversized-container error, got %v", err)
	}
}

func TestOpenExistingContainerAcceptsSupportedMatchingHeaderMaxSize(t *testing.T) {
	const maxSize = ContainerHdrLen + 128
	for _, major := range []uint16{LegacyContainerFormatVersionMajor, ContainerFormatVersionMajor} {
		t.Run(fmt.Sprintf("major_%d", major), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "supported.bin")
			writeContainerHeaderFixture(t, path, major, maxSize)
			c, err := OpenReadOnlyContainer(path, maxSize)
			if err != nil {
				t.Fatalf("open supported header major=%d: %v", major, err)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("close supported container: %v", err)
			}
		})
	}
}

func TestFileContainerAppendRejectsOverflowAsContainerFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "append-overflow.bin")
	createTestContainerFile(t, path, math.MaxInt64)
	c, err := OpenWritableContainer(path, math.MaxInt64)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	defer func() { _ = c.Close() }()
	c.offset = math.MaxInt64 - 1

	if _, err := c.Append([]byte("xx")); !errors.Is(err, ErrContainerFull) {
		t.Fatalf("expected ErrContainerFull for overflowing append, got %v", err)
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

func TestSafeContainerPathRejectsUnsafeNamesAndAcceptsGeneratedName(t *testing.T) {
	root := t.TempDir()
	unsafeNames := []string{
		"../escape.bin",
		"/abs/container.bin",
		"C:\\container.bin",
		"//server/share/container.bin",
		"",
		"   ",
	}

	for _, name := range unsafeNames {
		if _, err := SafeContainerPath(root, name); err == nil {
			t.Fatalf("expected safe container path validation to reject %q", name)
		}
	}

	// Use a representative filename that matches the generated format:
	// container_<decimal-timestamp>_<decimal-random>.bin
	name := "container_1748296145123456789_4298172847123456.bin"
	path, err := SafeContainerPath(root, name)
	if err != nil {
		t.Fatalf("expected generated filename %q to validate, got: %v", name, err)
	}
	if !strings.HasPrefix(path, root+string(os.PathSeparator)) && path != filepath.Join(root, name) {
		t.Fatalf("unexpected safe path result: %s", path)
	}
}

func TestContainerOperationsRejectUnsafeFilenames(t *testing.T) {
	dbconn := setupContainerOpsTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	unsafeFilename := "../escape.bin"
	containerID := insertUnsafeContainerRow(t, dbconn, unsafeFilename)

	assertSealRejectsInvalidContainerFilename(t, dbconn, containerID, unsafeFilename, containersDir)
	assertInvalidContainerFilenameErr(t, QuarantineContainerInDir(dbconn, containerID, containersDir), "expected quarantine to reject unsafe filename")
	assertInvalidContainerFilenameErr(t, CheckContainerHashFileInDir(int(containerID), unsafeFilename, "deadbeef", containersDir), "expected hash check to reject unsafe filename")
}

func setupContainerOpsTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return dbconn
}

func insertUnsafeContainerRow(t *testing.T, dbconn *sql.DB, unsafeFilename string) int64 {
	t.Helper()

	res, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		 VALUES (?, ?, ?, FALSE, FALSE, FALSE)`,
		unsafeFilename,
		int64(ContainerHdrLen),
		int64(ContainerHdrLen+128),
	)
	if err != nil {
		t.Fatalf("insert unsafe container row: %v", err)
	}
	containerID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return containerID
}

func assertSealRejectsInvalidContainerFilename(t *testing.T, dbconn *sql.DB, containerID int64, unsafeFilename string, containersDir string) {
	t.Helper()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	err = SealContainerInDir(tx, containerID, unsafeFilename, containersDir)
	if rbErr := tx.Rollback(); rbErr != nil {
		t.Fatalf("rollback tx: %v", rbErr)
	}
	assertInvalidContainerFilenameErr(t, err, "expected seal to reject unsafe filename")
}

func assertInvalidContainerFilenameErr(t *testing.T, err error, message string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "invalid container filename") {
		t.Fatalf("%s, got: %v", message, err)
	}
}
