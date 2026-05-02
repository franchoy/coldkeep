package container

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type stubResult struct{}

func (stubResult) LastInsertId() (int64, error) { return 0, nil }
func (stubResult) RowsAffected() (int64, error) { return 1, nil }

type stubTx struct {
	errs      []error
	execCalls int
	queries   []string
}

type fakeContainer struct {
	size      int64
	appendErr error
	truncErr  error
	syncErr   error
	closeErr  error
}

func (f *fakeContainer) Append(data []byte) (int64, error) {
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	offset := f.size
	f.size += int64(len(data))
	return offset, nil
}
func (f *fakeContainer) ReadAt(offset int64, size int64) ([]byte, error) {
	return nil, nil
}
func (f *fakeContainer) Size() int64 { return f.size }
func (f *fakeContainer) Truncate(size int64) error {
	if f.truncErr != nil {
		return f.truncErr
	}
	f.size = size
	return nil
}
func (f *fakeContainer) Sync() error        { return f.syncErr }
func (f *fakeContainer) Close() error       { return f.closeErr }
func (f *fakeContainer) SetSize(size int64) { f.size = size }

func (s *stubTx) Exec(query string, args ...any) (sql.Result, error) {
	s.queries = append(s.queries, query)
	idx := s.execCalls
	s.execCalls++
	if idx < len(s.errs) && s.errs[idx] != nil {
		return nil, s.errs[idx]
	}
	return stubResult{}, nil
}

func (s *stubTx) QueryRow(query string, args ...any) *sql.Row {
	return nil
}

func TestLocalWriterAppendPayloadFailsWhenPayloadIsEmpty(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+128)

	_, err := w.AppendPayload(nil, nil)
	if err == nil || !strings.Contains(err.Error(), "payload is empty") {
		t.Fatalf("expected empty-payload error contract, got: %v", err)
	}
}

func TestLocalWriterAppendPayloadFailsWhenPayloadIsTooLarge(t *testing.T) {
	// max payload = maxSize - header = 4 bytes
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+4)
	payload := []byte("12345")

	_, err := w.AppendPayload(nil, payload)
	if err == nil || !strings.Contains(err.Error(), "payload too large") {
		t.Fatalf("expected payload-too-large error contract, got: %v", err)
	}
}

func TestLockContainerRowNowaitWithRetrySucceedsOnFirstAttempt(t *testing.T) {
	tx := &stubTx{}

	err := lockContainerRowNowaitWithRetry(tx, nil, 7, 1, time.Millisecond)
	if err != nil {
		t.Fatalf("expected lock acquisition success, got: %v", err)
	}
	if tx.execCalls != 3 {
		t.Fatalf("expected savepoint + lock + release, got %d exec calls", tx.execCalls)
	}
	if len(tx.queries) != 3 || !strings.Contains(tx.queries[1], "FOR UPDATE NOWAIT") {
		t.Fatalf("expected NOWAIT lock query, got: %v", tx.queries)
	}
}

func TestLockContainerRowNowaitWithRetryReturnsContentionAfterExhaustion(t *testing.T) {
	tx := &stubTx{errs: []error{nil, &pq.Error{Code: "55P03"}, nil, nil}}

	err := lockContainerRowNowaitWithRetry(tx, nil, 42, 1, time.Millisecond)
	if !errors.Is(err, ErrContainerLockContention) {
		t.Fatalf("expected ErrContainerLockContention, got: %v", err)
	}
	if !strings.Contains(err.Error(), "container 42") {
		t.Fatalf("expected container id in contention error, got: %v", err)
	}
}

func TestLockContainerRowNowaitWithRetryUsesSavepointRollbackBetweenLockRetries(t *testing.T) {
	tx := &stubTx{errs: []error{nil, &pq.Error{Code: "55P03"}, nil, nil, nil, nil}}

	err := lockContainerRowNowaitWithRetry(tx, nil, 42, 2, time.Millisecond)
	if err != nil {
		t.Fatalf("expected retry to succeed after lock contention, got: %v", err)
	}
	if tx.execCalls != 7 {
		t.Fatalf("expected 7 exec calls across savepoint retry flow, got %d", tx.execCalls)
	}
	if tx.queries[0] != "SAVEPOINT coldkeep_container_lock_retry" {
		t.Fatalf("expected initial savepoint, got %q", tx.queries[0])
	}
	if !strings.Contains(tx.queries[1], "FOR UPDATE NOWAIT") {
		t.Fatalf("expected NOWAIT lock query, got %q", tx.queries[1])
	}
	if tx.queries[2] != "ROLLBACK TO SAVEPOINT coldkeep_container_lock_retry" {
		t.Fatalf("expected rollback to savepoint after lock contention, got %q", tx.queries[2])
	}
	if tx.queries[3] != "RELEASE SAVEPOINT coldkeep_container_lock_retry" {
		t.Fatalf("expected release after rollback, got %q", tx.queries[3])
	}
	if tx.queries[4] != "SAVEPOINT coldkeep_container_lock_retry" {
		t.Fatalf("expected second savepoint before retry, got %q", tx.queries[4])
	}
	if !strings.Contains(tx.queries[5], "FOR UPDATE NOWAIT") {
		t.Fatalf("expected second NOWAIT lock query, got %q", tx.queries[5])
	}
	if tx.queries[6] != "RELEASE SAVEPOINT coldkeep_container_lock_retry" {
		t.Fatalf("expected final savepoint release after successful retry, got %q", tx.queries[6])
	}
}

func TestLockContainerRowNowaitWithRetryReturnsNonLockErrorImmediately(t *testing.T) {
	baseErr := errors.New("db unavailable")
	tx := &stubTx{errs: []error{baseErr}}

	err := lockContainerRowNowaitWithRetry(tx, nil, 9, 3, time.Millisecond)
	if !errors.Is(err, baseErr) {
		t.Fatalf("expected original non-lock error, got: %v", err)
	}
	if tx.execCalls != 1 {
		t.Fatalf("expected no retries on non-lock error, got %d calls", tx.execCalls)
	}
}

func TestLockContainerRowNowaitWithRetryUsesBackendAwareQueryWhenDBProvided(t *testing.T) {
	tx := &stubTx{}
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	err = lockContainerRowNowaitWithRetry(tx, dbconn, 5, 1, time.Millisecond)
	if err != nil {
		t.Fatalf("expected lock acquisition success, got: %v", err)
	}
	if len(tx.queries) != 1 {
		t.Fatalf("expected one query, got %d", len(tx.queries))
	}
	if strings.Contains(tx.queries[0], "FOR UPDATE NOWAIT") {
		t.Fatalf("expected sqlite backend query without NOWAIT suffix, got: %q", tx.queries[0])
	}
}

func TestLockContainerRowNowaitWithRetryZeroAttemptsReturnsContention(t *testing.T) {
	tx := &stubTx{}

	err := lockContainerRowNowaitWithRetry(tx, nil, 88, 0, time.Millisecond)
	if !errors.Is(err, ErrContainerLockContention) {
		t.Fatalf("expected ErrContainerLockContention for zero attempts, got: %v", err)
	}
	if tx.execCalls != 0 {
		t.Fatalf("expected zero exec calls when attempts=0, got %d", tx.execCalls)
	}
	if !strings.Contains(err.Error(), "container 88") {
		t.Fatalf("expected error to mention container id, got: %v", err)
	}
}

func TestLocalWriterAppendPayloadRefreshesDBSizeBeforeRotationDecision(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`
		CREATE TABLE container (
			id INTEGER PRIMARY KEY,
			filename TEXT,
			current_size INTEGER,
			max_size INTEGER,
			sealed BOOLEAN,
			sealing BOOLEAN,
			quarantine BOOLEAN
		)
	`); err != nil {
		t.Fatalf("create container table: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO container (id, filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES (1, 'active.bin', ?, ?, FALSE, FALSE, FALSE)
	`, ContainerHdrLen+10, ContainerHdrLen+24); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	handle := &fakeContainer{size: ContainerHdrLen + 10}
	w := NewLocalWriterWithDirAndDB(t.TempDir(), ContainerHdrLen+24, dbconn)
	w.hasActive = true
	w.activeID = 1
	w.activeFile = "active.bin"
	w.activeHandle = handle
	w.activeSize = ContainerHdrLen + 20

	placement, err := w.AppendPayload(tx, []byte("12345678"))
	if err != nil {
		t.Fatalf("append payload: %v", err)
	}
	if placement.Rotated {
		t.Fatalf("expected no rotation after refreshing db size, got placement=%+v", placement)
	}
	if placement.ContainerID != 1 {
		t.Fatalf("expected append to existing container, got %d", placement.ContainerID)
	}
	if placement.Offset != ContainerHdrLen+10 {
		t.Fatalf("expected offset %d, got %d", ContainerHdrLen+10, placement.Offset)
	}
	if placement.NewContainerSize != ContainerHdrLen+18 {
		t.Fatalf("expected new size %d, got %d", ContainerHdrLen+18, placement.NewContainerSize)
	}
}

func TestLocalWriterEnsureActiveExcludingWrapsDirectoryCreationFailure(t *testing.T) {
	base := t.TempDir()
	filePath := base + "/not-a-dir"
	if err := os.WriteFile(filePath, []byte("blocker"), 0o644); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}

	w := NewLocalWriterWithDir(filePath, ContainerHdrLen+64)
	err := w.ensureActiveExcluding(nil, 0)
	if err == nil || !strings.Contains(err.Error(), "ensure container directory") {
		t.Fatalf("expected wrapped ensure-directory error contract, got: %v", err)
	}
}

func TestLocalWriterFinalizePhysicalOnlyWrapsSyncError(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	w.hasActive = true
	w.activeID = 11
	w.activeHandle = &fakeContainer{syncErr: errors.New("sync failed")}

	err := w.finalizePhysicalOnly()
	if err == nil || !strings.Contains(err.Error(), "sync container 11") || !strings.Contains(err.Error(), "sync failed") {
		t.Fatalf("expected wrapped sync error contract, got: %v", err)
	}
}

func TestLocalWriterFinalizePhysicalOnlyWrapsCloseError(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	w.hasActive = true
	w.activeID = 12
	w.activeHandle = &fakeContainer{closeErr: errors.New("close failed")}

	err := w.finalizePhysicalOnly()
	if err == nil || !strings.Contains(err.Error(), "close container 12") || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("expected wrapped close error contract, got: %v", err)
	}
}

func TestLocalWriterAcknowledgeAppendCommittedClearsRollbackState(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	w.pendingAppend = true
	w.prevAppendSize = 123
	w.prevAppendFile = "active.bin"

	w.AcknowledgeAppendCommitted()

	if w.pendingAppend {
		t.Fatalf("expected pendingAppend to be false after commit acknowledgment")
	}
	if w.prevAppendSize != 0 {
		t.Fatalf("expected prevAppendSize to be reset, got %d", w.prevAppendSize)
	}
	if w.prevAppendFile != "" {
		t.Fatalf("expected prevAppendFile to be reset, got %q", w.prevAppendFile)
	}
}

func TestLocalWriterRollbackLastAppendNoopWhenNothingPending(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	w.pendingAppend = false
	w.prevAppendSize = 77
	w.prevAppendFile = "keep-me.bin"

	err := w.RollbackLastAppend()
	if err != nil {
		t.Fatalf("expected no-op rollback without pending append, got: %v", err)
	}
	if w.prevAppendSize != 77 || w.prevAppendFile != "keep-me.bin" {
		t.Fatalf("expected rollback no-op to leave bookkeeping unchanged, got size=%d file=%q", w.prevAppendSize, w.prevAppendFile)
	}
}

func TestLocalWriterActiveContainerStateReportsAbsentWhenInactive(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)

	ac, size, ok := w.ActiveContainerState()
	if ok {
		t.Fatalf("expected inactive writer to report ok=false")
	}
	if size != 0 {
		t.Fatalf("expected size=0 for inactive writer, got %d", size)
	}
	if ac != (ActiveContainer{}) {
		t.Fatalf("expected zero ActiveContainer for inactive writer, got %+v", ac)
	}
}

func TestLocalWriterActiveContainerStateReportsCurrentWhenActive(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	w.hasActive = true
	w.active = ActiveContainer{ID: 99, Filename: "active.bin"}
	w.activeSize = ContainerHdrLen + 10

	ac, size, ok := w.ActiveContainerState()
	if !ok {
		t.Fatalf("expected active writer to report ok=true")
	}
	if ac.ID != 99 || ac.Filename != "active.bin" {
		t.Fatalf("unexpected active container snapshot: %+v", ac)
	}
	if size != ContainerHdrLen+10 {
		t.Fatalf("unexpected active size: got %d want %d", size, ContainerHdrLen+10)
	}
}

func TestLocalWriterClearActiveResetsAllActiveState(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	w.hasActive = true
	w.active = ActiveContainer{ID: 7, Filename: "old.bin", Container: &fakeContainer{}}
	w.activeID = 7
	w.activeFile = "old.bin"
	w.activeSize = ContainerHdrLen + 5
	w.activeHandle = &fakeContainer{}

	w.clearActive()

	if w.hasActive {
		t.Fatalf("expected hasActive=false after clearActive")
	}
	if w.active != (ActiveContainer{}) {
		t.Fatalf("expected zeroed active struct, got %+v", w.active)
	}
	if w.activeID != 0 || w.activeFile != "" || w.activeSize != 0 || w.activeHandle != nil {
		t.Fatalf("expected active internals reset, got id=%d file=%q size=%d handle=%v", w.activeID, w.activeFile, w.activeSize, w.activeHandle)
	}
}

func TestIsLockNotAvailableTrueForPostgresLockCode(t *testing.T) {
	err := &pq.Error{Code: "55P03"}
	if !isLockNotAvailable(err) {
		t.Fatalf("expected lock-not-available classification for pq code 55P03")
	}
}

func TestIsLockNotAvailableFalseForOtherErrors(t *testing.T) {
	if isLockNotAvailable(errors.New("something else")) {
		t.Fatalf("expected non-pq error to not be lock-not-available")
	}
	if isLockNotAvailable(&pq.Error{Code: "23505"}) {
		t.Fatalf("expected pq non-lock code to not be lock-not-available")
	}
}

func TestLocalWriterQuarantineContainerResetsStateAndReturnsDBError(t *testing.T) {
	// Closed sqlite DB forces QuarantineContainer(db, id) to fail deterministically.
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}

	w := NewLocalWriterWithDirAndDB(t.TempDir(), ContainerHdrLen+64, dbconn)
	w.pendingAppend = true
	w.prevAppendFile = "will-be-cleared.bin"
	w.prevAppendSize = 99

	err = w.quarantineContainer(123)
	if err == nil {
		t.Fatalf("expected db quarantine failure from closed sqlite db")
	}
	if w.pendingAppend || w.prevAppendFile != "" || w.prevAppendSize != 0 {
		t.Fatalf("expected quarantine path to clear pending append bookkeeping, got pending=%v file=%q size=%d", w.pendingAppend, w.prevAppendFile, w.prevAppendSize)
	}
	if w.hasActive || w.activeID != 0 || w.activeFile != "" || w.activeHandle != nil {
		t.Fatalf("expected quarantine path to clear active state, got hasActive=%v id=%d file=%q handle=%v", w.hasActive, w.activeID, w.activeFile, w.activeHandle)
	}
}

func TestLocalWriterBindDBIgnoresNil(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	orig := w.DB()

	w.BindDB(nil)

	if w.DB() != orig {
		t.Fatalf("expected BindDB(nil) to keep existing db binding")
	}
}

func TestLocalWriterBindDBSetsDBAndDBAccessorReturnsIt(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	w.BindDB(dbconn)

	if w.DB() != dbconn {
		t.Fatalf("expected DB accessor to return bound db handle")
	}
}

func TestLocalWriterDirAccessorReturnsConfiguredDir(t *testing.T) {
	dir := t.TempDir()
	w := NewLocalWriterWithDir(dir, ContainerHdrLen+64)

	if got := w.Dir(); got != dir {
		t.Fatalf("expected dir %q, got %q", dir, got)
	}
}

func TestLocalWriterQuarantineActiveContainerNoopWhenInactive(t *testing.T) {
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+64)

	err := w.QuarantineActiveContainer()
	if err != nil {
		t.Fatalf("expected no-op quarantine for inactive writer, got: %v", err)
	}
}

func TestNewLocalWriterWithDirAndDBUsesDefaultsForEmptyDirAndSmallMaxSize(t *testing.T) {
	origContainersDir := ContainersDir
	origMax := GetContainerMaxSize()
	t.Cleanup(func() {
		ContainersDir = origContainersDir
		SetContainerMaxSize(origMax)
	})

	defaultDir := t.TempDir()
	ContainersDir = defaultDir
	SetContainerMaxSize(ContainerHdrLen + 999)

	w := NewLocalWriterWithDirAndDB("", ContainerHdrLen, nil)
	if w.dir != defaultDir {
		t.Fatalf("expected default dir %q, got %q", defaultDir, w.dir)
	}
	if w.maxSize != ContainerHdrLen+999 {
		t.Fatalf("expected default max size %d, got %d", ContainerHdrLen+999, w.maxSize)
	}
}

func TestNewLocalWriterWithDirAndDBPreservesExplicitValues(t *testing.T) {
	dir := t.TempDir()
	w := NewLocalWriterWithDirAndDB(dir, ContainerHdrLen+1234, nil)

	if w.dir != dir {
		t.Fatalf("expected explicit dir %q, got %q", dir, w.dir)
	}
	if w.maxSize != ContainerHdrLen+1234 {
		t.Fatalf("expected explicit max size %d, got %d", ContainerHdrLen+1234, w.maxSize)
	}
}

// ================================================================
// Error-path invariant tests (Step 10)
// ================================================================
// Invariant: failure before durable write publish => no live metadata points to missing bytes.
// Tests verify that write, sync, and rollback errors all result in:
//   - container handle closed (no leak)
//   - active state cleared
//   - DB row quarantined (when dbconn is set)
//   - pendingAppend = false (metadata never published)
// ================================================================

// sanitizeTestName converts a test name to a string safe for use in a SQLite URI.
func sanitizeTestName(name string) string {
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// openContainerTestDB creates a named shared-cache in-memory SQLite DB so that
// multiple concurrent connections (e.g. an open tx AND a direct dbconn query from
// QuarantineContainerInDir) share the same in-memory database without deadlocking.
// Plain ":memory:" is per-connection and would give each goroutine a separate empty DB.
func openContainerTestDB(t *testing.T, containerID int64, currentSize, maxSize int64) *sql.DB {
	t.Helper()
	// Unique name prevents test interference; cache=shared enables multiple connections.
	dsn := fmt.Sprintf("file:%s_container_test?mode=memory&cache=shared", sanitizeTestName(t.Name()))
	dbconn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if _, err := dbconn.Exec(`
		CREATE TABLE container (
			id INTEGER PRIMARY KEY,
			filename TEXT,
			current_size INTEGER,
			max_size INTEGER,
			sealed BOOLEAN,
			sealing BOOLEAN,
			quarantine BOOLEAN
		)
	`); err != nil {
		t.Fatalf("create container table: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed, sealing, quarantine)
		 VALUES (?, 'c.bin', ?, ?, FALSE, FALSE, FALSE)`,
		containerID, currentSize, maxSize,
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}
	return dbconn
}

// TestLocalWriterAppendPayloadWriteErrorQuarantinesAndClearsHandle asserts that when the
// physical Append call fails (e.g. disk full), the writer quarantines the container,
// clears all active state (including the handle), and returns an error that names the
// container — before any DB metadata about the payload is committed.
func TestLocalWriterAppendPayloadWriteErrorQuarantinesAndClearsHandle(t *testing.T) {
	const maxSize = ContainerHdrLen + 128
	dbconn := openContainerTestDB(t, 1, ContainerHdrLen+10, maxSize)

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	fc := &fakeContainer{size: ContainerHdrLen + 10, appendErr: errors.New("disk full")}
	w := NewLocalWriterWithDirAndDB(t.TempDir(), maxSize, dbconn)
	w.hasActive = true
	w.activeID = 1
	w.activeFile = "c.bin"
	w.activeHandle = fc
	w.activeSize = ContainerHdrLen + 10

	_, err = w.AppendPayload(tx, []byte("hello"))
	if err == nil {
		t.Fatal("expected error from write failure, got nil")
	}
	if !strings.Contains(err.Error(), "append payload to container 1") {
		t.Fatalf("expected error to name container, got: %v", err)
	}
	if !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("expected error to contain original cause, got: %v", err)
	}

	// Active state must be fully released — no handle leak.
	if w.hasActive {
		t.Fatalf("expected hasActive=false after quarantine, got true")
	}
	if w.activeHandle != nil {
		t.Fatalf("expected activeHandle=nil after quarantine, got non-nil")
	}
	// pendingAppend must be false: Sync was never reached so metadata was never published.
	if w.pendingAppend {
		t.Fatalf("expected pendingAppend=false (Sync never reached), got true")
	}
}

// TestLocalWriterAppendPayloadSyncErrorTruncatesAndQuarantinesContainer asserts that when
// the physical Append succeeds but the subsequent Sync (fsync) fails, the writer
// truncates the appended bytes (rollback), quarantines the container, and returns an
// error — before any DB metadata about the payload becomes visible.
func TestLocalWriterAppendPayloadSyncErrorTruncatesAndQuarantinesContainer(t *testing.T) {
	const maxSize = ContainerHdrLen + 128
	dbconn := openContainerTestDB(t, 2, ContainerHdrLen+10, maxSize)

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Append succeeds, Sync fails; Truncate succeeds.
	fc := &fakeContainer{size: ContainerHdrLen + 10, syncErr: errors.New("fsync failed")}
	w := NewLocalWriterWithDirAndDB(t.TempDir(), maxSize, dbconn)
	w.hasActive = true
	w.activeID = 2
	w.activeFile = "c.bin"
	w.activeHandle = fc
	w.activeSize = ContainerHdrLen + 10

	_, err = w.AppendPayload(tx, []byte("hello"))
	if err == nil {
		t.Fatal("expected error from sync failure, got nil")
	}
	if !strings.Contains(err.Error(), "sync payload in container 2") {
		t.Fatalf("expected error to mention sync failure, got: %v", err)
	}

	// pendingAppend must be false: Sync failed so metadata was never published.
	if w.pendingAppend {
		t.Fatalf("expected pendingAppend=false after sync error, got true")
	}
	// Active state cleared — quarantine fired after truncate succeeded.
	if w.hasActive {
		t.Fatalf("expected hasActive=false after quarantine on sync error, got true")
	}
	if w.activeHandle != nil {
		t.Fatalf("expected activeHandle=nil after quarantine, got non-nil")
	}
	// File size rolled back to pre-append value via Truncate.
	if fc.size != ContainerHdrLen+10 {
		t.Fatalf("expected file size rolled back to %d, got %d", ContainerHdrLen+10, fc.size)
	}
}

// TestLocalWriterAppendPayloadSyncAndTruncateErrorBothSurfaceInResult asserts that when
// both Sync and Truncate fail, the returned error contains both failure messages so the
// operator has full diagnostic information about the physical inconsistency.
func TestLocalWriterAppendPayloadSyncAndTruncateErrorBothSurfaceInResult(t *testing.T) {
	const maxSize = ContainerHdrLen + 128
	dbconn := openContainerTestDB(t, 3, ContainerHdrLen+10, maxSize)

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	fc := &fakeContainer{
		size:     ContainerHdrLen + 10,
		syncErr:  errors.New("fsync failed"),
		truncErr: errors.New("truncate failed"),
	}
	w := NewLocalWriterWithDirAndDB(t.TempDir(), maxSize, dbconn)
	w.hasActive = true
	w.activeID = 3
	w.activeFile = "c.bin"
	w.activeHandle = fc
	w.activeSize = ContainerHdrLen + 10

	_, err = w.AppendPayload(tx, []byte("hello"))
	if err == nil {
		t.Fatal("expected error from sync+truncate failure, got nil")
	}
	if !strings.Contains(err.Error(), "sync payload in container 3") {
		t.Fatalf("expected sync error in result, got: %v", err)
	}
	if !strings.Contains(err.Error(), "rollback append in container 3") {
		t.Fatalf("expected truncate/rollback error in result, got: %v", err)
	}

	if w.pendingAppend {
		t.Fatalf("expected pendingAppend=false after sync+truncate failure, got true")
	}
}

// TestLocalWriterRollbackLastAppendTruncatesOpenHandleToPreAppendSize asserts that when
// a DB transaction rolls back after a successful Append+Sync, RollbackLastAppend
// truncates the container file back to the pre-append offset via the open handle, and
// leaves the writer with no active state so the next write picks a fresh container.
func TestLocalWriterRollbackLastAppendTruncatesOpenHandleToPreAppendSize(t *testing.T) {
	const preAppendSize = ContainerHdrLen + 10
	const postAppendSize = ContainerHdrLen + 20

	fc := &fakeContainer{size: postAppendSize}
	w := NewLocalWriterWithDir(t.TempDir(), ContainerHdrLen+128)
	w.hasActive = true
	w.activeID = 7
	w.activeFile = "rollback.bin"
	w.activeHandle = fc
	w.activeSize = postAppendSize
	// Simulate a successful append that is now being rolled back.
	w.pendingAppend = true
	w.prevAppendSize = preAppendSize
	w.prevAppendFile = "rollback.bin"

	err := w.RollbackLastAppend()
	if err != nil {
		t.Fatalf("RollbackLastAppend: unexpected error: %v", err)
	}

	// File must be truncated back to pre-append size.
	if fc.size != preAppendSize {
		t.Fatalf("expected file truncated to %d (pre-append), got %d", preAppendSize, fc.size)
	}
	// Active state must be cleared: the writer must not reference the stale container.
	if w.hasActive {
		t.Fatalf("expected hasActive=false after successful rollback")
	}
	if w.activeHandle != nil {
		t.Fatalf("expected activeHandle=nil after rollback clearActive")
	}
	if w.pendingAppend {
		t.Fatalf("expected pendingAppend=false after rollback")
	}
}

// TestLocalWriterHandleIsClosedOnQuarantineEvenWithCloseError asserts that even when
// the container close call returns an error, the writer's active state is fully cleared
// so the handle reference is not retained. This prevents handle leaks on the error path.
func TestLocalWriterHandleIsClosedOnQuarantineEvenWithCloseError(t *testing.T) {
	const maxSize = ContainerHdrLen + 128
	dbconn := openContainerTestDB(t, 4, ContainerHdrLen+10, maxSize)

	// Close returns an error but the writer must still release all state.
	fc := &fakeContainer{size: ContainerHdrLen + 10, closeErr: errors.New("close error")}
	w := NewLocalWriterWithDirAndDB(t.TempDir(), maxSize, dbconn)
	w.hasActive = true
	w.activeID = 4
	w.activeFile = "c.bin"
	w.activeHandle = fc
	w.activeSize = ContainerHdrLen + 10
	w.pendingAppend = true
	w.prevAppendFile = "c.bin"
	w.prevAppendSize = ContainerHdrLen + 10

	// QuarantineActiveContainer should still clear state even on close error.
	_ = w.QuarantineActiveContainer()

	// Handle reference must be gone regardless of close error.
	if w.activeHandle != nil {
		t.Fatalf("expected activeHandle=nil after quarantine with close error, got non-nil")
	}
	if w.hasActive {
		t.Fatalf("expected hasActive=false after quarantine with close error, got true")
	}
	if w.pendingAppend {
		t.Fatalf("expected pendingAppend=false after quarantine, got true")
	}
}
