package container

import (
	"database/sql"
	"errors"
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
	syncErr  error
	closeErr error
}

func (f *fakeContainer) Append(data []byte) (int64, error) { return 0, nil }
func (f *fakeContainer) ReadAt(offset int64, size int64) ([]byte, error) {
	return nil, nil
}
func (f *fakeContainer) Size() int64               { return ContainerHdrLen }
func (f *fakeContainer) Truncate(size int64) error { return nil }
func (f *fakeContainer) Sync() error               { return f.syncErr }
func (f *fakeContainer) Close() error              { return f.closeErr }

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
	if tx.execCalls != 1 {
		t.Fatalf("expected one exec call, got %d", tx.execCalls)
	}
	if len(tx.queries) != 1 || !strings.Contains(tx.queries[0], "FOR UPDATE NOWAIT") {
		t.Fatalf("expected NOWAIT lock query, got: %v", tx.queries)
	}
}

func TestLockContainerRowNowaitWithRetryReturnsContentionAfterExhaustion(t *testing.T) {
	tx := &stubTx{errs: []error{&pq.Error{Code: "55P03"}}}

	err := lockContainerRowNowaitWithRetry(tx, nil, 42, 1, time.Millisecond)
	if !errors.Is(err, ErrContainerLockContention) {
		t.Fatalf("expected ErrContainerLockContention, got: %v", err)
	}
	if !strings.Contains(err.Error(), "container 42") {
		t.Fatalf("expected container id in contention error, got: %v", err)
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
