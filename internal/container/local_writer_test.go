package container

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
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
