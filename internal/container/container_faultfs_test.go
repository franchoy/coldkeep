package container

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/fsx/faultfs"
	_ "github.com/mattn/go-sqlite3"
)

func openContainerFaultDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s_container_faults?mode=memory&cache=shared", sanitizeTestName(t.Name()))
	dbconn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func newFaultedContainerWriter(t *testing.T, maxSize int64, script *faultfs.Script) (*sql.DB, *sql.Tx, *LocalWriter, string) {
	t.Helper()

	dbconn := openContainerFaultDB(t)
	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	dir := t.TempDir()
	w := NewLocalWriterWithDirAndDB(dir, maxSize, dbconn)
	w.fs = faultfs.New(fsx.Default(), script)
	return dbconn, tx, w, dir
}

func readSingleContainerFile(t *testing.T, dir string) (string, []byte) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one container file in %s, got %d", dir, len(entries))
	}
	path := filepath.Join(dir, entries[0].Name())
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return path, got
}

func TestContainerFaultFSOpenFileFailureFailsClosed(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 1024
	openErr := errors.New("container open fault")
	_, tx, w, dir := newFaultedContainerWriter(t, maxSize, faultfs.NewScript(faultfs.Fault{
		Op:  faultfs.OpOpenFile,
		Err: openErr,
	}))

	_, err := w.AppendPayload(tx, []byte("hello container"))
	if !errors.Is(err, openErr) {
		t.Fatalf("append error = %v, want open fault", err)
	}
	if w.hasActive || w.activeHandle != nil || w.pendingAppend {
		t.Fatalf("expected writer state cleared after open failure, got hasActive=%v activeHandle=%v pendingAppend=%v", w.hasActive, w.activeHandle, w.pendingAppend)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no container files after open failure, got %d", len(entries))
	}
}

func TestContainerFaultFSHeaderWriteFailureFailsClosed(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 1024
	_, tx, w, dir := newFaultedContainerWriter(t, maxSize, faultfs.NewScript(faultfs.Fault{
		Op:    faultfs.OpWrite,
		After: 1,
		Err:   faultfs.ErrFaultWrite,
	}))

	_, err := w.AppendPayload(tx, []byte("hello container"))
	if !errors.Is(err, faultfs.ErrFaultWrite) {
		t.Fatalf("append error = %v, want ErrFaultWrite", err)
	}
	if w.hasActive || w.activeHandle != nil || w.pendingAppend {
		t.Fatalf("expected writer state cleared after header failure, got hasActive=%v activeHandle=%v pendingAppend=%v", w.hasActive, w.activeHandle, w.pendingAppend)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected cleanup to remove failed header container, got %d files", len(entries))
	}
}

func TestContainerFaultFSPayloadWriteFailureFailsClosed(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + fileContainerWriteBufferSize + 4096
	payload := bytes.Repeat([]byte("p"), fileContainerWriteBufferSize+1)
	_, tx, w, dir := newFaultedContainerWriter(t, maxSize, faultfs.NewScript(faultfs.Fault{
		Op:    faultfs.OpWrite,
		After: 2,
		Err:   faultfs.ErrFaultWrite,
	}))

	_, err := w.AppendPayload(tx, payload)
	if !errors.Is(err, faultfs.ErrFaultWrite) {
		t.Fatalf("append error = %v, want ErrFaultWrite", err)
	}
	if !w.hasActive || w.activeID <= 0 || w.activeHandle != nil || w.pendingAppend || !w.rollbackPoisoned {
		t.Fatalf("expected unresolved quarantine to retain poisoned identity after payload failure, got hasActive=%v activeID=%d activeHandle=%v pendingAppend=%v poisoned=%v", w.hasActive, w.activeID, w.activeHandle, w.pendingAppend, w.rollbackPoisoned)
	}
	if _, poisonErr := w.AppendPayload(nil, []byte("must be refused")); !errors.Is(poisonErr, errUnresolvedRollback) {
		t.Fatalf("append after unresolved payload-failure quarantine = %v, want poisoned-writer refusal", poisonErr)
	}
	path, got := readSingleContainerFile(t, dir)
	if len(got) != ContainerHdrLen {
		t.Fatalf("expected failed payload container %s to remain at header size %d, got %d", path, ContainerHdrLen, len(got))
	}
}

func TestContainerFaultFSPartialPayloadWriteFailsClosed(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + fileContainerWriteBufferSize + 4096
	payload := bytes.Repeat([]byte("x"), fileContainerWriteBufferSize+1)
	_, tx, w, dir := newFaultedContainerWriter(t, maxSize, faultfs.NewScript(faultfs.Fault{
		Op:            faultfs.OpWrite,
		After:         2,
		PartialWriteN: 3,
		Err:           faultfs.ErrFaultWrite,
	}))

	_, err := w.AppendPayload(tx, payload)
	if !errors.Is(err, faultfs.ErrFaultWrite) {
		t.Fatalf("append error = %v, want ErrFaultWrite", err)
	}
	if !w.hasActive || w.activeID <= 0 || w.activeHandle != nil || w.pendingAppend || !w.rollbackPoisoned {
		t.Fatalf("expected unresolved quarantine to retain poisoned identity after partial payload failure, got hasActive=%v activeID=%d activeHandle=%v pendingAppend=%v poisoned=%v", w.hasActive, w.activeID, w.activeHandle, w.pendingAppend, w.rollbackPoisoned)
	}
	if _, poisonErr := w.AppendPayload(nil, []byte("must be refused")); !errors.Is(poisonErr, errUnresolvedRollback) {
		t.Fatalf("append after unresolved partial-write quarantine = %v, want poisoned-writer refusal", poisonErr)
	}
	path, got := readSingleContainerFile(t, dir)
	if len(got) != ContainerHdrLen+3 {
		t.Fatalf("expected partial payload container %s to contain header+3 bytes, got %d", path, len(got))
	}
}

func TestContainerFaultFSSyncFailureFailsClosed(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 1024
	_, tx, w, dir := newFaultedContainerWriter(t, maxSize, faultfs.NewScript(faultfs.Fault{
		Op:  faultfs.OpSync,
		Err: faultfs.ErrFaultSync,
	}))

	_, err := w.AppendPayload(tx, []byte("payload"))
	if !errors.Is(err, faultfs.ErrFaultSync) {
		t.Fatalf("append error = %v, want ErrFaultSync", err)
	}
	if w.hasActive || w.activeHandle != nil || w.pendingAppend {
		t.Fatalf("expected writer state cleared after sync failure, got hasActive=%v activeHandle=%v pendingAppend=%v", w.hasActive, w.activeHandle, w.pendingAppend)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) > 1 {
		t.Fatalf("expected at most one container file after sync failure, got %d", len(entries))
	}
	if len(entries) == 1 {
		path, got := readSingleContainerFile(t, dir)
		if len(got) != ContainerHdrLen {
			t.Fatalf("expected sync-failure container %s to roll back to header size %d, got %d", path, ContainerHdrLen, len(got))
		}
	}
}

func TestLocalWriterFinalizeFailureQuarantinesAndPreventsReuse(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 1024
	script := faultfs.NewScript(faultfs.Fault{
		Op:    faultfs.OpClose,
		After: 2,
		Err:   faultfs.ErrFaultClose,
	})
	dbconn, tx, w, _ := newFaultedContainerWriter(t, maxSize, script)

	first, err := w.AppendPayload(tx, []byte("first payload"))
	if err != nil {
		t.Fatalf("append first payload: %v", err)
	}
	w.AcknowledgeAppendCommitted()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit first payload: %v", err)
	}

	if err := w.FinalizeContainer(); !errors.Is(err, faultfs.ErrFaultClose) {
		t.Fatalf("FinalizeContainer error = %v, want close failure", err)
	}
	var quarantined bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = ?`, first.ContainerID).Scan(&quarantined); err != nil {
		t.Fatalf("query first container quarantine: %v", err)
	}
	if !quarantined {
		t.Fatalf("container %d quarantine = false, want true", first.ContainerID)
	}
	if w.hasActive || w.activeHandle != nil {
		t.Fatalf("writer retained unsafe active container %d", first.ContainerID)
	}

	secondTx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin second transaction: %v", err)
	}
	t.Cleanup(func() { _ = secondTx.Rollback() })
	second, err := w.AppendPayload(secondTx, []byte("second payload"))
	if err != nil {
		t.Fatalf("append second payload: %v", err)
	}
	if second.ContainerID == first.ContainerID {
		t.Fatalf("unsafe quarantined container %d was reused", first.ContainerID)
	}
}

func prepareCommittedContainerWithPendingRollback(t *testing.T, maxSize int64, script *faultfs.Script, pendingPayload []byte) (*sql.DB, *LocalWriter, int64) {
	t.Helper()

	dbconn, firstTx, w, _ := newFaultedContainerWriter(t, maxSize, script)
	first, err := w.AppendPayload(firstTx, []byte("committed payload"))
	if err != nil {
		t.Fatalf("append committed payload: %v", err)
	}
	if err := UpdateContainerSize(firstTx, first.ContainerID, first.NewContainerSize); err != nil {
		t.Fatalf("update committed container size: %v", err)
	}
	if err := firstTx.Commit(); err != nil {
		t.Fatalf("commit initial payload: %v", err)
	}
	w.AcknowledgeAppendCommitted()

	rollbackTx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin rollback transaction: %v", err)
	}
	if _, err := w.AppendPayload(rollbackTx, pendingPayload); err != nil {
		_ = rollbackTx.Rollback()
		t.Fatalf("append rollback payload: %v", err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("roll back metadata transaction: %v", err)
	}

	return dbconn, w, first.ContainerID
}

func queryContainerQuarantine(t *testing.T, dbconn *sql.DB, containerID int64) bool {
	t.Helper()

	var quarantined bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = ?`, containerID).Scan(&quarantined); err != nil {
		t.Fatalf("query container %d quarantine: %v", containerID, err)
	}
	return quarantined
}

func appendAfterRollbackSafetyBoundary(t *testing.T, dbconn *sql.DB, w *LocalWriter, payload []byte) (LocalPlacement, error) {
	t.Helper()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin append-after-rollback transaction: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return w.AppendPayload(tx, payload)
}

func TestRollbackCloseFailureQuarantinesBeforeClearingActiveIdentity(t *testing.T) {
	t.Parallel()

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpClose, After: 2, Err: faultfs.ErrFaultClose})
	dbconn, w, affectedID := prepareCommittedContainerWithPendingRollback(
		t,
		ContainerHdrLen+1024,
		script,
		[]byte("rollback payload"),
	)

	rollbackErr := w.RollbackLastAppend()
	if !errors.Is(rollbackErr, faultfs.ErrFaultClose) {
		t.Fatalf("RollbackLastAppend error = %v, want close failure", rollbackErr)
	}
	if got := script.CallCount(faultfs.OpTruncate); got != 1 {
		t.Fatalf("truncate calls = %d, want 1", got)
	}
	if got := script.CallCount(faultfs.OpClose); got != 2 {
		t.Fatalf("close calls before quarantine = %d, want 2", got)
	}

	if err := w.QuarantineActiveContainer(); err != nil {
		t.Fatalf("quarantine affected container: %v", err)
	}
	if !queryContainerQuarantine(t, dbconn, affectedID) {
		t.Fatalf("container %d was not durably quarantined", affectedID)
	}

	next, err := appendAfterRollbackSafetyBoundary(t, dbconn, w, []byte("safe payload"))
	if err != nil {
		t.Fatalf("append after successful quarantine: %v", err)
	}
	if next.ContainerID == affectedID {
		t.Fatalf("quarantined container %d was reused", affectedID)
	}
}

func TestRollbackLastAppendJoinsTruncateAndCloseFailures(t *testing.T) {
	t.Parallel()

	script := faultfs.NewScript(
		faultfs.Fault{Op: faultfs.OpTruncate, Err: faultfs.ErrFaultTruncate},
		faultfs.Fault{Op: faultfs.OpClose, After: 2, Err: faultfs.ErrFaultClose},
	)
	dbconn, w, affectedID := prepareCommittedContainerWithPendingRollback(
		t,
		ContainerHdrLen+1024,
		script,
		[]byte("rollback payload"),
	)

	rollbackErr := w.RollbackLastAppend()
	if !errors.Is(rollbackErr, faultfs.ErrFaultTruncate) {
		t.Fatalf("RollbackLastAppend error = %v, want truncate failure", rollbackErr)
	}
	if !errors.Is(rollbackErr, faultfs.ErrFaultClose) {
		t.Fatalf("RollbackLastAppend error = %v, want close failure", rollbackErr)
	}
	if got := script.CallCount(faultfs.OpClose); got != 2 {
		t.Fatalf("close calls before quarantine = %d, want 2", got)
	}
	if err := w.QuarantineActiveContainer(); err != nil {
		t.Fatalf("quarantine affected container: %v", err)
	}
	if !queryContainerQuarantine(t, dbconn, affectedID) {
		t.Fatalf("container %d was not durably quarantined", affectedID)
	}
}

func TestRollbackTruncateFailureStillClosesAndQuarantines(t *testing.T) {
	t.Parallel()

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpTruncate, Err: faultfs.ErrFaultTruncate})
	dbconn, w, affectedID := prepareCommittedContainerWithPendingRollback(
		t,
		ContainerHdrLen+1024,
		script,
		[]byte("rollback payload"),
	)

	rollbackErr := w.RollbackLastAppend()
	if !errors.Is(rollbackErr, faultfs.ErrFaultTruncate) {
		t.Fatalf("RollbackLastAppend error = %v, want truncate failure", rollbackErr)
	}
	if got := script.CallCount(faultfs.OpClose); got != 2 {
		t.Fatalf("close calls before quarantine = %d, want 2", got)
	}
	if err := w.QuarantineActiveContainer(); err != nil {
		t.Fatalf("quarantine affected container: %v", err)
	}
	if !queryContainerQuarantine(t, dbconn, affectedID) {
		t.Fatalf("container %d was not durably quarantined", affectedID)
	}
}

func TestRollbackPostTruncateVerificationFailureQuarantines(t *testing.T) {
	t.Parallel()

	// The first Stat opens the existing container. The second is rollback's
	// post-truncate verification.
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpStat, After: 2, Err: faultfs.ErrFaultStat})
	dbconn, w, affectedID := prepareCommittedContainerWithPendingRollback(
		t,
		ContainerHdrLen+1024,
		script,
		[]byte("rollback payload"),
	)

	rollbackErr := w.RollbackLastAppend()
	if !errors.Is(rollbackErr, faultfs.ErrFaultStat) {
		t.Fatalf("RollbackLastAppend error = %v, want stat failure", rollbackErr)
	}
	if err := w.QuarantineActiveContainer(); err != nil {
		t.Fatalf("quarantine affected container: %v", err)
	}
	if !queryContainerQuarantine(t, dbconn, affectedID) {
		t.Fatalf("container %d was not durably quarantined", affectedID)
	}
}

func TestRollbackQuarantineFailureRetainsPoisonedIdentity(t *testing.T) {
	t.Parallel()

	// Stat calls: open existing container, rollback verification, quarantine.
	script := faultfs.NewScript(
		faultfs.Fault{Op: faultfs.OpClose, After: 2, Err: faultfs.ErrFaultClose},
		faultfs.Fault{Op: faultfs.OpStat, After: 3, Err: faultfs.ErrFaultStat},
	)
	dbconn, w, affectedID := prepareCommittedContainerWithPendingRollback(
		t,
		ContainerHdrLen+1024,
		script,
		[]byte("rollback payload"),
	)

	rollbackErr := w.RollbackLastAppend()
	if !errors.Is(rollbackErr, faultfs.ErrFaultClose) {
		t.Fatalf("RollbackLastAppend error = %v, want close failure", rollbackErr)
	}
	quarantineErr := w.QuarantineActiveContainer()
	if !errors.Is(quarantineErr, faultfs.ErrFaultStat) {
		t.Fatalf("QuarantineActiveContainer error = %v, want quarantine stat failure", quarantineErr)
	}
	if queryContainerQuarantine(t, dbconn, affectedID) {
		t.Fatalf("container %d was marked quarantined despite durable quarantine failure", affectedID)
	}

	w.AcknowledgeAppendCommitted()
	if _, err := appendAfterRollbackSafetyBoundary(t, dbconn, w, []byte("must be refused")); err == nil {
		t.Fatalf("poisoned writer accepted append after failed quarantine")
	}

	// The fault is one-shot. Retrying the existing quarantine API must operate on
	// the retained identity and recover once durable exclusion succeeds.
	if err := w.QuarantineActiveContainer(); err != nil {
		t.Fatalf("retry quarantine affected container: %v", err)
	}
	if !queryContainerQuarantine(t, dbconn, affectedID) {
		t.Fatalf("container %d was not quarantined on retry", affectedID)
	}
}

func TestSuccessfulRollbackClearsActiveState(t *testing.T) {
	t.Parallel()

	dbconn, w, affectedID := prepareCommittedContainerWithPendingRollback(
		t,
		ContainerHdrLen+1024,
		faultfs.NewScript(),
		[]byte("rollback payload"),
	)

	if err := w.RollbackLastAppend(); err != nil {
		t.Fatalf("RollbackLastAppend: %v", err)
	}
	if queryContainerQuarantine(t, dbconn, affectedID) {
		t.Fatalf("successful rollback unexpectedly quarantined container %d", affectedID)
	}
	if w.hasActive || w.activeHandle != nil || w.pendingAppend {
		t.Fatalf("successful rollback retained active/pending state")
	}
	if _, err := appendAfterRollbackSafetyBoundary(t, dbconn, w, []byte("normal append")); err != nil {
		t.Fatalf("append after successful rollback: %v", err)
	}
}

func TestClosedContainerRollbackFailureCanStillQuarantineAffectedContainer(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 64
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpTruncate, Err: faultfs.ErrFaultTruncate})
	dbconn, firstTx, w, _ := newFaultedContainerWriter(t, maxSize, script)
	first, err := w.AppendPayload(firstTx, []byte("base"))
	if err != nil {
		t.Fatalf("append committed base: %v", err)
	}
	if err := UpdateContainerSize(firstTx, first.ContainerID, first.NewContainerSize); err != nil {
		t.Fatalf("update committed base size: %v", err)
	}
	if err := firstTx.Commit(); err != nil {
		t.Fatalf("commit base: %v", err)
	}
	w.AcknowledgeAppendCommitted()

	rollbackTx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin full-container rollback transaction: %v", err)
	}
	remaining := maxSize - first.NewContainerSize
	placement, err := w.AppendPayload(rollbackTx, bytes.Repeat([]byte("x"), int(remaining)))
	if err != nil {
		_ = rollbackTx.Rollback()
		t.Fatalf("append full-container rollback payload: %v", err)
	}
	if !placement.Full {
		_ = rollbackTx.Rollback()
		t.Fatalf("expected full placement")
	}
	if err := w.FinalizeContainer(); err != nil {
		_ = rollbackTx.Rollback()
		t.Fatalf("finalize full container: %v", err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("roll back full-container metadata: %v", err)
	}

	rollbackErr := w.RollbackLastAppend()
	if !errors.Is(rollbackErr, faultfs.ErrFaultTruncate) {
		t.Fatalf("RollbackLastAppend error = %v, want path truncate failure", rollbackErr)
	}
	if err := w.QuarantineActiveContainer(); err != nil {
		t.Fatalf("quarantine closed affected container: %v", err)
	}
	if !queryContainerQuarantine(t, dbconn, first.ContainerID) {
		t.Fatalf("closed container %d was not durably quarantined", first.ContainerID)
	}
}

func TestContainerFaultFSRemoveFailurePreservesRetireFailure(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 1024
	removeErr := faultfs.ErrFaultRemove
	_, tx, w, dir := newFaultedContainerWriter(t, maxSize, faultfs.NewScript(
		faultfs.Fault{Op: faultfs.OpWrite, After: 1, Err: faultfs.ErrFaultWrite},
		faultfs.Fault{Op: faultfs.OpRemove, Err: removeErr},
	))

	_, err := w.AppendPayload(tx, []byte("hello container"))
	if !errors.Is(err, faultfs.ErrFaultWrite) {
		t.Fatalf("append error = %v, want ErrFaultWrite", err)
	}
	if !errors.Is(err, removeErr) {
		t.Fatalf("append error = %v, want remove fault to be preserved", err)
	}
	if w.hasActive || w.activeHandle != nil || w.pendingAppend {
		t.Fatalf("expected writer state cleared after retire failure, got hasActive=%v activeHandle=%v pendingAppend=%v", w.hasActive, w.activeHandle, w.pendingAppend)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) > 1 {
		t.Fatalf("expected at most one container file after remove failure, got %d", len(entries))
	}
}
