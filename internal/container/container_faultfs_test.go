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
	if w.hasActive || w.activeHandle != nil || w.pendingAppend {
		t.Fatalf("expected writer state cleared after payload failure, got hasActive=%v activeHandle=%v pendingAppend=%v", w.hasActive, w.activeHandle, w.pendingAppend)
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
	if w.hasActive || w.activeHandle != nil || w.pendingAppend {
		t.Fatalf("expected writer state cleared after partial payload failure, got hasActive=%v activeHandle=%v pendingAppend=%v", w.hasActive, w.activeHandle, w.pendingAppend)
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
