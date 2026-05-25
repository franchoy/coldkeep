package container

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"testing"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	_ "github.com/mattn/go-sqlite3"
)

// openSeamTestDB opens an in-memory SQLite database with the full Coldkeep
// schema applied. Tests that only exercise the happy-path write cycle do not
// need shared-cache isolation because quarantine (dbconn-direct) is never
// triggered.
func openSeamTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

// TestContainerSeamDefaultFSPreservesWriteBehavior verifies that the default
// OS-backed filesystem seam (LocalWriter.fs == fsx.Default()) writes bytes
// correctly through the full container creation and append path.
func TestContainerSeamDefaultFSPreservesWriteBehavior(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 512
	dbconn := openSeamTestDB(t)
	dir := t.TempDir()

	w := NewLocalWriterWithDirAndDB(dir, maxSize, dbconn)

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	payload := []byte("phase7-container-seam-default")
	placement, err := w.AppendPayload(tx, payload)
	if err != nil {
		t.Fatalf("append payload: %v", err)
	}
	w.AcknowledgeAppendCommitted()

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	if err := w.FinalizeContainer(); err != nil {
		t.Fatalf("finalize container: %v", err)
	}

	// Verify bytes on disk at the reported offset.
	containerPath := filepath.Join(w.Dir(), placement.Filename)
	rc, err := OpenReadOnlyContainer(containerPath, maxSize)
	if err != nil {
		t.Fatalf("open readonly container: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := rc.ReadAt(placement.Offset, int64(len(payload)))
	if err != nil {
		t.Fatalf("read payload from container: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("bytes mismatch: got %q want %q", got, payload)
	}
}

// TestContainerSeamNoopFSMatchesDefaultBehavior verifies that wrapping the
// OS-backed filesystem with NoopFS produces byte-identical results to the
// default write path, confirming the seam is behavior-preserving through
// MkdirAll, OpenFile, and Stat operations.
func TestContainerSeamNoopFSMatchesDefaultBehavior(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 512
	dbconn := openSeamTestDB(t)
	dir := t.TempDir()

	w := NewLocalWriterWithDirAndDB(dir, maxSize, dbconn)
	// Inject NoopFS wrapping the OS-backed FS after construction.
	// All subsequent FS-seam calls (MkdirAll, OpenFile, Stat, Remove) route
	// through the noop wrapper before reaching the OS.
	w.fs = fsx.NewNoop(fsx.Default())

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	payload := []byte("phase7-container-seam-noop")
	placement, err := w.AppendPayload(tx, payload)
	if err != nil {
		t.Fatalf("append payload with noop fs: %v", err)
	}
	w.AcknowledgeAppendCommitted()

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	if err := w.FinalizeContainer(); err != nil {
		t.Fatalf("finalize container: %v", err)
	}

	// Verify bytes are identical to what a default-FS run would produce.
	containerPath := filepath.Join(w.Dir(), placement.Filename)
	rc, err := OpenReadOnlyContainer(containerPath, maxSize)
	if err != nil {
		t.Fatalf("open readonly container: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := rc.ReadAt(placement.Offset, int64(len(payload)))
	if err != nil {
		t.Fatalf("read payload from container: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("bytes mismatch with noop fs: got %q want %q", got, payload)
	}
}

// TestContainerSeamPreservesRetireRemoveBehavior verifies that the quarantine
// path routes through w.fs (the FS seam) and produces the same DB-state and
// active-state outcome regardless of which FS implementation backs the seam.
func TestContainerSeamPreservesRetireRemoveBehavior(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 256
	// openContainerTestDB uses shared-cache so the quarantine path (which
	// queries dbconn directly) and the tx path share the same in-memory DB.
	dbconn := openContainerTestDB(t, 1, ContainerHdrLen, maxSize)

	dir := t.TempDir()
	path := filepath.Join(dir, "c.bin")
	createTestContainerFile(t, path, maxSize)

	handle, err := OpenWritableContainer(path, maxSize)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}

	w := NewLocalWriterWithDirAndDB(dir, maxSize, dbconn)
	// Inject NoopFS to verify seam flows through w.fs.Stat inside
	// quarantineContainerInDirWithFS.
	w.fs = fsx.NewNoop(fsx.Default())
	w.hasActive = true
	w.active = ActiveContainer{ID: 1, Filename: "c.bin", Container: handle, MaxSize: maxSize}
	w.activeID = 1
	w.activeFile = "c.bin"
	w.activeHandle = handle
	w.activeSize = ContainerHdrLen

	if err := w.quarantineContainer(1); err != nil {
		t.Fatalf("quarantine container: %v", err)
	}

	// Active state must be cleared.
	if w.hasActive || w.activeID != 0 || w.activeHandle != nil || w.activeFile != "" {
		t.Fatalf("expected active state cleared after quarantine, got hasActive=%v id=%d file=%q",
			w.hasActive, w.activeID, w.activeFile)
	}

	// DB row must be quarantined.
	var quarantine bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = ?`, 1).Scan(&quarantine); err != nil {
		t.Fatalf("query quarantine flag: %v", err)
	}
	if !quarantine {
		t.Fatalf("expected container 1 to be quarantined in DB after quarantine path")
	}
}

// TestContainerFilesystemEquivalenceDefaultAndNoop is a head-to-head
// equivalence test: it writes the same payload with two independent
// LocalWriter instances — one backed by fsx.Default() and one backed by
// fsx.NewNoop(fsx.Default()) — then reads both back and asserts
// bytes.Equal, proving the seam is byte-for-byte behavior-preserving.
func TestContainerFilesystemEquivalenceDefaultAndNoop(t *testing.T) {
	t.Parallel()

	const maxSize = ContainerHdrLen + 512
	payload := []byte("phase9-container-equivalence")

	// --- default FS ---
	dbDefault := openSeamTestDB(t)
	dirDefault := t.TempDir()
	wDefault := NewLocalWriterWithDirAndDB(dirDefault, maxSize, dbDefault)

	txDefault, err := dbDefault.Begin()
	if err != nil {
		t.Fatalf("begin tx (default): %v", err)
	}
	defer func() { _ = txDefault.Rollback() }()

	placementDefault, err := wDefault.AppendPayload(txDefault, payload)
	if err != nil {
		t.Fatalf("append payload (default): %v", err)
	}
	wDefault.AcknowledgeAppendCommitted()
	if err := txDefault.Commit(); err != nil {
		t.Fatalf("commit tx (default): %v", err)
	}
	if err := wDefault.FinalizeContainer(); err != nil {
		t.Fatalf("finalize container (default): %v", err)
	}

	rcDefault, err := OpenReadOnlyContainer(filepath.Join(wDefault.Dir(), placementDefault.Filename), maxSize)
	if err != nil {
		t.Fatalf("open readonly container (default): %v", err)
	}
	defer func() { _ = rcDefault.Close() }()

	defaultBytes, err := rcDefault.ReadAt(placementDefault.Offset, int64(len(payload)))
	if err != nil {
		t.Fatalf("read payload (default): %v", err)
	}

	// --- noop FS ---
	dbNoop := openSeamTestDB(t)
	dirNoop := t.TempDir()
	wNoop := NewLocalWriterWithDirAndDB(dirNoop, maxSize, dbNoop)
	wNoop.fs = fsx.NewNoop(fsx.Default())

	txNoop, err := dbNoop.Begin()
	if err != nil {
		t.Fatalf("begin tx (noop): %v", err)
	}
	defer func() { _ = txNoop.Rollback() }()

	placementNoop, err := wNoop.AppendPayload(txNoop, payload)
	if err != nil {
		t.Fatalf("append payload (noop): %v", err)
	}
	wNoop.AcknowledgeAppendCommitted()
	if err := txNoop.Commit(); err != nil {
		t.Fatalf("commit tx (noop): %v", err)
	}
	if err := wNoop.FinalizeContainer(); err != nil {
		t.Fatalf("finalize container (noop): %v", err)
	}

	rcNoop, err := OpenReadOnlyContainer(filepath.Join(wNoop.Dir(), placementNoop.Filename), maxSize)
	if err != nil {
		t.Fatalf("open readonly container (noop): %v", err)
	}
	defer func() { _ = rcNoop.Close() }()

	noopBytes, err := rcNoop.ReadAt(placementNoop.Offset, int64(len(payload)))
	if err != nil {
		t.Fatalf("read payload (noop): %v", err)
	}

	// --- head-to-head equivalence assertion ---
	if !bytes.Equal(defaultBytes, noopBytes) {
		t.Fatalf("filesystem equivalence failure: default produced %q, noop produced %q", defaultBytes, noopBytes)
	}
}
