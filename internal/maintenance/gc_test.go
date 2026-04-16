package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/verify"
)

func requireDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run DB-backed maintenance tests")
	}
}

func applySchema(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var logicalFileTable sql.NullString
	if err := dbconn.QueryRow(`SELECT to_regclass('public.logical_file')`).Scan(&logicalFileTable); err == nil && logicalFileTable.Valid {
		return
	}

	if strings.TrimSpace(dbschema.PostgresSchema) == "" {
		t.Fatalf("embedded postgres schema is empty")
	}

	if _, err := dbconn.Exec(dbschema.PostgresSchema); err != nil && !isDuplicateSchemaError(err) {
		t.Fatalf("apply schema: %v", err)
	}
}

func isDuplicateSchemaError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "42710")
}

func resetDB(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	_, err := dbconn.Exec(`
		TRUNCATE TABLE
			snapshot_file,
			snapshot,
			file_chunk,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func TestRunGCWithAdvisoryUnlockFailureStillSucceeds(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})
	container.ContainersDir = containersDir

	filename := "gc-unlock-failure.bin"
	containerPath := filepath.Join(containersDir, filename)
	if err := os.WriteFile(containerPath, []byte("gc unlock failure test"), 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)`,
		filename,
		int64(len("gc unlock failure test")),
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	originalUnlock := gcAdvisoryUnlock
	gcAdvisoryUnlock = func(_ context.Context, _ *sql.DB) error {
		return errors.New("forced advisory unlock failure")
	}
	t.Cleanup(func() {
		gcAdvisoryUnlock = originalUnlock
	})

	result, err := RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("gc should succeed despite advisory unlock failure: %v", err)
	}
	if result.AffectedContainers != 1 {
		t.Fatalf("expected one affected container, got %d", result.AffectedContainers)
	}

	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&remaining); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected container row to be deleted, got %d", remaining)
	}
}

func TestRunGCRefusesOnPhysicalIntegrityIssues(t *testing.T) {
	// This test stubs gcPhysicalIntegrityCheck to simulate a drifted graph.
	// No DB connection required — the refusal path is exercised before any
	// container work begins.
	requireDB(t)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{
			OrphanPhysicalFileRows:    0,
			LogicalRefCountMismatches: 3,
			NegativeLogicalRefCounts:  0,
		}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected GC to be refused but got no error")
	}
	if code, ok := invariants.Code(gcErr); !ok || code != invariants.CodeGCRefusedIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeGCRefusedIntegrity, code, ok, gcErr)
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected error to mention 'GC refused', got: %v", gcErr)
	}
	if !strings.Contains(gcErr.Error(), "ref_count_mismatches=3") {
		t.Fatalf("expected error to include mismatch count, got: %v", gcErr)
	}
}

func TestRunGCRefusesOnOrphanPhysicalFileRows(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{OrphanPhysicalFileRows: 2}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected GC to be refused but got no error")
	}
	if code, ok := invariants.Code(gcErr); !ok || code != invariants.CodeGCRefusedIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeGCRefusedIntegrity, code, ok, gcErr)
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused' in error, got: %v", gcErr)
	}
	if !strings.Contains(gcErr.Error(), "orphan_rows=2") {
		t.Fatalf("expected orphan_rows=2 in error, got: %v", gcErr)
	}
}

func TestRunGCRefusesOnNegativeLogicalRefCounts(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{NegativeLogicalRefCounts: 1}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected GC to be refused but got no error")
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused' in error, got: %v", gcErr)
	}
	if !strings.Contains(gcErr.Error(), "negative_ref_counts=1") {
		t.Fatalf("expected negative_ref_counts=1 in error, got: %v", gcErr)
	}
}

func TestRunGCDryRunRefusesOnDriftedGraph(t *testing.T) {
	// Dry-run GC is subject to the same physical-root integrity pre-flight as
	// real GC. "What would be deleted" is only meaningful on a coherent graph.
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{LogicalRefCountMismatches: 1}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(true /* dryRun */, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected dry-run GC to be refused but got no error")
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused' in dry-run error, got: %v", gcErr)
	}
}

func TestRunGCSucceedsAfterRepairLogicalRefCounts(t *testing.T) {
	// Full operator recovery path:
	// 1. Healthy graph: logical_file + physical_file rows consistent.
	// 2. Corrupt: drift logical_file.ref_count so integrity check fires.
	// 3. GC refuses (real CheckPhysicalFileGraphIntegrity, no stub).
	// 4. Repair: RepairLogicalRefCountsResultWithDB fixes ref_count.
	// 5. GC succeeds (no containers to collect, but no refusal).
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Step 1: insert a consistent logical_file + physical_file pair.
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status)
		VALUES ('gc-repair-smoke.bin', 1024, 'aabbcc', 1, 'COMPLETED')
		RETURNING id
	`).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
		VALUES ('/data/gc-repair-smoke.bin', $1, TRUE)
	`, logicalID); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

	// Step 2: drift ref_count (1→5) to create a mismatch.
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = 5 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("corrupt ref_count: %v", err)
	}

	// Step 3: GC must be refused.
	if _, gcErr := RunGCWithContainersDirResult(false, t.TempDir()); gcErr == nil {
		t.Fatal("expected GC to be refused on drifted graph but got no error")
	} else if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused', got: %v", gcErr)
	}

	// Step 4: repair via RepairLogicalRefCountsResultWithDB.
	repairResult, repairErr := RepairLogicalRefCountsResultWithDB(dbconn)
	if repairErr != nil {
		t.Fatalf("RepairLogicalRefCountsResultWithDB: %v", repairErr)
	}
	if repairResult.UpdatedLogicalFiles != 1 {
		t.Fatalf("expected 1 updated logical_file, got %d", repairResult.UpdatedLogicalFiles)
	}

	// Step 5: GC must now succeed (clean graph, no containers to collect).
	gcResult, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr != nil {
		t.Fatalf("GC should succeed after repair, got: %v", gcErr)
	}
	if gcResult.AffectedContainers != 0 {
		t.Fatalf("expected 0 affected containers, got %d", gcResult.AffectedContainers)
	}
}

// setupSnapshotRetainedContainer creates a sealed, empty (live_ref_count == 0)
// container whose sole chunk is logically reachable via a snapshot_file. It
// returns the container ID and filename so callers can assert GC behaviour.
// The file on disk is written to containersDir.
func setupSnapshotRetainedContainer(t *testing.T, dbconn *sql.DB, containersDir string) (containerID int64, filename string) {
	t.Helper()

	// Insert a logical file and its chunk, leaving live_ref_count = 0 to
	// simulate a state where the ref-count model says "reclaimable" but the
	// snapshot layer says "retained".
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status)
		VALUES ('snap-retained.bin', 512, 'deadbeef01', 0, 'COMPLETED')
		RETURNING id
	`).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (hash, size, live_ref_count, pin_count)
		VALUES ('deadbeef01chunk', 512, 0, 0)
		RETURNING id
	`).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO file_chunk (logical_file_id, chunk_id, sequence_index, offset_in_file)
		VALUES ($1, $2, 0, 0)
	`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	filename = "snap-retained.bin"
	containerPath := filepath.Join(containersDir, filename)
	if err := os.WriteFile(containerPath, []byte("snap retained test"), 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, TRUE, FALSE)
		RETURNING id
	`, filename, int64(len("snap retained test")), container.GetContainerMaxSize()).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO blocks (container_id, chunk_id, offset_in_container, compressed_size)
		VALUES ($1, $2, 0, 512)
	`, containerID, chunkID); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}

	// Attach a snapshot that retains the logical file.
	if _, err := dbconn.Exec(`
		INSERT INTO snapshot (id, created_at, type) VALUES ('snap-gc-guard-1', NOW(), 'full')
	`); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO snapshot_file (snapshot_id, path, logical_file_id)
		VALUES ('snap-gc-guard-1', '/snap/snap-retained.bin', $1)
	`, logicalID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	return containerID, filename
}

// TestRunGCDoesNotDeleteSnapshotRetainedContainer verifies that a sealed
// container whose chunks are reachable from a snapshot is not reclaimed by GC,
// even when all chunk live_ref_counts are zero.
func TestRunGCDoesNotDeleteSnapshotRetainedContainer(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	containerID, filename := setupSnapshotRetainedContainer(t, dbconn, containersDir)

	result, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("GC should succeed: %v", gcErr)
	}
	if result.AffectedContainers != 0 {
		t.Fatalf("expected 0 affected containers (snapshot-retained), got %d", result.AffectedContainers)
	}
	if result.SnapshotRetainedContainers != 1 {
		t.Fatalf("expected 1 snapshot-retained container, got %d", result.SnapshotRetainedContainers)
	}

	// Container row and file must still exist.
	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&remaining); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected container row to survive GC, got count=%d", remaining)
	}
	if _, err := os.Stat(filepath.Join(containersDir, filename)); err != nil {
		t.Fatalf("expected container file to survive GC: %v", err)
	}
}

// TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable verifies
// that dry-run GC does not flag snapshot-retained containers as reclaimable.
func TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	setupSnapshotRetainedContainer(t, dbconn, containersDir)

	result, gcErr := RunGCWithContainersDirResult(true /* dryRun */, containersDir)
	if gcErr != nil {
		t.Fatalf("dry-run GC should succeed: %v", gcErr)
	}
	if result.AffectedContainers != 0 {
		t.Fatalf("expected 0 reclaimable containers in dry-run (snapshot-retained), got %d", result.AffectedContainers)
	}
	if result.SnapshotRetainedContainers != 1 {
		t.Fatalf("expected 1 snapshot-retained container in dry-run result, got %d", result.SnapshotRetainedContainers)
	}
}
