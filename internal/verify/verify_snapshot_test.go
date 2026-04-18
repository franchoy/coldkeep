package verify

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/invariants"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

func insertSnapshotRowForVerify(t *testing.T, dbconn *sql.DB, snapshotID string) {
	t.Helper()
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ($1, $2, $3)`, snapshotID, time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot %q: %v", snapshotID, err)
	}
}

func TestVerifySystemStandardPassesWithConsistentSnapshotReachability(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"snapshot-ok.txt", int64(0), strings.Repeat("d", 64), filestate.LogicalFileCompleted, int64(0),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	insertSnapshotRowForVerify(t, dbconn, "snap-ok-1")
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path(path) VALUES ($1) ON CONFLICT(path) DO NOTHING`, "docs/snapshot-ok.txt"); err != nil {
		t.Fatalf("insert snapshot_path row: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, (SELECT id FROM snapshot_path WHERE path = $2), $3)`, "snap-ok-1", "docs/snapshot-ok.txt", logicalID); err != nil {
		t.Fatalf("insert snapshot_file row: %v", err)
	}

	if err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir()); err != nil {
		t.Fatalf("verify standard should pass on consistent snapshot reachability graph: %v", err)
	}
}

func TestVerifySystemStandardDetectsOrphanSnapshotLogicalReference(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path(path) VALUES ($1) ON CONFLICT(path) DO NOTHING`, "docs/missing.txt"); err != nil {
		t.Fatalf("insert snapshot_path row: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, (SELECT id FROM snapshot_path WHERE path = $2), $3)`, "snap-orphan-1", "docs/missing.txt", int64(9999)); err != nil {
		t.Fatalf("insert orphan snapshot_file row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "orphan_snapshot_logical_refs=1") {
		t.Fatalf("expected orphan snapshot logical ref verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodeSnapshotGraphOrphanLogicalRef {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeSnapshotGraphOrphanLogicalRef, code, ok, err)
	}
}

func TestVerifySystemStandardDetectsOrphanSnapshotPathReference(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"snapshot-path-orphan.txt", int64(0), strings.Repeat("p", 64), filestate.LogicalFileCompleted, int64(0),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	insertSnapshotRowForVerify(t, dbconn, "snap-path-orphan-1")
	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)`,
		"snap-path-orphan-1", int64(9999), logicalID,
	); err != nil {
		t.Fatalf("insert orphan snapshot path ref row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "orphan_snapshot_path_refs=1") {
		t.Fatalf("expected orphan snapshot path ref verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodeSnapshotGraphOrphanLogicalRef {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeSnapshotGraphOrphanLogicalRef, code, ok, err)
	}
}

func TestVerifySystemStandardDetectsSnapshotInvalidLifecycleState(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"snapshot-aborted.txt", int64(0), strings.Repeat("e", 64), filestate.LogicalFileAborted, int64(0),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	insertSnapshotRowForVerify(t, dbconn, "snap-lifecycle-1")
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path(path) VALUES ($1) ON CONFLICT(path) DO NOTHING`, "docs/snapshot-aborted.txt"); err != nil {
		t.Fatalf("insert snapshot_path row: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, (SELECT id FROM snapshot_path WHERE path = $2), $3)`, "snap-lifecycle-1", "docs/snapshot-aborted.txt", logicalID); err != nil {
		t.Fatalf("insert snapshot_file row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "invalid_snapshot_lifecycle_states=1") {
		t.Fatalf("expected invalid snapshot lifecycle verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodeSnapshotGraphInvalidLifecycle {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeSnapshotGraphInvalidLifecycle, code, ok, err)
	}
}

func TestVerifySystemStandardDetectsSnapshotRetainedMissingChunkGraph(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"snapshot-missing-graph.txt", int64(128), strings.Repeat("f", 64), filestate.LogicalFileCompleted, int64(0),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	insertSnapshotRowForVerify(t, dbconn, "snap-graph-1")
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path(path) VALUES ($1) ON CONFLICT(path) DO NOTHING`, "docs/snapshot-missing-graph.txt"); err != nil {
		t.Fatalf("insert snapshot_path row: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, (SELECT id FROM snapshot_path WHERE path = $2), $3)`, "snap-graph-1", "docs/snapshot-missing-graph.txt", logicalID); err != nil {
		t.Fatalf("insert snapshot_file row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "retained_missing_chunk_graph=1") {
		t.Fatalf("expected snapshot retained missing chunk graph verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodeSnapshotGraphIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeSnapshotGraphIntegrity, code, ok, err)
	}
}
