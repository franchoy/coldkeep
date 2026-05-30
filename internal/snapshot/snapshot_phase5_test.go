package snapshot

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// TestSnapshotSourceQueryFiltersCompletedLogicalFiles verifies that the
// snapshot source query includes the lf.status = 'COMPLETED' filter (S1).
func TestSnapshotSourceQueryFiltersCompletedLogicalFiles(t *testing.T) {
	dbconn := openTestDB(t)
	query := strings.ToLower(snapshotSourceQuery(dbconn))
	if !strings.Contains(query, "lf.status = 'completed'") {
		t.Fatalf("expected snapshot source query to filter lf.status = 'COMPLETED', got %q", query)
	}
}

// TestCreateSnapshotExcludesIncompleteLogicalFiles verifies that a physical_file
// linked to an incomplete (non-COMPLETED) logical_file is not included in the
// snapshot (S1 behavior).
func TestCreateSnapshotExcludesIncompleteLogicalFiles(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Insert a COMPLETED logical file linked to a physical file.
	resCompleted, err := db.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, 'v1-simple-rolling')`,
		"complete.txt", int64(10), "hash-s1-completed", "COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert completed logical_file: %v", err)
	}
	completedID, _ := resCompleted.LastInsertId()
	if _, err := db.Exec(
		`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete) VALUES (?, ?, NULL, NULL, 1)`,
		"complete.txt", completedID,
	); err != nil {
		t.Fatalf("insert physical_file for completed: %v", err)
	}

	// Insert a PROCESSING logical file linked to a different physical file.
	resProcessing, err := db.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, 'v1-simple-rolling')`,
		"processing.txt", int64(10), "hash-s1-processing", "PROCESSING",
	)
	if err != nil {
		t.Fatalf("insert processing logical_file: %v", err)
	}
	processingID, _ := resProcessing.LastInsertId()
	if _, err := db.Exec(
		`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete) VALUES (?, ?, NULL, NULL, 1)`,
		"processing.txt", processingID,
	); err != nil {
		t.Fatalf("insert physical_file for processing: %v", err)
	}

	if err := CreateSnapshot(ctx, db, "snap-s1-filter", "full", nil, nil, nil); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	rows, err := db.Query(
		`SELECT sp.path FROM snapshot_file sf JOIN snapshot_path sp ON sp.id = sf.path_id WHERE sf.snapshot_id = ?`,
		"snap-s1-filter",
	)
	if err != nil {
		t.Fatalf("query snapshot_file paths: %v", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatalf("scan path: %v", err)
		}
		paths = append(paths, p)
	}
	if rows.Err() != nil {
		t.Fatalf("rows iteration: %v", rows.Err())
	}

	for _, p := range paths {
		if p == "processing.txt" {
			t.Fatalf("snapshot included processing.txt (incomplete logical file); snapshot must only contain COMPLETED files")
		}
	}

	found := false
	for _, p := range paths {
		if p == "complete.txt" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("snapshot did not include complete.txt (COMPLETED logical file); got paths: %v", paths)
	}
}

// TestSnapshotAncestorCycleExistsDetectsCycle verifies that snapshotAncestorCycleExists
// returns true when the ancestry chain forms a cycle (S2 helper unit test).
func TestSnapshotAncestorCycleExistsDetectsCycle(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Create two snapshots manually and inject a cycle: A.parent_id = B, B.parent_id = A.
	if _, err := db.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, datetime('now'), 'full'), (?, datetime('now'), 'full')`,
		"snap-cycle-a", "snap-cycle-b",
	); err != nil {
		t.Fatalf("insert cycle snapshots: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-cycle-b", "snap-cycle-a"); err != nil {
		t.Fatalf("set parent_id for snap-cycle-a: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-cycle-a", "snap-cycle-b"); err != nil {
		t.Fatalf("set parent_id for snap-cycle-b: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	hasCycle, err := snapshotAncestorCycleExists(ctx, tx, "snap-cycle-a", "snap-new", 100)
	if err != nil {
		t.Fatalf("snapshotAncestorCycleExists: %v", err)
	}
	if !hasCycle {
		t.Fatal("expected cycle detection to return true for a→b→a loop")
	}
}

// TestSnapshotAncestorCycleExistsNoCycleLinearChain verifies that
// snapshotAncestorCycleExists returns false for a valid linear ancestry chain.
func TestSnapshotAncestorCycleExistsNoCycleLinearChain(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Create a linear chain: snap-root ← snap-mid ← snap-leaf (no cycle).
	if _, err := db.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, datetime('now'), 'full'), (?, datetime('now'), 'full'), (?, datetime('now'), 'full')`,
		"snap-root", "snap-mid", "snap-leaf",
	); err != nil {
		t.Fatalf("insert linear chain snapshots: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-root", "snap-mid"); err != nil {
		t.Fatalf("set parent_id for snap-mid: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-mid", "snap-leaf"); err != nil {
		t.Fatalf("set parent_id for snap-leaf: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	hasCycle, err := snapshotAncestorCycleExists(ctx, tx, "snap-leaf", "snap-new-child", 100)
	if err != nil {
		t.Fatalf("snapshotAncestorCycleExists: %v", err)
	}
	if hasCycle {
		t.Fatal("expected no cycle for linear chain snap-root←snap-mid←snap-leaf")
	}
}

// TestCreateSnapshotRejectsCyclicParentAncestry verifies that CreateSnapshot
// rejects a parent_id whose ancestry chain contains a cycle (S2 end-to-end).
func TestCreateSnapshotRejectsCyclicParentAncestry(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Create a cycle in the DB: snap-s2-a ↔ snap-s2-b.
	if _, err := db.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, datetime('now'), 'full'), (?, datetime('now'), 'full')`,
		"snap-s2-a", "snap-s2-b",
	); err != nil {
		t.Fatalf("insert cyclic snapshots: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-s2-b", "snap-s2-a"); err != nil {
		t.Fatalf("set snap-s2-a parent_id: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-s2-a", "snap-s2-b"); err != nil {
		t.Fatalf("set snap-s2-b parent_id: %v", err)
	}

	parentID := "snap-s2-a"
	err := CreateSnapshot(ctx, db, "snap-s2-new", "full", nil, &parentID, nil)
	if err == nil {
		t.Fatal("expected CreateSnapshot to reject a parent with cyclic ancestry, got nil")
	}
	if !strings.Contains(err.Error(), "cyclic ancestry") {
		t.Fatalf("expected cyclic ancestry error, got: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-s2-new").Scan(&count); err != nil {
		t.Fatalf("query snapshot count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no snapshot row after cyclic parent rejection, got %d", count)
	}
}

// TestSnapshotAncestorCycleExistsDetectsTargetIDInChain verifies that
// snapshotAncestorCycleExists returns true when targetID appears in the
// ancestry chain (defensive check for re-used IDs).
func TestSnapshotAncestorCycleExistsDetectsTargetIDInChain(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Chain: snap-ancestor ← snap-target (snap-target is in the ancestry).
	if _, err := db.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, datetime('now'), 'full'), (?, datetime('now'), 'full')`,
		"snap-anc-root", "snap-anc-target",
	); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-anc-root", "snap-anc-target"); err != nil {
		t.Fatalf("set snap-anc-target parent_id: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Traverse from snap-anc-target looking for snap-anc-target as targetID.
	// snap-anc-target is the startID AND the targetID — it's found immediately.
	hasCycle, err := snapshotAncestorCycleExists(ctx, tx, "snap-anc-target", "snap-anc-target", 100)
	if err != nil {
		t.Fatalf("snapshotAncestorCycleExists: %v", err)
	}
	if !hasCycle {
		t.Fatal("expected cycle detection to return true when startID == targetID")
	}
}

// TestSnapshotAncestorCycleExistsMaxDepthFailsClosed verifies that exceeding
// maxDepth returns true (fail-closed) rather than silently accepting a
// potentially unbounded chain.
func TestSnapshotAncestorCycleExistsMaxDepthFailsClosed(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Create a linear chain of 5 snapshots with no cycle.
	ids := []string{"snap-d0", "snap-d1", "snap-d2", "snap-d3", "snap-d4"}
	for _, id := range ids {
		if _, err := db.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, datetime('now'), 'full')`, id); err != nil {
			t.Fatalf("insert snapshot %s: %v", id, err)
		}
	}
	for i := 1; i < len(ids); i++ {
		if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, ids[i-1], ids[i]); err != nil {
			t.Fatalf("set parent_id chain: %v", err)
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// With maxDepth=2, traversal of a chain of depth 4 should fail-closed.
	hasCycle, err := snapshotAncestorCycleExists(ctx, tx, ids[len(ids)-1], "snap-new", 2)
	if err != nil {
		t.Fatalf("snapshotAncestorCycleExists: %v", err)
	}
	if !hasCycle {
		t.Fatal("expected maxDepth exceeded to return true (fail-closed)")
	}
}

// TestCreateSnapshotParentIDPointsToNonExistentSnapshot verifies that existing
// error handling for a missing parent still works after the cycle-check addition.
func TestCreateSnapshotParentIDPointsToNonExistentSnapshotAfterCycleCheck(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFile(t, db, "hash-s2-missing-parent")
	insertPhysicalFile(t, db, "file.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	parentID := "snap-never-existed-s2"
	err := CreateSnapshot(ctx, db, "snap-s2-orphan", "full", nil, &parentID, nil)
	if err == nil {
		t.Fatal("expected error when parent snapshot not found")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' error for missing parent, got: %v", err)
	}
}
