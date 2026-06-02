package engine_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
)

// openSnapshotTestDB opens an in-memory SQLite DB with the coldkeep schema applied.
func openSnapshotTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := idb.RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	return dbconn
}

// insertTestSnapshot inserts a snapshot row and returns the inserted ID.
func insertTestSnapshot(t *testing.T, db *sql.DB, id, snapType, label, parentID string, ts time.Time) {
	t.Helper()
	var err error
	if parentID == "" {
		_, err = db.ExecContext(context.Background(),
			`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
			id, ts.UTC().Format(time.RFC3339), snapType, label)
	} else {
		_, err = db.ExecContext(context.Background(),
			`INSERT INTO snapshot (id, created_at, type, label, parent_id) VALUES ($1, $2, $3, $4, $5)`,
			id, ts.UTC().Format(time.RFC3339), snapType, label, parentID)
	}
	if err != nil {
		t.Fatalf("insertTestSnapshot %s: %v", id, err)
	}
}

// insertTestSnapshotFile inserts a logical_file (if not already present),
// snapshot_path, and snapshot_file row. Returns the logical file ID.
// Uses INSERT OR IGNORE so two snapshots can share the same logical_file.
func insertTestSnapshotFile(t *testing.T, db *sql.DB, snapshotID, path string, size int64) int64 {
	t.Helper()
	ctx := context.Background()
	hash := path + ":hash"

	// Insert logical_file — ignore duplicates (shared content across snapshots).
	_, err := db.ExecContext(ctx,
		`INSERT OR IGNORE INTO logical_file (original_name, total_size, file_hash, ref_count, status) VALUES ($1, $2, $3, 1, 'COMPLETED')`,
		path, size, hash)
	if err != nil {
		t.Fatalf("insert logical_file for %s: %v", path, err)
	}
	lfID := lookupTestLogicalFileID(t, db, ctx, hash, size, path)

	// Insert snapshot_path.
	_, err = db.ExecContext(ctx, `INSERT OR IGNORE INTO snapshot_path (path) VALUES ($1)`, path)
	if err != nil {
		t.Fatalf("insert snapshot_path for %s: %v", path, err)
	}
	pathID := lookupTestSnapshotPathID(t, db, ctx, path)

	// Insert snapshot_file.
	_, err = db.ExecContext(ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ($1, $2, $3, $4)`,
		snapshotID, pathID, lfID, size)
	if err != nil {
		t.Fatalf("insert snapshot_file for %s/%s: %v", snapshotID, path, err)
	}
	return lfID
}

func lookupTestLogicalFileID(t *testing.T, db *sql.DB, ctx context.Context, hash string, size int64, path string) int64 {
	t.Helper()

	stmt, err := db.PrepareContext(ctx, `SELECT id FROM logical_file WHERE file_hash = ? AND total_size = ?`)
	if err != nil {
		t.Fatalf("prepare logical_file lookup for %s: %v", path, err)
	}
	defer func() { _ = stmt.Close() }()

	var lfID int64
	if err := stmt.QueryRowContext(ctx, hash, size).Scan(&lfID); err != nil {
		t.Fatalf("lookup logical_file for %s: %v", path, err)
	}
	return lfID
}

func lookupTestSnapshotPathID(t *testing.T, db *sql.DB, ctx context.Context, path string) int64 {
	t.Helper()

	stmt, err := db.PrepareContext(ctx, `SELECT id FROM snapshot_path WHERE path = ?`)
	if err != nil {
		t.Fatalf("prepare snapshot_path lookup for %s: %v", path, err)
	}
	defer func() { _ = stmt.Close() }()

	var pathID int64
	if err := stmt.QueryRowContext(ctx, path).Scan(&pathID); err != nil {
		t.Fatalf("lookup snapshot_path for %s: %v", path, err)
	}
	return pathID
}

// TestSnapshotListRoutesThroughEngine verifies that SnapshotList returns a
// correctly mapped result for snapshots in the database.
func TestSnapshotListRoutesThroughEngine(t *testing.T) {
	db := openSnapshotTestDB(t)
	t1 := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	t2 := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	insertTestSnapshot(t, db, "snap-list-A", "full", "alpha", "", t1)
	insertTestSnapshot(t, db, "snap-list-B", "partial", "beta", "", t2)

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.SnapshotList(context.Background(), engine.SnapshotListRequest{})
	if err != nil {
		t.Fatalf("SnapshotList: %v", err)
	}
	if result.Count != 2 {
		t.Fatalf("expected Count=2, got %d", result.Count)
	}
	if len(result.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(result.Snapshots))
	}
	// newest first
	if result.Snapshots[0].ID != "snap-list-B" {
		t.Errorf("first snapshot: got %q, want snap-list-B", result.Snapshots[0].ID)
	}
}

// TestSnapshotListTypeFilter verifies that Type filtering works through the engine.
func TestSnapshotListTypeFilter(t *testing.T) {
	db := openSnapshotTestDB(t)
	t1 := time.Now().UTC().Truncate(time.Second)
	insertTestSnapshot(t, db, "snap-full-1", "full", "", "", t1)
	insertTestSnapshot(t, db, "snap-partial-1", "partial", "", "", t1.Add(time.Second))

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	got, err := eng.SnapshotList(context.Background(), engine.SnapshotListRequest{Type: "full"})
	if err != nil {
		t.Fatalf("SnapshotList type=full: %v", err)
	}
	if got.Count != 1 || got.Snapshots[0].ID != "snap-full-1" {
		t.Errorf("expected snap-full-1 only, got %+v", got.Snapshots)
	}
}

// TestSnapshotListLabelAndMeta verifies that Label and ParentID are surfaced
// through the engine snapshot list result.
func TestSnapshotListLabelAndMeta(t *testing.T) {
	db := openSnapshotTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	insertTestSnapshot(t, db, "snap-parent", "full", "parent-label", "", now)
	insertTestSnapshot(t, db, "snap-child", "full", "child-label", "snap-parent", now.Add(time.Second))

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.SnapshotList(context.Background(), engine.SnapshotListRequest{})
	if err != nil {
		t.Fatalf("SnapshotList: %v", err)
	}
	// newest first → snap-child is index 0
	child := result.Snapshots[0]
	if child.Label != "child-label" {
		t.Errorf("Label: got %q, want child-label", child.Label)
	}
	if child.ParentID != "snap-parent" {
		t.Errorf("ParentID: got %q, want snap-parent", child.ParentID)
	}
}

// TestSnapshotShowNotFound verifies the engine returns an error for a missing snapshot.
func TestSnapshotShowNotFound(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	_, err = eng.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "no-such-snap"})
	if err == nil {
		t.Fatal("expected error for missing snapshot, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got %q", err.Error())
	}
}

// TestSnapshotShowReturnsMetaAndFiles verifies that SnapshotShow returns the
// snapshot metadata and correctly maps the files list.
func TestSnapshotShowReturnsMetaAndFiles(t *testing.T) {
	db := openSnapshotTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	insertTestSnapshot(t, db, "snap-show-1", "full", "show-label", "", now)
	insertTestSnapshotFile(t, db, "snap-show-1", "docs/a.txt", 100)
	insertTestSnapshotFile(t, db, "snap-show-1", "docs/b.txt", 200)

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "snap-show-1"})
	if err != nil {
		t.Fatalf("SnapshotShow: %v", err)
	}
	if result.Snapshot.ID != "snap-show-1" {
		t.Errorf("Snapshot.ID: got %q, want snap-show-1", result.Snapshot.ID)
	}
	if result.Snapshot.Label != "show-label" {
		t.Errorf("Snapshot.Label: got %q, want show-label", result.Snapshot.Label)
	}
	if result.MatchedFileCount != 2 {
		t.Errorf("MatchedFileCount: got %d, want 2", result.MatchedFileCount)
	}
	if result.TotalFileCount != 2 {
		t.Errorf("TotalFileCount: got %d, want 2", result.TotalFileCount)
	}
}

// TestSnapshotStatsBasic verifies that SnapshotStats returns correct aggregate
// counts for a snapshot with no parent.
func TestSnapshotStatsBasic(t *testing.T) {
	db := openSnapshotTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	insertTestSnapshot(t, db, "snap-stats-1", "full", "", "", now)
	insertTestSnapshotFile(t, db, "snap-stats-1", "file-a.txt", 512)
	insertTestSnapshotFile(t, db, "snap-stats-1", "file-b.txt", 1024)

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.SnapshotStats(context.Background(), engine.SnapshotStatsRequest{SnapshotID: "snap-stats-1"})
	if err != nil {
		t.Fatalf("SnapshotStats: %v", err)
	}
	if result.SnapshotFileCount != 2 {
		t.Errorf("SnapshotFileCount: got %d, want 2", result.SnapshotFileCount)
	}
	if result.TotalSizeBytes != 1536 {
		t.Errorf("TotalSizeBytes: got %d, want 1536", result.TotalSizeBytes)
	}
	if result.HasReuse {
		t.Error("HasReuse: expected false for snapshot with no parent")
	}
	if result.LineageStatus == "" {
		t.Error("LineageStatus: expected non-empty for snapshot with no parent")
	}
}

// TestSnapshotDiffSummaryFastPath verifies that SnapshotDiff with Summary:true
// uses the fast path and returns only aggregate counts.
func TestSnapshotDiffSummaryFastPath(t *testing.T) {
	db := openSnapshotTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	insertTestSnapshot(t, db, "snap-diff-base", "full", "", "", now)
	insertTestSnapshot(t, db, "snap-diff-target", "full", "", "", now.Add(time.Second))

	// base has file-common.txt and file-removed.txt
	insertTestSnapshotFile(t, db, "snap-diff-base", "common.txt", 100)
	insertTestSnapshotFile(t, db, "snap-diff-base", "removed.txt", 200)
	// target has file-common.txt and file-added.txt
	insertTestSnapshotFile(t, db, "snap-diff-target", "common.txt", 100)
	insertTestSnapshotFile(t, db, "snap-diff-target", "added.txt", 300)

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
		BaseID:   "snap-diff-base",
		TargetID: "snap-diff-target",
		Summary:  true,
	})
	if err != nil {
		t.Fatalf("SnapshotDiff summary: %v", err)
	}
	if !result.SummaryMode {
		t.Error("SummaryMode: expected true")
	}
	if result.Entries != nil {
		t.Errorf("Entries: expected nil in summary mode, got %v", result.Entries)
	}
	if result.Summary.Added != 1 {
		t.Errorf("Summary.Added: got %d, want 1", result.Summary.Added)
	}
	if result.Summary.Removed != 1 {
		t.Errorf("Summary.Removed: got %d, want 1", result.Summary.Removed)
	}
}

// TestSnapshotDiffFullReturnsEntries verifies that SnapshotDiff without Summary
// returns all change entries with the correct change type.
func TestSnapshotDiffFullReturnsEntries(t *testing.T) {
	db := openSnapshotTestDB(t)
	seedSnapshotDiffFullFixture(t, db)

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
		BaseID:   "snap-df-base",
		TargetID: "snap-df-target",
	})
	if err != nil {
		t.Fatalf("SnapshotDiff full: %v", err)
	}

	assertSnapshotDiffFullResult(t, result)
}

func seedSnapshotDiffFullFixture(t *testing.T, db *sql.DB) {
	t.Helper()

	now := time.Now().UTC().Truncate(time.Second)
	insertTestSnapshot(t, db, "snap-df-base", "full", "", "", now)
	insertTestSnapshot(t, db, "snap-df-target", "full", "", "", now.Add(time.Second))
	insertTestSnapshotFile(t, db, "snap-df-base", "kept.txt", 100)
	insertTestSnapshotFile(t, db, "snap-df-base", "gone.txt", 200)
	insertTestSnapshotFile(t, db, "snap-df-target", "kept.txt", 100)
	insertTestSnapshotFile(t, db, "snap-df-target", "new.txt", 400)
}

func assertSnapshotDiffFullResult(t *testing.T, result engine.SnapshotDiffResult) {
	t.Helper()

	if result.SummaryMode {
		t.Error("SummaryMode: expected false")
	}
	if result.BaseID != "snap-df-base" || result.TargetID != "snap-df-target" {
		t.Errorf("IDs: got base=%q target=%q", result.BaseID, result.TargetID)
	}
	assertSnapshotDiffChangeCounts(t, result, 1, 1)
}

func assertSnapshotDiffChangeCounts(t *testing.T, result engine.SnapshotDiffResult, wantAdded, wantRemoved int) {
	t.Helper()

	added, removed := countSnapshotDiffChanges(result.Entries)
	if added != wantAdded {
		t.Errorf("added entries: got %d, want %d", added, wantAdded)
	}
	if removed != wantRemoved {
		t.Errorf("removed entries: got %d, want %d", removed, wantRemoved)
	}
}

func countSnapshotDiffChanges(entries []engine.SnapshotDiffEntry) (int, int) {
	added := 0
	removed := 0
	for _, entry := range entries {
		switch entry.Change {
		case "added":
			added++
		case "removed":
			removed++
		}
	}
	return added, removed
}
