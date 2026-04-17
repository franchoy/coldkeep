package retention

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	idb "github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := idb.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return dbconn
}

func insertLogical(t *testing.T, dbconn *sql.DB, name string) int64 {
	t.Helper()
	res, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
		name, int64(10), name+"-hash", "COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert logical_file %q: %v", name, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return id
}

func insertLogicalWithSize(t *testing.T, dbconn *sql.DB, name string, size int64) int64 {
	t.Helper()
	res, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
		name, size, name+"-hash", "COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert logical_file %q (size=%d): %v", name, size, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return id
}

func insertSnapshot(t *testing.T, dbconn *sql.DB, id string) {
	t.Helper()
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`,
		id, time.Now().UTC(), "full",
	); err != nil {
		t.Fatalf("insert snapshot %q: %v", id, err)
	}
}

func TestListRetainedLogicalFileIDs(t *testing.T) {
	dbconn := openTestDB(t)
	ctx := context.Background()

	logicalCurrentOnly := insertLogical(t, dbconn, "current-only")
	logicalSnapshotOnly := insertLogical(t, dbconn, "snapshot-only")
	logicalShared := insertLogical(t, dbconn, "shared")

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 1), (?, ?, 1)`,
		"/data/current-only", logicalCurrentOnly,
		"/data/shared", logicalShared,
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	insertSnapshot(t, dbconn, "snap-r1")
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id) VALUES (?, ?, ?), (?, ?, ?)`,
		"snap-r1", "snap/snapshot-only", logicalSnapshotOnly,
		"snap-r1", "snap/shared", logicalShared,
	); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	currentIDs, err := ListCurrentReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		t.Fatalf("ListCurrentReferencedLogicalFileIDs: %v", err)
	}
	if !reflect.DeepEqual(currentIDs, map[int64]struct{}{
		logicalCurrentOnly: {},
		logicalShared:      {},
	}) {
		t.Fatalf("unexpected current IDs: %+v", currentIDs)
	}

	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		t.Fatalf("ListSnapshotReferencedLogicalFileIDs: %v", err)
	}
	if !reflect.DeepEqual(snapshotIDs, map[int64]struct{}{
		logicalSnapshotOnly: {},
		logicalShared:       {},
	}) {
		t.Fatalf("unexpected snapshot IDs: %+v", snapshotIDs)
	}

	allIDs, err := ListAllRetainedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		t.Fatalf("ListAllRetainedLogicalFileIDs: %v", err)
	}
	if !reflect.DeepEqual(allIDs, map[int64]struct{}{
		logicalCurrentOnly:  {},
		logicalSnapshotOnly: {},
		logicalShared:       {},
	}) {
		t.Fatalf("unexpected retained IDs union: %+v", allIDs)
	}
}

func TestIsLogicalFileReferencedBySnapshot(t *testing.T) {
	dbconn := openTestDB(t)
	ctx := context.Background()

	logicalRetained := insertLogical(t, dbconn, "retained")
	logicalUnretained := insertLogical(t, dbconn, "unretained")

	insertSnapshot(t, dbconn, "snap-retain")
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id) VALUES (?, ?, ?)`,
		"snap-retain", "retained/path", logicalRetained,
	); err != nil {
		t.Fatalf("insert snapshot_file row: %v", err)
	}

	retained, err := IsLogicalFileReferencedBySnapshot(ctx, dbconn, logicalRetained)
	if err != nil {
		t.Fatalf("IsLogicalFileReferencedBySnapshot(retained): %v", err)
	}
	if !retained {
		t.Fatalf("expected logical_file_id=%d to be retained by snapshot", logicalRetained)
	}

	unretained, err := IsLogicalFileReferencedBySnapshot(ctx, dbconn, logicalUnretained)
	if err != nil {
		t.Fatalf("IsLogicalFileReferencedBySnapshot(unretained): %v", err)
	}
	if unretained {
		t.Fatalf("expected logical_file_id=%d to not be retained by snapshot", logicalUnretained)
	}
}

func TestIsLogicalFileReferencedBySnapshotWithoutSnapshotTable(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	retained, err := IsLogicalFileReferencedBySnapshot(context.Background(), dbconn, 1)
	if err != nil {
		t.Fatalf("IsLogicalFileReferencedBySnapshot without snapshot table: %v", err)
	}
	if retained {
		t.Fatalf("expected retained=false when snapshot table is missing")
	}
}

func TestComputeReachabilitySummary(t *testing.T) {
	dbconn := openTestDB(t)
	ctx := context.Background()

	logicalCurrentOnly := insertLogical(t, dbconn, "rsum-current-only")
	logicalSnapshotOnly := insertLogical(t, dbconn, "rsum-snapshot-only")
	logicalShared := insertLogical(t, dbconn, "rsum-shared")
	logicalOrphaned := insertLogical(t, dbconn, "rsum-orphaned") // referenced by neither

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 1), (?, ?, 1)`,
		"/data/rsum-current-only", logicalCurrentOnly,
		"/data/rsum-shared", logicalShared,
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	insertSnapshot(t, dbconn, "rsum-snap-1")
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id) VALUES (?, ?, ?), (?, ?, ?)`,
		"rsum-snap-1", "/snap/rsum-snapshot-only", logicalSnapshotOnly,
		"rsum-snap-1", "/snap/rsum-shared", logicalShared,
	); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	summary, err := ComputeReachabilitySummary(ctx, dbconn)
	if err != nil {
		t.Fatalf("ComputeReachabilitySummary: %v", err)
	}

	// CurrentLogicalIDs must contain only physical_file-referenced IDs.
	if _, ok := summary.CurrentLogicalIDs[logicalCurrentOnly]; !ok {
		t.Errorf("CurrentLogicalIDs missing logicalCurrentOnly=%d", logicalCurrentOnly)
	}
	if _, ok := summary.CurrentLogicalIDs[logicalShared]; !ok {
		t.Errorf("CurrentLogicalIDs missing logicalShared=%d", logicalShared)
	}
	if _, ok := summary.CurrentLogicalIDs[logicalSnapshotOnly]; ok {
		t.Errorf("CurrentLogicalIDs unexpectedly contains logicalSnapshotOnly=%d", logicalSnapshotOnly)
	}

	// SnapshotLogicalIDs must contain only snapshot_file-referenced IDs.
	if _, ok := summary.SnapshotLogicalIDs[logicalSnapshotOnly]; !ok {
		t.Errorf("SnapshotLogicalIDs missing logicalSnapshotOnly=%d", logicalSnapshotOnly)
	}
	if _, ok := summary.SnapshotLogicalIDs[logicalShared]; !ok {
		t.Errorf("SnapshotLogicalIDs missing logicalShared=%d", logicalShared)
	}
	if _, ok := summary.SnapshotLogicalIDs[logicalCurrentOnly]; ok {
		t.Errorf("SnapshotLogicalIDs unexpectedly contains logicalCurrentOnly=%d", logicalCurrentOnly)
	}

	// RetainedLogicalIDs must be the union of both.
	for _, id := range []int64{logicalCurrentOnly, logicalSnapshotOnly, logicalShared} {
		if _, ok := summary.RetainedLogicalIDs[id]; !ok {
			t.Errorf("RetainedLogicalIDs missing id=%d", id)
		}
	}

	// Truly orphaned logical file must not appear in any set.
	for name, set := range map[string]map[int64]struct{}{
		"CurrentLogicalIDs":  summary.CurrentLogicalIDs,
		"SnapshotLogicalIDs": summary.SnapshotLogicalIDs,
		"RetainedLogicalIDs": summary.RetainedLogicalIDs,
	} {
		if _, ok := set[logicalOrphaned]; ok {
			t.Errorf("%s unexpectedly contains orphaned id=%d", name, logicalOrphaned)
		}
	}
}

func TestSnapshotReferenceCountAndByteHelpers(t *testing.T) {
	dbconn := openTestDB(t)
	ctx := context.Background()

	logicalCurrentOnly := insertLogicalWithSize(t, dbconn, "helper-current-only", 100)
	logicalSnapshotOnly := insertLogicalWithSize(t, dbconn, "helper-snapshot-only", 200)
	logicalShared := insertLogicalWithSize(t, dbconn, "helper-shared", 300)
	insertLogicalWithSize(t, dbconn, "helper-orphaned", 400)

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 1), (?, ?, 1)`,
		"/data/helper-current-only", logicalCurrentOnly,
		"/data/helper-shared", logicalShared,
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	insertSnapshot(t, dbconn, "helper-snap-1")
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id) VALUES (?, ?, ?), (?, ?, ?)`,
		"helper-snap-1", "/snap/helper-snapshot-only", logicalSnapshotOnly,
		"helper-snap-1", "/snap/helper-shared", logicalShared,
	); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	snapshotReferencedCount, err := CountSnapshotReferencedLogicalFiles(ctx, dbconn)
	if err != nil {
		t.Fatalf("CountSnapshotReferencedLogicalFiles: %v", err)
	}
	if snapshotReferencedCount != 2 {
		t.Fatalf("expected snapshot referenced logical count=2, got %d", snapshotReferencedCount)
	}

	snapshotOnlyCount, err := CountSnapshotOnlyLogicalFiles(ctx, dbconn)
	if err != nil {
		t.Fatalf("CountSnapshotOnlyLogicalFiles: %v", err)
	}
	if snapshotOnlyCount != 1 {
		t.Fatalf("expected snapshot-only logical count=1, got %d", snapshotOnlyCount)
	}

	snapshotReferencedBytes, err := SumSnapshotReferencedLogicalBytes(ctx, dbconn)
	if err != nil {
		t.Fatalf("SumSnapshotReferencedLogicalBytes: %v", err)
	}
	if snapshotReferencedBytes != 500 {
		t.Fatalf("expected snapshot referenced bytes=500, got %d", snapshotReferencedBytes)
	}
}

func TestSnapshotHelperQueriesWithoutSnapshotTableReturnZero(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	snapshotReferencedCount, err := CountSnapshotReferencedLogicalFiles(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CountSnapshotReferencedLogicalFiles without snapshot table: %v", err)
	}
	if snapshotReferencedCount != 0 {
		t.Fatalf("expected snapshot referenced count=0 when snapshot table missing, got %d", snapshotReferencedCount)
	}

	snapshotReferencedBytes, err := SumSnapshotReferencedLogicalBytes(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("SumSnapshotReferencedLogicalBytes without snapshot table: %v", err)
	}
	if snapshotReferencedBytes != 0 {
		t.Fatalf("expected snapshot referenced bytes=0 when snapshot table missing, got %d", snapshotReferencedBytes)
	}
}

func TestClassifyRetention(t *testing.T) {
	t.Run("all three buckets populated", func(t *testing.T) {
		currentOnly := int64(1)
		snapshotOnly := int64(2)
		shared := int64(3)

		summary := &ReachabilitySummary{
			CurrentLogicalIDs: map[int64]struct{}{
				currentOnly: {},
				shared:      {},
			},
			SnapshotLogicalIDs: map[int64]struct{}{
				snapshotOnly: {},
				shared:       {},
			},
			RetainedLogicalIDs: map[int64]struct{}{
				currentOnly:  {},
				snapshotOnly: {},
				shared:       {},
			},
		}

		c := ClassifyRetention(summary)

		if !reflect.DeepEqual(c.CurrentOnly, map[int64]struct{}{currentOnly: {}}) {
			t.Errorf("CurrentOnly unexpected: %v", c.CurrentOnly)
		}
		if !reflect.DeepEqual(c.SnapshotOnly, map[int64]struct{}{snapshotOnly: {}}) {
			t.Errorf("SnapshotOnly unexpected: %v", c.SnapshotOnly)
		}
		if !reflect.DeepEqual(c.Shared, map[int64]struct{}{shared: {}}) {
			t.Errorf("Shared unexpected: %v", c.Shared)
		}
	})

	t.Run("no snapshots — all current-only", func(t *testing.T) {
		summary := &ReachabilitySummary{
			CurrentLogicalIDs:  map[int64]struct{}{1: {}, 2: {}},
			SnapshotLogicalIDs: map[int64]struct{}{},
			RetainedLogicalIDs: map[int64]struct{}{1: {}, 2: {}},
		}

		c := ClassifyRetention(summary)

		if len(c.CurrentOnly) != 2 {
			t.Errorf("expected 2 CurrentOnly, got %d", len(c.CurrentOnly))
		}
		if len(c.SnapshotOnly) != 0 {
			t.Errorf("expected 0 SnapshotOnly, got %d", len(c.SnapshotOnly))
		}
		if len(c.Shared) != 0 {
			t.Errorf("expected 0 Shared, got %d", len(c.Shared))
		}
	})

	t.Run("no current state — all snapshot-only", func(t *testing.T) {
		summary := &ReachabilitySummary{
			CurrentLogicalIDs:  map[int64]struct{}{},
			SnapshotLogicalIDs: map[int64]struct{}{10: {}, 20: {}},
			RetainedLogicalIDs: map[int64]struct{}{10: {}, 20: {}},
		}

		c := ClassifyRetention(summary)

		if len(c.CurrentOnly) != 0 {
			t.Errorf("expected 0 CurrentOnly, got %d", len(c.CurrentOnly))
		}
		if !reflect.DeepEqual(c.SnapshotOnly, map[int64]struct{}{10: {}, 20: {}}) {
			t.Errorf("SnapshotOnly unexpected: %v", c.SnapshotOnly)
		}
		if len(c.Shared) != 0 {
			t.Errorf("expected 0 Shared, got %d", len(c.Shared))
		}
	})

	t.Run("empty summary — all buckets empty", func(t *testing.T) {
		summary := &ReachabilitySummary{
			CurrentLogicalIDs:  map[int64]struct{}{},
			SnapshotLogicalIDs: map[int64]struct{}{},
			RetainedLogicalIDs: map[int64]struct{}{},
		}

		c := ClassifyRetention(summary)

		if len(c.CurrentOnly) != 0 || len(c.SnapshotOnly) != 0 || len(c.Shared) != 0 {
			t.Errorf("expected all empty buckets for empty summary")
		}
	})

	t.Run("buckets are mutually exclusive and exhaustive", func(t *testing.T) {
		summary := &ReachabilitySummary{
			CurrentLogicalIDs:  map[int64]struct{}{1: {}, 3: {}},
			SnapshotLogicalIDs: map[int64]struct{}{2: {}, 3: {}},
			RetainedLogicalIDs: map[int64]struct{}{1: {}, 2: {}, 3: {}},
		}

		c := ClassifyRetention(summary)

		all := make(map[int64]int)
		for id := range c.CurrentOnly {
			all[id]++
		}
		for id := range c.SnapshotOnly {
			all[id]++
		}
		for id := range c.Shared {
			all[id]++
		}

		// Every retained ID must appear in exactly one bucket.
		for id := range summary.RetainedLogicalIDs {
			if all[id] != 1 {
				t.Errorf("id=%d appears in %d buckets, expected exactly 1", id, all[id])
			}
		}
		// Total across buckets must equal the retained set.
		total := len(c.CurrentOnly) + len(c.SnapshotOnly) + len(c.Shared)
		if total != len(summary.RetainedLogicalIDs) {
			t.Errorf("bucket total=%d != retained set size=%d", total, len(summary.RetainedLogicalIDs))
		}
	})
}

// TestRetentionByLogicalIDNotPath verifies Case 5: the same logical file
// referenced at different paths across multiple snapshots is counted exactly
// once in every retained set — no double counting, no path dependency.
func TestRetentionByLogicalIDNotPath(t *testing.T) {
	dbconn := openTestDB(t)
	ctx := context.Background()

	logicalID := insertLogical(t, dbconn, "path-independent")

	insertSnapshot(t, dbconn, "snap-path-a")
	insertSnapshot(t, dbconn, "snap-path-b")

	// Same logical file retained under different paths across two snapshots.
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id) VALUES (?, ?, ?), (?, ?, ?)`,
		"snap-path-a", "docs/version1.txt", logicalID,
		"snap-path-b", "archive/version2.txt", logicalID,
	); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		t.Fatalf("ListSnapshotReferencedLogicalFileIDs: %v", err)
	}
	if len(snapshotIDs) != 1 {
		t.Fatalf("expected exactly 1 distinct logical ID (no double counting), got %d: %v", len(snapshotIDs), snapshotIDs)
	}
	if _, ok := snapshotIDs[logicalID]; !ok {
		t.Fatalf("expected logicalID=%d in snapshot retained set", logicalID)
	}

	allIDs, err := ListAllRetainedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		t.Fatalf("ListAllRetainedLogicalFileIDs: %v", err)
	}
	if len(allIDs) != 1 {
		t.Fatalf("expected exactly 1 retained logical ID in union, got %d: %v", len(allIDs), allIDs)
	}

	summary, err := ComputeReachabilitySummary(ctx, dbconn)
	if err != nil {
		t.Fatalf("ComputeReachabilitySummary: %v", err)
	}
	if len(summary.SnapshotLogicalIDs) != 1 {
		t.Fatalf("expected SnapshotLogicalIDs size=1, got %d", len(summary.SnapshotLogicalIDs))
	}
	if len(summary.RetainedLogicalIDs) != 1 {
		t.Fatalf("expected RetainedLogicalIDs size=1, got %d", len(summary.RetainedLogicalIDs))
	}
}

// TestDanglingSnapshotRefIsConservativelyRetained verifies Case 6: a
// snapshot_file row whose logical_file_id no longer exists in logical_file
// is included in SnapshotLogicalIDs by the reachability helper. The phantom ID
// is inert — no live file_chunk rows exist for it — so GC is unaffected.
// The correct detection path for this condition is verify, not GC.
// This test documents that the retention layer behaves conservatively (phantom
// IDs are retained rather than silently dropped) rather than masking bad state.
func TestDanglingSnapshotRefIsConservativelyRetained(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`
CREATE TABLE IF NOT EXISTS snapshot (id TEXT PRIMARY KEY, created_at TIMESTAMP, type TEXT);
CREATE TABLE IF NOT EXISTS snapshot_file (id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id TEXT, path TEXT, logical_file_id INTEGER);
`); err != nil {
		t.Fatalf("create minimal tables: %v", err)
	}

	// Insert a snapshot_file row pointing to a nonexistent logical_file_id.
	phantomLogicalID := int64(9999)
	if _, err := dbconn.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id) VALUES (?, ?, ?)`,
		"snap-dangling", "docs/ghost.txt", phantomLogicalID,
	); err != nil {
		t.Fatalf("insert dangling snapshot_file row: %v", err)
	}

	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("ListSnapshotReferencedLogicalFileIDs with dangling ref: %v", err)
	}
	// Conservative: the phantom ID is included, not silently dropped.
	if _, ok := snapshotIDs[phantomLogicalID]; !ok {
		t.Fatalf("expected phantom logical_file_id=%d to be conservatively retained, got snapshotIDs=%v", phantomLogicalID, snapshotIDs)
	}
}
