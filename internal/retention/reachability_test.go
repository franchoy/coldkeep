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
