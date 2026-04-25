package maintenance

import (
	"context"
	"database/sql"
	"testing"
	"time"

	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/tests/testdb"
	_ "github.com/mattn/go-sqlite3"
)

func openStatsTestDB(t *testing.T) *sql.DB {
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

func insertStatsLogicalFile(t *testing.T, dbconn *sql.DB, name string, totalSize int64) int64 {
	t.Helper()
	res, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, 'v1-simple-rolling')`,
		name, totalSize, name+"-hash", "COMPLETED",
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

func TestRunStatsResultIncludesSnapshotRetentionVisibility(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	currentOnlyID := insertStatsLogicalFile(t, dbconn, "current-only", 11)
	snapshotOnlyID := insertStatsLogicalFile(t, dbconn, "snapshot-only", 22)
	sharedID := insertStatsLogicalFile(t, dbconn, "shared", 33)
	insertStatsLogicalFile(t, dbconn, "unreferenced", 44)

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 1), (?, ?, 1)`,
		"/data/current-only", currentOnlyID,
		"/data/shared", sharedID,
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`,
		"snap-stats-retention", time.Now().UTC(), "full",
	); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-stats-retention", "snap/snapshot-only", snapshotOnlyID)
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-stats-retention", "snap/shared", sharedID)

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if stats.SnapshotRetention.CurrentOnlyLogicalFiles != 1 || stats.SnapshotRetention.CurrentOnlyBytes != 11 {
		t.Fatalf("unexpected current-only stats: %+v", stats.SnapshotRetention)
	}
	if stats.SnapshotRetention.SnapshotReferencedLogicalFiles != 2 || stats.SnapshotRetention.SnapshotReferencedBytes != 55 {
		t.Fatalf("unexpected snapshot-referenced stats: %+v", stats.SnapshotRetention)
	}
	if stats.SnapshotRetention.SnapshotOnlyLogicalFiles != 1 || stats.SnapshotRetention.SnapshotOnlyBytes != 22 {
		t.Fatalf("unexpected snapshot-only stats: %+v", stats.SnapshotRetention)
	}
	if stats.SnapshotRetention.SharedLogicalFiles != 1 || stats.SnapshotRetention.SharedBytes != 33 {
		t.Fatalf("unexpected shared stats: %+v", stats.SnapshotRetention)
	}
	if got := stats.SnapshotRetention.CurrentOnlyLogicalFiles + stats.SnapshotRetention.SnapshotOnlyLogicalFiles + stats.SnapshotRetention.SharedLogicalFiles; got != 3 {
		t.Fatalf("unexpected retained logical file total=%d stats=%+v", got, stats.SnapshotRetention)
	}
}

func TestRunStatsResultIncludesChunkCountsByVersion(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES
		 ('stats-v1-a', 10, 'COMPLETED', 0, 'v1-simple-rolling'),
		 ('stats-v1-b', 11, 'PROCESSING', 0, 'v1-simple-rolling'),
		 ('stats-v2-a', 12, 'ABORTED', 0, 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert chunk rows: %v", err)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if stats.ChunkCountsByVersion == nil {
		t.Fatal("expected chunk_counts_by_version map to be initialized")
	}
	if got := stats.ChunkCountsByVersion["v1-simple-rolling"]; got != 2 {
		t.Fatalf("expected v1-simple-rolling count=2, got %d", got)
	}
	if got := stats.ChunkCountsByVersion["v2-fastcdc"]; got != 1 {
		t.Fatalf("expected v2-fastcdc count=1, got %d", got)
	}
}
