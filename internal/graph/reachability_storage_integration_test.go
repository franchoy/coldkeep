package graph_test

import (
	"context"
	"database/sql"
	"testing"

	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/graph"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	_ "github.com/mattn/go-sqlite3"
)

type storedPathGraphFixture struct {
	logicalID int64
	chunkID   int64
	paths     []string
}

func TestStoredPathUnlinkUpdatesCurrentRootsWithoutDeletingLogicalGraph(t *testing.T) {
	dbconn := openStoredPathGraphTestDB(t)
	svc := graph.NewService(dbconn)
	fixture := seedStoredPathGraphFixture(t, dbconn)
	assertCurrentRootIDs(t, svc, fixture.logicalID)
	removeStoredPathGraphMapping(t, dbconn, fixture.paths[0], "remove first stored path")
	assertCurrentRootIDs(t, svc, fixture.logicalID)
	removeStoredPathGraphMapping(t, dbconn, fixture.paths[1], "remove final stored path")
	assertCurrentRootIDs(t, svc)
	assertLogicalChunkGraphPreserved(t, dbconn, fixture.logicalID)
}

func openStoredPathGraphTestDB(t *testing.T) *sql.DB {
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

func seedStoredPathGraphFixture(t *testing.T, dbconn *sql.DB) storedPathGraphFixture {
	t.Helper()
	var fixture storedPathGraphFixture
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"roots-current.txt", int64(32), "roots-current-hash", filestate.LogicalFileCompleted, int64(2), "v2-fastcdc",
	).Scan(&fixture.logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"roots-current-chunk", int64(32), filestate.ChunkCompleted, int64(1), int64(0), "v2-fastcdc",
	).Scan(&fixture.chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, fixture.logicalID, fixture.chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
	fixture.paths = []string{"/roots/current-a.txt", "/roots/current-b.txt"}
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $2), ($3, $2)`, fixture.paths[0], fixture.logicalID, fixture.paths[1]); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}
	return fixture
}

func removeStoredPathGraphMapping(t *testing.T, dbconn *sql.DB, path, operation string) {
	t.Helper()
	if _, err := storage.RemoveFileByStoredPathWithStorageContextResult(storage.StorageContext{DB: dbconn}, path); err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
}

func assertCurrentRootIDs(t *testing.T, svc *graph.Service, wantIDs ...int64) {
	t.Helper()
	roots, err := svc.CurrentLogicalFileRoots(context.Background())
	if err != nil {
		t.Fatalf("CurrentLogicalFileRoots: %v", err)
	}
	if len(roots) != len(wantIDs) {
		t.Fatalf("unexpected current roots: got %#v wantIDs=%v", roots, wantIDs)
	}
	for i, wantID := range wantIDs {
		if roots[i].ID != wantID {
			t.Fatalf("unexpected current roots: got %#v wantIDs=%v", roots, wantIDs)
		}
	}
}

func assertLogicalChunkGraphPreserved(t *testing.T, dbconn *sql.DB, logicalID int64) {
	t.Helper()
	var logicalCount, fileChunkCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, logicalID).Scan(&fileChunkCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if logicalCount != 1 || fileChunkCount != 1 {
		t.Fatalf("expected logical/file_chunk graph to remain after final unlink, got logical=%d file_chunk=%d", logicalCount, fileChunkCount)
	}
}
