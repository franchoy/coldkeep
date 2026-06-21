package graph

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	idb "github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	_ "github.com/mattn/go-sqlite3"
)

func openGraphTestDB(t *testing.T) *sql.DB {
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

func TestGetReachableChunksFromSnapshots(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, "1", "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "docs/a.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}

	lfRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-hash", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	logicalFileID, err := lfRes.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", logicalFileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	chunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-a", 40, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk a: %v", err)
	}
	chunkAID, err := chunkARes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk a last insert id: %v", err)
	}

	chunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-b", 60, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk b: %v", err)
	}
	chunkBID, err := chunkBRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk b last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, logicalFileID, chunkAID, 0, logicalFileID, chunkBID, 1); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	reachable, err := svc.GetReachableChunks(context.Background(), []string{"1"})
	if err != nil {
		t.Fatalf("GetReachableChunks: %v", err)
	}

	if len(reachable) != 2 {
		t.Fatalf("expected 2 reachable chunks, got %d", len(reachable))
	}
	if _, ok := reachable[chunkAID]; !ok {
		t.Fatalf("expected chunk %d to be reachable", chunkAID)
	}
	if _, ok := reachable[chunkBID]; !ok {
		t.Fatalf("expected chunk %d to be reachable", chunkBID)
	}
}

func TestCurrentLogicalFileRoots(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-root-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	lfAID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "b.txt", 200, "lf-root-b", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	lfBID, _ := lfBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?), (?, ?)`, "/a.txt", lfAID, "/b.txt", lfBID); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	roots, err := svc.CurrentLogicalFileRoots(context.Background())
	if err != nil {
		t.Fatalf("CurrentLogicalFileRoots: %v", err)
	}
	if len(roots) != 2 {
		t.Fatalf("expected 2 roots, got %d", len(roots))
	}
	if roots[0].Type != EntityLogicalFile || roots[1].Type != EntityLogicalFile {
		t.Fatalf("expected logical_file roots, got %#v", roots)
	}
}

func TestStoredPathUnlinkUpdatesCurrentRootsWithoutDeletingLogicalGraph(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"roots-current.txt",
		int64(32),
		"roots-current-hash",
		filestate.LogicalFileCompleted,
		int64(2),
		"v2-fastcdc",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"roots-current-chunk",
		int64(32),
		filestate.ChunkCompleted,
		int64(1),
		int64(0),
		"v2-fastcdc",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	pathA := "/roots/current-a.txt"
	pathB := "/roots/current-b.txt"
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $2), ($3, $2)`, pathA, logicalID, pathB); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	before, err := svc.CurrentLogicalFileRoots(context.Background())
	if err != nil {
		t.Fatalf("CurrentLogicalFileRoots before remove: %v", err)
	}
	if len(before) != 1 || before[0].ID != logicalID {
		t.Fatalf("unexpected roots before remove: %#v", before)
	}

	if _, err := storage.RemoveFileByStoredPathWithStorageContextResult(storage.StorageContext{DB: dbconn}, pathA); err != nil {
		t.Fatalf("remove first stored path: %v", err)
	}
	afterFirst, err := svc.CurrentLogicalFileRoots(context.Background())
	if err != nil {
		t.Fatalf("CurrentLogicalFileRoots after first remove: %v", err)
	}
	if len(afterFirst) != 1 || afterFirst[0].ID != logicalID {
		t.Fatalf("logical file should remain a current root after one-of-many unlink: %#v", afterFirst)
	}

	if _, err := storage.RemoveFileByStoredPathWithStorageContextResult(storage.StorageContext{DB: dbconn}, pathB); err != nil {
		t.Fatalf("remove final stored path: %v", err)
	}
	afterSecond, err := svc.CurrentLogicalFileRoots(context.Background())
	if err != nil {
		t.Fatalf("CurrentLogicalFileRoots after final remove: %v", err)
	}
	if len(afterSecond) != 0 {
		t.Fatalf("expected no current roots after final unlink, got %#v", afterSecond)
	}

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

func TestSnapshotRetainedLogicalFileRemainsReachableWithoutCurrentMappings(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	var logicalID, chunkID, pathID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"snapshot-only.txt",
		int64(48),
		"snapshot-only-hash",
		filestate.LogicalFileCompleted,
		int64(0),
		"v2-fastcdc",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"snapshot-only-chunk",
		int64(48),
		filestate.ChunkCompleted,
		int64(1),
		int64(0),
		"v2-fastcdc",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ($1, CURRENT_TIMESTAMP, $2)`, "snap-roots", "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if err := dbconn.QueryRow(`INSERT INTO snapshot_path (path) VALUES ($1) RETURNING id`, "/snapshots/only.txt").Scan(&pathID); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)`, "snap-roots", pathID, logicalID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	currentRoots, err := svc.CurrentLogicalFileRoots(context.Background())
	if err != nil {
		t.Fatalf("CurrentLogicalFileRoots: %v", err)
	}
	if len(currentRoots) != 0 {
		t.Fatalf("expected no current roots, got %#v", currentRoots)
	}
	snapshotRoots, err := svc.SnapshotRoots(context.Background(), nil)
	if err != nil {
		t.Fatalf("SnapshotRoots: %v", err)
	}
	if len(snapshotRoots) != 1 || snapshotRoots[0].ID != logicalID {
		t.Fatalf("expected snapshot root for logical file, got %#v", snapshotRoots)
	}
	roots, err := svc.GCRoots(context.Background(), GCRootOptions{})
	if err != nil {
		t.Fatalf("GCRoots: %v", err)
	}
	if len(roots) != 1 || roots[0].ID != logicalID {
		t.Fatalf("expected retained logical file in GC roots, got %#v", roots)
	}
	reachable, err := svc.ReachableChunksFromRoots(context.Background(), roots)
	if err != nil {
		t.Fatalf("ReachableChunksFromRoots: %v", err)
	}
	if _, ok := reachable[chunkID]; !ok {
		t.Fatalf("expected chunk %d to remain reachable from snapshot roots", chunkID)
	}
}

func TestZeroReferenceLogicalFileWithoutSnapshotIsNotCurrentOrSnapshotRoot(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	var logicalID, chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"zero-unretained.txt",
		int64(24),
		"zero-unretained-hash",
		filestate.LogicalFileCompleted,
		int64(0),
		"v2-fastcdc",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"zero-unretained-chunk",
		int64(24),
		filestate.ChunkCompleted,
		int64(1),
		int64(0),
		"v2-fastcdc",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	currentRoots, err := svc.CurrentLogicalFileRoots(context.Background())
	if err != nil {
		t.Fatalf("CurrentLogicalFileRoots: %v", err)
	}
	if len(currentRoots) != 0 {
		t.Fatalf("expected no current roots, got %#v", currentRoots)
	}
	snapshotRoots, err := svc.SnapshotRoots(context.Background(), nil)
	if err != nil {
		t.Fatalf("SnapshotRoots: %v", err)
	}
	if len(snapshotRoots) != 0 {
		t.Fatalf("expected no snapshot roots, got %#v", snapshotRoots)
	}
	roots, err := svc.GCRoots(context.Background(), GCRootOptions{})
	if err != nil {
		t.Fatalf("GCRoots: %v", err)
	}
	if len(roots) != 0 {
		t.Fatalf("expected no GC roots, got %#v", roots)
	}
	reachable, err := svc.ReachableChunksFromRoots(context.Background(), roots)
	if err != nil {
		t.Fatalf("ReachableChunksFromRoots: %v", err)
	}
	if len(reachable) != 0 {
		t.Fatalf("expected no reachable chunks without current or snapshot roots, got %#v", reachable)
	}
}

func TestSnapshotRootsExcludeSnapshotIDs(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?), (?, CURRENT_TIMESTAMP, ?)`, "snap-1", "full", "snap-2", "full"); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?), (?)`, "docs/a.txt", "docs/b.txt"); err != nil {
		t.Fatalf("insert snapshot paths: %v", err)
	}

	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-snap-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	lfAID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "b.txt", 100, "lf-snap-b", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	lfBID, _ := lfBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "snap-1", "docs/a.txt", lfAID, "snap-2", "docs/b.txt", lfBID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	roots, err := svc.SnapshotRoots(context.Background(), []string{"snap-1"})
	if err != nil {
		t.Fatalf("SnapshotRoots: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("expected 1 root after exclusion, got %d", len(roots))
	}
	if roots[0].Type != EntityLogicalFile || roots[0].ID != lfBID {
		t.Fatalf("unexpected roots after exclusion: %#v", roots)
	}
}

func TestReachableChunksFromRoots(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	lfRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "reachable.txt", 64, "lf-reach", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	lfID, _ := lfRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-reach", 64, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, lfID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	reachable, err := svc.ReachableChunksFromRoots(context.Background(), []NodeID{{Type: EntityLogicalFile, ID: lfID}})
	if err != nil {
		t.Fatalf("ReachableChunksFromRoots: %v", err)
	}
	if len(reachable) != 1 {
		t.Fatalf("expected 1 reachable chunk, got %d", len(reachable))
	}
	if _, ok := reachable[chunkID]; !ok {
		t.Fatalf("expected chunk %d to be reachable", chunkID)
	}
}

func TestGCRootsMergesCurrentAndSnapshotRoots(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	lfCurrentRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "current.txt", 64, "lf-gc-current", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert current logical_file: %v", err)
	}
	lfCurrentID, _ := lfCurrentRes.LastInsertId()
	lfSnapshotRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "snapshot.txt", 64, "lf-gc-snap", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert snapshot logical_file: %v", err)
	}
	lfSnapshotID, _ := lfSnapshotRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?)`, "/current.txt", lfCurrentID); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, "snap-1", "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "/snapshot.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "snap-1", "/snapshot.txt", lfSnapshotID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	roots, err := svc.GCRoots(context.Background(), GCRootOptions{})
	if err != nil {
		t.Fatalf("GCRoots: %v", err)
	}
	if len(roots) != 2 {
		t.Fatalf("expected 2 merged roots, got %d", len(roots))
	}
}

func TestGetReachableChunksDeduplicatesSharedChunks(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?), (?, CURRENT_TIMESTAMP, ?)`, "1", "full", "2", "full"); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?), (?)`, "docs/a.txt", "docs/b.txt"); err != nil {
		t.Fatalf("insert snapshot_path rows: %v", err)
	}

	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	lfAID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "b.txt", 100, "lf-b", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	lfBID, _ := lfBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", lfAID, "2", "docs/b.txt", lfBID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "shared-chunk", 50, "COMPLETED", 2, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert shared chunk: %v", err)
	}
	sharedChunkID, _ := chunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, lfAID, sharedChunkID, 0, lfBID, sharedChunkID, 0); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	reachable, err := svc.GetReachableChunks(context.Background(), []string{"1", "2"})
	if err != nil {
		t.Fatalf("GetReachableChunks: %v", err)
	}

	if len(reachable) != 1 {
		t.Fatalf("expected 1 deduplicated reachable chunk, got %d", len(reachable))
	}
	if _, ok := reachable[sharedChunkID]; !ok {
		t.Fatalf("expected shared chunk %d to be reachable", sharedChunkID)
	}
}

func TestGetReachableChunks(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?), (?, CURRENT_TIMESTAMP, ?)`, "1", "full", "2", "full"); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?), (?)`, "docs/a.txt", "docs/b.txt"); err != nil {
		t.Fatalf("insert snapshot_path rows: %v", err)
	}

	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	lfAID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "b.txt", 100, "lf-b", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	lfBID, _ := lfBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", lfAID, "2", "docs/b.txt", lfBID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	chunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-a", 40, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk a: %v", err)
	}
	chunkAID, _ := chunkARes.LastInsertId()
	chunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-b", 50, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk b: %v", err)
	}
	chunkBID, _ := chunkBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, lfAID, chunkAID, 0, lfBID, chunkBID, 0); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	allReachable, err := svc.GetReachableChunks(context.Background(), []string{"1", "2"})
	if err != nil {
		t.Fatalf("GetReachableChunks (before delete): %v", err)
	}
	if len(allReachable) != 2 {
		t.Fatalf("expected 2 reachable chunks before delete, got %d", len(allReachable))
	}

	if _, err := dbconn.Exec(`DELETE FROM snapshot_file WHERE snapshot_id = ?`, "2"); err != nil {
		t.Fatalf("delete snapshot_file rows: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM snapshot WHERE id = ?`, "2"); err != nil {
		t.Fatalf("delete snapshot row: %v", err)
	}

	reachableAfterDelete, err := svc.GetReachableChunks(context.Background(), []string{"1", "2"})
	if err != nil {
		t.Fatalf("GetReachableChunks (after delete): %v", err)
	}
	if len(reachableAfterDelete) != 1 {
		t.Fatalf("expected 1 reachable chunk after delete, got %d", len(reachableAfterDelete))
	}
	if _, ok := reachableAfterDelete[chunkAID]; !ok {
		t.Fatalf("expected chunk %d to remain reachable", chunkAID)
	}
	if _, ok := reachableAfterDelete[chunkBID]; ok {
		t.Fatalf("expected chunk %d to be unreachable after delete", chunkBID)
	}
}

func TestGetReachableChunksMatchesLegacyAcrossRandomizedFixtures(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for iter := 0; iter < 25; iter++ {
		dbconn := openGraphTestDB(t)
		svc := NewService(dbconn)

		snapshotCount := 2 + rng.Intn(4) // 2..5
		logicalCount := 3 + rng.Intn(6)  // 3..8
		chunkCount := 4 + rng.Intn(8)    // 4..11

		for i := 1; i <= snapshotCount; i++ {
			if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, fmt.Sprintf("%d", i), "full"); err != nil {
				t.Fatalf("iter=%d insert snapshot %d: %v", iter, i, err)
			}
		}

		logicalIDs := make([]int64, 0, logicalCount)
		for i := 0; i < logicalCount; i++ {
			res, err := dbconn.Exec(
				`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
				fmt.Sprintf("f-%d-%d.txt", iter, i),
				100+i,
				fmt.Sprintf("lf-%d-%d", iter, i),
				"COMPLETED",
				"v2-fastcdc",
			)
			if err != nil {
				t.Fatalf("iter=%d insert logical_file %d: %v", iter, i, err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				t.Fatalf("iter=%d logical_file last insert id %d: %v", iter, i, err)
			}
			logicalIDs = append(logicalIDs, id)
		}

		chunkIDs := make([]int64, 0, chunkCount)
		for i := 0; i < chunkCount; i++ {
			res, err := dbconn.Exec(
				`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`,
				fmt.Sprintf("chunk-%d-%d", iter, i),
				16+i,
				"COMPLETED",
				1,
				"v2-fastcdc",
			)
			if err != nil {
				t.Fatalf("iter=%d insert chunk %d: %v", iter, i, err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				t.Fatalf("iter=%d chunk last insert id %d: %v", iter, i, err)
			}
			chunkIDs = append(chunkIDs, id)
		}

		for lfIdx, logicalID := range logicalIDs {
			assignmentCount := 1 + rng.Intn(minInt(4, len(chunkIDs)))
			perm := rng.Perm(len(chunkIDs))
			for order := 0; order < assignmentCount; order++ {
				chunkID := chunkIDs[perm[order]]
				if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, logicalID, chunkID, order); err != nil {
					t.Fatalf("iter=%d insert file_chunk lfIdx=%d order=%d: %v", iter, lfIdx, order, err)
				}
			}
		}

		for snapshotID := 1; snapshotID <= snapshotCount; snapshotID++ {
			if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, fmt.Sprintf("docs/iter-%d-snap-%d", iter, snapshotID)); err != nil {
				t.Fatalf("iter=%d insert snapshot_path for snapshot %d: %v", iter, snapshotID, err)
			}

			assignmentCount := 1 + rng.Intn(minInt(3, len(logicalIDs)))
			perm := rng.Perm(len(logicalIDs))
			for i := 0; i < assignmentCount; i++ {
				logicalID := logicalIDs[perm[i]]
				path := fmt.Sprintf("docs/iter-%d-snap-%d-file-%d", iter, snapshotID, i)
				if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, path); err != nil {
					t.Fatalf("iter=%d insert snapshot_file path snapshot=%d idx=%d: %v", iter, snapshotID, i, err)
				}
				if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, fmt.Sprintf("%d", snapshotID), path, logicalID); err != nil {
					t.Fatalf("iter=%d insert snapshot_file snapshot=%d idx=%d: %v", iter, snapshotID, i, err)
				}
			}
		}

		queryCount := 1 + rng.Intn(snapshotCount)
		perm := rng.Perm(snapshotCount)
		snapshotIDs := make([]string, 0, queryCount)
		for i := 0; i < queryCount; i++ {
			snapshotIDs = append(snapshotIDs, fmt.Sprintf("%d", perm[i]+1))
		}

		graphReachable, err := svc.GetReachableChunks(context.Background(), snapshotIDs)
		if err != nil {
			t.Fatalf("iter=%d graph reachable: %v", iter, err)
		}

		legacyReachable, err := legacyReachableChunksForSnapshots(context.Background(), dbconn, snapshotIDs)
		if err != nil {
			t.Fatalf("iter=%d legacy reachable: %v", iter, err)
		}

		if len(graphReachable) != len(legacyReachable) {
			t.Fatalf("iter=%d mismatch sizes graph=%d legacy=%d snapshotIDs=%v", iter, len(graphReachable), len(legacyReachable), snapshotIDs)
		}
		for id := range legacyReachable {
			if _, ok := graphReachable[id]; !ok {
				t.Fatalf("iter=%d graph missing chunk=%d snapshotIDs=%v", iter, id, snapshotIDs)
			}
		}
		for id := range graphReachable {
			if _, ok := legacyReachable[id]; !ok {
				t.Fatalf("iter=%d graph has extra chunk=%d snapshotIDs=%v", iter, id, snapshotIDs)
			}
		}
	}
}

func legacyReachableChunksForSnapshots(ctx context.Context, dbconn *sql.DB, snapshotIDs []string) (map[int64]struct{}, error) {
	if len(snapshotIDs) == 0 {
		return map[int64]struct{}{}, nil
	}

	placeholders := make([]string, len(snapshotIDs))
	args := make([]any, len(snapshotIDs))
	for i, id := range snapshotIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT fc.chunk_id
		FROM snapshot_file sf
		JOIN file_chunk fc ON fc.logical_file_id = sf.logical_file_id
		WHERE sf.snapshot_id IN (%s)
		ORDER BY fc.chunk_id
	`, strings.Join(placeholders, ","))

	rows, err := dbconn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64]struct{})
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return nil, err
		}
		out[chunkID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
