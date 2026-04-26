package graph

import (
	"context"
	"database/sql"
	"testing"

	idb "github.com/franchoy/coldkeep/internal/db"
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

	reachable, err := svc.GetReachableChunks(context.Background(), []int64{1})
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

	reachable, err := svc.GetReachableChunks(context.Background(), []int64{1, 2})
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
