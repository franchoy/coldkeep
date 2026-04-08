package storage

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func TestRemoveFailsWhenLogicalFileNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	err = RemoveFileWithDB(dbconn, 999)
	if err == nil || !strings.Contains(err.Error(), "file ID 999 not found") {
		t.Fatalf("expected file-not-found error contract, got: %v", err)
	}
}

func TestRemoveFailsWhenFileIsProcessing(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"processing-file.bin", int64(8), strings.Repeat("e", 64), filestate.LogicalFileProcessing,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	err = RemoveFileWithDB(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "is still PROCESSING and cannot be removed") {
		t.Fatalf("expected PROCESSING error contract, got: %v", err)
	}
}

func TestRemoveFailsOnInvalidLiveRefCountTransition(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Insert a chunk whose live_ref_count is already 0 — decrement should fail.
	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, 0) RETURNING id`,
		strings.Repeat("a", 64), int64(4), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"zero-ref-file.bin", int64(4), strings.Repeat("b", 64), filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	err = RemoveFileWithDB(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "invalid live_ref_count transition for chunk") {
		t.Fatalf("expected invalid live_ref_count error contract, got: %v", err)
	}
}

func TestGetLogicalFileInfoWithDBFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"logical-file-info.bin",
		int64(10),
		strings.Repeat("f", 64),
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	info, err := GetLogicalFileInfoWithDB(dbconn, fileID)
	if err != nil {
		t.Fatalf("GetLogicalFileInfoWithDB: %v", err)
	}
	if info.FileID != fileID {
		t.Fatalf("unexpected file id: got=%d want=%d", info.FileID, fileID)
	}
	if info.OriginalName != "logical-file-info.bin" {
		t.Fatalf("unexpected original name: got=%q", info.OriginalName)
	}
	if info.Status != filestate.LogicalFileCompleted {
		t.Fatalf("unexpected status: got=%q want=%q", info.Status, filestate.LogicalFileCompleted)
	}
}

func TestGetLogicalFileInfoWithDBNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	_, err = GetLogicalFileInfoWithDB(dbconn, 123456)
	if err == nil {
		t.Fatal("expected sql.ErrNoRows for missing logical file")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got: %v", err)
	}
}
