package storage

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

func TestRemoveLastStoredPathSurvivesSQLiteMigrationReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "catalog.sqlite")

	dbconn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}

	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations first pass: %v", err)
	}

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES (?, ?, ?, ?, ?, ?) RETURNING id`,
		"removed.bin",
		int64(4),
		"remove-migration-hash",
		filestate.LogicalFileCompleted,
		int64(1),
		"v1-simple-rolling",
	).Scan(&logicalID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert logical file: %v", err)
	}

	storedPath := filepath.Join(t.TempDir(), "removed.txt")
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 0)`, storedPath, logicalID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert physical_file row: %v", err)
	}

	result, err := RemoveFileByStoredPathWithStorageContextResult(StorageContext{DB: dbconn}, storedPath)
	if err != nil {
		_ = dbconn.Close()
		t.Fatalf("remove by stored path: %v", err)
	}
	if result.RemainingRefCount != 0 {
		_ = dbconn.Close()
		t.Fatalf("expected remaining_ref_count=0, got %d", result.RemainingRefCount)
	}

	var logicalExists int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = ?`, logicalID).Scan(&logicalExists); err != nil {
		_ = dbconn.Close()
		t.Fatalf("check logical file existence before close: %v", err)
	}
	if logicalExists != 1 {
		_ = dbconn.Close()
		t.Fatalf("expected logical file to remain before close, got count=%d", logicalExists)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = ?`, logicalID).Scan(&refCount); err != nil {
		_ = dbconn.Close()
		t.Fatalf("read logical ref_count before close: %v", err)
	}
	if refCount != 0 {
		_ = dbconn.Close()
		t.Fatalf("expected logical ref_count=0 before close, got %d", refCount)
	}

	var physicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = ?`, logicalID).Scan(&physicalCount); err != nil {
		_ = dbconn.Close()
		t.Fatalf("count physical mappings before close: %v", err)
	}
	if physicalCount != 0 {
		_ = dbconn.Close()
		t.Fatalf("expected zero physical mappings before close, got %d", physicalCount)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}

	dbconn, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations after reopen: %v", err)
	}

	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = ?`, logicalID).Scan(&logicalExists); err != nil {
		t.Fatalf("check logical file existence after reopen: %v", err)
	}
	if logicalExists != 1 {
		t.Fatalf("expected logical file to remain after reopen, got count=%d", logicalExists)
	}

	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = ?`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read logical ref_count after reopen: %v", err)
	}
	if refCount != 0 {
		t.Fatalf("expected logical ref_count=0 after reopen, got %d", refCount)
	}

	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = ?`, logicalID).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical mappings after reopen: %v", err)
	}
	if physicalCount != 0 {
		t.Fatalf("expected zero physical mappings after reopen, got %d", physicalCount)
	}

	var migratedCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = ? AND path LIKE '/migrated/%'`, logicalID).Scan(&migratedCount); err != nil {
		t.Fatalf("count migrated mappings after reopen: %v", err)
	}
	if migratedCount != 0 {
		t.Fatalf("expected no migrated mappings after reopen, got %d", migratedCount)
	}

	var removedPathCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path = ?`, storedPath).Scan(&removedPathCount); err != nil {
		t.Fatalf("count removed path after reopen: %v", err)
	}
	if removedPathCount != 0 {
		t.Fatalf("expected removed stored path to remain absent after reopen, got count=%d", removedPathCount)
	}
}
