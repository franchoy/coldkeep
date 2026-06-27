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
	dbconn := openMigratingSQLiteDB(t, dbPath)
	logicalID, storedPath := seedRemoveMigrationFixture(t, dbconn)
	removeStoredPathToZero(t, dbconn, storedPath)
	assertRemoveMigrationZeroState(t, dbconn, logicalID, "before close")
	reopened := reopenMigratingSQLiteDB(t, dbconn, dbPath)
	assertRemoveMigrationZeroState(t, reopened, logicalID, "after reopen")
	assertRemovedStoredPathAbsent(t, reopened, storedPath)
}

func openMigratingSQLiteDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func seedRemoveMigrationFixture(t *testing.T, dbconn *sql.DB) (int64, string) {
	t.Helper()
	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES (?, ?, ?, ?, ?, ?) RETURNING id`,
		"removed.bin", int64(4), "remove-migration-hash", filestate.LogicalFileCompleted, int64(1), "v1-simple-rolling",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	storedPath := filepath.Join(t.TempDir(), "removed.txt")
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 0)`, storedPath, logicalID); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}
	return logicalID, storedPath
}

func removeStoredPathToZero(t *testing.T, dbconn *sql.DB, storedPath string) {
	t.Helper()
	result, err := RemoveFileByStoredPathWithStorageContextResult(StorageContext{DB: dbconn}, storedPath)
	if err != nil {
		t.Fatalf("remove by stored path: %v", err)
	}
	if result.RemainingRefCount != 0 {
		t.Fatalf("expected remaining_ref_count=0, got %d", result.RemainingRefCount)
	}
}

func reopenMigratingSQLiteDB(t *testing.T, dbconn *sql.DB, dbPath string) *sql.DB {
	t.Helper()
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}
	reopened, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := db.RunMigrations(reopened); err != nil {
		t.Fatalf("run migrations after reopen: %v", err)
	}
	return reopened
}

func assertRemoveMigrationZeroState(t *testing.T, dbconn *sql.DB, logicalID int64, phase string) {
	t.Helper()
	var logicalExists int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = ?`, logicalID).Scan(&logicalExists); err != nil {
		t.Fatalf("check logical file existence %s: %v", phase, err)
	}
	if logicalExists != 1 {
		t.Fatalf("expected logical file to remain %s, got count=%d", phase, logicalExists)
	}
	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = ?`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read logical ref_count %s: %v", phase, err)
	}
	if refCount != 0 {
		t.Fatalf("expected logical ref_count=0 %s, got %d", phase, refCount)
	}
	var physicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = ?`, logicalID).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical mappings %s: %v", phase, err)
	}
	if physicalCount != 0 {
		t.Fatalf("expected zero physical mappings %s, got %d", phase, physicalCount)
	}
	var migratedCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = ? AND path LIKE '/migrated/%'`, logicalID).Scan(&migratedCount); err != nil {
		t.Fatalf("count migrated mappings %s: %v", phase, err)
	}
	if migratedCount != 0 {
		t.Fatalf("expected no migrated mappings %s, got %d", phase, migratedCount)
	}
}

func assertRemovedStoredPathAbsent(t *testing.T, dbconn *sql.DB, storedPath string) {
	t.Helper()
	var removedPathCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path = ?`, storedPath).Scan(&removedPathCount); err != nil {
		t.Fatalf("count removed path after reopen: %v", err)
	}
	if removedPathCount != 0 {
		t.Fatalf("expected removed stored path to remain absent after reopen, got count=%d", removedPathCount)
	}
}
