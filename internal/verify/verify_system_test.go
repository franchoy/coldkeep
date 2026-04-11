package verify

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func openVerifyTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestVerifySystemStandardPassesOnConsistentPhysicalGraph(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"healthy.bin", int64(0), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(2),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0), ($3, $2, 0)`,
		"/healthy/a", logicalID, "/healthy/b",
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	if err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir()); err != nil {
		t.Fatalf("verify standard should pass on consistent physical graph: %v", err)
	}
}

func TestVerifySystemStandardDetectsOrphanPhysicalFileRows(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`, "/orphan/path", int64(999)); err != nil {
		t.Fatalf("insert orphan physical_file row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "orphan physical_file rows=1") {
		t.Fatalf("expected orphan physical_file verification error, got: %v", err)
	}
}

func TestVerifySystemStandardDetectsLogicalRefCountMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"mismatch.bin", int64(0), strings.Repeat("b", 64), filestate.LogicalFileCompleted, int64(5),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`, "/mismatch/path", logicalID); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "logical ref_count mismatches=1") {
		t.Fatalf("expected logical ref_count mismatch verification error, got: %v", err)
	}
}

func TestVerifySystemStandardDetectsNegativeLogicalRefCount(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable sqlite check constraints: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count) VALUES ($1, $2, $3, $4, $5)`,
		"negative.bin", int64(0), strings.Repeat("c", 64), filestate.LogicalFileCompleted, int64(-1),
	); err != nil {
		t.Fatalf("insert logical file with negative ref_count: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "negative logical ref_count rows=1") {
		t.Fatalf("expected negative logical ref_count verification error, got: %v", err)
	}
}
