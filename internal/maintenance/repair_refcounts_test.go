package maintenance

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

func openMaintenanceSQLiteDB(t *testing.T) *sql.DB {
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

func TestRepairLogicalRefCountsResultWithDBRepairsMismatch(t *testing.T) {
	dbconn := openMaintenanceSQLiteDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"repair.bin", int64(0), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(5),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`,
		"/repair/path", logicalID,
	); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	result, err := RepairLogicalRefCountsResultWithDB(dbconn)
	if err != nil {
		t.Fatalf("repair logical ref_counts: %v", err)
	}
	if result.UpdatedLogicalFiles != 1 {
		t.Fatalf("expected one repaired logical_file row, got %+v", result)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read repaired logical_file.ref_count: %v", err)
	}
	if refCount != 1 {
		t.Fatalf("expected repaired ref_count=1, got %d", refCount)
	}

	if err := verify.VerifySystemStandardWithContainersDir(dbconn, t.TempDir()); err != nil {
		t.Fatalf("verify should pass after explicit ref_count repair: %v", err)
	}
}

func TestRepairLogicalRefCountsResultWithDBNoopOnHealthyState(t *testing.T) {
	dbconn := openMaintenanceSQLiteDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"healthy.bin", int64(0), strings.Repeat("b", 64), filestate.LogicalFileCompleted, int64(2),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0), ($3, $2, 0)`,
		"/healthy/one", logicalID, "/healthy/two",
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	result, err := RepairLogicalRefCountsResultWithDB(dbconn)
	if err != nil {
		t.Fatalf("repair healthy state should not fail: %v", err)
	}
	if result.UpdatedLogicalFiles != 0 || result.OrphanPhysicalFileRows != 0 || result.ScannedLogicalFiles != 1 {
		t.Fatalf("unexpected healthy repair result: %+v", result)
	}
}

func TestRepairLogicalRefCountsResultWithDBRefusesOrphanPhysicalRows(t *testing.T) {
	dbconn := openMaintenanceSQLiteDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`,
		"/orphan/path", int64(999),
	); err != nil {
		t.Fatalf("insert orphan physical_file row: %v", err)
	}

	_, err := RepairLogicalRefCountsResultWithDB(dbconn)
	if err == nil || !strings.Contains(err.Error(), "ref_count repair refused: orphan physical_file rows=1") {
		t.Fatalf("expected orphan physical_file repair refusal, got: %v", err)
	}
}
