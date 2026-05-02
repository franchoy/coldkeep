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

func openMaintenanceTestDB(t *testing.T) *sql.DB {
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

func insertMaintenanceLogicalFile(t *testing.T, dbconn *sql.DB) int64 {
	t.Helper()
	var id int64
	err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		"verify-target.bin", int64(1), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(0), "v1-simple-rolling",
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	return id
}

func TestVerifySystemRejectsInvalidLevel(t *testing.T) {
	dbconn := openMaintenanceTestDB(t)
	defer func() { _ = dbconn.Close() }()

	err := verifySystem(dbconn, t.TempDir(), verify.VerifyLevel(99))
	if err == nil || !strings.Contains(err.Error(), "invalid system verify level") {
		t.Fatalf("expected invalid system level error, got: %v", err)
	}
}

func TestVerifyFileRejectsMissingLogicalFile(t *testing.T) {
	dbconn := openMaintenanceTestDB(t)
	defer func() { _ = dbconn.Close() }()

	err := verifyFile(dbconn, t.TempDir(), 999, verify.VerifyStandard)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected missing file error, got: %v", err)
	}
}

func TestVerifyFileRejectsInvalidLevelAfterExistenceCheck(t *testing.T) {
	dbconn := openMaintenanceTestDB(t)
	defer func() { _ = dbconn.Close() }()

	id := insertMaintenanceLogicalFile(t, dbconn)
	err := verifyFile(dbconn, t.TempDir(), int(id), verify.VerifyLevel(99))
	if err == nil || !strings.Contains(err.Error(), "invalid file verify level") {
		t.Fatalf("expected invalid file level error, got: %v", err)
	}
}
