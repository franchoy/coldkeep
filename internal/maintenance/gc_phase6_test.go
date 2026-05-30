package maintenance

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

// openGCPhase6TestDB opens an in-memory SQLite DB with the full schema applied.
func openGCPhase6TestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	return dbconn
}

// TestApplySQLiteSessionPragmasEnablesForeignKeys verifies that
// ApplySQLiteSessionPragmas sets PRAGMA foreign_keys = ON (X1).
func TestApplySQLiteSessionPragmasEnablesForeignKeys(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.ApplySQLiteSessionPragmas(dbconn); err != nil {
		t.Fatalf("ApplySQLiteSessionPragmas: %v", err)
	}

	var fkEnabled int
	if err := dbconn.QueryRow(`PRAGMA foreign_keys;`).Scan(&fkEnabled); err != nil {
		t.Fatalf("query foreign_keys pragma: %v", err)
	}
	if fkEnabled != 1 {
		t.Fatalf("expected foreign_keys = 1 after ApplySQLiteSessionPragmas, got %d", fkEnabled)
	}
}

// TestGCLiveRunFailsClosedOnSQLiteBackend verifies that RunGCWithContainersDirResult
// with dryRun=false returns an error when the DB backend is SQLite (G1).
func TestGCLiveRunFailsClosedOnSQLiteBackend(t *testing.T) {
	sqliteDB := openGCPhase6TestDB(t)

	original := gcConnectDB
	gcConnectDB = func() (*sql.DB, error) { return sqliteDB, nil }
	t.Cleanup(func() { gcConnectDB = original })

	_, err := RunGCWithContainersDirResult(false /* dryRun=false */, t.TempDir())
	if err == nil {
		t.Fatal("expected RunGCWithContainersDirResult to fail-closed on SQLite live GC, got nil")
	}
	if !strings.Contains(err.Error(), "not supported on the SQLite backend") {
		t.Fatalf("expected SQLite backend error, got: %v", err)
	}
}

// TestGCDryRunAllowedOnSQLiteBackend verifies that RunGCWithContainersDirResult
// with dryRun=true does NOT error immediately on the SQLite backend (G1).
// It is expected to proceed past the advisory-lock step and either complete
// or fail later (e.g., during integrity check) — but not fail on the backend check.
func TestGCDryRunAllowedOnSQLiteBackend(t *testing.T) {
	sqliteDB := openGCPhase6TestDB(t)

	original := gcConnectDB
	gcConnectDB = func() (*sql.DB, error) { return sqliteDB, nil }
	t.Cleanup(func() { gcConnectDB = original })

	_, err := RunGCWithContainersDirResult(true /* dryRun=true */, t.TempDir())
	// The dry-run on SQLite must NOT fail with the backend-rejection message.
	if err != nil && strings.Contains(err.Error(), "not supported on the SQLite backend") {
		t.Fatalf("dry-run GC should not be rejected on SQLite backend, got: %v", err)
	}
	// Any other error (e.g., integrity or DB query) is acceptable — we only
	// verify the backend check does not reject the dry-run path.
}
