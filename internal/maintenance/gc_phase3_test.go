package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	_ "github.com/mattn/go-sqlite3"
)

// openGCPhase3TestDB returns an in-memory SQLite connection with the full
// schema applied, suitable for testing GC query behavior.
func openGCPhase3TestDB(t *testing.T) *sql.DB {
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

// insertGCPhase3Container inserts a container row with the given flags into the
// in-memory test database and fatals on error. Extracted to keep
// TestGCSealedContainerQueryExcludesSealingTrue within cyclomatic complexity limits.
func insertGCPhase3Container(t *testing.T, dbconn *sql.DB, filename string, maxSize int64, sealed, sealing, quarantine bool) {
	t.Helper()
	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		 VALUES (?, 1024, ?, ?, ?, ?)`,
		filename, maxSize, sealed, sealing, quarantine,
	); err != nil {
		t.Fatalf("insert container %q: %v", filename, err)
	}
}

// TestGCSealedContainerQueryExcludesSealingTrue verifies that the sealed
// container scan query (G4) excludes containers where sealing=TRUE.
// Containers being sealed are mid-write and must not be scanned for GC.
func TestGCSealedContainerQueryExcludesSealingTrue(t *testing.T) {
	dbconn := openGCPhase3TestDB(t)

	maxSize := int64(64 * 1024 * 1024)

	// Container 1: sealed=TRUE, sealing=FALSE, quarantine=FALSE — eligible for GC scan
	insertGCPhase3Container(t, dbconn, "eligible.ck", maxSize, true, false, false)
	// Container 2: sealed=TRUE, sealing=TRUE, quarantine=FALSE — must be excluded (mid-seal)
	insertGCPhase3Container(t, dbconn, "mid-seal.ck", maxSize, true, true, false)
	// Container 3: sealed=FALSE — excluded by sealed=TRUE clause
	insertGCPhase3Container(t, dbconn, "active.ck", maxSize, false, false, false)
	// Container 4: quarantine=TRUE — excluded
	insertGCPhase3Container(t, dbconn, "quarantined.ck", maxSize, true, false, true)

	ctx, cancel := context.Background(), func() {}
	_ = cancel

	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename
		FROM container WHERE quarantine = FALSE AND sealed = TRUE AND sealing = FALSE
		ORDER BY id ASC
	`)
	if err != nil {
		t.Fatalf("query sealed containers: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var filenames []string
	for rows.Next() {
		var id int64
		var filename string
		if err := rows.Scan(&id, &filename); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		filenames = append(filenames, filename)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	if len(filenames) != 1 || filenames[0] != "eligible.ck" {
		t.Fatalf("expected only [eligible.ck] from sealed scan, got: %v", filenames)
	}
}

// TestIsContainerFKViolationRecognizesPostgreSQLError verifies that
// isContainerFKViolation correctly identifies PostgreSQL FK error messages.
func TestIsContainerFKViolationRecognizesPostgreSQLError(t *testing.T) {
	pgErr := errors.New("ERROR: update or delete on table \"container\" violates foreign key constraint \"blocks_container_id_fkey\" on table \"blocks\"")
	if !isContainerFKViolation(pgErr) {
		t.Fatalf("expected PostgreSQL FK error to be recognized, err=%v", pgErr)
	}
}

// TestIsContainerFKViolationRecognizesSQLiteError verifies that
// isContainerFKViolation correctly identifies SQLite FK error messages.
func TestIsContainerFKViolationRecognizesSQLiteError(t *testing.T) {
	sqliteErr := errors.New("FOREIGN KEY constraint failed")
	if !isContainerFKViolation(sqliteErr) {
		t.Fatalf("expected SQLite FK error to be recognized, err=%v", sqliteErr)
	}
}

// TestIsContainerFKViolationIgnoresUnrelatedErrors verifies that
// isContainerFKViolation does not misclassify unrelated errors.
func TestIsContainerFKViolationIgnoresUnrelatedErrors(t *testing.T) {
	unrelated := errors.New("connection refused")
	if isContainerFKViolation(unrelated) {
		t.Fatalf("expected unrelated error not to be classified as FK violation")
	}
	if isContainerFKViolation(nil) {
		t.Fatalf("expected nil not to be classified as FK violation")
	}
}

// TestGCFKViolationWrapsAsInvariantError verifies that the CodeGCFKViolation
// invariant code is correctly wrapped and extractable.
func TestGCFKViolationWrapsAsInvariantError(t *testing.T) {
	cause := errors.New("FOREIGN KEY constraint failed")
	wrapped := invariants.New(
		invariants.CodeGCFKViolation,
		fmt.Sprintf("GC: FK violation deleting container id=%d — container still has live refs; run verify to diagnose", 42),
		cause,
	)

	code, ok := invariants.Code(wrapped)
	if !ok {
		t.Fatalf("expected invariants.Code to extract code from wrapped error")
	}
	if code != invariants.CodeGCFKViolation {
		t.Fatalf("expected code=%s, got=%s", invariants.CodeGCFKViolation, code)
	}

	action := invariants.RecommendedActionForCode(code)
	if action == "" {
		t.Fatalf("expected non-empty recommended action for CodeGCFKViolation")
	}
}
