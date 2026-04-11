package listing

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"

	_ "github.com/mattn/go-sqlite3"
)

// openTestDB returns a non-nil *sql.DB backed by an in-memory SQLite connection.
// The listing package checks db != nil before any query, so we only need a valid
// handle — no schema migrations are required for argument-parsing tests.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSearchFilesFailsWhenDBIsNil(t *testing.T) {
	_, err := SearchFilesResultWithDB(nil, nil)
	if err == nil || !strings.Contains(err.Error(), "db connection is nil") {
		t.Fatalf("expected db-nil error contract, got: %v", err)
	}
}

func TestSearchFilesFailsWhenNameArgIsMissing(t *testing.T) {
	_, err := SearchFilesResultWithDB(openTestDB(t), []string{"--name"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --name") {
		t.Fatalf("expected missing-name error contract, got: %v", err)
	}
}

func TestSearchFilesFailsWhenMinSizeArgIsMissing(t *testing.T) {
	_, err := SearchFilesResultWithDB(openTestDB(t), []string{"--min-size"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --min-size") {
		t.Fatalf("expected missing-min-size error contract, got: %v", err)
	}
}

func TestSearchFilesFailsWhenMaxSizeArgIsMissing(t *testing.T) {
	_, err := SearchFilesResultWithDB(openTestDB(t), []string{"--max-size"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --max-size") {
		t.Fatalf("expected missing-max-size error contract, got: %v", err)
	}
}

func TestSearchFilesFailsWhenLimitArgIsMissing(t *testing.T) {
	_, err := SearchFilesResultWithDB(openTestDB(t), []string{"--limit"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --limit") {
		t.Fatalf("expected missing-limit error contract, got: %v", err)
	}
}

func TestSearchFilesFailsWhenOffsetArgIsMissing(t *testing.T) {
	_, err := SearchFilesResultWithDB(openTestDB(t), []string{"--offset"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --offset") {
		t.Fatalf("expected missing-offset error contract, got: %v", err)
	}
}

func TestSearchFilesFailsWhenMinSizeArgIsInvalid(t *testing.T) {
	_, err := SearchFilesResultWithDB(openTestDB(t), []string{"--min-size", "-1"})
	if err == nil || !strings.Contains(err.Error(), "invalid --min-size value") {
		t.Fatalf("expected invalid-min-size error contract, got: %v", err)
	}
}

func TestSearchFilesFailsWhenMaxSizeArgIsInvalid(t *testing.T) {
	_, err := SearchFilesResultWithDB(openTestDB(t), []string{"--max-size", "not-a-number"})
	if err == nil || !strings.Contains(err.Error(), "invalid --max-size value") {
		t.Fatalf("expected invalid-max-size error contract, got: %v", err)
	}
}

func TestSearchFilesUsesCurrentStatePhysicalPaths(t *testing.T) {
	dbconn := openTestDB(t)
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	insertLogical := func(name, hash string, size int64, status string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
			 VALUES ($1, $2, $3, $4, $5)
			 RETURNING id`,
			name,
			size,
			hash,
			status,
			0,
		).Scan(&id); err != nil {
			t.Fatalf("insert logical_file %q: %v", name, err)
		}
		return id
	}

	idReport := insertLogical("report-logical", "hash-report", 120, filestate.LogicalFileCompleted)
	idOther := insertLogical("other-logical", "hash-other", 42, filestate.LogicalFileCompleted)
	_ = insertLogical("aborted-logical", "hash-aborted", 90, filestate.LogicalFileAborted)

	seed := []struct {
		path string
		id   int64
	}{
		{path: "/home/u/docs/monthly-report.txt", id: idReport},
		{path: "/tmp/archive/report-copy.txt", id: idReport},
		{path: "/home/u/docs/notes.txt", id: idOther},
	}
	for _, row := range seed {
		if _, err := dbconn.Exec(
			`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
			row.path,
			row.id,
			1,
		); err != nil {
			t.Fatalf("insert physical_file %q: %v", row.path, err)
		}
	}

	records, err := SearchFilesResultWithDB(dbconn, []string{"--name", "report"})
	if err != nil {
		t.Fatalf("search by name/path term: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 report path matches, got %d", len(records))
	}
	for _, rec := range records {
		if !strings.Contains(strings.ToLower(rec.Name), "report") {
			t.Fatalf("expected record path to include report, got %q", rec.Name)
		}
		if rec.ID != idReport {
			t.Fatalf("expected report logical ID %d, got %d", idReport, rec.ID)
		}
	}

	bySize, err := SearchFilesResultWithDB(dbconn, []string{"--min-size", "100"})
	if err != nil {
		t.Fatalf("search by min-size: %v", err)
	}
	if len(bySize) != 2 {
		t.Fatalf("expected 2 rows for logical size>=100 (same logical across two paths), got %d", len(bySize))
	}

	exactPath, err := SearchFilesResultWithDB(dbconn, []string{"--name", "/home/u/docs/monthly-report.txt"})
	if err != nil {
		t.Fatalf("search by exact stored path token: %v", err)
	}
	if len(exactPath) != 1 || exactPath[0].Name != "/home/u/docs/monthly-report.txt" {
		t.Fatalf("expected exact path match result, got %+v", exactPath)
	}

}
