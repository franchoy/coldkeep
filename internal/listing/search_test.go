package listing

import (
	"database/sql"
	"strings"
	"testing"

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
