package listing

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openListTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestListFilesFailsWhenDBIsNil(t *testing.T) {
	_, err := ListFilesResultWithDB(nil, nil)
	if err == nil || !strings.Contains(err.Error(), "db connection is nil") {
		t.Fatalf("expected db-nil error contract, got: %v", err)
	}
}

func TestListFilesFailsWhenLimitArgIsMissing(t *testing.T) {
	_, err := ListFilesResultWithDB(openListTestDB(t), []string{"--limit"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --limit") {
		t.Fatalf("expected missing-limit error contract, got: %v", err)
	}
}

func TestListFilesFailsWhenOffsetArgIsInvalid(t *testing.T) {
	_, err := ListFilesResultWithDB(openListTestDB(t), []string{"--offset", "-1"})
	if err == nil || !strings.Contains(err.Error(), "invalid --offset") {
		t.Fatalf("expected invalid-offset error contract, got: %v", err)
	}
}

func TestListFilesFailsWhenLimitArgExceedsMaximum(t *testing.T) {
	_, err := ListFilesResultWithDB(openListTestDB(t), []string{"--limit", "10001"})
	if err == nil || !strings.Contains(err.Error(), "invalid --limit") {
		t.Fatalf("expected invalid-limit error contract, got: %v", err)
	}
}
