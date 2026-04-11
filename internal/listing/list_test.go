package listing

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"

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

func TestListFilesReturnsCurrentStatePhysicalPaths(t *testing.T) {
	dbconn := openListTestDB(t)
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	insertLogical := func(name, hash string, status string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
			 VALUES ($1, $2, $3, $4, $5)
			 RETURNING id`,
			name,
			int64(10),
			hash,
			status,
			0,
		).Scan(&id); err != nil {
			t.Fatalf("insert logical_file %q: %v", name, err)
		}
		return id
	}

	completedID := insertLogical("logical-a", "hash-a", filestate.LogicalFileCompleted)
	abortedID := insertLogical("logical-b", "hash-b", filestate.LogicalFileAborted)

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
		"/data/a-one.txt",
		completedID,
		1,
	); err != nil {
		t.Fatalf("insert first physical_file row: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
		"/data/a-two.txt",
		completedID,
		1,
	); err != nil {
		t.Fatalf("insert second physical_file row: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
		"/data/aborted.txt",
		abortedID,
		1,
	); err != nil {
		t.Fatalf("insert aborted physical_file row: %v", err)
	}

	records, err := ListFilesResultWithDB(dbconn, nil)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 current-state records for completed logical_file only, got %d", len(records))
	}

	paths := map[string]bool{}
	for _, rec := range records {
		paths[rec.Name] = true
		if rec.ID != completedID {
			t.Fatalf("expected listed row logical id %d, got %d", completedID, rec.ID)
		}
	}

	for _, expectedPath := range []string{"/data/a-one.txt", "/data/a-two.txt"} {
		if !paths[expectedPath] {
			t.Fatalf("missing listed current-state path %q in %+v", expectedPath, paths)
		}
	}

	limited, err := ListFilesResultWithDB(dbconn, []string{"--limit", "1", "--offset", "1"})
	if err != nil {
		t.Fatalf("list files with pagination: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected one paginated result, got %d", len(limited))
	}
	if !strings.HasPrefix(limited[0].Name, "/data/a-") {
		t.Fatalf("unexpected paginated path: %s", limited[0].Name)
	}

}
