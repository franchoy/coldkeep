package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

func TestEngineListAndSearchFiles(t *testing.T) {
	dbconn := openSnapshotTestDB(t)
	seedEngineCurrentFile(t, dbconn, 1, "/docs/report.txt", "hash-report", 120, filestate.LogicalFileCompleted)
	seedEngineCurrentFile(t, dbconn, 2, "/docs/notes.txt", "hash-notes", 40, filestate.LogicalFileCompleted)
	seedEngineCurrentFile(t, dbconn, 3, "/docs/aborted.txt", "hash-aborted", 80, filestate.LogicalFileAborted)
	eng, err := engine.New(engine.Config{DB: dbconn})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	listed, err := eng.ListFiles(context.Background(), engine.ListFilesRequest{})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if got := currentFileNames(listed.Files); !reflect.DeepEqual(got, []string{"/docs/notes.txt", "/docs/report.txt"}) {
		t.Fatalf("ListFiles paths: %v", got)
	}
	if listed.Files[1].ID != 1 || listed.Files[1].FileHash != "hash-report" || listed.Files[1].SizeBytes != 120 || listed.Files[1].CreatedAt == "" {
		t.Fatalf("ListFiles projection: %+v", listed.Files[1])
	}

	searched, err := eng.SearchFiles(context.Background(), engine.SearchFilesRequest{
		NameContains: []string{"docs", "report"}, MinSizeBytes: []int64{100}, MaxSizeBytes: []int64{200},
	})
	if err != nil || len(searched.Files) != 1 || searched.Files[0].Name != "/docs/report.txt" {
		t.Fatalf("SearchFiles: got (%+v, %v)", searched, err)
	}
}

func TestEngineCurrentFileValidationAndCancellation(t *testing.T) {
	dbconn := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: dbconn})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	negative := int64(-1)
	if _, err := eng.ListFiles(context.Background(), engine.ListFilesRequest{Limit: &negative}); !engine.IsCode(err, engine.ErrorInvalidArgument) {
		t.Fatalf("negative ListFiles limit: %v", err)
	}
	if _, err := eng.SearchFiles(context.Background(), engine.SearchFilesRequest{NameContains: []string{" "}}); !engine.IsCode(err, engine.ErrorInvalidArgument) {
		t.Fatalf("blank SearchFiles name: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := eng.ListFiles(cancelled, engine.ListFilesRequest{}); !errors.Is(err, context.Canceled) || !engine.IsCode(err, engine.ErrorCancelled) {
		t.Fatalf("cancelled ListFiles: %v", err)
	}
}

func seedEngineCurrentFile(t *testing.T, dbconn *sql.DB, id int64, path, hash string, size int64, status string) {
	t.Helper()
	if _, err := dbconn.Exec(`
INSERT INTO logical_file (id, original_name, total_size, file_hash, status, ref_count, chunker_version)
VALUES ($1, $2, $3, $4, $5, 1, 'v1-simple-rolling')`, id, path, size, hash, status); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	if _, err := dbconn.Exec(`
INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
VALUES ($1, $2, 1)`, path, id); err != nil {
		t.Fatalf("insert physical file: %v", err)
	}
}

func currentFileNames(files []engine.CurrentFile) []string {
	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.Name
	}
	return names
}
