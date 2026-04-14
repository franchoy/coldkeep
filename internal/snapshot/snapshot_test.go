package snapshot

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	idb "github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

// openTestDB opens an in-memory SQLite database with the full migration applied.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := idb.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

// insertLogicalFile inserts a minimal logical_file row and returns its id.
func insertLogicalFile(t *testing.T, db *sql.DB, hash string) int64 {
	t.Helper()
	res, err := db.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
		"file.txt", int64(128), hash, "COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert logical_file hash=%s: %v", hash, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return id
}

// ---- NormalizeSnapshotPath tests ----

func TestNormalizeSnapshotPathValid(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"docs/a.txt", "docs/a.txt"},
		{"./docs/a.txt", "docs/a.txt"},
		{"docs//a.txt", "docs/a.txt"},
		{"a/b/c.txt", "a/b/c.txt"},
		{"./././file.txt", "file.txt"},
		{"a//b//c.txt", "a/b/c.txt"},
	}
	for _, tc := range cases {
		got, err := NormalizeSnapshotPath(tc.input)
		if err != nil {
			t.Errorf("NormalizeSnapshotPath(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if got != tc.want {
			t.Errorf("NormalizeSnapshotPath(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestNormalizeSnapshotPathInvalid(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"/absolute/path",
		"/",
	}
	for _, input := range cases {
		_, err := NormalizeSnapshotPath(input)
		if err == nil {
			t.Errorf("NormalizeSnapshotPath(%q) expected error, got nil", input)
		}
	}
}

// ---- InsertSnapshot tests ----

func TestInsertSnapshotSucceeds(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{
		ID:        "test-uuid-1",
		CreatedAt: time.Now().UTC(),
		Type:      "full",
	}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, s.ID).Scan(&count); err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 snapshot row, got %d", count)
	}
}

func TestInsertSnapshotWithLabel(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{
		ID:        "test-uuid-label",
		CreatedAt: time.Now().UTC(),
		Type:      "partial",
		Label:     sql.NullString{String: "my-label", Valid: true},
	}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot with label: %v", err)
	}

	var label sql.NullString
	if err := db.QueryRow(`SELECT label FROM snapshot WHERE id = ?`, s.ID).Scan(&label); err != nil {
		t.Fatalf("query label: %v", err)
	}
	if !label.Valid || label.String != "my-label" {
		t.Fatalf("expected label 'my-label', got %v", label)
	}
}

func TestInsertSnapshotRejectsInvalidType(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{
		ID:        "test-uuid-bad-type",
		CreatedAt: time.Now().UTC(),
		Type:      "incremental",
	}
	err := InsertSnapshot(ctx, db, s)
	if err == nil {
		t.Fatal("expected error for invalid type, got nil")
	}
	if !strings.Contains(err.Error(), "type must be") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertSnapshotRejectsEmptyID(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{CreatedAt: time.Now().UTC(), Type: "full"}
	err := InsertSnapshot(ctx, db, s)
	if err == nil || !strings.Contains(err.Error(), "id cannot be empty") {
		t.Fatalf("expected id-empty error, got: %v", err)
	}
}

func TestInsertSnapshotRejectsZeroCreatedAt(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{ID: "test-uuid-zero-time", Type: "full"}
	err := InsertSnapshot(ctx, db, s)
	if err == nil || !strings.Contains(err.Error(), "created_at cannot be zero") {
		t.Fatalf("expected created_at-zero error, got: %v", err)
	}
}

func TestInsertSnapshotImmutable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{
		ID:        "test-uuid-immutable",
		CreatedAt: time.Now().UTC(),
		Type:      "full",
	}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("first InsertSnapshot: %v", err)
	}

	// Inserting the same ID again must fail (PRIMARY KEY violation).
	err := InsertSnapshot(ctx, db, s)
	if err == nil {
		t.Fatal("expected duplicate-id insert to fail, got nil")
	}
}

// ---- InsertSnapshotFile tests ----

func TestInsertSnapshotFileSucceeds(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalID := insertLogicalFile(t, db, "hash-sf-1")
	s := Snapshot{ID: "snap-sf-1", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	sf := SnapshotFile{
		SnapshotID:    s.ID,
		Path:          "docs/readme.txt",
		LogicalFileID: logicalID,
		Size:          sql.NullInt64{Int64: 128, Valid: true},
	}
	id, err := InsertSnapshotFile(ctx, db, sf)
	if err != nil {
		t.Fatalf("InsertSnapshotFile: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}
}

func TestInsertSnapshotFileNormalizesPath(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalID := insertLogicalFile(t, db, "hash-sf-norm")
	s := Snapshot{ID: "snap-norm", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	sf := SnapshotFile{
		SnapshotID:    s.ID,
		Path:          "./docs//readme.txt",
		LogicalFileID: logicalID,
	}
	if _, err := InsertSnapshotFile(ctx, db, sf); err != nil {
		t.Fatalf("InsertSnapshotFile: %v", err)
	}

	var path string
	if err := db.QueryRow(`SELECT path FROM snapshot_file WHERE snapshot_id = ?`, s.ID).Scan(&path); err != nil {
		t.Fatalf("query path: %v", err)
	}
	if path != "docs/readme.txt" {
		t.Fatalf("expected normalized path 'docs/readme.txt', got %q", path)
	}
}

func TestInsertSnapshotFileDuplicatePathFails(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalID := insertLogicalFile(t, db, "hash-dup")
	s := Snapshot{ID: "snap-dup", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	sf := SnapshotFile{SnapshotID: s.ID, Path: "docs/a.txt", LogicalFileID: logicalID}
	if _, err := InsertSnapshotFile(ctx, db, sf); err != nil {
		t.Fatalf("first InsertSnapshotFile: %v", err)
	}

	// Same path within the same snapshot must fail.
	_, err := InsertSnapshotFile(ctx, db, sf)
	if err == nil {
		t.Fatal("expected duplicate path insert to fail, got nil")
	}
}

func TestInsertSnapshotFileSamePathDifferentSnapshotSucceeds(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalID := insertLogicalFile(t, db, "hash-multi-snap")

	for i, snapID := range []string{"snap-a", "snap-b"} {
		s := Snapshot{ID: snapID, CreatedAt: time.Now().UTC(), Type: "full"}
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %d: %v", i, err)
		}
		sf := SnapshotFile{SnapshotID: snapID, Path: "docs/shared.txt", LogicalFileID: logicalID}
		if _, err := InsertSnapshotFile(ctx, db, sf); err != nil {
			t.Fatalf("InsertSnapshotFile snap=%s: %v", snapID, err)
		}
	}
}

func TestInsertSnapshotFileRejectsNonExistentLogicalFile(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{ID: "snap-missing-lf", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	sf := SnapshotFile{SnapshotID: s.ID, Path: "file.txt", LogicalFileID: 99999}
	_, err := InsertSnapshotFile(ctx, db, sf)
	if err == nil {
		t.Fatal("expected error for non-existent logical_file, got nil")
	}
	if !strings.Contains(err.Error(), "non-existent logical_file") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertSnapshotFileRejectsInvalidPath(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalID := insertLogicalFile(t, db, "hash-bad-path")
	s := Snapshot{ID: "snap-bad-path", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	sf := SnapshotFile{SnapshotID: s.ID, Path: "/absolute/path", LogicalFileID: logicalID}
	_, err := InsertSnapshotFile(ctx, db, sf)
	if err == nil {
		t.Fatal("expected error for absolute path, got nil")
	}
}
