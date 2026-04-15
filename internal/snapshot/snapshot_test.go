package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
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

func insertLogicalFileWithSize(t *testing.T, db *sql.DB, hash string, totalSize int64) int64 {
	t.Helper()
	res, err := db.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status) VALUES (?, ?, ?, ?)`,
		fmt.Sprintf("%s.txt", hash), totalSize, hash, "COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert logical_file hash=%s size=%d: %v", hash, totalSize, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return id
}

func insertPhysicalFile(t *testing.T, db *sql.DB, path string, logicalFileID int64, mode sql.NullInt64, mtime sql.NullTime) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete) VALUES (?, ?, ?, ?, ?)`,
		path,
		logicalFileID,
		mode,
		mtime,
		1,
	)
	if err != nil {
		t.Fatalf("insert physical_file path=%q logical_file_id=%d: %v", path, logicalFileID, err)
	}
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
		{"a\\b.txt", "a/b.txt"},
		{".\\docs\\a.txt", "docs/a.txt"},
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
		" docs/a.txt",
		"docs/a.txt ",
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

func TestInsertSnapshotFileRejectsNonExistentSnapshot(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalID := insertLogicalFile(t, db, "hash-missing-snapshot")
	sf := SnapshotFile{SnapshotID: "snap-does-not-exist", Path: "docs/missing.txt", LogicalFileID: logicalID}

	_, err := InsertSnapshotFile(ctx, db, sf)
	if err == nil {
		t.Fatal("expected error for non-existent snapshot_id, got nil")
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

// ---- CreateSnapshot tests ----

func TestCreateSnapshotFullCopiesAllPhysicalFiles(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	mtimeA := time.Now().UTC().Truncate(time.Second)
	mtimeB := mtimeA.Add(2 * time.Minute)

	logicalA := insertLogicalFileWithSize(t, db, "hash-full-a", 101)
	logicalB := insertLogicalFileWithSize(t, db, "hash-full-b", 202)
	logicalC := insertLogicalFileWithSize(t, db, "hash-full-c", 303)

	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{Int64: 0o644, Valid: true}, sql.NullTime{Time: mtimeA, Valid: true})
	insertPhysicalFile(t, db, "docs/b.txt", logicalB, sql.NullInt64{Int64: 0o600, Valid: true}, sql.NullTime{Time: mtimeB, Valid: true})
	insertPhysicalFile(t, db, "img/x.png", logicalC, sql.NullInt64{}, sql.NullTime{})

	label := "phase2-full"
	if err := CreateSnapshot(ctx, db, "snap-full-phase2", "full", &label, nil); err != nil {
		t.Fatalf("CreateSnapshot full: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ? AND type = ?`, "snap-full-phase2", "full").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected 1 snapshot row, got %d", snapshotCount)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-full-phase2").Scan(&fileCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if fileCount != 3 {
		t.Fatalf("expected 3 snapshot_file rows, got %d", fileCount)
	}

	rows, err := db.Query(`
		SELECT path, logical_file_id, size, mode
		FROM snapshot_file
		WHERE snapshot_id = ?
	`, "snap-full-phase2")
	if err != nil {
		t.Fatalf("query snapshot_file rows: %v", err)
	}
	defer func() { _ = rows.Close() }()

	seen := map[string]struct{}{}
	for rows.Next() {
		var (
			path          string
			logicalFileID int64
			size          sql.NullInt64
			mode          sql.NullInt64
		)
		if err := rows.Scan(&path, &logicalFileID, &size, &mode); err != nil {
			t.Fatalf("scan snapshot_file row: %v", err)
		}
		seen[path] = struct{}{}
		if !size.Valid {
			t.Fatalf("expected size to be preserved for path=%s", path)
		}

		switch path {
		case "docs/a.txt":
			if logicalFileID != logicalA || size.Int64 != 101 || !mode.Valid || mode.Int64 != 0o644 {
				t.Fatalf("unexpected metadata for docs/a.txt: logical_file_id=%d size=%v mode=%v", logicalFileID, size, mode)
			}
		case "docs/b.txt":
			if logicalFileID != logicalB || size.Int64 != 202 || !mode.Valid || mode.Int64 != 0o600 {
				t.Fatalf("unexpected metadata for docs/b.txt: logical_file_id=%d size=%v mode=%v", logicalFileID, size, mode)
			}
		case "img/x.png":
			if logicalFileID != logicalC || size.Int64 != 303 {
				t.Fatalf("unexpected metadata for img/x.png: logical_file_id=%d size=%v", logicalFileID, size)
			}
		default:
			t.Fatalf("unexpected snapshot path: %s", path)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate snapshot_file rows: %v", err)
	}

	if len(seen) != 3 {
		t.Fatalf("expected 3 unique paths in snapshot, got %d", len(seen))
	}
}

func TestCreateSnapshotPartialFiltersExactAndDirectory(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-partial-a", 11)
	logicalB := insertLogicalFileWithSize(t, db, "hash-partial-b", 22)
	logicalC := insertLogicalFileWithSize(t, db, "hash-partial-c", 33)

	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "img/x.png", logicalC, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-partial-phase2", "partial", nil, []string{"img/x.png", "docs/"}); err != nil {
		t.Fatalf("CreateSnapshot partial: %v", err)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-partial-phase2").Scan(&fileCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if fileCount != 3 {
		t.Fatalf("expected 3 snapshot_file rows for exact+directory filters, got %d", fileCount)
	}
}

func TestCreateSnapshotPartialAllowsEmptyDirectoryMatch(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-empty-a", 9)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-empty-dir", "partial", nil, []string{"empty/"}); err != nil {
		t.Fatalf("CreateSnapshot empty-directory partial should succeed: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-empty-dir").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected snapshot row to exist, got count=%d", snapshotCount)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-empty-dir").Scan(&fileCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if fileCount != 0 {
		t.Fatalf("expected empty snapshot_file set, got %d rows", fileCount)
	}
}

func TestCreateSnapshotPartialMissingExactPathRollsBack(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-rollback-a", 17)
	insertPhysicalFile(t, db, "a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-rollback-missing", "partial", nil, []string{"a.txt", "missing.txt"})
	if err == nil {
		t.Fatal("expected partial snapshot to fail when an exact path is missing")
	}
	if !strings.Contains(err.Error(), "path not found") {
		t.Fatalf("expected path-not-found error, got: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-rollback-missing").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected rollback to remove snapshot row, got count=%d", snapshotCount)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-rollback-missing").Scan(&fileCount); err != nil {
		t.Fatalf("query snapshot_file row count: %v", err)
	}
	if fileCount != 0 {
		t.Fatalf("expected rollback to remove snapshot_file rows, got count=%d", fileCount)
	}
}

func TestCreateSnapshotPartialDeduplicatesInputPaths(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-dedupe-a", 44)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-dedupe", "partial", nil, []string{"docs/a.txt", "docs/a.txt", "./docs//a.txt"})
	if err != nil {
		t.Fatalf("CreateSnapshot partial with duplicate inputs: %v", err)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-dedupe").Scan(&fileCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if fileCount != 1 {
		t.Fatalf("expected 1 deduplicated snapshot_file row, got %d", fileCount)
	}
}

func TestCreateSnapshotPartialMatchesBackslashStoredExactPath(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-backslash-exact", 55)
	insertPhysicalFile(t, db, "docs\\a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-backslash-exact", "partial", nil, []string{"docs/a.txt"})
	if err != nil {
		t.Fatalf("CreateSnapshot partial exact against backslash-stored path: %v", err)
	}

	var path string
	if err := db.QueryRow(`SELECT path FROM snapshot_file WHERE snapshot_id = ?`, "snap-backslash-exact").Scan(&path); err != nil {
		t.Fatalf("query snapshot_file path: %v", err)
	}
	if path != "docs/a.txt" {
		t.Fatalf("expected normalized snapshot path docs/a.txt, got %q", path)
	}
}

func TestCreateSnapshotPartialMatchesBackslashStoredDirectory(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-backslash-dir-a", 12)
	logicalB := insertLogicalFileWithSize(t, db, "hash-backslash-dir-b", 13)
	logicalC := insertLogicalFileWithSize(t, db, "hash-backslash-dir-c", 14)

	insertPhysicalFile(t, db, "docs\\a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "docs\\nested\\b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "img\\x.png", logicalC, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-backslash-dir", "partial", nil, []string{"docs/"})
	if err != nil {
		t.Fatalf("CreateSnapshot partial directory against backslash-stored paths: %v", err)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-backslash-dir").Scan(&fileCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if fileCount != 2 {
		t.Fatalf("expected 2 matched rows from backslash-stored docs directory, got %d", fileCount)
	}

	rows, err := db.Query(`SELECT path FROM snapshot_file WHERE snapshot_id = ? ORDER BY path`, "snap-backslash-dir")
	if err != nil {
		t.Fatalf("query snapshot_file rows: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			t.Fatalf("scan snapshot_file path: %v", err)
		}
		got = append(got, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate snapshot_file rows: %v", err)
	}

	want := []string{"docs/a.txt", "docs/nested/b.txt"}
	if len(got) != len(want) {
		t.Fatalf("path count mismatch: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("path mismatch at index %d: got=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestCreateSnapshotFullNormalizesAbsoluteStoredPaths(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-abs-a", 77)
	insertPhysicalFile(t, db, "/tmp/input/docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-abs-full", "full", nil, nil)
	if err != nil {
		t.Fatalf("CreateSnapshot full with absolute stored path: %v", err)
	}

	var path string
	if err := db.QueryRow(`SELECT path FROM snapshot_file WHERE snapshot_id = ?`, "snap-abs-full").Scan(&path); err != nil {
		t.Fatalf("query snapshot_file path: %v", err)
	}
	if path != "tmp/input/docs/a.txt" {
		t.Fatalf("expected absolute stored path to normalize into relative snapshot path, got %q", path)
	}
}

func TestCreateSnapshotPartialRejectsInvalidInputPathAndRollsBack(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-invalid-input", 88)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-invalid-input", "partial", nil, []string{"/absolute/invalid"})
	if err == nil {
		t.Fatal("expected invalid-path error for partial snapshot input")
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-invalid-input").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected rollback to remove snapshot row on invalid input path, got count=%d", snapshotCount)
	}
}

func TestCreateSnapshotPartialMixedValidInvalidPathRollsBack(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-mixed-invalid", 99)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-mixed-invalid", "partial", nil, []string{"docs/a.txt", "/bad"})
	if err == nil {
		t.Fatal("expected mixed valid/invalid paths to fail")
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-mixed-invalid").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected rollback to remove snapshot row for mixed valid/invalid paths, got count=%d", snapshotCount)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-mixed-invalid").Scan(&fileCount); err != nil {
		t.Fatalf("query snapshot_file row count: %v", err)
	}
	if fileCount != 0 {
		t.Fatalf("expected rollback to remove snapshot_file rows for mixed valid/invalid paths, got count=%d", fileCount)
	}
}

func TestCreateSnapshotPartialDeterministicAcrossInputOrder(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-deterministic-a", 21)
	logicalB := insertLogicalFileWithSize(t, db, "hash-deterministic-b", 22)
	logicalC := insertLogicalFileWithSize(t, db, "hash-deterministic-c", 23)

	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "img/x.png", logicalC, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-deterministic-1", "partial", nil, []string{"docs/", "img/x.png"}); err != nil {
		t.Fatalf("CreateSnapshot first ordering: %v", err)
	}
	if err := CreateSnapshot(ctx, db, "snap-deterministic-2", "partial", nil, []string{"img/x.png", "docs/"}); err != nil {
		t.Fatalf("CreateSnapshot second ordering: %v", err)
	}

	rows, err := db.Query(`
		SELECT a.path, a.logical_file_id, a.size, a.mode, a.mtime
		FROM snapshot_file a
		JOIN snapshot_file b ON b.path = a.path
		WHERE a.snapshot_id = ? AND b.snapshot_id = ?
		ORDER BY a.path
	`, "snap-deterministic-1", "snap-deterministic-2")
	if err != nil {
		t.Fatalf("query joined deterministic snapshot rows: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var joinedCount int
	for rows.Next() {
		joinedCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate joined deterministic rows: %v", err)
	}
	if joinedCount != 3 {
		t.Fatalf("expected deterministic snapshots to match all 3 paths, got %d", joinedCount)
	}
}

func TestCreateSnapshotPartialExactPathDoesNotAutoExpandDirectory(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-exact-vs-dir-a", 31)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	err := CreateSnapshot(ctx, db, "snap-exact-no-dir-expand", "partial", nil, []string{"docs"})
	if err == nil {
		t.Fatal("expected exact path 'docs' to fail when only docs/* files exist")
	}
	if !strings.Contains(err.Error(), "path not found") {
		t.Fatalf("expected path-not-found error for exact path docs, got: %v", err)
	}

	err = CreateSnapshot(ctx, db, "snap-dir-expand", "partial", nil, []string{"docs/"})
	if err != nil {
		t.Fatalf("expected directory prefix docs/ to succeed, got: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-dir-expand").Scan(&count); err != nil {
		t.Fatalf("count snapshot_file rows for docs/ snapshot: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected docs/ snapshot to include one row, got %d", count)
	}
}

// ---- RestoreSnapshot helper tests ----

func insertSnapshotFileRow(t *testing.T, db *sql.DB, snapshotID, path string, logicalFileID int64, mode sql.NullInt64, mtime sql.NullTime) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id, size, mode, mtime) VALUES (?, ?, ?, ?, ?, ?)`,
		snapshotID,
		path,
		logicalFileID,
		sql.NullInt64{Int64: 1, Valid: true},
		mode,
		mtime,
	)
	if err != nil {
		t.Fatalf("insert snapshot_file row snapshot_id=%s path=%s: %v", snapshotID, path, err)
	}
}

func TestResolveSnapshotRestoreSelectionMissingSnapshotFails(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_, _, err := resolveSnapshotRestoreSelection(ctx, db, "snap-missing", nil)
	if err == nil || !strings.Contains(err.Error(), "snapshot not found") {
		t.Fatalf("expected missing snapshot error, got: %v", err)
	}
}

func TestResolveSnapshotRestoreSelectionExactAndDirectory(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-rsel-a", 1)
	logicalB := insertLogicalFileWithSize(t, db, "hash-rsel-b", 2)
	s := Snapshot{ID: "snap-rsel", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	selected, exact, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/", "docs/a.txt"})
	if err != nil {
		t.Fatalf("resolveSnapshotRestoreSelection: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 selected rows, got %d", len(selected))
	}
	if len(exact) != 1 || exact[0] != "docs/a.txt" {
		t.Fatalf("exact filters mismatch: %v", exact)
	}
}

func TestResolveSnapshotRestoreSelectionExactMissingFails(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-rsel-missing", 1)
	s := Snapshot{ID: "snap-rsel-missing", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	_, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/missing.txt"})
	if err == nil || !strings.Contains(err.Error(), "path not found in snapshot") {
		t.Fatalf("expected exact missing path error, got: %v", err)
	}
}

func TestPlanSnapshotRestoreOutputsRejectsOverrideForMultiFile(t *testing.T) {
	rows := []snapshotRestoreRow{{Path: "docs/a.txt", LogicalFileID: 1}, {Path: "docs/b.txt", LogicalFileID: 2}}
	_, err := planSnapshotRestoreOutputs(rows, []string{"docs/a.txt", "docs/b.txt"}, RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOverride,
		Destination:     "./out.bin",
	})
	if err == nil || !strings.Contains(err.Error(), "single exact-path") {
		t.Fatalf("expected override multi-file rejection, got: %v", err)
	}
}

func TestPlanSnapshotRestoreOutputsRejectsConflictsWhenOverwriteOff(t *testing.T) {
	tmp := t.TempDir()
	conflictPath := filepath.Join(tmp, "conflict.txt")
	if err := os.WriteFile(conflictPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("write conflict file: %v", err)
	}

	rows := []snapshotRestoreRow{{Path: "docs/a.txt", LogicalFileID: 1}}
	_, err := planSnapshotRestoreOutputs(rows, []string{"docs/a.txt"}, RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOverride,
		Destination:     conflictPath,
		Overwrite:       false,
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected overwrite-off conflict error, got: %v", err)
	}
}

func TestResolveSnapshotRestoreSelectionExactOnly(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-rsel-exact-a", 1)
	logicalB := insertLogicalFileWithSize(t, db, "hash-rsel-exact-b", 2)
	s := Snapshot{ID: "snap-rsel-exact-only", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	selected, exact, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/a.txt"})
	if err != nil {
		t.Fatalf("resolveSnapshotRestoreSelection exact: %v", err)
	}
	if len(exact) != 1 || exact[0] != "docs/a.txt" {
		t.Fatalf("exact filters mismatch: %v", exact)
	}
	if len(selected) != 1 || selected[0].Path != "docs/a.txt" {
		t.Fatalf("expected only docs/a.txt selected, got %+v", selected)
	}
}

func TestResolveSnapshotRestoreSelectionEmptyDirectoryPrefixSucceeds(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-rsel-empty-dir", 1)
	s := Snapshot{ID: "snap-rsel-empty-dir", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	selected, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"empty/"})
	if err != nil {
		t.Fatalf("resolveSnapshotRestoreSelection empty directory should succeed: %v", err)
	}
	if len(selected) != 0 {
		t.Fatalf("expected zero selected rows for empty directory prefix, got %d", len(selected))
	}
}

func TestResolveSnapshotRestoreSelectionDeterministicAcrossInputOrder(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-rsel-det-a", 1)
	logicalB := insertLogicalFileWithSize(t, db, "hash-rsel-det-b", 2)
	logicalC := insertLogicalFileWithSize(t, db, "hash-rsel-det-c", 3)
	s := Snapshot{ID: "snap-rsel-deterministic", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "img/x.png", logicalC, sql.NullInt64{}, sql.NullTime{})

	selectedA, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/", "img/x.png"})
	if err != nil {
		t.Fatalf("resolve selection A: %v", err)
	}
	selectedB, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"img/x.png", "docs/"})
	if err != nil {
		t.Fatalf("resolve selection B: %v", err)
	}

	if len(selectedA) != len(selectedB) {
		t.Fatalf("selection length mismatch: %d vs %d", len(selectedA), len(selectedB))
	}
	for i := range selectedA {
		if selectedA[i].Path != selectedB[i].Path || selectedA[i].LogicalFileID != selectedB[i].LogicalFileID {
			t.Fatalf("deterministic selection mismatch at %d: A=%+v B=%+v", i, selectedA[i], selectedB[i])
		}
	}
}

func TestPlanSnapshotRestoreOutputsPreflightCreatesDestinationDirectories(t *testing.T) {
	tmp := t.TempDir()
	prefix := filepath.Join(tmp, "restore-root")
	targetDir := filepath.Join(prefix, "docs")
	rows := []snapshotRestoreRow{{Path: "docs/a.txt", LogicalFileID: 1}}

	if _, err := os.Stat(targetDir); !os.IsNotExist(err) {
		t.Fatalf("expected target directory to not exist before preflight, err=%v", err)
	}

	_, err := planSnapshotRestoreOutputs(rows, []string{"docs/"}, RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationPrefix,
		Destination:     prefix,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("planSnapshotRestoreOutputs should preflight destination dirs: %v", err)
	}

	if stat, err := os.Stat(targetDir); err != nil || !stat.IsDir() {
		t.Fatalf("expected destination directory created during preflight, stat=%v err=%v", stat, err)
	}
}
