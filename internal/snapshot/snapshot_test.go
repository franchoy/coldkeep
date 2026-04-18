package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/retention"
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
	if err := db.QueryRow(`SELECT sp.path FROM snapshot_file sf JOIN snapshot_path sp ON sp.id = sf.path_id WHERE sf.snapshot_id = ?`, s.ID).Scan(&path); err != nil {
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
	if err := CreateSnapshot(ctx, db, "snap-full-phase2", "full", &label, nil, nil); err != nil {
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
		SELECT sp.path, sf.logical_file_id, sf.size, sf.mode
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sf.snapshot_id = ?
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

	if err := CreateSnapshot(ctx, db, "snap-partial-phase2", "partial", nil, nil, []string{"img/x.png", "docs/"}); err != nil {
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

	if err := CreateSnapshot(ctx, db, "snap-empty-dir", "partial", nil, nil, []string{"empty/"}); err != nil {
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

	err := CreateSnapshot(ctx, db, "snap-rollback-missing", "partial", nil, nil, []string{"a.txt", "missing.txt"})
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

	err := CreateSnapshot(ctx, db, "snap-dedupe", "partial", nil, nil, []string{"docs/a.txt", "docs/a.txt", "./docs//a.txt"})
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

	err := CreateSnapshot(ctx, db, "snap-backslash-exact", "partial", nil, nil, []string{"docs/a.txt"})
	if err != nil {
		t.Fatalf("CreateSnapshot partial exact against backslash-stored path: %v", err)
	}

	var path string
	if err := db.QueryRow(`SELECT sp.path FROM snapshot_file sf JOIN snapshot_path sp ON sp.id = sf.path_id WHERE sf.snapshot_id = ?`, "snap-backslash-exact").Scan(&path); err != nil {
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

	err := CreateSnapshot(ctx, db, "snap-backslash-dir", "partial", nil, nil, []string{"docs/"})
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

	rows, err := db.Query(`SELECT sp.path FROM snapshot_file sf JOIN snapshot_path sp ON sp.id = sf.path_id WHERE sf.snapshot_id = ? ORDER BY sp.path`, "snap-backslash-dir")
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

	err := CreateSnapshot(ctx, db, "snap-abs-full", "full", nil, nil, nil)
	if err != nil {
		t.Fatalf("CreateSnapshot full with absolute stored path: %v", err)
	}

	var path string
	if err := db.QueryRow(`SELECT sp.path FROM snapshot_file sf JOIN snapshot_path sp ON sp.id = sf.path_id WHERE sf.snapshot_id = ?`, "snap-abs-full").Scan(&path); err != nil {
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

	err := CreateSnapshot(ctx, db, "snap-invalid-input", "partial", nil, nil, []string{"/absolute/invalid"})
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

	err := CreateSnapshot(ctx, db, "snap-mixed-invalid", "partial", nil, nil, []string{"docs/a.txt", "/bad"})
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

	if err := CreateSnapshot(ctx, db, "snap-deterministic-1", "partial", nil, nil, []string{"docs/", "img/x.png"}); err != nil {
		t.Fatalf("CreateSnapshot first ordering: %v", err)
	}
	if err := CreateSnapshot(ctx, db, "snap-deterministic-2", "partial", nil, nil, []string{"img/x.png", "docs/"}); err != nil {
		t.Fatalf("CreateSnapshot second ordering: %v", err)
	}

	rows, err := db.Query(`
		SELECT sp.path, a.logical_file_id, a.size, a.mode, a.mtime
		FROM snapshot_file a
		JOIN snapshot_file b ON b.path_id = a.path_id
		JOIN snapshot_path sp ON sp.id = a.path_id
		WHERE a.snapshot_id = ? AND b.snapshot_id = ?
		ORDER BY sp.path
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

	err := CreateSnapshot(ctx, db, "snap-exact-no-dir-expand", "partial", nil, nil, []string{"docs"})
	if err == nil {
		t.Fatal("expected exact path 'docs' to fail when only docs/* files exist")
	}
	if !strings.Contains(err.Error(), "path not found") {
		t.Fatalf("expected path-not-found error for exact path docs, got: %v", err)
	}

	err = CreateSnapshot(ctx, db, "snap-dir-expand", "partial", nil, nil, []string{"docs/"})
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

func TestCreateSnapshotReusesSnapshotPathRowsAcrossSnapshots(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-path-reuse-a", 31)
	logicalB := insertLogicalFileWithSize(t, db, "hash-path-reuse-b", 32)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-reuse-1", "full", nil, nil, nil); err != nil {
		t.Fatalf("CreateSnapshot first full: %v", err)
	}
	if err := CreateSnapshot(ctx, db, "snap-reuse-2", "partial", nil, nil, []string{"docs/a.txt"}); err != nil {
		t.Fatalf("CreateSnapshot second partial: %v", err)
	}

	var snapshotPathCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_path`).Scan(&snapshotPathCount); err != nil {
		t.Fatalf("count snapshot_path rows: %v", err)
	}
	if snapshotPathCount != 2 {
		t.Fatalf("expected exactly 2 normalized snapshot_path rows for two distinct paths, got %d", snapshotPathCount)
	}

	var docsAPathRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_path WHERE path = ?`, "docs/a.txt").Scan(&docsAPathRows); err != nil {
		t.Fatalf("count docs/a.txt snapshot_path rows: %v", err)
	}
	if docsAPathRows != 1 {
		t.Fatalf("expected docs/a.txt to exist once in snapshot_path, got %d rows", docsAPathRows)
	}

	var firstPathID int64
	if err := db.QueryRow(`
		SELECT sf.path_id
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sf.snapshot_id = ? AND sp.path = ?
	`, "snap-reuse-1", "docs/a.txt").Scan(&firstPathID); err != nil {
		t.Fatalf("query first snapshot path_id for docs/a.txt: %v", err)
	}

	var secondPathID int64
	if err := db.QueryRow(`
		SELECT sf.path_id
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sf.snapshot_id = ? AND sp.path = ?
	`, "snap-reuse-2", "docs/a.txt").Scan(&secondPathID); err != nil {
		t.Fatalf("query second snapshot path_id for docs/a.txt: %v", err)
	}

	if firstPathID != secondPathID {
		t.Fatalf("expected snapshots to reuse the same path_id for docs/a.txt, got first=%d second=%d", firstPathID, secondPathID)
	}
}

func TestCreateSnapshotStoresParentIDForFullSnapshotWithoutChangingSelectionBehavior(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalDocs := insertLogicalFileWithSize(t, db, "hash-parent-inert-docs", 17)
	logicalImg := insertLogicalFileWithSize(t, db, "hash-parent-inert-img", 29)
	insertPhysicalFile(t, db, "docs/a.txt", logicalDocs, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "img/x.png", logicalImg, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-parent-inert", "full", nil, nil, nil); err != nil {
		t.Fatalf("CreateSnapshot parent full: %v", err)
	}

	parentID := "snap-parent-inert"
	if err := CreateSnapshot(ctx, db, "snap-child-inert", "full", nil, &parentID, nil); err != nil {
		t.Fatalf("CreateSnapshot child full with parent_id: %v", err)
	}

	var storedParentID sql.NullString
	if err := db.QueryRow(`SELECT parent_id FROM snapshot WHERE id = ?`, "snap-child-inert").Scan(&storedParentID); err != nil {
		t.Fatalf("query child snapshot parent_id: %v", err)
	}
	if !storedParentID.Valid || storedParentID.String != parentID {
		t.Fatalf("expected stored parent_id=%q, got %+v", parentID, storedParentID)
	}

	gotChild, err := GetSnapshot(ctx, db, "snap-child-inert")
	if err != nil {
		t.Fatalf("GetSnapshot child: %v", err)
	}
	if !gotChild.ParentID.Valid || gotChild.ParentID.String != parentID {
		t.Fatalf("expected model ParentID=%q, got %+v", parentID, gotChild.ParentID)
	}

	files, err := ListSnapshotFiles(ctx, db, "snap-child-inert", 10, nil)
	if err != nil {
		t.Fatalf("ListSnapshotFiles child: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected full snapshot to include both paths, got %d files", len(files))
	}
	if files[0].Path != "docs/a.txt" || files[1].Path != "img/x.png" {
		t.Fatalf("expected full snapshot behavior to remain docs/a.txt,img/x.png, got paths=%q,%q", files[0].Path, files[1].Path)
	}
}

func TestCreateSnapshotWithParentUsesCurrentStateNotParentContents(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalDocs := insertLogicalFileWithSize(t, db, "hash-parent-state-docs", 17)
	insertPhysicalFile(t, db, "docs/a.txt", logicalDocs, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-parent-state", "full", nil, nil, nil); err != nil {
		t.Fatalf("CreateSnapshot parent full: %v", err)
	}

	// Change live state after parent creation: child must capture this new file too.
	logicalImg := insertLogicalFileWithSize(t, db, "hash-parent-state-img", 29)
	insertPhysicalFile(t, db, "img/x.png", logicalImg, sql.NullInt64{}, sql.NullTime{})

	parentID := "snap-parent-state"
	if err := CreateSnapshot(ctx, db, "snap-child-state", "full", nil, &parentID, nil); err != nil {
		t.Fatalf("CreateSnapshot child full with parent_id: %v", err)
	}

	parentFiles, err := ListSnapshotFiles(ctx, db, "snap-parent-state", 10, nil)
	if err != nil {
		t.Fatalf("ListSnapshotFiles parent: %v", err)
	}
	if len(parentFiles) != 1 {
		t.Fatalf("expected parent snapshot to remain at its original single file, got %d", len(parentFiles))
	}
	if parentFiles[0].Path != "docs/a.txt" {
		t.Fatalf("expected parent snapshot path docs/a.txt, got %q", parentFiles[0].Path)
	}

	childFiles, err := ListSnapshotFiles(ctx, db, "snap-child-state", 10, nil)
	if err != nil {
		t.Fatalf("ListSnapshotFiles child: %v", err)
	}
	if len(childFiles) != 2 {
		t.Fatalf("expected child snapshot to reflect current state with 2 files, got %d", len(childFiles))
	}
	if childFiles[0].Path != "docs/a.txt" || childFiles[1].Path != "img/x.png" {
		t.Fatalf("expected child snapshot paths docs/a.txt,img/x.png, got paths=%q,%q", childFiles[0].Path, childFiles[1].Path)
	}
}

func TestCreateSnapshotRejectsParentLineageForChildPartialSnapshot(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalDocs := insertLogicalFileWithSize(t, db, "hash-scope-child-partial-docs", 17)
	logicalImg := insertLogicalFileWithSize(t, db, "hash-scope-child-partial-img", 29)
	insertPhysicalFile(t, db, "docs/a.txt", logicalDocs, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "img/x.png", logicalImg, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-parent-full", "full", nil, nil, nil); err != nil {
		t.Fatalf("CreateSnapshot parent full: %v", err)
	}

	parentID := "snap-parent-full"
	err := CreateSnapshot(ctx, db, "snap-child-partial", "partial", nil, &parentID, []string{"docs/"})
	if err == nil {
		t.Fatal("expected child partial lineage create to fail")
	}
	if !strings.Contains(err.Error(), "--from is currently supported only for full snapshots") {
		t.Fatalf("expected child partial lineage restriction error, got: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-child-partial").Scan(&snapshotCount); err != nil {
		t.Fatalf("query child snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected no child snapshot row for partial lineage rejection, got count=%d", snapshotCount)
	}
}

func TestCreateSnapshotRejectsParentLineageForParentPartialSnapshot(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalDocs := insertLogicalFileWithSize(t, db, "hash-scope-parent-partial-docs", 17)
	logicalImg := insertLogicalFileWithSize(t, db, "hash-scope-parent-partial-img", 29)
	insertPhysicalFile(t, db, "docs/a.txt", logicalDocs, sql.NullInt64{}, sql.NullTime{})
	insertPhysicalFile(t, db, "img/x.png", logicalImg, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-parent-partial", "partial", nil, nil, []string{"docs/"}); err != nil {
		t.Fatalf("CreateSnapshot parent partial: %v", err)
	}

	parentID := "snap-parent-partial"
	err := CreateSnapshot(ctx, db, "snap-child-full", "full", nil, &parentID, nil)
	if err == nil {
		t.Fatal("expected parent partial lineage create to fail")
	}
	if !strings.Contains(err.Error(), `parent snapshot "snap-parent-partial" is partial; --from is currently supported only for full snapshots`) {
		t.Fatalf("expected parent partial lineage restriction error, got: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-child-full").Scan(&snapshotCount); err != nil {
		t.Fatalf("query child snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected no child snapshot row for parent partial lineage rejection, got count=%d", snapshotCount)
	}
}

func TestCreateSnapshotRejectsMissingParentAndRollsBack(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-missing-parent-a", 10)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	parentID := "snap-parent-missing"
	err := CreateSnapshot(ctx, db, "snap-child-missing-parent", "full", nil, &parentID, nil)
	if err == nil {
		t.Fatal("expected missing parent snapshot to fail")
	}
	if !strings.Contains(err.Error(), `parent snapshot "snap-parent-missing" not found`) {
		t.Fatalf("expected parent-not-found error, got: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-child-missing-parent").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected no snapshot row when parent is missing, got count=%d", snapshotCount)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-child-missing-parent").Scan(&fileCount); err != nil {
		t.Fatalf("query snapshot_file row count: %v", err)
	}
	if fileCount != 0 {
		t.Fatalf("expected no snapshot_file rows when parent is missing, got count=%d", fileCount)
	}
}

func TestCreateSnapshotRejectsSelfParenting(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-self-parent-a", 10)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	parentID := "snap-self-parent"
	err := CreateSnapshot(ctx, db, "snap-self-parent", "full", nil, &parentID, nil)
	if err == nil {
		t.Fatal("expected self-parent snapshot to fail")
	}
	if !strings.Contains(err.Error(), `parent snapshot "snap-self-parent" cannot reference itself`) {
		t.Fatalf("expected self-parent validation error, got: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-self-parent").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected no snapshot row for self-parent failure, got count=%d", snapshotCount)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-self-parent").Scan(&fileCount); err != nil {
		t.Fatalf("query snapshot_file row count: %v", err)
	}
	if fileCount != 0 {
		t.Fatalf("expected no snapshot_file rows for self-parent failure, got count=%d", fileCount)
	}
}

func TestCreateSnapshotWithoutParentStoresNullParentID(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-null-parent-a", 10)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-null-parent", "full", nil, nil, nil); err != nil {
		t.Fatalf("CreateSnapshot full without parent: %v", err)
	}

	var storedParentID sql.NullString
	if err := db.QueryRow(`SELECT parent_id FROM snapshot WHERE id = ?`, "snap-null-parent").Scan(&storedParentID); err != nil {
		t.Fatalf("query snapshot parent_id: %v", err)
	}
	if storedParentID.Valid {
		t.Fatalf("expected NULL parent_id when parent is omitted, got %+v", storedParentID)
	}
}

func TestCreateSnapshotWithParentFailureLeavesNoChildRows(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-parent-rollback-a", 10)
	insertPhysicalFile(t, db, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	if err := CreateSnapshot(ctx, db, "snap-parent-ok", "full", nil, nil, nil); err != nil {
		t.Fatalf("CreateSnapshot parent full: %v", err)
	}

	parentID := "snap-parent-ok"
	err := CreateSnapshot(ctx, db, "snap-child-rollback", "partial", nil, &parentID, []string{"docs/"})
	if err == nil {
		t.Fatal("expected child snapshot create with parent to fail for unsupported partial lineage")
	}
	if !strings.Contains(err.Error(), "--from is currently supported only for full snapshots") {
		t.Fatalf("expected full-only lineage error, got: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, "snap-child-rollback").Scan(&snapshotCount); err != nil {
		t.Fatalf("query snapshot row count: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected no snapshot row after rollback, got count=%d", snapshotCount)
	}

	var fileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-child-rollback").Scan(&fileCount); err != nil {
		t.Fatalf("query snapshot_file row count: %v", err)
	}
	if fileCount != 0 {
		t.Fatalf("expected no snapshot_file rows after rollback, got count=%d", fileCount)
	}
}

// ---- RestoreSnapshot helper tests ----

func insertSnapshotFileRow(t *testing.T, db *sql.DB, snapshotID, path string, logicalFileID int64, mode sql.NullInt64, mtime sql.NullTime) {
	t.Helper()
	insertSnapshotFileRowWithSize(t, db, snapshotID, path, logicalFileID, sql.NullInt64{Int64: 1, Valid: true}, mode, mtime)
}

func insertSnapshotFileRowWithSize(t *testing.T, db *sql.DB, snapshotID, path string, logicalFileID int64, size sql.NullInt64, mode sql.NullInt64, mtime sql.NullTime) {
	t.Helper()
	ctx := context.Background()
	pathID, err := ResolveSnapshotPath(ctx, db, path)
	if err != nil {
		t.Fatalf("resolve snapshot_path for insert snapshot_id=%s path=%s: %v", snapshotID, path, err)
	}
	_, err = db.Exec(
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size, mode, mtime) VALUES (?, ?, ?, ?, ?, ?)`,
		snapshotID,
		pathID,
		logicalFileID,
		size,
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

	_, _, err := resolveSnapshotRestoreSelection(ctx, db, "snap-missing", nil, nil)
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

	selected, exact, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/", "docs/a.txt"}, nil)
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

	_, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/missing.txt"}, nil)
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

	selected, exact, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/a.txt"}, nil)
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

	selected, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"empty/"}, nil)
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

	selectedA, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/", "img/x.png"}, nil)
	if err != nil {
		t.Fatalf("resolve selection A: %v", err)
	}
	selectedB, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"img/x.png", "docs/"}, nil)
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

func TestPlanSnapshotRestoreOutputsDoesNotCreateDestinationDirectories(t *testing.T) {
	tmp := t.TempDir()
	prefix := filepath.Join(tmp, "restore-root")
	targetDir := filepath.Join(prefix, "docs")
	rows := []snapshotRestoreRow{{Path: "docs/a.txt", LogicalFileID: 1}}

	if _, err := os.Stat(targetDir); !os.IsNotExist(err) {
		t.Fatalf("expected target directory to not exist before planning, err=%v", err)
	}

	_, err := planSnapshotRestoreOutputs(rows, []string{"docs/"}, RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationPrefix,
		Destination:     prefix,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("planSnapshotRestoreOutputs should succeed without side effects: %v", err)
	}

	if _, err := os.Stat(targetDir); !os.IsNotExist(err) {
		t.Fatalf("expected planner to remain side-effect free, target dir stat err=%v", err)
	}
}

// ---- End-to-end RestoreSnapshot tests ----

// TestSnapshotRestoreSelectionFullNoFilters verifies that resolveSnapshotRestoreSelection
// selects all snapshot_file rows when no path filters are provided.
func TestSnapshotRestoreSelectionFullNoFilters(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot with two files
	logicalA := insertLogicalFileWithSize(t, db, "hash-full-a", 5)
	logicalB := insertLogicalFileWithSize(t, db, "hash-full-b", 7)
	s := Snapshot{ID: "snap-full", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	// Insert snapshot_file rows with metadata
	mode644 := sql.NullInt64{Int64: int64(0o644), Valid: true}
	mtime := sql.NullTime{Time: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, mode644, mtime)
	insertSnapshotFileRow(t, db, s.ID, "img/b.png", logicalB, mode644, mtime)

	// Test: Verify RestoreSnapshot can be called with full restore (no paths)
	// Note: Actual file restoration requires storage backend setup; this test validates the selection/planning logic

	// Get the selection without running full restore (to avoid storage backend dependency)
	rows, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{}, nil)
	if err != nil {
		t.Fatalf("resolve selection for full restore: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for full restore, got %d", len(rows))
	}
}

// TestSnapshotRestoreSelectionPartialExact verifies that an exact path filter selects only
// the matching snapshot_file row.
func TestSnapshotRestoreSelectionPartialExact(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot with exact file paths
	logicalA := insertLogicalFileWithSize(t, db, "hash-exact-a", 5)
	logicalB := insertLogicalFileWithSize(t, db, "hash-exact-b", 7)
	s := Snapshot{ID: "snap-exact", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	// Test: Partial restore with exact path - verify selection works correctly
	rows, exact, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/a.txt"}, nil)
	if err != nil {
		t.Fatalf("RestoreSnapshot partial exact: %v", err)
	}
	if len(rows) != 1 || rows[0].Path != "docs/a.txt" {
		t.Fatalf("expected 1 row for exact restore, got %d", len(rows))
	}
	if len(exact) != 1 || exact[0] != "docs/a.txt" {
		t.Fatalf("expected exact filter 'docs/a.txt', got %v", exact)
	}
}

// TestSnapshotRestoreSelectionPartialDirectory verifies that a directory prefix filter
// (trailing slash) selects all files under that directory.
func TestSnapshotRestoreSelectionPartialDirectory(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot with multiple files in directory
	logicalA := insertLogicalFileWithSize(t, db, "hash-dir-a", 5)
	logicalB := insertLogicalFileWithSize(t, db, "hash-dir-b", 7)
	s := Snapshot{ID: "snap-dir", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	// Test: Partial restore with directory prefix (trailing slash) - verify selection matches multiple files
	rows, exact, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/"}, nil)
	if err != nil {
		t.Fatalf("RestoreSnapshot partial directory: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for directory prefix, got %d", len(rows))
	}
	if len(exact) != 0 {
		t.Fatalf("expected 0 exact filters for directory prefix, got %d", len(exact))
	}
}

func TestRestoreSnapshotExactMissingPathFails(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot with one file
	logicalA := insertLogicalFileWithSize(t, db, "hash-missing", 5)
	s := Snapshot{ID: "snap-missing", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	sgctx := storage.StorageContext{DB: db}
	opts := RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOriginal,
		StorageContext:  &sgctx,
	}

	// Test: Request exact path that doesn't exist in snapshot
	_, err := RestoreSnapshot(ctx, db, s.ID, []string{"docs/missing.txt"}, opts)
	if err == nil || !strings.Contains(err.Error(), "path not found in snapshot") {
		t.Fatalf("expected exact missing path error, got: %v", err)
	}
}

// TestSnapshotFileRowPreservesMetadata verifies that mode and mtime stored in snapshot_file
// are preserved and surfaced by resolveSnapshotRestoreSelection.
func TestSnapshotFileRowPreservesMetadata(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot with specific metadata
	logicalA := insertLogicalFileWithSize(t, db, "hash-meta", 5)
	s := Snapshot{ID: "snap-meta", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	mode600 := sql.NullInt64{Int64: int64(0o600), Valid: true}
	mtime := sql.NullTime{Time: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	insertSnapshotFileRow(t, db, s.ID, "secret.txt", logicalA, mode600, mtime)

	// Note: We can't fully test metadata application without actual storage integration,
	// but we can test that the metadata is preserved in snapshot_file rows and passed through planning
	rows, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"secret.txt"}, nil)
	if err != nil {
		t.Fatalf("resolve selection: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if !rows[0].Mode.Valid || rows[0].Mode.Int64 != int64(0o600) {
		t.Fatalf("expected mode 0o600, got %v", rows[0].Mode)
	}
	if !rows[0].MTime.Valid || rows[0].MTime.Time != mtime.Time {
		t.Fatalf("expected mtime %v, got %v", mtime.Time, rows[0].MTime.Time)
	}
}

// TestSnapshotRestoreSelectionReturnsRowsForNoMetadataOpts verifies that the selection
// phase is independent of the NoMetadata flag (which only affects execution).
func TestSnapshotRestoreSelectionReturnsRowsForNoMetadataOpts(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot with metadata that should be skipped
	logicalA := insertLogicalFileWithSize(t, db, "hash-nometa", 5)
	s := Snapshot{ID: "snap-nometa", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	mode600 := sql.NullInt64{Int64: int64(0o600), Valid: true}
	mtime := sql.NullTime{Time: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	insertSnapshotFileRow(t, db, s.ID, "file.txt", logicalA, mode600, mtime)

	// Test: Verify that --no-metadata flag is accepted in options
	// (actual skipping is tested in the applySnapshotMetadata unit tests and integration tests)
	// Verify that RestoreSnapshot setup phase accepts the NoMetadata option
	// by calling resolveSnapshotRestoreSelection which doesn't validate metadata flags
	rows, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{}, nil)
	if err != nil {
		t.Fatalf("RestoreSnapshot with --no-metadata: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row selected, got %d", len(rows))
	}
}

func TestRestoreSnapshotStrictMetadataRejectedTogetherWithNoMetadata(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot
	logicalA := insertLogicalFileWithSize(t, db, "hash-strict", 5)
	s := Snapshot{ID: "snap-strict", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	insertSnapshotFileRow(t, db, s.ID, "file.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	sgctx := storage.StorageContext{DB: db}
	opts := RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOriginal,
		StorageContext:  &sgctx,
		StrictMetadata:  true,
		NoMetadata:      true, // Both flags set - should fail
	}

	// Test: RestoreSnapshot rejects conflicting metadata flags
	_, err := RestoreSnapshot(ctx, db, s.ID, []string{}, opts)
	if err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("expected strict+no-metadata conflict error, got: %v", err)
	}
}

func TestRestoreSnapshotEmptyDirectoryPrefix(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot with files in different directory
	logicalA := insertLogicalFileWithSize(t, db, "hash-empty", 5)
	s := Snapshot{ID: "snap-empty", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	sgctx := storage.StorageContext{DB: db}
	opts := RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOriginal,
		StorageContext:  &sgctx,
	}

	// Test: Request directory prefix that matches no files - should succeed with zero restores
	result, err := RestoreSnapshot(ctx, db, s.ID, []string{"empty/"}, opts)
	if err != nil {
		t.Fatalf("RestoreSnapshot empty directory prefix: %v", err)
	}
	if result.RestoredFiles != 0 {
		t.Fatalf("expected 0 restored files for empty directory prefix, got %d", result.RestoredFiles)
	}
}

func TestRestoreSnapshotInvalidSnapshotID(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	sgctx := storage.StorageContext{DB: db}
	opts := RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOriginal,
		StorageContext:  &sgctx,
	}

	// Test: Request non-existent snapshot
	_, err := RestoreSnapshot(ctx, db, "nonexistent-snap", []string{}, opts)
	if err == nil || !strings.Contains(err.Error(), "snapshot not found") {
		t.Fatalf("expected snapshot not found error, got: %v", err)
	}
}

func TestRestoreSnapshotNilStorageContext(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Setup: Create snapshot
	logicalA := insertLogicalFileWithSize(t, db, "hash-nil-ctx", 5)
	s := Snapshot{ID: "snap-nil", CreatedAt: time.Now().UTC(), Type: "partial"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	insertSnapshotFileRow(t, db, s.ID, "file.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	opts := RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOriginal,
		StorageContext:  nil, // Missing required storage context
	}

	// Test: Restore without storage context fails
	_, err := RestoreSnapshot(ctx, db, s.ID, []string{}, opts)
	if err == nil || !strings.Contains(err.Error(), "storage context") {
		t.Fatalf("expected storage context error, got: %v", err)
	}
}

// ---- planSnapshotRestoreOutputs output-path tests ----

// TestPlanSnapshotRestoreOutputsPrefixModeProducesCorrectPaths verifies that prefix mode
// prepends the destination root to each snapshot-relative path, producing correct absolute
// output paths.
func TestPlanSnapshotRestoreOutputsPrefixModeProducesCorrectPaths(t *testing.T) {
	tmp := t.TempDir()
	prefixRoot := filepath.Join(tmp, "restore-out")
	rows := []snapshotRestoreRow{
		{Path: "docs/a.txt", LogicalFileID: 1},
		{Path: "img/b.png", LogicalFileID: 2},
	}

	plans, err := planSnapshotRestoreOutputs(rows, []string{}, RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationPrefix,
		Destination:     prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("planSnapshotRestoreOutputs prefix: %v", err)
	}
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}

	wantDocs := filepath.Join(prefixRoot, "docs", "a.txt")
	wantImg := filepath.Join(prefixRoot, "img", "b.png")
	if plans[0].OutputPath != wantDocs {
		t.Fatalf("plan[0] output path: want %q got %q", wantDocs, plans[0].OutputPath)
	}
	if plans[1].OutputPath != wantImg {
		t.Fatalf("plan[1] output path: want %q got %q", wantImg, plans[1].OutputPath)
	}

	// Verify snapshot paths are preserved in the plan items.
	if plans[0].Path != "docs/a.txt" {
		t.Fatalf("plan[0] snapshot path: want %q got %q", "docs/a.txt", plans[0].Path)
	}
	if plans[1].Path != "img/b.png" {
		t.Fatalf("plan[1] snapshot path: want %q got %q", "img/b.png", plans[1].Path)
	}
}

// TestPlanSnapshotRestoreOutputsOriginalModePreservesRelativePath verifies that original mode
// keeps the snapshot-relative path unchanged as the output path.
func TestPlanSnapshotRestoreOutputsOriginalModePreservesRelativePath(t *testing.T) {
	rows := []snapshotRestoreRow{
		{Path: "docs/a.txt", LogicalFileID: 1},
	}

	plans, err := planSnapshotRestoreOutputs(rows, []string{}, RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOriginal,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("planSnapshotRestoreOutputs original: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}

	// In original mode, output path equals the snapshot relative path (cleaned).
	want := filepath.Clean("docs/a.txt")
	if plans[0].OutputPath != want {
		t.Fatalf("original mode output path: want %q got %q", want, plans[0].OutputPath)
	}
}

// TestPlanSnapshotRestoreOutputsCollisionDetectionTriggers verifies that the planner rejects
// two rows that resolve to the same output path (e.g., duplicate snapshot_file rows).
// The selection phase deduplicates, but the planner enforces this as a safety net.
func TestPlanSnapshotRestoreOutputsCollisionDetectionTriggers(t *testing.T) {
	// Simulate duplicate rows that the selection phase failed to deduplicate.
	rows := []snapshotRestoreRow{
		{Path: "docs/a.txt", LogicalFileID: 1},
		{Path: "docs/a.txt", LogicalFileID: 2}, // same output path → collision
	}

	_, err := planSnapshotRestoreOutputs(rows, []string{}, RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationOriginal,
		Overwrite:       true,
	})
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("expected output path collision error, got: %v", err)
	}
}

// ---- applySnapshotMetadata unit tests ----

// TestApplySnapshotMetadataAppliesChmodAndChtimes verifies that when both mode and mtime are
// valid, applySnapshotMetadata writes them to the target file.
func TestApplySnapshotMetadataAppliesChmodAndChtimes(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.txt")
	if err := os.WriteFile(target, []byte("content"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}

	wantMode := os.FileMode(0o600)
	wantMtime := time.Date(2020, 6, 15, 10, 30, 0, 0, time.UTC)

	opts := RestoreSnapshotOptions{StrictMetadata: false, NoMetadata: false}
	if err := applySnapshotMetadata(
		target,
		sql.NullInt64{Int64: int64(wantMode), Valid: true},
		sql.NullTime{Time: wantMtime, Valid: true},
		opts,
	); err != nil {
		t.Fatalf("applySnapshotMetadata: %v", err)
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat target after metadata: %v", err)
	}
	if got := info.Mode().Perm(); got != wantMode {
		t.Fatalf("mode: want %04o got %04o", wantMode, got)
	}
	if got := info.ModTime().UTC(); !got.Equal(wantMtime) {
		t.Fatalf("mtime: want %v got %v", wantMtime, got)
	}
}

// TestApplySnapshotMetadataNoMetadataSkipsChmodAndChtimes verifies that --no-metadata leaves
// the file's existing mode and mtime untouched.
func TestApplySnapshotMetadataNoMetadataSkipsChmodAndChtimes(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.txt")
	if err := os.WriteFile(target, []byte("content"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}

	before, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	// Pass mode 0o600 and an old mtime; both should be ignored.
	opts := RestoreSnapshotOptions{NoMetadata: true}
	if err := applySnapshotMetadata(
		target,
		sql.NullInt64{Int64: int64(0o600), Valid: true},
		sql.NullTime{Time: time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
		opts,
	); err != nil {
		t.Fatalf("applySnapshotMetadata with NoMetadata should not error: %v", err)
	}

	after, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if after.Mode() != before.Mode() {
		t.Fatalf("--no-metadata must not change mode: before=%v after=%v", before.Mode(), after.Mode())
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("--no-metadata must not change mtime: before=%v after=%v", before.ModTime(), after.ModTime())
	}
}

// TestApplySnapshotMetadataStrictPropagatesChmodError verifies that --strict causes
// applySnapshotMetadata to return an error when chmod fails (e.g., path does not exist).
func TestApplySnapshotMetadataStrictPropagatesChmodError(t *testing.T) {
	nonExistent := filepath.Join(t.TempDir(), "does-not-exist.txt")

	opts := RestoreSnapshotOptions{StrictMetadata: true}
	err := applySnapshotMetadata(
		nonExistent,
		sql.NullInt64{Int64: int64(0o600), Valid: true},
		sql.NullTime{},
		opts,
	)
	if err == nil {
		t.Fatalf("expected --strict to propagate chmod error, got nil")
	}
	if !strings.Contains(err.Error(), "apply snapshot metadata") {
		t.Fatalf("expected 'apply snapshot metadata' in error message, got: %v", err)
	}
}

// TestApplySnapshotMetadataStrictNotSetLogsAndContinues verifies that without --strict,
// a chmod failure is logged but does not cause applySnapshotMetadata to return an error.
func TestApplySnapshotMetadataStrictNotSetLogsAndContinues(t *testing.T) {
	nonExistent := filepath.Join(t.TempDir(), "does-not-exist.txt")

	opts := RestoreSnapshotOptions{StrictMetadata: false}
	err := applySnapshotMetadata(
		nonExistent,
		sql.NullInt64{Int64: int64(0o600), Valid: true},
		sql.NullTime{},
		opts,
	)
	if err != nil {
		t.Fatalf("without --strict, chmod error should be logged not returned; got: %v", err)
	}
}

// ---- Phase 4 snapshot visibility tests ----

func TestListSnapshotsReturnsOrderedRows(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	older := Snapshot{ID: "snap-old", CreatedAt: time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC), Type: "full"}
	newer := Snapshot{ID: "snap-new", CreatedAt: time.Date(2026, 1, 11, 8, 0, 0, 0, time.UTC), Type: "partial", Label: sql.NullString{String: "docs-backup", Valid: true}}
	for _, item := range []Snapshot{older, newer} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	items, err := ListSnapshots(ctx, db, SnapshotListFilter{})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(items))
	}
	if items[0].ID != "snap-new" || items[1].ID != "snap-old" {
		t.Fatalf("expected created_at descending order, got ids=%v,%v", items[0].ID, items[1].ID)
	}
}

func TestSnapshotParentIDRoundTripsInListAndGet(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	parent := Snapshot{ID: "snap-parent-model", CreatedAt: time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC), Type: "full"}
	child := Snapshot{
		ID:        "snap-child-model",
		CreatedAt: time.Date(2026, 1, 11, 8, 0, 0, 0, time.UTC),
		Type:      "partial",
		ParentID:  sql.NullString{String: parent.ID, Valid: true},
	}
	for _, item := range []Snapshot{parent, child} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	listed, err := ListSnapshots(ctx, db, SnapshotListFilter{})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(listed))
	}
	if listed[0].ID != child.ID {
		t.Fatalf("expected child snapshot first by created_at, got %s", listed[0].ID)
	}
	if !listed[0].ParentID.Valid || listed[0].ParentID.String != parent.ID {
		t.Fatalf("expected child parent_id=%q in list, got %+v", parent.ID, listed[0].ParentID)
	}
	if listed[1].ParentID.Valid {
		t.Fatalf("expected parent snapshot parent_id NULL in list, got %+v", listed[1].ParentID)
	}

	gotChild, err := GetSnapshot(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshot child: %v", err)
	}
	if !gotChild.ParentID.Valid || gotChild.ParentID.String != parent.ID {
		t.Fatalf("expected child parent_id=%q in get, got %+v", parent.ID, gotChild.ParentID)
	}
}

func TestListSnapshotsFiltersByTypeLabelAndLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	items := []Snapshot{
		{ID: "snap-a", CreatedAt: time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC), Type: "full", Label: sql.NullString{String: "backup-2026", Valid: true}},
		{ID: "snap-b", CreatedAt: time.Date(2026, 1, 11, 8, 0, 0, 0, time.UTC), Type: "partial", Label: sql.NullString{String: "docs-backup", Valid: true}},
		{ID: "snap-c", CreatedAt: time.Date(2026, 1, 12, 8, 0, 0, 0, time.UTC), Type: "full", Label: sql.NullString{String: "backup-2027", Valid: true}},
	}
	for _, item := range items {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	typeFilter := "full"
	labelFilter := "backup"
	result, err := ListSnapshots(ctx, db, SnapshotListFilter{Type: &typeFilter, Label: &labelFilter, Limit: 1})
	if err != nil {
		t.Fatalf("ListSnapshots with filters: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 filtered snapshot, got %d", len(result))
	}
	if result[0].ID != "snap-c" {
		t.Fatalf("expected latest matching snapshot snap-c, got %s", result[0].ID)
	}
}

func TestListSnapshotsFiltersBySinceUntil(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	items := []Snapshot{
		{ID: "snap-jan", CreatedAt: time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC), Type: "full"},
		{ID: "snap-feb", CreatedAt: time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC), Type: "full"},
		{ID: "snap-mar", CreatedAt: time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC), Type: "full"},
	}
	for _, item := range items {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	since := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	result, err := ListSnapshots(ctx, db, SnapshotListFilter{Since: &since, Until: &until})
	if err != nil {
		t.Fatalf("ListSnapshots since/until: %v", err)
	}
	if len(result) != 1 || result[0].ID != "snap-feb" {
		t.Fatalf("expected only snap-feb in range, got %+v", result)
	}
}

func TestGetSnapshotAndListSnapshotFilesSortedAndLimited(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-show-a", 5)
	logicalB := insertLogicalFileWithSize(t, db, "hash-show-b", 7)
	logicalC := insertLogicalFileWithSize(t, db, "hash-show-c", 9)
	s := Snapshot{ID: "snap-show", CreatedAt: time.Now().UTC(), Type: "partial", Label: sql.NullString{String: "inspect", Valid: true}}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "img/x.png", logicalC, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	gotSnapshot, err := GetSnapshot(ctx, db, s.ID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if gotSnapshot.ID != s.ID || gotSnapshot.Type != s.Type {
		t.Fatalf("snapshot metadata mismatch: got=%+v want=%+v", gotSnapshot, s)
	}

	files, err := ListSnapshotFiles(ctx, db, s.ID, 2, nil)
	if err != nil {
		t.Fatalf("ListSnapshotFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 limited files, got %d", len(files))
	}
	if files[0].Path != "docs/a.txt" || files[1].Path != "docs/b.txt" {
		t.Fatalf("expected sorted files docs/a.txt, docs/b.txt; got %+v", files)
	}
}

func TestListSnapshotFilesMissingSnapshotFails(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_, err := ListSnapshotFiles(ctx, db, "snap-missing-show", 10, nil)
	if err == nil || !strings.Contains(err.Error(), "snapshot not found") {
		t.Fatalf("expected missing snapshot error, got: %v", err)
	}
}

func TestGetSnapshotStatsGlobalAndPerSnapshot(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-stats-a", 5)
	logicalB := insertLogicalFileWithSize(t, db, "hash-stats-b", 7)
	s1 := Snapshot{ID: "snap-stats-1", CreatedAt: time.Now().UTC(), Type: "full"}
	s2 := Snapshot{ID: "snap-stats-2", CreatedAt: time.Now().UTC().Add(time.Second), Type: "partial"}
	for _, item := range []Snapshot{s1, s2} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}
	insertSnapshotFileRow(t, db, s1.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s1.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s2.ID, "img/x.png", logicalA, sql.NullInt64{}, sql.NullTime{})

	globalStats, err := GetSnapshotStats(ctx, db, "")
	if err != nil {
		t.Fatalf("GetSnapshotStats global: %v", err)
	}
	if globalStats.SnapshotCount != 2 || globalStats.SnapshotFileCount != 3 || globalStats.TotalSizeBytes != 3 {
		t.Fatalf("unexpected global stats: %+v", globalStats)
	}

	perSnapshot, err := GetSnapshotStats(ctx, db, s1.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats per snapshot: %v", err)
	}
	if perSnapshot.SnapshotCount != 1 || perSnapshot.SnapshotFileCount != 2 || perSnapshot.TotalSizeBytes != 2 {
		t.Fatalf("unexpected per-snapshot stats: %+v", perSnapshot)
	}
	if perSnapshot.ParentSnapshotID.Valid || perSnapshot.ReusedFileCount.Valid || perSnapshot.NewFileCount.Valid || perSnapshot.ReuseRatioPct.Valid {
		t.Fatalf("expected no lineage metrics when snapshot has no parent, got: %+v", perSnapshot)
	}
}

func TestGetSnapshotStatsIncludesLineageReuseBreakdown(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalDocsV1 := insertLogicalFileWithSize(t, db, "hash-lineage-docs-v1", 10)
	logicalDocsV2 := insertLogicalFileWithSize(t, db, "hash-lineage-docs-v2", 10)
	logicalImg := insertLogicalFileWithSize(t, db, "hash-lineage-img", 11)

	parent := Snapshot{ID: "snap-lineage-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/a.txt", logicalDocsV1, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, parent.ID, "img/x.png", logicalImg, sql.NullInt64{}, sql.NullTime{})

	child := Snapshot{
		ID:        "snap-lineage-child",
		CreatedAt: time.Now().UTC().Add(time.Second),
		Type:      "full",
		ParentID:  sql.NullString{String: parent.ID, Valid: true},
	}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "docs/a.txt", logicalDocsV2, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, child.ID, "img/x.png", logicalImg, sql.NullInt64{}, sql.NullTime{})

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child: %v", err)
	}

	if !stats.ParentSnapshotID.Valid || stats.ParentSnapshotID.String != parent.ID {
		t.Fatalf("expected parent snapshot id %q, got %+v", parent.ID, stats.ParentSnapshotID)
	}
	if !stats.ReusedFileCount.Valid || stats.ReusedFileCount.Int64 != 1 {
		t.Fatalf("expected reused_file_count=1, got %+v", stats.ReusedFileCount)
	}
	if !stats.NewFileCount.Valid || stats.NewFileCount.Int64 != 1 {
		t.Fatalf("expected new_file_count=1, got %+v", stats.NewFileCount)
	}
	if !stats.ReuseRatioPct.Valid || stats.ReuseRatioPct.Float64 != 50.0 {
		t.Fatalf("expected reuse_ratio_pct=50.0, got %+v", stats.ReuseRatioPct)
	}
}

func TestGetSnapshotStatsSkipsLineageWhenParentDeleted(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalParent := insertLogicalFileWithSize(t, db, "hash-lineage-parent-deleted", 10)
	logicalChild := insertLogicalFileWithSize(t, db, "hash-lineage-child-deleted", 11)

	parent := Snapshot{ID: "snap-stats-parent-delete", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/a.txt", logicalParent, sql.NullInt64{}, sql.NullTime{})

	child := Snapshot{ID: "snap-stats-child-delete", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full", ParentID: sql.NullString{String: parent.ID, Valid: true}}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "docs/a.txt", logicalChild, sql.NullInt64{}, sql.NullTime{})

	if err := DeleteSnapshot(ctx, db, parent.ID); err != nil {
		t.Fatalf("DeleteSnapshot parent: %v", err)
	}

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child after parent delete: %v", err)
	}
	if stats.SnapshotFileCount != 1 {
		t.Fatalf("expected child total file count=1, got %+v", stats)
	}
	if stats.ParentSnapshotID.Valid || stats.ReusedFileCount.Valid || stats.NewFileCount.Valid || stats.ReuseRatioPct.Valid {
		t.Fatalf("expected lineage metrics to be skipped after parent deletion, got %+v", stats)
	}
}

func TestGetSnapshotStatsSkipsLineageWhenParentMissingMetadataRemains(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lineage-missing-parent", 12)

	child := Snapshot{ID: "snap-stats-child-missing-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	// Simulate legacy/corrupt state where parent metadata points to a missing snapshot.
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-never-existed", child.ID); err != nil {
		t.Fatalf("inject missing parent metadata: %v", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child with missing parent metadata: %v", err)
	}
	if stats.SnapshotFileCount != 1 {
		t.Fatalf("expected child total file count=1, got %+v", stats)
	}
	if stats.ParentSnapshotID.Valid || stats.ReusedFileCount.Valid || stats.NewFileCount.Valid || stats.ReuseRatioPct.Valid {
		t.Fatalf("expected lineage metrics to be skipped when parent is missing, got %+v", stats)
	}
}

func TestGetSnapshotStatsLineageEdgeCaseEmptyChildSnapshot(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lineage-empty-parent", 10)

	parent := Snapshot{ID: "snap-lineage-empty-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	child := Snapshot{ID: "snap-lineage-empty-child", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full", ParentID: sql.NullString{String: parent.ID, Valid: true}}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child: %v", err)
	}
	if stats.SnapshotFileCount != 0 {
		t.Fatalf("expected total files=0, got %+v", stats)
	}
	if !stats.ReusedFileCount.Valid || stats.ReusedFileCount.Int64 != 0 {
		t.Fatalf("expected reused=0, got %+v", stats.ReusedFileCount)
	}
	if !stats.NewFileCount.Valid || stats.NewFileCount.Int64 != 0 {
		t.Fatalf("expected new=0, got %+v", stats.NewFileCount)
	}
	if !stats.ReuseRatioPct.Valid || stats.ReuseRatioPct.Float64 != 0.0 {
		t.Fatalf("expected reuse ratio 0.0, got %+v", stats.ReuseRatioPct)
	}
}

func TestGetSnapshotStatsLineageEdgeCaseIdenticalSnapshots(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lineage-identical-a", 10)
	logicalB := insertLogicalFileWithSize(t, db, "hash-lineage-identical-b", 11)

	parent := Snapshot{ID: "snap-lineage-identical-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, parent.ID, "img/x.png", logicalB, sql.NullInt64{}, sql.NullTime{})

	child := Snapshot{ID: "snap-lineage-identical-child", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full", ParentID: sql.NullString{String: parent.ID, Valid: true}}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, child.ID, "img/x.png", logicalB, sql.NullInt64{}, sql.NullTime{})

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child: %v", err)
	}
	if stats.SnapshotFileCount != 2 {
		t.Fatalf("expected total files=2, got %+v", stats)
	}
	if !stats.ReusedFileCount.Valid || stats.ReusedFileCount.Int64 != 2 {
		t.Fatalf("expected reused=2, got %+v", stats.ReusedFileCount)
	}
	if !stats.NewFileCount.Valid || stats.NewFileCount.Int64 != 0 {
		t.Fatalf("expected new=0, got %+v", stats.NewFileCount)
	}
	if !stats.ReuseRatioPct.Valid || stats.ReuseRatioPct.Float64 != 100.0 {
		t.Fatalf("expected reuse ratio 100.0, got %+v", stats.ReuseRatioPct)
	}
}

func TestGetSnapshotStatsLineageEdgeCaseCompletelyDifferentSnapshots(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalParent := insertLogicalFileWithSize(t, db, "hash-lineage-diff-parent", 10)
	logicalChildA := insertLogicalFileWithSize(t, db, "hash-lineage-diff-child-a", 11)
	logicalChildB := insertLogicalFileWithSize(t, db, "hash-lineage-diff-child-b", 12)

	parent := Snapshot{ID: "snap-lineage-diff-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/a.txt", logicalParent, sql.NullInt64{}, sql.NullTime{})

	child := Snapshot{ID: "snap-lineage-diff-child", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full", ParentID: sql.NullString{String: parent.ID, Valid: true}}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "new/a.txt", logicalChildA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, child.ID, "new/b.txt", logicalChildB, sql.NullInt64{}, sql.NullTime{})

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child: %v", err)
	}
	if stats.SnapshotFileCount != 2 {
		t.Fatalf("expected total files=2, got %+v", stats)
	}
	if !stats.ReusedFileCount.Valid || stats.ReusedFileCount.Int64 != 0 {
		t.Fatalf("expected reused=0, got %+v", stats.ReusedFileCount)
	}
	if !stats.NewFileCount.Valid || stats.NewFileCount.Int64 != 2 {
		t.Fatalf("expected new=2, got %+v", stats.NewFileCount)
	}
	if !stats.ReuseRatioPct.Valid || stats.ReuseRatioPct.Float64 != 0.0 {
		t.Fatalf("expected reuse ratio 0.0, got %+v", stats.ReuseRatioPct)
	}
}

func TestGetSnapshotStatsCountsBrandNewPathAsNew(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalShared := insertLogicalFileWithSize(t, db, "hash-lineage-new-shared", 10)
	logicalNew := insertLogicalFileWithSize(t, db, "hash-lineage-new-path", 11)

	parent := Snapshot{ID: "snap-lineage-new-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/a.txt", logicalShared, sql.NullInt64{}, sql.NullTime{})

	child := Snapshot{ID: "snap-lineage-new-child", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full", ParentID: sql.NullString{String: parent.ID, Valid: true}}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "docs/a.txt", logicalShared, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, child.ID, "docs/new.txt", logicalNew, sql.NullInt64{}, sql.NullTime{})

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child: %v", err)
	}
	if stats.SnapshotFileCount != 2 {
		t.Fatalf("expected total files=2, got %+v", stats)
	}
	if !stats.ReusedFileCount.Valid || stats.ReusedFileCount.Int64 != 1 {
		t.Fatalf("expected reused=1, got %+v", stats.ReusedFileCount)
	}
	if !stats.NewFileCount.Valid || stats.NewFileCount.Int64 != 1 {
		t.Fatalf("expected new=1 for brand-new child path, got %+v", stats.NewFileCount)
	}
	if !stats.ReuseRatioPct.Valid || stats.ReuseRatioPct.Float64 != 50.0 {
		t.Fatalf("expected reuse ratio 50.0, got %+v", stats.ReuseRatioPct)
	}
}

func TestGetSnapshotStatsIgnoresParentOnlyDeletedFiles(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalKeep := insertLogicalFileWithSize(t, db, "hash-lineage-del-keep", 10)
	logicalDeleted := insertLogicalFileWithSize(t, db, "hash-lineage-del-parent-only", 11)

	parent := Snapshot{ID: "snap-lineage-del-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/keep.txt", logicalKeep, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, parent.ID, "docs/deleted.txt", logicalDeleted, sql.NullInt64{}, sql.NullTime{})

	child := Snapshot{ID: "snap-lineage-del-child", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full", ParentID: sql.NullString{String: parent.ID, Valid: true}}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "docs/keep.txt", logicalKeep, sql.NullInt64{}, sql.NullTime{})

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats child: %v", err)
	}
	if stats.SnapshotFileCount != 1 {
		t.Fatalf("expected child total files=1, got %+v", stats)
	}
	if !stats.ReusedFileCount.Valid || stats.ReusedFileCount.Int64 != 1 {
		t.Fatalf("expected reused=1, got %+v", stats.ReusedFileCount)
	}
	if !stats.NewFileCount.Valid || stats.NewFileCount.Int64 != 0 {
		t.Fatalf("expected new=0 (parent-only deleted path must not be counted), got %+v", stats.NewFileCount)
	}
	if !stats.ReuseRatioPct.Valid || stats.ReuseRatioPct.Float64 != 100.0 {
		t.Fatalf("expected reuse ratio 100.0, got %+v", stats.ReuseRatioPct)
	}
}

func TestGetSnapshotStatsSkipsLineageForPartialSnapshotParentMetadata(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lineage-partial-scope", 10)

	parent := Snapshot{ID: "snap-lineage-partial-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, parent); err != nil {
		t.Fatalf("InsertSnapshot parent: %v", err)
	}

	child := Snapshot{ID: "snap-lineage-partial-child", CreatedAt: time.Now().UTC().Add(time.Second), Type: "partial"}
	if err := InsertSnapshot(ctx, db, child); err != nil {
		t.Fatalf("InsertSnapshot partial child: %v", err)
	}
	insertSnapshotFileRow(t, db, child.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	// Simulate legacy/corrupt partial lineage metadata and ensure stats does not
	// attempt misleading cross-scope reused/new computation.
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, parent.ID, child.ID); err != nil {
		t.Fatalf("inject partial lineage metadata: %v", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	stats, err := GetSnapshotStats(ctx, db, child.ID)
	if err != nil {
		t.Fatalf("GetSnapshotStats partial child: %v", err)
	}
	if stats.SnapshotFileCount != 1 {
		t.Fatalf("expected partial child total files=1, got %+v", stats)
	}
	if stats.ParentSnapshotID.Valid || stats.ReusedFileCount.Valid || stats.NewFileCount.Valid || stats.ReuseRatioPct.Valid {
		t.Fatalf("expected lineage metrics to be skipped for partial snapshot scope, got %+v", stats)
	}
}

func TestDeleteSnapshotRemovesSnapshotRowsOnly(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-delete-a", 5)
	logicalB := insertLogicalFileWithSize(t, db, "hash-delete-b", 7)
	s1 := Snapshot{ID: "snap-delete-1", CreatedAt: time.Now().UTC(), Type: "full"}
	s2 := Snapshot{ID: "snap-delete-2", CreatedAt: time.Now().UTC().Add(time.Second), Type: "partial"}
	for _, item := range []Snapshot{s1, s2} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}
	insertSnapshotFileRow(t, db, s1.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s1.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s2.ID, "img/x.png", logicalA, sql.NullInt64{}, sql.NullTime{})

	before, err := retention.ComputeReachabilitySummary(ctx, db)
	if err != nil {
		t.Fatalf("ComputeReachabilitySummary before delete: %v", err)
	}
	if _, ok := before.RetainedLogicalIDs[logicalA]; !ok {
		t.Fatalf("expected logicalA=%d to be retained before delete", logicalA)
	}
	if _, ok := before.RetainedLogicalIDs[logicalB]; !ok {
		t.Fatalf("expected logicalB=%d to be retained before delete", logicalB)
	}

	if err := DeleteSnapshot(ctx, db, s1.ID); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, s1.ID).Scan(&snapshotCount); err != nil {
		t.Fatalf("query deleted snapshot: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected deleted snapshot row to be gone, got count=%d", snapshotCount)
	}

	var deletedFileCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, s1.ID).Scan(&deletedFileCount); err != nil {
		t.Fatalf("query deleted snapshot files: %v", err)
	}
	if deletedFileCount != 0 {
		t.Fatalf("expected deleted snapshot_file rows to be gone, got count=%d", deletedFileCount)
	}

	var survivingSnapshotCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, s2.ID).Scan(&survivingSnapshotCount); err != nil {
		t.Fatalf("query surviving snapshot: %v", err)
	}
	if survivingSnapshotCount != 1 {
		t.Fatalf("expected other snapshot to remain, got count=%d", survivingSnapshotCount)
	}

	var logicalCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM logical_file`).Scan(&logicalCount); err != nil {
		t.Fatalf("query logical_file count: %v", err)
	}
	if logicalCount != 2 {
		t.Fatalf("expected logical_file rows untouched, got count=%d", logicalCount)
	}

	var logicalBCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = ?`, logicalB).Scan(&logicalBCount); err != nil {
		t.Fatalf("query logical_file logicalB=%d: %v", logicalB, err)
	}
	if logicalBCount != 1 {
		t.Fatalf("expected snapshot delete to leave logicalB row intact, got count=%d", logicalBCount)
	}

	after, err := retention.ComputeReachabilitySummary(ctx, db)
	if err != nil {
		t.Fatalf("ComputeReachabilitySummary after delete: %v", err)
	}
	if _, ok := after.RetainedLogicalIDs[logicalA]; !ok {
		t.Fatalf("expected logicalA=%d to remain retained after delete", logicalA)
	}
	if _, ok := after.RetainedLogicalIDs[logicalB]; ok {
		t.Fatalf("expected logicalB=%d to become unretained after deleting snapshot %s", logicalB, s1.ID)
	}
}

// ---- Phase 5 snapshot diff tests ----

func TestDiffSnapshotsEmptyVsEmpty(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-empty-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-empty-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, item := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots empty vs empty: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Fatalf("expected no diff entries, got %d", len(result.Entries))
	}
	if result.Summary.Added != 0 || result.Summary.Removed != 0 || result.Summary.Modified != 0 {
		t.Fatalf("expected zero summary, got %+v", result.Summary)
	}
}

func TestDiffSnapshotsIdenticalNoChanges(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logical := insertLogicalFileWithSize(t, db, "hash-diff-identical", 10)
	base := Snapshot{ID: "snap-diff-identical-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-identical-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, item := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	insertSnapshotFileRow(t, db, base.ID, "docs/a.txt", logical, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/a.txt", logical, sql.NullInt64{Int64: int64(0o600), Valid: true}, sql.NullTime{Time: time.Now().UTC(), Valid: true})

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots identical: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Fatalf("expected no diff entries for identical logical IDs, got %+v", result.Entries)
	}
}

func TestDiffSnapshotsAddedRemovedModifiedMixed(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalBaseOnly := insertLogicalFileWithSize(t, db, "hash-diff-base-only", 10)
	logicalTargetOnly := insertLogicalFileWithSize(t, db, "hash-diff-target-only", 11)
	logicalModA := insertLogicalFileWithSize(t, db, "hash-diff-mod-a", 12)
	logicalModB := insertLogicalFileWithSize(t, db, "hash-diff-mod-b", 13)

	base := Snapshot{ID: "snap-diff-mixed-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-mixed-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, item := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	insertSnapshotFileRow(t, db, base.ID, "docs/old.txt", logicalBaseOnly, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, base.ID, "docs/config.yaml", logicalModA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/new.txt", logicalTargetOnly, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/config.yaml", logicalModB, sql.NullInt64{}, sql.NullTime{})

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots mixed: %v", err)
	}

	if result.Summary.Added != 1 || result.Summary.Removed != 1 || result.Summary.Modified != 1 {
		t.Fatalf("unexpected summary: %+v", result.Summary)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Entries))
	}

	if result.Entries[0].Path != "docs/config.yaml" || result.Entries[0].Type != "modified" {
		t.Fatalf("expected first entry modified docs/config.yaml, got %+v", result.Entries[0])
	}
	if result.Entries[1].Path != "docs/new.txt" || result.Entries[1].Type != "added" {
		t.Fatalf("expected second entry added docs/new.txt, got %+v", result.Entries[1])
	}
	if result.Entries[2].Path != "docs/old.txt" || result.Entries[2].Type != "removed" {
		t.Fatalf("expected third entry removed docs/old.txt, got %+v", result.Entries[2])
	}
}

func TestDiffSnapshotsDeterministicAcrossInsertionOrder(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-diff-det-a", 10)
	logicalB := insertLogicalFileWithSize(t, db, "hash-diff-det-b", 11)
	logicalC := insertLogicalFileWithSize(t, db, "hash-diff-det-c", 12)

	base := Snapshot{ID: "snap-diff-det-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-det-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, item := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	// Intentionally unsorted insert order.
	insertSnapshotFileRow(t, db, base.ID, "img/z.png", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, base.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/a.txt", logicalC, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	result1, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots first run: %v", err)
	}
	result2, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots second run: %v", err)
	}

	if len(result1.Entries) != len(result2.Entries) {
		t.Fatalf("determinism mismatch in entry count: %d vs %d", len(result1.Entries), len(result2.Entries))
	}
	for i := range result1.Entries {
		if result1.Entries[i].Path != result2.Entries[i].Path || result1.Entries[i].Type != result2.Entries[i].Type {
			t.Fatalf("determinism mismatch at %d: %+v vs %+v", i, result1.Entries[i], result2.Entries[i])
		}
	}
}

func TestDiffSnapshotsPathNormalizationBackslashMatchesSlash(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logical := insertLogicalFileWithSize(t, db, "hash-diff-norm", 10)
	base := Snapshot{ID: "snap-diff-norm-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-norm-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, item := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	insertSnapshotFileRow(t, db, base.ID, "docs\\a.txt", logical, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/a.txt", logical, sql.NullInt64{}, sql.NullTime{})

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots normalization: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Fatalf("expected normalized path equality, got diff entries: %+v", result.Entries)
	}
}

func TestDiffSnapshotsErrorsForMissingAndEmptyIDs(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if _, err := DiffSnapshots(ctx, db, "", "snap-target", nil); err == nil || !strings.Contains(err.Error(), "base snapshot id cannot be empty") {
		t.Fatalf("expected base empty id error, got: %v", err)
	}
	if _, err := DiffSnapshots(ctx, db, "snap-base", "", nil); err == nil || !strings.Contains(err.Error(), "target snapshot id cannot be empty") {
		t.Fatalf("expected target empty id error, got: %v", err)
	}
	if _, err := DiffSnapshots(ctx, db, "missing-base", "missing-target", nil); err == nil || !strings.Contains(err.Error(), "snapshot not found") {
		t.Fatalf("expected missing base snapshot error, got: %v", err)
	}

	base := Snapshot{ID: "snap-diff-error-base", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, base); err != nil {
		t.Fatalf("InsertSnapshot base: %v", err)
	}
	if _, err := DiffSnapshots(ctx, db, base.ID, "missing-target", nil); err == nil || !strings.Contains(err.Error(), "snapshot not found") {
		t.Fatalf("expected missing target snapshot error, got: %v", err)
	}
}

func TestDiffSnapshotsOnlyAdded(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-diff-only-added-a", 10)
	logicalB := insertLogicalFileWithSize(t, db, "hash-diff-only-added-b", 11)

	base := Snapshot{ID: "snap-diff-only-added-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-only-added-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	insertSnapshotFileRow(t, db, target.ID, "new/alpha.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "new/beta.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots only added: %v", err)
	}
	if result.Summary.Added != 2 || result.Summary.Removed != 0 || result.Summary.Modified != 0 {
		t.Fatalf("expected 2 added, got summary=%+v", result.Summary)
	}
	for _, e := range result.Entries {
		if e.Type != "added" {
			t.Fatalf("expected all entries to be 'added', got %+v", e)
		}
	}
}

func TestDiffSnapshotsOnlyRemoved(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-diff-only-removed-a", 10)
	logicalB := insertLogicalFileWithSize(t, db, "hash-diff-only-removed-b", 11)

	base := Snapshot{ID: "snap-diff-only-removed-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-only-removed-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	insertSnapshotFileRow(t, db, base.ID, "old/alpha.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, base.ID, "old/beta.txt", logicalB, sql.NullInt64{}, sql.NullTime{})

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots only removed: %v", err)
	}
	if result.Summary.Removed != 2 || result.Summary.Added != 0 || result.Summary.Modified != 0 {
		t.Fatalf("expected 2 removed, got summary=%+v", result.Summary)
	}
	for _, e := range result.Entries {
		if e.Type != "removed" {
			t.Fatalf("expected all entries to be 'removed', got %+v", e)
		}
	}
}

func TestDiffSnapshotsOnlyModified(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalOld := insertLogicalFileWithSize(t, db, "hash-diff-only-mod-old", 10)
	logicalNew := insertLogicalFileWithSize(t, db, "hash-diff-only-mod-new", 20)

	base := Snapshot{ID: "snap-diff-only-modified-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-only-modified-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	insertSnapshotFileRow(t, db, base.ID, "data/file.bin", logicalOld, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "data/file.bin", logicalNew, sql.NullInt64{}, sql.NullTime{})

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots only modified: %v", err)
	}
	if result.Summary.Modified != 1 || result.Summary.Added != 0 || result.Summary.Removed != 0 {
		t.Fatalf("expected 1 modified, got summary=%+v", result.Summary)
	}
	if len(result.Entries) != 1 || result.Entries[0].Type != "modified" || result.Entries[0].Path != "data/file.bin" {
		t.Fatalf("expected single modified entry for data/file.bin, got %+v", result.Entries)
	}
}

func TestDiffSnapshotsLargeDatasetPerformanceSanity(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-large-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-large-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, item := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}

	const files = 1200
	for i := 0; i < files; i++ {
		logicalBase := insertLogicalFileWithSize(t, db, fmt.Sprintf("hash-diff-large-base-%d", i), int64(10+i))
		logicalTarget := logicalBase
		if i%200 == 0 {
			logicalTarget = insertLogicalFileWithSize(t, db, fmt.Sprintf("hash-diff-large-target-%d", i), int64(20+i))
		}
		path := fmt.Sprintf("docs/file-%04d.txt", i)
		insertSnapshotFileRow(t, db, base.ID, path, logicalBase, sql.NullInt64{}, sql.NullTime{})
		insertSnapshotFileRow(t, db, target.ID, path, logicalTarget, sql.NullInt64{}, sql.NullTime{})
	}

	// Add/Remove specific edge paths.
	baseOnlyLogical := insertLogicalFileWithSize(t, db, "hash-diff-large-base-only", 999)
	targetOnlyLogical := insertLogicalFileWithSize(t, db, "hash-diff-large-target-only", 998)
	insertSnapshotFileRow(t, db, base.ID, "docs/removed.txt", baseOnlyLogical, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/added.txt", targetOnlyLogical, sql.NullInt64{}, sql.NullTime{})

	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, nil)
	if err != nil {
		t.Fatalf("DiffSnapshots large dataset: %v", err)
	}
	if result.Summary.Modified == 0 {
		t.Fatalf("expected at least one modified entry in large dataset, got summary=%+v", result.Summary)
	}
	if result.Summary.Added == 0 || result.Summary.Removed == 0 {
		t.Fatalf("expected added and removed entries in large dataset, got summary=%+v", result.Summary)
	}
}

// ---- SnapshotQuery.Match() unit tests ----

func TestSnapshotQueryMatchNilMatchesAll(t *testing.T) {
	var q *SnapshotQuery
	e := SnapshotFileEntry{Path: "docs/a.txt", Size: sql.NullInt64{Int64: 100, Valid: true}}
	if !q.Match(e) {
		t.Fatal("nil query should match all entries")
	}
}

func TestSnapshotQueryMatchExactPath(t *testing.T) {
	q := &SnapshotQuery{ExactPaths: map[string]struct{}{"docs/a.txt": {}}}

	cases := []struct {
		path string
		want bool
	}{
		{"docs/a.txt", true},
		{"docs/b.txt", false},
		{"docs/a.txt.bak", false},
		{"", false},
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: tc.path}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match(%q) exact = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchPrefix(t *testing.T) {
	q := &SnapshotQuery{Prefixes: []string{"docs/"}}

	cases := []struct {
		path string
		want bool
	}{
		{"docs/a.txt", true},
		{"docs/sub/b.txt", true},
		{"docs_backup/a.txt", false}, // boundary correctness: "docs_backup/" != "docs/"
		{"img/a.png", false},
		{"docs", false}, // no trailing slash means this is NOT under "docs/"
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: tc.path}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match(%q) prefix = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchGlobPattern(t *testing.T) {
	q := &SnapshotQuery{Pattern: "docs/*.txt"}

	cases := []struct {
		path string
		want bool
	}{
		{"docs/a.txt", true},
		{"docs/readme.txt", true},
		{"docs/a.md", false},
		{"img/a.txt", false},
		{"docs/sub/a.txt", false}, // glob * doesn't match /
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: tc.path}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match(%q) glob = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchGlobPatternDoesNotCrossSlashBoundaries(t *testing.T) {
	q := &SnapshotQuery{Pattern: "*.txt"}

	cases := []struct {
		path string
		want bool
	}{
		{"a.txt", true},
		{"docs/a.txt", false},
		{"docs/sub/a.txt", false},
		{"a.txt.bak", false},
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: tc.path}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match(%q) glob boundary = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchGlobPatternUsesNormalizedSlashPaths(t *testing.T) {
	q := &SnapshotQuery{Pattern: "docs/*.txt"}

	cases := []struct {
		path string
		want bool
	}{
		{"docs/a.txt", true},
		{"docs/sub/a.txt", false},
		{"docs\\a.txt", false},
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: tc.path}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match(%q) normalized slash glob = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchRegex(t *testing.T) {
	re := regexp.MustCompile(`\.log$`)
	q := &SnapshotQuery{Regex: re}

	cases := []struct {
		path string
		want bool
	}{
		{"app.log", true},
		{"logs/app.log", true},
		{"app.log.1", false},
		{"docs/readme.txt", false},
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: tc.path}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match(%q) regex = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchSizeRange(t *testing.T) {
	minSize := int64(100)
	maxSize := int64(500)
	q := &SnapshotQuery{MinSize: &minSize, MaxSize: &maxSize}

	cases := []struct {
		size  sql.NullInt64
		want  bool
		label string
	}{
		{sql.NullInt64{Int64: 100, Valid: true}, true, "at min boundary"},
		{sql.NullInt64{Int64: 500, Valid: true}, true, "at max boundary"},
		{sql.NullInt64{Int64: 300, Valid: true}, true, "within range"},
		{sql.NullInt64{Int64: 99, Valid: true}, false, "below min"},
		{sql.NullInt64{Int64: 501, Valid: true}, false, "above max"},
		{sql.NullInt64{Valid: false}, true, "no size passes both bounds"},
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: "file.txt", Size: tc.size}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match size %v (%s) = %v, want %v", tc.size, tc.label, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchTimeRange(t *testing.T) {
	anchor := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	before := anchor.Add(-time.Hour)
	after := anchor.Add(time.Hour)

	q := &SnapshotQuery{ModifiedAfter: &before, ModifiedBefore: &after}

	cases := []struct {
		mtime sql.NullTime
		want  bool
		label string
	}{
		{sql.NullTime{Time: anchor, Valid: true}, true, "within range"},
		{sql.NullTime{Time: before.Add(-time.Second), Valid: true}, false, "before lower bound"},
		{sql.NullTime{Time: after.Add(time.Second), Valid: true}, false, "after upper bound"},
		{sql.NullTime{Valid: false}, true, "no mtime passes both bounds"},
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: "file.txt", MTime: tc.mtime}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match mtime %v (%s) = %v, want %v", tc.mtime, tc.label, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchCombinedFilters(t *testing.T) {
	minSize := int64(50)
	re := regexp.MustCompile(`\.txt$`)
	q := &SnapshotQuery{
		Prefixes: []string{"docs/"},
		Regex:    re,
		MinSize:  &minSize,
	}

	cases := []struct {
		path  string
		size  sql.NullInt64
		want  bool
		label string
	}{
		{"docs/readme.txt", sql.NullInt64{Int64: 100, Valid: true}, true, "all pass"},
		{"img/readme.txt", sql.NullInt64{Int64: 100, Valid: true}, false, "wrong prefix"},
		{"docs/readme.md", sql.NullInt64{Int64: 100, Valid: true}, false, "wrong extension (regex)"},
		{"docs/readme.txt", sql.NullInt64{Int64: 10, Valid: true}, false, "too small"},
		{"docs/readme.txt", sql.NullInt64{Valid: false}, true, "no size passes min check"},
	}
	for _, tc := range cases {
		e := SnapshotFileEntry{Path: tc.path, Size: tc.size}
		got := q.Match(e)
		if got != tc.want {
			t.Errorf("Match combined (%s) = %v, want %v", tc.label, got, tc.want)
		}
	}
}

func TestSnapshotQueryMatchEmptyQueryMatchesAll(t *testing.T) {
	q := &SnapshotQuery{} // non-nil but empty
	e := SnapshotFileEntry{Path: "anything/file.txt", Size: sql.NullInt64{Int64: 9999, Valid: true}}
	if !q.Match(e) {
		t.Fatal("empty query should match all entries")
	}
}

// TestSnapshotQueryMatchBadPatternNoMatch documents that a malformed glob pattern
// stored in Query.Pattern causes Match() to return false (no match) rather than
// panicking or producing undefined behavior. In practice this state should never
// be reached because parseSnapshotQuery validates the pattern with path.Match.
func TestSnapshotQueryMatchBadPatternNoMatch(t *testing.T) {
	q := &SnapshotQuery{Pattern: "[unclosed"}
	e := SnapshotFileEntry{Path: "docs/a.txt"}
	// Must not panic and must treat it as non-matching.
	if q.Match(e) {
		t.Fatal("malformed glob pattern should not match any path")
	}
}

// ---- ListSnapshotFiles with query tests ----

func TestListSnapshotFilesWithPrefixQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lsf-q-a", 100)
	logicalB := insertLogicalFileWithSize(t, db, "hash-lsf-q-b", 200)
	logicalC := insertLogicalFileWithSize(t, db, "hash-lsf-q-c", 300)

	s := Snapshot{ID: "snap-lsf-query", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "img/c.png", logicalC, sql.NullInt64{}, sql.NullTime{})

	q := &SnapshotQuery{Prefixes: []string{"docs/"}}
	files, err := ListSnapshotFiles(ctx, db, s.ID, 0, q)
	if err != nil {
		t.Fatalf("ListSnapshotFiles with query: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files under docs/, got %d", len(files))
	}
	for _, f := range files {
		if !strings.HasPrefix(f.Path, "docs/") {
			t.Errorf("unexpected path %q not under docs/", f.Path)
		}
	}
}

func TestListSnapshotFilesWithPatternQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lsf-pat-a", 100)
	logicalB := insertLogicalFileWithSize(t, db, "hash-lsf-pat-b", 200)
	logicalC := insertLogicalFileWithSize(t, db, "hash-lsf-pat-c", 300)

	s := Snapshot{ID: "snap-lsf-pattern", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.md", logicalB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "img/c.png", logicalC, sql.NullInt64{}, sql.NullTime{})

	q := &SnapshotQuery{Pattern: "docs/*.txt"}
	files, err := ListSnapshotFiles(ctx, db, s.ID, 0, q)
	if err != nil {
		t.Fatalf("ListSnapshotFiles with pattern: %v", err)
	}
	if len(files) != 1 || files[0].Path != "docs/a.txt" {
		t.Fatalf("expected only docs/a.txt, got %+v", files)
	}
}

func TestListSnapshotFilesNormalizesPathBeforeQueryMatch(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lsf-norm-a", 100)
	s := Snapshot{ID: "snap-lsf-normalized-query", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	// Insert a legacy/backslash row directly to verify list/query normalizes before matching.
	insertSnapshotFileRow(t, db, s.ID, "docs\\a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	q := &SnapshotQuery{Pattern: "docs/*.txt"}
	files, err := ListSnapshotFiles(ctx, db, s.ID, 0, q)
	if err != nil {
		t.Fatalf("ListSnapshotFiles with normalized matching: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 matched file, got %d", len(files))
	}
	if files[0].Path != "docs/a.txt" {
		t.Fatalf("expected normalized output path docs/a.txt, got %q", files[0].Path)
	}
}

func TestListSnapshotFilesQueryEmptyResult(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-lsf-empty-q", 100)
	s := Snapshot{ID: "snap-lsf-empty-q", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logicalA, sql.NullInt64{}, sql.NullTime{})

	q := &SnapshotQuery{Prefixes: []string{"nonexistent/"}}
	files, err := ListSnapshotFiles(ctx, db, s.ID, 0, q)
	if err != nil {
		t.Fatalf("ListSnapshotFiles empty result: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(files))
	}
}

func TestListSnapshotFilesQueryWithLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{ID: "snap-lsf-qlimit", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	for i := 0; i < 5; i++ {
		logical := insertLogicalFileWithSize(t, db, fmt.Sprintf("hash-lsf-ql-%d", i), int64(100+i))
		insertSnapshotFileRow(t, db, s.ID, fmt.Sprintf("docs/file%d.txt", i), logical, sql.NullInt64{}, sql.NullTime{})
	}

	q := &SnapshotQuery{Prefixes: []string{"docs/"}}
	files, err := ListSnapshotFiles(ctx, db, s.ID, 3, q)
	if err != nil {
		t.Fatalf("ListSnapshotFiles with limit: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 files with limit=3, got %d", len(files))
	}
}

func TestListSnapshotFilesQueryDeterministicOrder(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{ID: "snap-lsf-qorder", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	for _, name := range []string{"z.txt", "a.txt", "m.txt"} {
		logical := insertLogicalFileWithSize(t, db, "hash-lsf-qord-"+name, 100)
		insertSnapshotFileRow(t, db, s.ID, "docs/"+name, logical, sql.NullInt64{}, sql.NullTime{})
	}

	q := &SnapshotQuery{Prefixes: []string{"docs/"}}
	files, err := ListSnapshotFiles(ctx, db, s.ID, 0, q)
	if err != nil {
		t.Fatalf("ListSnapshotFiles order: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(files))
	}
	// Order must be deterministic: a.txt < m.txt < z.txt
	if files[0].Path != "docs/a.txt" || files[1].Path != "docs/m.txt" || files[2].Path != "docs/z.txt" {
		t.Fatalf("unexpected order: %v", files)
	}
}

// ---- DiffSnapshots with query tests ----

func TestDiffSnapshotsWithPrefixQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-q-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-q-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	logA := insertLogicalFileWithSize(t, db, "hash-dq-a", 100)
	logB := insertLogicalFileWithSize(t, db, "hash-dq-b", 200)
	logC := insertLogicalFileWithSize(t, db, "hash-dq-c", 300)
	logD := insertLogicalFileWithSize(t, db, "hash-dq-d", 400)

	// base: docs/a.txt, img/x.png
	// target: docs/b.txt (added), img/x.png (unchanged), docs/a.txt removed
	insertSnapshotFileRow(t, db, base.ID, "docs/a.txt", logA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, base.ID, "img/x.png", logB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/b.txt", logC, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "img/x.png", logB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "img/y.png", logD, sql.NullInt64{}, sql.NullTime{})

	// Filter to only docs/ — should see docs/a.txt removed, docs/b.txt added; img/ ignored
	q := &SnapshotQuery{Prefixes: []string{"docs/"}}
	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, q)
	if err != nil {
		t.Fatalf("DiffSnapshots with query: %v", err)
	}
	if result.Summary.Added != 1 || result.Summary.Removed != 1 || result.Summary.Modified != 0 {
		t.Fatalf("unexpected summary with docs/ query: %+v", result.Summary)
	}
	for _, e := range result.Entries {
		if !strings.HasPrefix(e.Path, "docs/") {
			t.Errorf("unexpected non-docs path in filtered diff: %s", e.Path)
		}
	}
}

func TestDiffSnapshotsWithRegexQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-qre-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-qre-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	logA := insertLogicalFileWithSize(t, db, "hash-dqre-a", 100)
	logB := insertLogicalFileWithSize(t, db, "hash-dqre-b", 200)
	logC := insertLogicalFileWithSize(t, db, "hash-dqre-c", 300)
	logD := insertLogicalFileWithSize(t, db, "hash-dqre-d", 400)

	insertSnapshotFileRow(t, db, base.ID, "app.log", logA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, base.ID, "config.yaml", logB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "app.log", logC, sql.NullInt64{}, sql.NullTime{})     // modified
	insertSnapshotFileRow(t, db, target.ID, "config.yaml", logB, sql.NullInt64{}, sql.NullTime{}) // unchanged
	insertSnapshotFileRow(t, db, target.ID, "error.log", logD, sql.NullInt64{}, sql.NullTime{})   // added

	re := regexp.MustCompile(`\.log$`)
	q := &SnapshotQuery{Regex: re}
	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, q)
	if err != nil {
		t.Fatalf("DiffSnapshots with regex: %v", err)
	}
	// Only .log files: app.log modified, error.log added. config.yaml excluded.
	if result.Summary.Added != 1 || result.Summary.Modified != 1 || result.Summary.Removed != 0 {
		t.Fatalf("unexpected regex-filtered diff summary: %+v", result.Summary)
	}
	for _, e := range result.Entries {
		if !strings.HasSuffix(e.Path, ".log") {
			t.Errorf("unexpected non-.log path in filtered diff: %s", e.Path)
		}
	}
}

func TestDiffSnapshotsWithSizeQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-qsize-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-qsize-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	logRemoved := insertLogicalFileWithSize(t, db, "hash-dqsize-removed", 50)
	logModifiedBase := insertLogicalFileWithSize(t, db, "hash-dqsize-mod-base", 120)
	logModifiedTarget := insertLogicalFileWithSize(t, db, "hash-dqsize-mod-target", 260)
	logAdded := insertLogicalFileWithSize(t, db, "hash-dqsize-added", 500)

	insertSnapshotFileRowWithSize(t, db, base.ID, "docs/removed.txt", logRemoved, sql.NullInt64{Int64: 50, Valid: true}, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRowWithSize(t, db, base.ID, "docs/modified.txt", logModifiedBase, sql.NullInt64{Int64: 120, Valid: true}, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRowWithSize(t, db, target.ID, "docs/modified.txt", logModifiedTarget, sql.NullInt64{Int64: 260, Valid: true}, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRowWithSize(t, db, target.ID, "docs/added.txt", logAdded, sql.NullInt64{Int64: 500, Valid: true}, sql.NullInt64{}, sql.NullTime{})

	t.Run("min-size uses diff-side metadata", func(t *testing.T) {
		minSize := int64(300)
		q := &SnapshotQuery{MinSize: &minSize}
		result, err := DiffSnapshots(ctx, db, base.ID, target.ID, q)
		if err != nil {
			t.Fatalf("DiffSnapshots size query: %v", err)
		}
		if result.Summary.Added != 1 || result.Summary.Removed != 0 || result.Summary.Modified != 0 {
			t.Fatalf("unexpected summary for min-size filter: %+v", result.Summary)
		}
		if len(result.Entries) != 1 || result.Entries[0].Path != "docs/added.txt" {
			t.Fatalf("expected only docs/added.txt, got %+v", result.Entries)
		}
	})

	t.Run("max-size can match removed via base metadata", func(t *testing.T) {
		maxSize := int64(60)
		q := &SnapshotQuery{MaxSize: &maxSize}
		result, err := DiffSnapshots(ctx, db, base.ID, target.ID, q)
		if err != nil {
			t.Fatalf("DiffSnapshots size query: %v", err)
		}
		if result.Summary.Added != 0 || result.Summary.Removed != 1 || result.Summary.Modified != 0 {
			t.Fatalf("unexpected summary for max-size filter: %+v", result.Summary)
		}
		if len(result.Entries) != 1 || result.Entries[0].Path != "docs/removed.txt" || result.Entries[0].Type != DiffRemoved {
			t.Fatalf("expected only docs/removed.txt removed entry, got %+v", result.Entries)
		}
	})
}

func TestDiffSnapshotsWithMTimeQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-qtime-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-qtime-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	oldTime := time.Date(2024, 1, 10, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2024, 1, 10, 11, 0, 0, 0, time.UTC)
	cutoff := time.Date(2024, 1, 10, 10, 30, 0, 0, time.UTC)

	logRemoved := insertLogicalFileWithSize(t, db, "hash-dqtime-removed", 50)
	logModifiedBase := insertLogicalFileWithSize(t, db, "hash-dqtime-mod-base", 120)
	logModifiedTarget := insertLogicalFileWithSize(t, db, "hash-dqtime-mod-target", 260)
	logAdded := insertLogicalFileWithSize(t, db, "hash-dqtime-added", 500)

	insertSnapshotFileRowWithSize(t, db, base.ID, "docs/removed.txt", logRemoved, sql.NullInt64{Int64: 50, Valid: true}, sql.NullInt64{}, sql.NullTime{Time: oldTime, Valid: true})
	insertSnapshotFileRowWithSize(t, db, base.ID, "docs/modified.txt", logModifiedBase, sql.NullInt64{Int64: 120, Valid: true}, sql.NullInt64{}, sql.NullTime{Time: oldTime, Valid: true})
	insertSnapshotFileRowWithSize(t, db, target.ID, "docs/modified.txt", logModifiedTarget, sql.NullInt64{Int64: 260, Valid: true}, sql.NullInt64{}, sql.NullTime{Time: newTime, Valid: true})
	insertSnapshotFileRowWithSize(t, db, target.ID, "docs/added.txt", logAdded, sql.NullInt64{Int64: 500, Valid: true}, sql.NullInt64{}, sql.NullTime{Time: newTime, Valid: true})

	q := &SnapshotQuery{ModifiedAfter: &cutoff}
	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, q)
	if err != nil {
		t.Fatalf("DiffSnapshots mtime query: %v", err)
	}
	if result.Summary.Added != 1 || result.Summary.Removed != 0 || result.Summary.Modified != 1 {
		t.Fatalf("unexpected summary for modified-after filter: %+v", result.Summary)
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d: %+v", len(result.Entries), result.Entries)
	}
	for _, entry := range result.Entries {
		if entry.Path == "docs/removed.txt" {
			t.Fatalf("removed entry should be excluded by modified-after cutoff: %+v", entry)
		}
	}
}

func TestDiffSnapshotsQueryEmptyResult(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-qempty-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-qempty-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	logA := insertLogicalFileWithSize(t, db, "hash-dqe-a", 100)
	logB := insertLogicalFileWithSize(t, db, "hash-dqe-b", 200)
	insertSnapshotFileRow(t, db, base.ID, "docs/a.txt", logA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, target.ID, "docs/b.txt", logB, sql.NullInt64{}, sql.NullTime{})

	q := &SnapshotQuery{Prefixes: []string{"nonexistent/"}}
	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, q)
	if err != nil {
		t.Fatalf("DiffSnapshots empty query result: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(result.Entries))
	}
	if result.Summary.Added != 0 || result.Summary.Removed != 0 || result.Summary.Modified != 0 {
		t.Fatalf("expected zero summary, got %+v", result.Summary)
	}
}

func TestDiffSnapshotsQuerySummaryMatchesFilteredEntries(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	base := Snapshot{ID: "snap-diff-qsum-base", CreatedAt: time.Now().UTC(), Type: "full"}
	target := Snapshot{ID: "snap-diff-qsum-target", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full"}
	for _, s := range []Snapshot{base, target} {
		if err := InsertSnapshot(ctx, db, s); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", s.ID, err)
		}
	}

	logA := insertLogicalFileWithSize(t, db, "hash-dqsum-a", 100)
	logB := insertLogicalFileWithSize(t, db, "hash-dqsum-b", 200)
	logC := insertLogicalFileWithSize(t, db, "hash-dqsum-c", 300)
	logD := insertLogicalFileWithSize(t, db, "hash-dqsum-d", 400)

	insertSnapshotFileRow(t, db, base.ID, "docs/a.txt", logA, sql.NullInt64{}, sql.NullTime{})    // removed
	insertSnapshotFileRow(t, db, base.ID, "docs/b.txt", logB, sql.NullInt64{}, sql.NullTime{})    // modified
	insertSnapshotFileRow(t, db, base.ID, "other/x.txt", logC, sql.NullInt64{}, sql.NullTime{})   // not in filter
	insertSnapshotFileRow(t, db, target.ID, "docs/b.txt", logD, sql.NullInt64{}, sql.NullTime{})  // modified
	insertSnapshotFileRow(t, db, target.ID, "docs/c.txt", logC, sql.NullInt64{}, sql.NullTime{})  // added
	insertSnapshotFileRow(t, db, target.ID, "other/x.txt", logC, sql.NullInt64{}, sql.NullTime{}) // not in filter

	q := &SnapshotQuery{Prefixes: []string{"docs/"}}
	result, err := DiffSnapshots(ctx, db, base.ID, target.ID, q)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}

	// Verify summary matches entries
	var gotAdded, gotRemoved, gotModified int64
	for _, e := range result.Entries {
		switch e.Type {
		case DiffAdded:
			gotAdded++
		case DiffRemoved:
			gotRemoved++
		case DiffModified:
			gotModified++
		}
	}
	if gotAdded != result.Summary.Added || gotRemoved != result.Summary.Removed || gotModified != result.Summary.Modified {
		t.Fatalf("summary mismatch: summary=%+v entries counts: added=%d removed=%d modified=%d",
			result.Summary, gotAdded, gotRemoved, gotModified)
	}
}

// ---- resolveSnapshotRestoreSelection with query tests ----

func TestResolveSnapshotRestoreSelectionWithQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{ID: "snap-rsel-q", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	logA := insertLogicalFile(t, db, "hash-rsel-qa")
	logB := insertLogicalFile(t, db, "hash-rsel-qb")
	logC := insertLogicalFile(t, db, "hash-rsel-qc")

	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.txt", logB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "img/x.png", logC, sql.NullInt64{}, sql.NullTime{})

	// No positional paths, but query filters to docs/ only
	q := &SnapshotQuery{Prefixes: []string{"docs/"}}
	selected, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, nil, q)
	if err != nil {
		t.Fatalf("resolveSnapshotRestoreSelection with query: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 selected rows, got %d", len(selected))
	}
	for _, row := range selected {
		if !strings.HasPrefix(row.Path, "docs/") {
			t.Errorf("unexpected path %q not under docs/", row.Path)
		}
	}
}

func TestResolveSnapshotRestoreSelectionQueryAndPositionalCombined(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{ID: "snap-rsel-combined", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	logA := insertLogicalFile(t, db, "hash-rsel-ca")
	logB := insertLogicalFile(t, db, "hash-rsel-cb")
	logC := insertLogicalFile(t, db, "hash-rsel-cc")
	logD := insertLogicalFile(t, db, "hash-rsel-cd")

	insertSnapshotFileRow(t, db, s.ID, "docs/a.txt", logA, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/b.md", logB, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "docs/c.txt", logC, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRow(t, db, s.ID, "img/x.png", logD, sql.NullInt64{}, sql.NullTime{})

	// Positional: docs/ (all docs); query: *.txt only
	q := &SnapshotQuery{Pattern: "docs/*.txt"}
	selected, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, []string{"docs/"}, q)
	if err != nil {
		t.Fatalf("resolveSnapshotRestoreSelection combined: %v", err)
	}
	// Should get docs/a.txt and docs/c.txt, not docs/b.md
	if len(selected) != 2 {
		t.Fatalf("expected 2 .txt files, got %d: %+v", len(selected), selected)
	}
	for _, row := range selected {
		if !strings.HasSuffix(row.Path, ".txt") {
			t.Errorf("unexpected non-.txt path %q in combined filter result", row.Path)
		}
	}
}

func TestResolveSnapshotRestoreSelectionSizeFilter(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	s := Snapshot{ID: "snap-rsel-size", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, s); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}

	// Small file: 50 bytes, large file: 500 bytes
	logSmall := insertLogicalFileWithSize(t, db, "hash-rsel-size-small", 50)
	logLarge := insertLogicalFileWithSize(t, db, "hash-rsel-size-large", 500)

	insertSnapshotFileRowWithSize(t, db, s.ID, "docs/small.txt", logSmall, sql.NullInt64{Int64: 50, Valid: true}, sql.NullInt64{}, sql.NullTime{})
	insertSnapshotFileRowWithSize(t, db, s.ID, "docs/large.txt", logLarge, sql.NullInt64{Int64: 500, Valid: true}, sql.NullInt64{}, sql.NullTime{})

	// Filter: only files >= 100 bytes
	minSize := int64(100)
	q := &SnapshotQuery{MinSize: &minSize}
	selected, _, err := resolveSnapshotRestoreSelection(ctx, db, s.ID, nil, q)
	if err != nil {
		t.Fatalf("resolveSnapshotRestoreSelection size filter: %v", err)
	}
	if len(selected) != 1 || selected[0].Path != "docs/large.txt" {
		t.Fatalf("expected only docs/large.txt (size>=100), got %+v", selected)
	}
}
