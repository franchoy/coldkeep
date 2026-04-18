package snapshot

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// openPathResolverDB opens an in-memory SQLite database with only the tables
// needed by the path resolver: schema_version and snapshot_path.
func openPathResolverDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ddl := `
		CREATE TABLE IF NOT EXISTS snapshot_path (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE CHECK (path != '')
		);
	`
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("create snapshot_path table: %v", err)
	}
	return db
}

func TestResolveSnapshotPathInsertsAndReturnsID(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	id, err := ResolveSnapshotPath(ctx, db, "docs/readme.txt")
	if err != nil {
		t.Fatalf("resolve new path: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}
}

func TestResolveSnapshotPathIsIdempotent(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	id1, err := ResolveSnapshotPath(ctx, db, "docs/readme.txt")
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	id2, err := ResolveSnapshotPath(ctx, db, "docs/readme.txt")
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("expected same id on second call, got %d vs %d", id1, id2)
	}
}

func TestResolveSnapshotPathRejectsEmpty(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	if _, err := ResolveSnapshotPath(ctx, db, ""); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestLoadSnapshotPathByIDReturnsPath(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	id, err := ResolveSnapshotPath(ctx, db, "docs/readme.txt")
	if err != nil {
		t.Fatalf("resolve path: %v", err)
	}

	path, err := LoadSnapshotPathByID(ctx, db, id)
	if err != nil {
		t.Fatalf("load path by id: %v", err)
	}
	if path != "docs/readme.txt" {
		t.Fatalf("unexpected path: got %q", path)
	}
}

func TestLoadSnapshotPathByIDRejectsInvalidID(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	if _, err := LoadSnapshotPathByID(ctx, db, 0); err == nil {
		t.Fatal("expected error for non-positive path id")
	}
}

func TestLoadSnapshotPathByIDFailsWhenMissing(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	if _, err := LoadSnapshotPathByID(ctx, db, 9999); err == nil {
		t.Fatal("expected error for missing path id")
	}
}

func TestResolveSnapshotPathsReturnsMapForAllPaths(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	paths := []string{"a/b.txt", "c/d.txt", "e/f.txt"}
	m, err := ResolveSnapshotPaths(ctx, db, paths)
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	for _, p := range paths {
		if _, ok := m[p]; !ok {
			t.Errorf("missing id for path %q", p)
		}
		if m[p] <= 0 {
			t.Errorf("non-positive id for path %q: %d", p, m[p])
		}
	}
}

func TestResolveSnapshotPathsDeduplicatesInput(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	paths := []string{"dup/path.txt", "dup/path.txt", "dup/path.txt"}
	m, err := ResolveSnapshotPaths(ctx, db, paths)
	if err != nil {
		t.Fatalf("resolve duplicate paths: %v", err)
	}
	if len(m) != 1 {
		t.Fatalf("expected 1 unique entry, got %d", len(m))
	}

	var rowCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_path WHERE path = 'dup/path.txt'`).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 row in snapshot_path, got %d", rowCount)
	}
}

func TestResolveSnapshotPathsIsIdempotentAcrossCalls(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	paths := []string{"x/y.txt", "x/z.txt"}

	m1, err := ResolveSnapshotPaths(ctx, db, paths)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	m2, err := ResolveSnapshotPaths(ctx, db, paths)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	for _, p := range paths {
		if m1[p] != m2[p] {
			t.Errorf("id changed across calls for %q: %d vs %d", p, m1[p], m2[p])
		}
	}
}

func TestResolveSnapshotPathsMixedExistingAndNew(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	// Pre-insert one path.
	if _, err := db.Exec(`INSERT INTO snapshot_path (path) VALUES ('pre/existing.txt')`); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}

	paths := []string{"pre/existing.txt", "brand/new.txt"}
	m, err := ResolveSnapshotPaths(ctx, db, paths)
	if err != nil {
		t.Fatalf("resolve mixed: %v", err)
	}
	for _, p := range paths {
		if m[p] <= 0 {
			t.Errorf("non-positive id for %q", p)
		}
	}
	if m["pre/existing.txt"] == m["brand/new.txt"] {
		t.Errorf("distinct paths must not share the same id")
	}
}

func TestResolveSnapshotPathsRejectsEmptyPath(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	if _, err := ResolveSnapshotPaths(ctx, db, []string{"good/path.txt", ""}); err == nil {
		t.Fatal("expected error for empty path in batch")
	}
}

func TestResolveSnapshotPathsEmptyInputReturnsEmptyMap(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	m, err := ResolveSnapshotPaths(ctx, db, nil)
	if err != nil {
		t.Fatalf("nil input: %v", err)
	}
	if len(m) != 0 {
		t.Fatalf("expected empty map, got %v", m)
	}

	m2, err := ResolveSnapshotPaths(ctx, db, []string{})
	if err != nil {
		t.Fatalf("empty slice input: %v", err)
	}
	if len(m2) != 0 {
		t.Fatalf("expected empty map, got %v", m2)
	}
}

func TestResolveSnapshotPathsAllDistinctIDs(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	paths := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	m, err := ResolveSnapshotPaths(ctx, db, paths)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	unique := make(map[int64]string)
	for p, id := range m {
		if prev, clash := unique[id]; clash {
			t.Errorf("id %d shared by %q and %q", id, prev, p)
		}
		unique[id] = p
	}
}

func TestLoadSnapshotPathsByIDReturnsAllPaths(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	resolved, err := ResolveSnapshotPaths(ctx, db, []string{"a.txt", "b.txt", "c.txt"})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}

	ids := []int64{resolved["a.txt"], resolved["b.txt"], resolved["c.txt"]}
	loaded, err := LoadSnapshotPathsByID(ctx, db, ids)
	if err != nil {
		t.Fatalf("load paths by id: %v", err)
	}
	if len(loaded) != 3 {
		t.Fatalf("expected 3 loaded paths, got %d", len(loaded))
	}
	for wantPath, id := range resolved {
		if got := loaded[id]; got != wantPath {
			t.Fatalf("unexpected path for id %d: got %q want %q", id, got, wantPath)
		}
	}
}

func TestLoadSnapshotPathsByIDDeduplicatesInput(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	id, err := ResolveSnapshotPath(ctx, db, "dup/path.txt")
	if err != nil {
		t.Fatalf("resolve path: %v", err)
	}

	loaded, err := LoadSnapshotPathsByID(ctx, db, []int64{id, id, id})
	if err != nil {
		t.Fatalf("load duplicate ids: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 loaded path, got %d", len(loaded))
	}
	if loaded[id] != "dup/path.txt" {
		t.Fatalf("unexpected loaded path: %q", loaded[id])
	}
}

func TestLoadSnapshotPathsByIDRejectsInvalidID(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	if _, err := LoadSnapshotPathsByID(ctx, db, []int64{1, 0}); err == nil {
		t.Fatal("expected error for non-positive path id in batch")
	}
}

func TestLoadSnapshotPathsByIDFailsWhenMissing(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	id, err := ResolveSnapshotPath(ctx, db, "present.txt")
	if err != nil {
		t.Fatalf("resolve path: %v", err)
	}

	if _, err := LoadSnapshotPathsByID(ctx, db, []int64{id, 9999}); err == nil {
		t.Fatal("expected error when any requested path id is missing")
	}
}

func TestLoadSnapshotPathsByIDEmptyInputReturnsEmptyMap(t *testing.T) {
	db := openPathResolverDB(t)
	ctx := context.Background()

	loaded, err := LoadSnapshotPathsByID(ctx, db, nil)
	if err != nil {
		t.Fatalf("nil input: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("expected empty result for nil input, got %v", loaded)
	}
}
