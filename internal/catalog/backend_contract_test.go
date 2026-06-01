package catalog_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/db"
)

// catalogBackend describes a database backend the catalog contract tests run
// against. SQLite always runs; PostgreSQL runs only when COLDKEEP_TEST_DB is set
// (the project-wide convention) and skips cleanly otherwise.
type catalogBackend struct {
	Name string
	Open func(t *testing.T) *sql.DB
}

// catalogBackends returns the backends the dual-backend contract suite exercises.
// The SQLite backend is unconditional. The PostgreSQL backend follows the
// existing project convention used by internal/db/migrations_test.go: it reads a
// DSN from the environment and is gated by COLDKEEP_TEST_DB. CI provides a
// postgres service and sets COLDKEEP_TEST_DB=1, so the PostgreSQL path runs in CI
// even though it skips during local development without a configured database.
func catalogBackends() []catalogBackend {
	return []catalogBackend{
		{Name: "sqlite", Open: openSQLiteCatalogTestDB},
		{Name: "postgres", Open: openPostgresCatalogTestDBOrSkip},
	}
}

// openSQLiteCatalogTestDB opens an in-memory SQLite database with the coldkeep
// schema applied. It mirrors the openTestDB helper used by the other catalog
// tests and is always available.
func openSQLiteCatalogTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open sqlite3: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	return dbconn
}

// openPostgresCatalogTestDBOrSkip provisions an isolated, throwaway PostgreSQL
// database for a single test and applies the coldkeep schema to it. It follows
// the temporary-database convention already used by the migration tests:
//
//   - skip when COLDKEEP_TEST_DB is unset (explicit, documented skip);
//   - read DSN parts from the environment with the project defaults;
//   - create a uniquely named database via an admin connection;
//   - apply the schema with auto-bootstrap enabled;
//   - drop the database (terminating backends first) on cleanup.
//
// This keeps PostgreSQL contract coverage available in CI without depending on
// local or production state.
func openPostgresCatalogTestDBOrSkip(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 (with DB_* DSN env) to run PostgreSQL catalog contract tests")
	}

	host := getenvOrDefaultCatalogTest("DB_HOST", "127.0.0.1")
	port := getenvOrDefaultCatalogTest("DB_PORT", "5432")
	user := getenvOrDefaultCatalogTest("DB_USER", "coldkeep")
	password := getenvOrDefaultCatalogTest("DB_PASSWORD", "coldkeep")
	sslMode := getenvOrDefaultCatalogTest("DB_SSLMODE", "disable")
	maintenanceDB := getenvOrDefaultCatalogTest("COLDKEEP_TEST_DB_MAINTENANCE", "postgres")

	adminConnStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		host, port, user, password, maintenanceDB, sslMode,
	)
	adminDB, err := sql.Open("postgres", adminConnStr)
	if err != nil {
		t.Fatalf("open postgres admin connection: %v", err)
	}
	if err := adminDB.Ping(); err != nil {
		_ = adminDB.Close()
		t.Fatalf("ping postgres admin connection: %v", err)
	}

	testDBName := fmt.Sprintf("coldkeep_catalog_contract_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", testDBName)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create temporary postgres database %s: %v", testDBName, err)
	}

	testConnStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		host, port, user, password, testDBName, sslMode,
	)
	dbconn, err := sql.Open("postgres", testConnStr)
	if err != nil {
		dropPostgresCatalogTestDB(adminDB, testDBName)
		_ = adminDB.Close()
		t.Fatalf("open postgres test database connection: %v", err)
	}
	if err := dbconn.Ping(); err != nil {
		_ = dbconn.Close()
		dropPostgresCatalogTestDB(adminDB, testDBName)
		_ = adminDB.Close()
		t.Fatalf("ping postgres test database connection: %v", err)
	}

	// Apply the schema via the backend-neutral bootstrap path. Auto-bootstrap is
	// scoped to this test process via t.Setenv.
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	if err := db.EnsurePostgresSchema(dbconn); err != nil {
		_ = dbconn.Close()
		dropPostgresCatalogTestDB(adminDB, testDBName)
		_ = adminDB.Close()
		t.Fatalf("apply postgres schema to %s: %v", testDBName, err)
	}

	t.Cleanup(func() {
		_ = dbconn.Close()
		dropPostgresCatalogTestDB(adminDB, testDBName)
		_ = adminDB.Close()
	})

	return dbconn
}

// dropPostgresCatalogTestDB terminates active backends on the temporary database
// and drops it. Errors are ignored: this is best-effort cleanup.
func dropPostgresCatalogTestDB(adminDB *sql.DB, name string) {
	_, _ = adminDB.Exec(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()
	`, name)
	_, _ = adminDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name))
}

func getenvOrDefaultCatalogTest(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// catalogFixtureBase is the fixed UTC base timestamp used by the fixture so that
// timestamp behavior is deterministic across SQLite and PostgreSQL.
var catalogFixtureBase = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// seedCatalogFixture inserts an identical logical fixture into either backend
// using backend-neutral SQL. All values are bound through $1-style placeholders
// with appropriate Go types (bool for the boolean column, time.Time for
// timestamps) so neither backend's literal conventions leak in.
//
// Fixture shape:
//
//	logical_file:
//	  1 current-file.txt    COMPLETED size=11 hash=h1
//	  2 snapshot-only-file  COMPLETED size=22 hash=h2
//	physical_file:
//	  /current/a.txt -> lf 1 (mtime set, is_metadata_complete=true)
//	  /current/b.txt -> lf 1 (mtime NULL, is_metadata_complete=false)
//	snapshot:
//	  snap-full  (full,    label "alpha", created base)
//	  snap-child (partial, label "beta",  created base+1h, parent snap-full)
//	snapshot_path:
//	  /snapshot/file.txt
//	snapshot_file:
//	  snap-full -> path -> lf 2
func seedCatalogFixture(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	ctx := context.Background()

	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := dbconn.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed exec failed: %v\nquery: %s", err, query)
		}
	}

	exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
	      VALUES ($1, $2, $3, $4, $5, $6)`,
		1, "current-file.txt", 11, "h1", 1, "COMPLETED")
	exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
	      VALUES ($1, $2, $3, $4, $5, $6)`,
		2, "snapshot-only-file.txt", 22, "h2", 1, "COMPLETED")

	// Physical file with full metadata (non-null mtime, is_metadata_complete=true).
	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
	      VALUES ($1, $2, $3, $4, $5)`,
		"/current/a.txt", 1, 0o644, catalogFixtureBase, true)
	// Physical file with NULL mtime and is_metadata_complete=false (nullable case).
	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
	      VALUES ($1, $2, $3, $4, $5)`,
		"/current/b.txt", 1, nil, nil, false)

	exec(`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		"snap-full", catalogFixtureBase, "full", "alpha")
	exec(`INSERT INTO snapshot (id, created_at, type, label, parent_id) VALUES ($1, $2, $3, $4, $5)`,
		"snap-child", catalogFixtureBase.Add(time.Hour), "partial", "beta", "snap-full")

	exec(`INSERT INTO snapshot_path (id, path) VALUES ($1, $2)`, 1, "/snapshot/file.txt")
	exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)`,
		"snap-full", 1, 2)
}

// TestCatalogContractFindLogicalFileAcrossBackends verifies FindLogicalFile
// returns identical results on every backend.
func TestCatalogContractFindLogicalFileAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			svc := catalog.NewServiceFromSQL(dbconn)
			ctx := context.Background()

			missing, err := svc.FindLogicalFile(ctx, 9999)
			if err != nil {
				t.Fatalf("FindLogicalFile(missing): %v", err)
			}
			if missing != nil {
				t.Fatalf("FindLogicalFile(missing): want nil, got %+v", missing)
			}

			got, err := svc.FindLogicalFile(ctx, 1)
			if err != nil {
				t.Fatalf("FindLogicalFile(1): %v", err)
			}
			if got == nil {
				t.Fatal("FindLogicalFile(1): want ref, got nil")
			}
			if got.ID != 1 || got.OriginalName != "current-file.txt" ||
				got.TotalSize != 11 || got.FileHash != "h1" ||
				got.RefCount != 1 || got.Status != "COMPLETED" {
				t.Fatalf("FindLogicalFile(1): unexpected ref %+v", got)
			}
		})
	}
}

// TestCatalogContractFindPhysicalFilesAcrossBackends verifies ordering, nullable
// metadata, and boolean handling (the most common SQLite/PostgreSQL trap) are
// consistent across backends.
func TestCatalogContractFindPhysicalFilesAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			svc := catalog.NewServiceFromSQL(dbconn)
			ctx := context.Background()

			empty, err := svc.FindPhysicalFilesForLogicalFile(ctx, 9999)
			if err != nil {
				t.Fatalf("FindPhysicalFilesForLogicalFile(missing): %v", err)
			}
			if len(empty) != 0 {
				t.Fatalf("FindPhysicalFilesForLogicalFile(missing): want empty, got %d", len(empty))
			}

			refs, err := svc.FindPhysicalFilesForLogicalFile(ctx, 1)
			if err != nil {
				t.Fatalf("FindPhysicalFilesForLogicalFile(1): %v", err)
			}
			if len(refs) != 2 {
				t.Fatalf("FindPhysicalFilesForLogicalFile(1): want 2 rows, got %d", len(refs))
			}

			// Ordered by path: /current/a.txt before /current/b.txt.
			if refs[0].Path != "/current/a.txt" || refs[1].Path != "/current/b.txt" {
				t.Fatalf("ordering: got %q then %q", refs[0].Path, refs[1].Path)
			}

			// Row a: full metadata, non-null mtime, IsMetadataComplete true.
			if refs[0].MTime == nil {
				t.Errorf("row a: expected non-nil MTime")
			} else if !refs[0].MTime.Equal(catalogFixtureBase) {
				t.Errorf("row a: MTime = %v, want %v", refs[0].MTime, catalogFixtureBase)
			}
			if !refs[0].IsMetadataComplete {
				t.Errorf("row a: IsMetadataComplete = false, want true")
			}

			// Row b: NULL mtime -> nil pointer, IsMetadataComplete false.
			if refs[1].MTime != nil {
				t.Errorf("row b: expected nil MTime for NULL, got %v", refs[1].MTime)
			}
			if refs[1].IsMetadataComplete {
				t.Errorf("row b: IsMetadataComplete = true, want false")
			}
		})
	}
}

// TestCatalogContractFindSnapshotAcrossBackends verifies snapshot identity,
// nullable parent/label, and timestamp parsing are consistent across backends.
func TestCatalogContractFindSnapshotAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			svc := catalog.NewServiceFromSQL(dbconn)
			ctx := context.Background()

			missing, err := svc.FindSnapshot(ctx, "does-not-exist")
			if err != nil {
				t.Fatalf("FindSnapshot(missing): %v", err)
			}
			if missing != nil {
				t.Fatalf("FindSnapshot(missing): want nil, got %+v", missing)
			}

			root, err := svc.FindSnapshot(ctx, "snap-full")
			if err != nil {
				t.Fatalf("FindSnapshot(snap-full): %v", err)
			}
			if root == nil {
				t.Fatal("FindSnapshot(snap-full): want ref, got nil")
			}
			if root.ID != "snap-full" || root.Type != "full" || root.Label != "alpha" {
				t.Fatalf("FindSnapshot(snap-full): unexpected ref %+v", root)
			}
			if root.ParentID != "" {
				t.Errorf("FindSnapshot(snap-full): ParentID = %q, want empty", root.ParentID)
			}
			if !root.CreatedAt.Equal(catalogFixtureBase) {
				t.Errorf("FindSnapshot(snap-full): CreatedAt = %v, want %v", root.CreatedAt, catalogFixtureBase)
			}

			child, err := svc.FindSnapshot(ctx, "snap-child")
			if err != nil {
				t.Fatalf("FindSnapshot(snap-child): %v", err)
			}
			if child == nil {
				t.Fatal("FindSnapshot(snap-child): want ref, got nil")
			}
			if child.Type != "partial" || child.Label != "beta" || child.ParentID != "snap-full" {
				t.Fatalf("FindSnapshot(snap-child): unexpected ref %+v", child)
			}
		})
	}
}

// TestCatalogContractListSnapshotsAcrossBackends verifies ordering (newest
// first), type filtering, label substring matching (LIKE), Since/Until bounds,
// and Limit are consistent across backends.
func TestCatalogContractListSnapshotsAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			svc := catalog.NewServiceFromSQL(dbconn)
			ctx := context.Background()

			all, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{})
			if err != nil {
				t.Fatalf("ListSnapshots(all): %v", err)
			}
			if len(all) != 2 {
				t.Fatalf("ListSnapshots(all): want 2, got %d", len(all))
			}
			// Newest first: snap-child (base+1h) before snap-full (base).
			if all[0].ID != "snap-child" || all[1].ID != "snap-full" {
				t.Fatalf("ListSnapshots(all): ordering got %q then %q", all[0].ID, all[1].ID)
			}

			full, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Type: "full"})
			if err != nil {
				t.Fatalf("ListSnapshots(type=full): %v", err)
			}
			if len(full) != 1 || full[0].ID != "snap-full" {
				t.Fatalf("ListSnapshots(type=full): got %+v", full)
			}

			labeled, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{LabelSubstring: "alph"})
			if err != nil {
				t.Fatalf("ListSnapshots(label~alph): %v", err)
			}
			if len(labeled) != 1 || labeled[0].ID != "snap-full" {
				t.Fatalf("ListSnapshots(label~alph): got %+v", labeled)
			}

			since := catalogFixtureBase.Add(30 * time.Minute)
			recent, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Since: &since})
			if err != nil {
				t.Fatalf("ListSnapshots(since): %v", err)
			}
			if len(recent) != 1 || recent[0].ID != "snap-child" {
				t.Fatalf("ListSnapshots(since): got %+v", recent)
			}

			until := catalogFixtureBase.Add(30 * time.Minute)
			older, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Until: &until})
			if err != nil {
				t.Fatalf("ListSnapshots(until): %v", err)
			}
			if len(older) != 1 || older[0].ID != "snap-full" {
				t.Fatalf("ListSnapshots(until): got %+v", older)
			}

			limited, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Limit: 1})
			if err != nil {
				t.Fatalf("ListSnapshots(limit=1): %v", err)
			}
			if len(limited) != 1 || limited[0].ID != "snap-child" {
				t.Fatalf("ListSnapshots(limit=1): got %+v", limited)
			}
		})
	}
}

// TestCatalogContractLoadReachabilityRootsAcrossBackends verifies the current
// and snapshot reachability sets are populated from the correct sources and
// remain separate. This boundary is safety-critical for Phase 6 GC planning.
func TestCatalogContractLoadReachabilityRootsAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			svc := catalog.NewServiceFromSQL(dbconn)
			ctx := context.Background()

			roots, err := svc.LoadReachabilityRoots(ctx)
			if err != nil {
				t.Fatalf("LoadReachabilityRoots: %v", err)
			}
			if roots == nil {
				t.Fatal("LoadReachabilityRoots: want non-nil")
			}

			// Current roots come from physical_file: logical file 1 (twice, but a set).
			if _, ok := roots.Current[1]; !ok {
				t.Error("Current should contain logical file 1")
			}
			if _, ok := roots.Current[2]; ok {
				t.Error("Current should NOT contain logical file 2 (snapshot-only)")
			}
			if len(roots.Current) != 1 {
				t.Errorf("Current should hold exactly 1 unique id, got %d", len(roots.Current))
			}

			// Snapshot roots come from snapshot_file: logical file 2.
			if _, ok := roots.Snapshot[2]; !ok {
				t.Error("Snapshot should contain logical file 2")
			}
			if _, ok := roots.Snapshot[1]; ok {
				t.Error("Snapshot should NOT contain logical file 1 (current-only)")
			}
			if len(roots.Snapshot) != 1 {
				t.Errorf("Snapshot should hold exactly 1 unique id, got %d", len(roots.Snapshot))
			}
		})
	}
}

// TestCatalogContractDeferredMethodsAcrossBackends verifies every deferred
// catalog method returns ErrNotImplemented consistently on both backends, making
// the incomplete boundary explicit rather than silently succeeding.
func TestCatalogContractDeferredMethodsAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			svc := catalog.NewServiceFromSQL(dbconn)
			ctx := context.Background()

			if _, err := svc.LoadSnapshotGraph(ctx); err != catalog.ErrNotImplemented {
				t.Errorf("LoadSnapshotGraph: want ErrNotImplemented, got %v", err)
			}
			if _, err := svc.LoadChunkPlacements(ctx, 1); err != catalog.ErrNotImplemented {
				t.Errorf("LoadChunkPlacements: want ErrNotImplemented, got %v", err)
			}
			if _, err := svc.LoadRestorePlanMetadata(ctx, catalog.RestorePlanInput{FileID: 1}); err != catalog.ErrNotImplemented {
				t.Errorf("LoadRestorePlanMetadata: want ErrNotImplemented, got %v", err)
			}
			if _, err := svc.LoadGCPlanMetadata(ctx, catalog.GCPlanInput{}); err != catalog.ErrNotImplemented {
				t.Errorf("LoadGCPlanMetadata: want ErrNotImplemented, got %v", err)
			}
		})
	}
}
