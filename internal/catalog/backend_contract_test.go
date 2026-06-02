package catalog_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

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

	cfg := loadPostgresCatalogTestConfig()
	dbconn := openPostgresCatalogTestConnection(t, cfg, cfg.Database, "test database")

	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	if err := db.EnsurePostgresSchema(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("apply postgres schema to %s: %v", cfg.Database, err)
	}
	resetCatalogContractTables(t, dbconn)

	t.Cleanup(func() {
		resetCatalogContractTables(t, dbconn)
		_ = dbconn.Close()
	})
	return dbconn
}

type postgresCatalogTestConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	SSLMode  string
	Database string
}

func loadPostgresCatalogTestConfig() postgresCatalogTestConfig {
	return postgresCatalogTestConfig{
		Host:     getenvOrDefaultCatalogTest("DB_HOST", "127.0.0.1"),
		Port:     getenvOrDefaultCatalogTest("DB_PORT", "5432"),
		User:     getenvOrDefaultCatalogTest("DB_USER", "coldkeep"),
		Password: getenvOrDefaultCatalogTest("DB_PASSWORD", "coldkeep"),
		SSLMode:  getenvOrDefaultCatalogTest("DB_SSLMODE", "disable"),
		Database: getenvOrDefaultCatalogTest("DB_NAME", "coldkeep"),
	}
}

func openPostgresCatalogTestConnection(t *testing.T, cfg postgresCatalogTestConfig, databaseName, purpose string) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("postgres", postgresCatalogTestConnString(cfg, databaseName))
	if err != nil {
		t.Fatalf("open postgres %s connection: %v", purpose, err)
	}
	if err := dbconn.Ping(); err != nil {
		_ = dbconn.Close()
		t.Fatalf("ping postgres %s connection: %v", purpose, err)
	}
	return dbconn
}

func postgresCatalogTestConnString(cfg postgresCatalogTestConfig, databaseName string) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, databaseName, cfg.SSLMode,
	)
}

func resetCatalogContractTables(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	for _, query := range catalogContractResetQueries() {
		if _, err := dbconn.Exec(query); err != nil {
			t.Fatalf("reset catalog contract table: %v", err)
		}
	}
}

func catalogContractResetQueries() []string {
	return []string{
		`DELETE FROM snapshot_file`,
		`DELETE FROM snapshot_path`,
		`DELETE FROM snapshot`,
		`DELETE FROM physical_file`,
		`DELETE FROM logical_file`,
	}
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

type logicalFileFinder interface {
	FindLogicalFile(context.Context, int64) (*catalog.LogicalFileRef, error)
}

// TestCatalogContractFindLogicalFileAcrossBackends verifies FindLogicalFile
// returns identical results on every backend.
func TestCatalogContractFindLogicalFileAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			assertCatalogFindLogicalFile(t, catalog.NewServiceFromSQL(dbconn))
		})
	}
}

func assertCatalogFindLogicalFile(t *testing.T, svc logicalFileFinder) {
	t.Helper()
	ctx := context.Background()
	assertMissingLogicalFile(t, svc, ctx, 9999)
	assertLogicalFileRef(t, svc, ctx, 1)
}

func assertMissingLogicalFile(t *testing.T, svc logicalFileFinder, ctx context.Context, id int64) {
	t.Helper()
	missing, err := svc.FindLogicalFile(ctx, id)
	if err != nil {
		t.Fatalf("FindLogicalFile(missing): %v", err)
	}
	if missing != nil {
		t.Fatalf("FindLogicalFile(missing): want nil, got %+v", missing)
	}
}

func assertLogicalFileRef(t *testing.T, svc logicalFileFinder, ctx context.Context, id int64) {
	t.Helper()
	got := requireLogicalFileRef(t, svc, ctx, id)
	assertLogicalFileFields(t, got, expectedLogicalFileRef())
}

func requireLogicalFileRef(t *testing.T, svc logicalFileFinder, ctx context.Context, id int64) *catalog.LogicalFileRef {
	t.Helper()
	got, err := svc.FindLogicalFile(ctx, id)
	if err != nil {
		t.Fatalf("FindLogicalFile(%d): %v", id, err)
	}
	if got == nil {
		t.Fatalf("FindLogicalFile(%d): want ref, got nil", id)
	}
	return got
}

func expectedLogicalFileRef() catalog.LogicalFileRef {
	return catalog.LogicalFileRef{
		ID:           1,
		OriginalName: "current-file.txt",
		TotalSize:    11,
		FileHash:     "h1",
		RefCount:     1,
		Status:       "COMPLETED",
	}
}

func assertLogicalFileFields(t *testing.T, got *catalog.LogicalFileRef, want catalog.LogicalFileRef) {
	t.Helper()
	if got.ID != want.ID {
		t.Errorf("ID: got %d, want %d", got.ID, want.ID)
	}
	if got.OriginalName != want.OriginalName {
		t.Errorf("OriginalName: got %q, want %q", got.OriginalName, want.OriginalName)
	}
	if got.TotalSize != want.TotalSize {
		t.Errorf("TotalSize: got %d, want %d", got.TotalSize, want.TotalSize)
	}
	if got.FileHash != want.FileHash {
		t.Errorf("FileHash: got %q, want %q", got.FileHash, want.FileHash)
	}
	if got.RefCount != want.RefCount {
		t.Errorf("RefCount: got %d, want %d", got.RefCount, want.RefCount)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %q, want %q", got.Status, want.Status)
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
			assertCatalogFindPhysicalFiles(t, catalog.NewServiceFromSQL(dbconn))
		})
	}
}

type physicalFileFinder interface {
	FindPhysicalFilesForLogicalFile(context.Context, int64) ([]catalog.PhysicalFileRef, error)
}

func assertCatalogFindPhysicalFiles(t *testing.T, svc physicalFileFinder) {
	t.Helper()
	ctx := context.Background()
	assertMissingPhysicalFiles(t, svc, ctx, 9999)
	refs := requirePhysicalFiles(t, svc, ctx, 1, 2)
	assertPhysicalFileOrdering(t, refs)
	assertCompletePhysicalFile(t, refs[0])
	assertIncompletePhysicalFile(t, refs[1])
}

func assertMissingPhysicalFiles(t *testing.T, svc physicalFileFinder, ctx context.Context, id int64) {
	t.Helper()
	refs := requirePhysicalFiles(t, svc, ctx, id, 0)
	if len(refs) != 0 {
		t.Fatalf("FindPhysicalFilesForLogicalFile(missing): want empty, got %d", len(refs))
	}
}

func requirePhysicalFiles(
	t *testing.T,
	svc physicalFileFinder,
	ctx context.Context,
	id int64,
	wantRows int,
) []catalog.PhysicalFileRef {
	t.Helper()
	refs, err := svc.FindPhysicalFilesForLogicalFile(ctx, id)
	if err != nil {
		t.Fatalf("FindPhysicalFilesForLogicalFile(%d): %v", id, err)
	}
	if len(refs) != wantRows {
		t.Fatalf("FindPhysicalFilesForLogicalFile(%d): want %d rows, got %d", id, wantRows, len(refs))
	}
	return refs
}

func assertPhysicalFileOrdering(t *testing.T, refs []catalog.PhysicalFileRef) {
	t.Helper()
	if refs[0].Path != "/current/a.txt" || refs[1].Path != "/current/b.txt" {
		t.Fatalf("ordering: got %q then %q", refs[0].Path, refs[1].Path)
	}
}

func assertCompletePhysicalFile(t *testing.T, ref catalog.PhysicalFileRef) {
	t.Helper()
	if ref.MTime == nil {
		t.Errorf("row a: expected non-nil MTime")
	} else if !ref.MTime.Equal(catalogFixtureBase) {
		t.Errorf("row a: MTime = %v, want %v", ref.MTime, catalogFixtureBase)
	}
	if !ref.IsMetadataComplete {
		t.Errorf("row a: IsMetadataComplete = false, want true")
	}
}

func assertIncompletePhysicalFile(t *testing.T, ref catalog.PhysicalFileRef) {
	t.Helper()
	if ref.MTime != nil {
		t.Errorf("row b: expected nil MTime for NULL, got %v", ref.MTime)
	}
	if ref.IsMetadataComplete {
		t.Errorf("row b: IsMetadataComplete = true, want false")
	}
}

// TestCatalogContractFindSnapshotAcrossBackends verifies snapshot identity,
// nullable parent/label, and timestamp parsing are consistent across backends.
func TestCatalogContractFindSnapshotAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			assertCatalogFindSnapshot(t, catalog.NewServiceFromSQL(dbconn))
		})
	}
}

type snapshotFinder interface {
	FindSnapshot(context.Context, string) (*catalog.SnapshotRef, error)
}

func assertCatalogFindSnapshot(t *testing.T, svc snapshotFinder) {
	t.Helper()
	ctx := context.Background()
	assertMissingCatalogSnapshot(t, svc, ctx, "does-not-exist")
	assertRootCatalogSnapshot(t, requireCatalogSnapshot(t, svc, ctx, "snap-full"))
	assertChildCatalogSnapshot(t, requireCatalogSnapshot(t, svc, ctx, "snap-child"))
}

func assertMissingCatalogSnapshot(t *testing.T, svc snapshotFinder, ctx context.Context, id string) {
	t.Helper()
	missing, err := svc.FindSnapshot(ctx, id)
	if err != nil {
		t.Fatalf("FindSnapshot(missing): %v", err)
	}
	if missing != nil {
		t.Fatalf("FindSnapshot(missing): want nil, got %+v", missing)
	}
}

func requireCatalogSnapshot(t *testing.T, svc snapshotFinder, ctx context.Context, id string) *catalog.SnapshotRef {
	t.Helper()
	ref, err := svc.FindSnapshot(ctx, id)
	if err != nil {
		t.Fatalf("FindSnapshot(%s): %v", id, err)
	}
	if ref == nil {
		t.Fatalf("FindSnapshot(%s): want ref, got nil", id)
	}
	return ref
}

func assertRootCatalogSnapshot(t *testing.T, ref *catalog.SnapshotRef) {
	t.Helper()
	if ref.ID != "snap-full" || ref.Type != "full" || ref.Label != "alpha" {
		t.Fatalf("FindSnapshot(snap-full): unexpected ref %+v", ref)
	}
	if ref.ParentID != "" {
		t.Errorf("FindSnapshot(snap-full): ParentID = %q, want empty", ref.ParentID)
	}
	if !ref.CreatedAt.Equal(catalogFixtureBase) {
		t.Errorf("FindSnapshot(snap-full): CreatedAt = %v, want %v", ref.CreatedAt, catalogFixtureBase)
	}
}

func assertChildCatalogSnapshot(t *testing.T, ref *catalog.SnapshotRef) {
	t.Helper()
	if ref.Type != "partial" || ref.Label != "beta" || ref.ParentID != "snap-full" {
		t.Fatalf("FindSnapshot(snap-child): unexpected ref %+v", ref)
	}
}

type snapshotLister interface {
	ListSnapshots(context.Context, catalog.SnapshotFilter) ([]catalog.SnapshotRef, error)
}

// TestCatalogContractListSnapshotsAcrossBackends verifies ordering (newest
// first), type filtering, label substring matching (LIKE), Since/Until bounds,
// and Limit are consistent across backends.
func TestCatalogContractListSnapshotsAcrossBackends(t *testing.T) {
	for _, backend := range catalogBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbconn := backend.Open(t)
			seedCatalogFixture(t, dbconn)
			assertCatalogListSnapshots(t, catalog.NewServiceFromSQL(dbconn))
		})
	}
}

func assertCatalogListSnapshots(t *testing.T, svc snapshotLister) {
	t.Helper()
	ctx := context.Background()
	assertAllSnapshots(t, svc, ctx)
	assertFilteredSnapshot(t, svc, ctx, catalog.SnapshotFilter{Type: "full"}, "type=full", "snap-full")
	assertFilteredSnapshot(t, svc, ctx, catalog.SnapshotFilter{LabelSubstring: "alph"}, "label~alph", "snap-full")
	assertSnapshotTimeBounds(t, svc, ctx)
	assertFilteredSnapshot(t, svc, ctx, catalog.SnapshotFilter{Limit: 1}, "limit=1", "snap-child")
}

func assertAllSnapshots(t *testing.T, svc snapshotLister, ctx context.Context) {
	t.Helper()
	all := requireSnapshots(t, svc, ctx, catalog.SnapshotFilter{}, "all")
	if len(all) != 2 {
		t.Fatalf("ListSnapshots(all): want 2, got %d", len(all))
	}
	if all[0].ID != "snap-child" || all[1].ID != "snap-full" {
		t.Fatalf("ListSnapshots(all): ordering got %q then %q", all[0].ID, all[1].ID)
	}
}

func assertSnapshotTimeBounds(t *testing.T, svc snapshotLister, ctx context.Context) {
	t.Helper()
	since := catalogFixtureBase.Add(30 * time.Minute)
	assertFilteredSnapshot(t, svc, ctx, catalog.SnapshotFilter{Since: &since}, "since", "snap-child")

	until := catalogFixtureBase.Add(30 * time.Minute)
	assertFilteredSnapshot(t, svc, ctx, catalog.SnapshotFilter{Until: &until}, "until", "snap-full")
}

func assertFilteredSnapshot(
	t *testing.T,
	svc snapshotLister,
	ctx context.Context,
	filter catalog.SnapshotFilter,
	label string,
	wantID string,
) {
	t.Helper()
	refs := requireSnapshots(t, svc, ctx, filter, label)
	if len(refs) != 1 || refs[0].ID != wantID {
		t.Fatalf("ListSnapshots(%s): got %+v", label, refs)
	}
}

func requireSnapshots(
	t *testing.T,
	svc snapshotLister,
	ctx context.Context,
	filter catalog.SnapshotFilter,
	label string,
) []catalog.SnapshotRef {
	t.Helper()
	refs, err := svc.ListSnapshots(ctx, filter)
	if err != nil {
		t.Fatalf("ListSnapshots(%s): %v", label, err)
	}
	return refs
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
