package catalog_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/catalog"
)

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
	exec := newCatalogFixtureExec(t, dbconn)
	seedCatalogLogicalFiles(exec)
	seedCatalogPhysicalFiles(exec)
	seedCatalogSnapshots(exec)
	seedCatalogSnapshotFiles(exec)
}

type catalogFixtureExec func(string, ...any)

func newCatalogFixtureExec(t *testing.T, dbconn *sql.DB) catalogFixtureExec {
	t.Helper()
	ctx := context.Background()
	return func(query string, args ...any) {
		t.Helper()
		if _, err := dbconn.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed exec failed: %v\nquery: %s", err, query)
		}
	}
}

func seedCatalogLogicalFiles(exec catalogFixtureExec) {
	exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
	      VALUES ($1, $2, $3, $4, $5, $6)
	      ON CONFLICT (id) DO UPDATE SET
	        original_name = EXCLUDED.original_name,
	        total_size = EXCLUDED.total_size,
	        file_hash = EXCLUDED.file_hash,
	        ref_count = EXCLUDED.ref_count,
	        status = EXCLUDED.status`,
		1, "current-file.txt", 11, "h1", 1, "COMPLETED")
	exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
	      VALUES ($1, $2, $3, $4, $5, $6)
	      ON CONFLICT (id) DO UPDATE SET
	        original_name = EXCLUDED.original_name,
	        total_size = EXCLUDED.total_size,
	        file_hash = EXCLUDED.file_hash,
	        ref_count = EXCLUDED.ref_count,
	        status = EXCLUDED.status`,
		2, "snapshot-only-file.txt", 22, "h2", 1, "COMPLETED")
}

func seedCatalogPhysicalFiles(exec catalogFixtureExec) {
	// Physical file with full metadata (non-null mtime, is_metadata_complete=true).
	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
	      VALUES ($1, $2, $3, $4, $5)
	      ON CONFLICT (path) DO UPDATE SET
	        logical_file_id = EXCLUDED.logical_file_id,
	        mode = EXCLUDED.mode,
	        mtime = EXCLUDED.mtime,
	        is_metadata_complete = EXCLUDED.is_metadata_complete`,
		"/current/a.txt", 1, 0o644, catalogFixtureBase, true)
	// Physical file with NULL mtime and is_metadata_complete=false (nullable case).
	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
	      VALUES ($1, $2, $3, $4, $5)
	      ON CONFLICT (path) DO UPDATE SET
	        logical_file_id = EXCLUDED.logical_file_id,
	        mode = EXCLUDED.mode,
	        mtime = EXCLUDED.mtime,
	        is_metadata_complete = EXCLUDED.is_metadata_complete`,
		"/current/b.txt", 1, nil, nil, false)
}

func seedCatalogSnapshots(exec catalogFixtureExec) {
	exec(`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)
	      ON CONFLICT (id) DO UPDATE SET
	        created_at = EXCLUDED.created_at,
	        type = EXCLUDED.type,
	        label = EXCLUDED.label,
	        parent_id = NULL`,
		"snap-full", catalogFixtureBase, "full", "alpha")
	exec(`INSERT INTO snapshot (id, created_at, type, label, parent_id) VALUES ($1, $2, $3, $4, $5)
	      ON CONFLICT (id) DO UPDATE SET
	        created_at = EXCLUDED.created_at,
	        type = EXCLUDED.type,
	        label = EXCLUDED.label,
	        parent_id = EXCLUDED.parent_id`,
		"snap-child", catalogFixtureBase.Add(time.Hour), "partial", "beta", "snap-full")
}

func seedCatalogSnapshotFiles(exec catalogFixtureExec) {
	exec(`INSERT INTO snapshot_path (id, path) VALUES ($1, $2)
	      ON CONFLICT (id) DO UPDATE SET path = EXCLUDED.path`,
		1, "/snapshot/file.txt")
	exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)
	      ON CONFLICT (snapshot_id, path_id) DO UPDATE SET logical_file_id = EXCLUDED.logical_file_id`,
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
			assertCatalogReachabilityRoots(t, catalog.NewServiceFromSQL(dbconn))
		})
	}
}

type reachabilityRootLoader interface {
	LoadReachabilityRoots(context.Context) (*catalog.ReachabilityRoots, error)
}

func assertCatalogReachabilityRoots(t *testing.T, svc reachabilityRootLoader) {
	t.Helper()
	roots := requireCatalogReachabilityRoots(t, svc)
	assertCatalogCurrentReachabilityRoots(t, roots.Current)
	assertCatalogSnapshotReachabilityRoots(t, roots.Snapshot)
}

func requireCatalogReachabilityRoots(t *testing.T, svc reachabilityRootLoader) *catalog.ReachabilityRoots {
	t.Helper()
	roots, err := svc.LoadReachabilityRoots(context.Background())
	if err != nil {
		t.Fatalf("LoadReachabilityRoots: %v", err)
	}
	if roots == nil {
		t.Fatal("LoadReachabilityRoots: want non-nil")
	}
	return roots
}

func assertCatalogCurrentReachabilityRoots(t *testing.T, current map[int64]struct{}) {
	t.Helper()
	assertCatalogReachabilityContains(t, current, 1, "Current should contain logical file 1")
	assertCatalogReachabilityMissing(t, current, 2, "Current should NOT contain logical file 2 (snapshot-only)")
	assertCatalogReachabilitySize(t, current, 1, "Current")
}

func assertCatalogSnapshotReachabilityRoots(t *testing.T, snapshot map[int64]struct{}) {
	t.Helper()
	assertCatalogReachabilityContains(t, snapshot, 2, "Snapshot should contain logical file 2")
	assertCatalogReachabilityMissing(t, snapshot, 1, "Snapshot should NOT contain logical file 1 (current-only)")
	assertCatalogReachabilitySize(t, snapshot, 1, "Snapshot")
}

func assertCatalogReachabilityContains(t *testing.T, set map[int64]struct{}, id int64, message string) {
	t.Helper()
	if _, ok := set[id]; !ok {
		t.Error(message)
	}
}

func assertCatalogReachabilityMissing(t *testing.T, set map[int64]struct{}, id int64, message string) {
	t.Helper()
	if _, ok := set[id]; ok {
		t.Error(message)
	}
}

func assertCatalogReachabilitySize(t *testing.T, set map[int64]struct{}, want int, label string) {
	t.Helper()
	if len(set) != want {
		t.Errorf("%s should hold exactly %d unique id, got %d", label, want, len(set))
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
			before := countCatalogLogicalFilesBackend(t, dbconn)

			graph, err := svc.LoadSnapshotGraph(ctx)
			if !errors.Is(err, catalog.ErrNotImplemented) {
				t.Errorf("LoadSnapshotGraph: want ErrNotImplemented via errors.Is, got %v", err)
			}
			if !catalog.IsDeferred(err) {
				t.Errorf("LoadSnapshotGraph: want catalog.IsDeferred=true, got %v", err)
			}
			if graph != nil {
				t.Errorf("LoadSnapshotGraph: want nil graph on deferred path, got %+v", graph)
			}

			placements, err := svc.LoadChunkPlacements(ctx, 1)
			if !errors.Is(err, catalog.ErrNotImplemented) {
				t.Errorf("LoadChunkPlacements: want ErrNotImplemented via errors.Is, got %v", err)
			}
			if !catalog.IsDeferred(err) {
				t.Errorf("LoadChunkPlacements: want catalog.IsDeferred=true, got %v", err)
			}
			if placements != nil {
				t.Errorf("LoadChunkPlacements: want nil placements on deferred path, got %+v", placements)
			}

			restorePlan, err := svc.LoadRestorePlanMetadata(ctx, catalog.RestorePlanInput{FileID: 1})
			if !errors.Is(err, catalog.ErrNotImplemented) {
				t.Errorf("LoadRestorePlanMetadata: want ErrNotImplemented via errors.Is, got %v", err)
			}
			if !catalog.IsDeferred(err) {
				t.Errorf("LoadRestorePlanMetadata: want catalog.IsDeferred=true, got %v", err)
			}
			if restorePlan != nil {
				t.Errorf("LoadRestorePlanMetadata: want nil metadata on deferred path, got %+v", restorePlan)
			}

			gcPlan, err := svc.LoadGCPlanMetadata(ctx, catalog.GCPlanInput{})
			if !errors.Is(err, catalog.ErrNotImplemented) {
				t.Errorf("LoadGCPlanMetadata: want ErrNotImplemented via errors.Is, got %v", err)
			}
			if !catalog.IsDeferred(err) {
				t.Errorf("LoadGCPlanMetadata: want catalog.IsDeferred=true, got %v", err)
			}
			if gcPlan != nil {
				t.Errorf("LoadGCPlanMetadata: want nil metadata on deferred path, got %+v", gcPlan)
			}

			after := countCatalogLogicalFilesBackend(t, dbconn)
			if after != before {
				t.Fatalf("deferred catalog methods should not mutate logical_file rows: before=%d after=%d", before, after)
			}
		})
	}
}

func countCatalogLogicalFilesBackend(t *testing.T, dbconn *sql.DB) int {
	t.Helper()

	var count int
	if err := dbconn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM logical_file`).Scan(&count); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	return count
}
