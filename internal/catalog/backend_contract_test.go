package catalog_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

var catalogFixtureBase = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

const catalogFixtureLargeID int64 = 4_000_000_000

// seedCatalogFixture is one backend-neutral fixture for every CAT contract.
// It deliberately includes null values, duplicate root inputs, equal snapshot
// timestamps, both reachability sources, and an unreferenced logical file.
func seedCatalogFixture(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := dbconn.ExecContext(context.Background(), query, args...); err != nil {
			t.Fatalf("seed exec failed: %v\nquery: %s", err, query)
		}
	}

	logicalFiles := []struct {
		id       int64
		name     string
		size     int64
		hash     string
		refCount int
		status   string
	}{
		{1, "current-file.txt", 11, "h1", 7, "COMPLETED"},
		{2, "snapshot-only-file.txt", 22, "h2", 1, "COMPLETED"},
		{3, "both-roots-file.txt", 33, "h3", 2, "COMPLETED"},
		{4, "unreferenced-file.txt", 44, "h4", 0, "ABORTED"},
		{5, "incomplete-current-file.txt", 55, "h5", 1, "PROCESSING"},
		{catalogFixtureLargeID, "large-id-file.txt", 4_000_000_001, "h-large", 9, "COMPLETED"},
	}
	for _, row := range logicalFiles {
		exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
              VALUES ($1, $2, $3, $4, $5, $6)`,
			row.id, row.name, row.size, row.hash, row.refCount, row.status)
	}

	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
          VALUES ($1, $2, $3, $4, $5)`, "/current/a.txt", 1, 0o644, catalogFixtureBase, true)
	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
          VALUES ($1, $2, $3, $4, $5)`, "/current/b.txt", 1, nil, nil, false)
	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
          VALUES ($1, $2, $3, $4, $5)`, "/current/both.txt", 3, 0o600, catalogFixtureBase.Add(time.Minute), true)
	exec(`INSERT INTO physical_file (path, logical_file_id, mode, mtime, is_metadata_complete)
          VALUES ($1, $2, $3, $4, $5)`, "/current/incomplete.txt", 5, nil, nil, false)

	type snapshotRow struct {
		id, typ       string
		created       time.Time
		label, parent any
	}
	for _, row := range []snapshotRow{
		{"snap-full", "full", catalogFixtureBase, "alpha", nil},
		{"snap-tie-a", "full", catalogFixtureBase, "tie-a", nil},
		{"snap-tie-b", "full", catalogFixtureBase, "tie-b", nil},
		{"snap-child", "partial", catalogFixtureBase.Add(time.Hour), "beta", "snap-full"},
		{"snap-null-label", "full", catalogFixtureBase.Add(2 * time.Hour), nil, nil},
	} {
		exec(`INSERT INTO snapshot (id, created_at, type, label, parent_id)
              VALUES ($1, $2, $3, $4, $5)`, row.id, row.created, row.typ, row.label, row.parent)
	}
	for _, row := range []struct {
		id   int64
		path string
	}{
		{1, "/snapshot/one.txt"},
		{2, "/snapshot/two.txt"},
		{3, "/snapshot/both.txt"},
	} {
		exec(`INSERT INTO snapshot_path (id, path) VALUES ($1, $2)`, row.id, row.path)
	}
	for _, row := range []struct {
		snapshotID            string
		pathID, logicalFileID int64
	}{
		{"snap-full", 1, 2},
		{"snap-tie-a", 2, 2},
		{"snap-child", 3, 3},
	} {
		exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ($1, $2, $3)`,
			row.snapshotID, row.pathID, row.logicalFileID)
	}
}

// CAT-001 proves logical-file lookup, missing results, large int64 values,
// deterministic reads, and non-mutation on both backends.
func TestCatalogContractFindLogicalFileAcrossBackends(t *testing.T) {
	forEachCatalogBackend(t, func(t *testing.T, backend backendtest.Backend) {
		seedCatalogFixture(t, backend.DB)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)
		assertNilLogicalFile(t, svc, 9999)
		assertLogicalFile(t, svc, 1, catalog.LogicalFileRef{ID: 1, OriginalName: "current-file.txt", TotalSize: 11, FileHash: "h1", RefCount: 7, Status: "COMPLETED"})
		assertLogicalFile(t, svc, catalogFixtureLargeID, catalog.LogicalFileRef{ID: catalogFixtureLargeID, OriginalName: "large-id-file.txt", TotalSize: 4_000_000_001, FileHash: "h-large", RefCount: 9, Status: "COMPLETED"})
		assertCancelledCatalogErrors(t, svc, backend.DB)
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("CAT-001 reads mutated catalog state: before=%+v after=%+v", before, after)
		}
	})
}

func assertNilLogicalFile(t *testing.T, svc interface {
	FindLogicalFile(context.Context, int64) (*catalog.LogicalFileRef, error)
}, id int64) {
	t.Helper()
	got, err := svc.FindLogicalFile(context.Background(), id)
	if err != nil || got != nil {
		t.Fatalf("FindLogicalFile(%d): got (%+v, %v), want (nil, nil)", id, got, err)
	}
}

func assertLogicalFile(t *testing.T, svc interface {
	FindLogicalFile(context.Context, int64) (*catalog.LogicalFileRef, error)
}, id int64, want catalog.LogicalFileRef) {
	t.Helper()
	first, err := svc.FindLogicalFile(context.Background(), id)
	if err != nil || first == nil {
		t.Fatalf("FindLogicalFile(%d): got (%+v, %v)", id, first, err)
	}
	second, err := svc.FindLogicalFile(context.Background(), id)
	if err != nil || second == nil {
		t.Fatalf("FindLogicalFile(%d) repeated: got (%+v, %v)", id, second, err)
	}
	if !reflect.DeepEqual(*first, want) || !reflect.DeepEqual(*second, want) {
		t.Fatalf("FindLogicalFile(%d): got %+v then %+v, want %+v", id, first, second, want)
	}
}

// CAT-002 proves deterministic physical-file ordering and nullable/boolean
// semantics. The public contract normalizes a NULL mode to zero.
func TestCatalogContractFindPhysicalFilesAcrossBackends(t *testing.T) {
	forEachCatalogBackend(t, func(t *testing.T, backend backendtest.Backend) {
		seedCatalogFixture(t, backend.DB)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)
		empty, err := svc.FindPhysicalFilesForLogicalFile(context.Background(), 9999)
		if err != nil || len(empty) != 0 {
			t.Fatalf("FindPhysicalFilesForLogicalFile(missing): got (%+v, %v), want empty nil-error result", empty, err)
		}
		refs := requirePhysicalFiles(t, svc, 1)
		if got := []string{refs[0].Path, refs[1].Path}; !reflect.DeepEqual(got, []string{"/current/a.txt", "/current/b.txt"}) {
			t.Fatalf("physical-file ordering: got %v", got)
		}
		if refs[0].LogicalFileID != 1 || refs[0].Mode != 0o644 || refs[0].MTime == nil || !refs[0].MTime.Equal(catalogFixtureBase) || !refs[0].IsMetadataComplete {
			t.Fatalf("complete physical file: got %+v", refs[0])
		}
		if refs[1].LogicalFileID != 1 || refs[1].Mode != 0 || refs[1].MTime != nil || refs[1].IsMetadataComplete {
			t.Fatalf("incomplete physical file: got %+v", refs[1])
		}
		if repeated := requirePhysicalFiles(t, svc, 1); !reflect.DeepEqual(refs, repeated) {
			t.Fatalf("physical-file reads are not deterministic: first=%+v repeated=%+v", refs, repeated)
		}
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("CAT-002 reads mutated catalog state: before=%+v after=%+v", before, after)
		}
	})
}

func requirePhysicalFiles(t *testing.T, svc interface {
	FindPhysicalFilesForLogicalFile(context.Context, int64) ([]catalog.PhysicalFileRef, error)
}, id int64) []catalog.PhysicalFileRef {
	t.Helper()
	refs, err := svc.FindPhysicalFilesForLogicalFile(context.Background(), id)
	if err != nil || len(refs) != 2 {
		t.Fatalf("FindPhysicalFilesForLogicalFile(%d): got (%+v, %v), want two rows", id, refs, err)
	}
	return refs
}

// CAT-003 proves snapshot lookup of root/child/null-label records, timestamp
// normalization, stable missing results, and repeatability.
func TestCatalogContractFindSnapshotAcrossBackends(t *testing.T) {
	forEachCatalogBackend(t, func(t *testing.T, backend backendtest.Backend) {
		seedCatalogFixture(t, backend.DB)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)
		missing, err := svc.FindSnapshot(context.Background(), "does-not-exist")
		if err != nil || missing != nil {
			t.Fatalf("FindSnapshot(missing): got (%+v, %v), want (nil, nil)", missing, err)
		}
		assertSnapshot(t, svc, "snap-full", "full", "alpha", "", catalogFixtureBase)
		assertSnapshot(t, svc, "snap-child", "partial", "beta", "snap-full", catalogFixtureBase.Add(time.Hour))
		assertSnapshot(t, svc, "snap-null-label", "full", "", "", catalogFixtureBase.Add(2*time.Hour))
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("CAT-003 reads mutated catalog state: before=%+v after=%+v", before, after)
		}
	})
}

func assertSnapshot(t *testing.T, svc interface {
	FindSnapshot(context.Context, string) (*catalog.SnapshotRef, error)
}, id, typ, label, parent string, created time.Time) {
	t.Helper()
	first, err := svc.FindSnapshot(context.Background(), id)
	if err != nil || first == nil {
		t.Fatalf("FindSnapshot(%q): got (%+v, %v)", id, first, err)
	}
	second, err := svc.FindSnapshot(context.Background(), id)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("FindSnapshot(%q) repeat: got (%+v, %v), first=%+v", id, second, err, first)
	}
	if first.ID != id || first.Type != typ || first.Label != label || first.ParentID != parent || !first.CreatedAt.Equal(created) {
		t.Fatalf("FindSnapshot(%q): got %+v", id, first)
	}
}

// CAT-004 proves list ordering, equal-time tie breaking, filters, inclusive
// time bounds, ordinary literal substring behavior, and limit semantics.
func TestCatalogContractListSnapshotsAcrossBackends(t *testing.T) {
	forEachCatalogBackend(t, func(t *testing.T, backend backendtest.Backend) {
		seedCatalogFixture(t, backend.DB)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{}, "snap-null-label", "snap-child", "snap-tie-b", "snap-tie-a", "snap-full")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{Type: "full", LabelSubstring: "tie"}, "snap-tie-b", "snap-tie-a")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{LabelSubstring: "bet"}, "snap-child")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{Since: timePointer(catalogFixtureBase.Add(time.Hour))}, "snap-null-label", "snap-child")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{Until: timePointer(catalogFixtureBase)}, "snap-tie-b", "snap-tie-a", "snap-full")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{Limit: 2}, "snap-null-label", "snap-child")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{Limit: 0}, "snap-null-label", "snap-child", "snap-tie-b", "snap-tie-a", "snap-full")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{Limit: -1}, "snap-null-label", "snap-child", "snap-tie-b", "snap-tie-a", "snap-full")
		assertSnapshotIDs(t, svc, catalog.SnapshotFilter{LabelSubstring: "absent"})
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("CAT-004 reads mutated catalog state: before=%+v after=%+v", before, after)
		}
	})
}

func timePointer(value time.Time) *time.Time { return &value }

func assertSnapshotIDs(t *testing.T, svc interface {
	ListSnapshots(context.Context, catalog.SnapshotFilter) ([]catalog.SnapshotRef, error)
}, filter catalog.SnapshotFilter, want ...string) {
	t.Helper()
	refs, err := svc.ListSnapshots(context.Background(), filter)
	if err != nil {
		t.Fatalf("ListSnapshots(%+v): %v", filter, err)
	}
	got := make([]string, len(refs))
	for i, ref := range refs {
		got[i] = ref.ID
	}
	if len(want) == 0 && len(got) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListSnapshots(%+v): got %v, want %v", filter, got, want)
	}
	for _, ref := range refs {
		if ref.ID == "snap-null-label" && ref.Label != "" {
			t.Fatalf("ListSnapshots null label: got %q, want public empty representation", ref.Label)
		}
	}
}

// CAT-005 proves GC-safety root sets are unique, separated by source, include
// a legitimately both-reachable file in both sets, and are independent maps.
func TestCatalogContractLoadReachabilityRootsAcrossBackends(t *testing.T) {
	forEachCatalogBackend(t, func(t *testing.T, backend backendtest.Backend) {
		seedCatalogFixture(t, backend.DB)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)
		roots, err := svc.LoadReachabilityRoots(context.Background())
		if err != nil || roots == nil {
			t.Fatalf("LoadReachabilityRoots: got (%+v, %v)", roots, err)
		}
		assertIDSet(t, roots.Current, 1, 3, 5)
		assertIDSet(t, roots.Snapshot, 2, 3)
		if _, ok := roots.Current[4]; ok {
			t.Fatal("unreferenced logical file appears in current roots")
		}
		if _, ok := roots.Snapshot[1]; ok {
			t.Fatal("current-only logical file appears in snapshot roots")
		}
		roots.Current[99] = struct{}{}
		if _, ok := roots.Snapshot[99]; ok {
			t.Fatal("current and snapshot root maps alias each other")
		}
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("CAT-005 reads mutated catalog state: before=%+v after=%+v", before, after)
		}
	})
}

func assertIDSet(t *testing.T, got map[int64]struct{}, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("root set length: got %d, want %d (%v)", len(got), len(want), want)
	}
	for _, id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("root set missing %d: got %v", id, got)
		}
	}
}

// CAT-006 proves deterministic snapshot graph parity and non-mutation.
func TestCatalogContractSnapshotGraphAcrossBackends(t *testing.T) {
	forEachCatalogBackend(t, func(t *testing.T, backend backendtest.Backend) {
		seedCatalogFixture(t, backend.DB)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)
		first, err := svc.LoadSnapshotGraph(context.Background())
		if err != nil {
			t.Fatalf("LoadSnapshotGraph: %v", err)
		}
		second, err := svc.LoadSnapshotGraph(context.Background())
		if err != nil {
			t.Fatalf("LoadSnapshotGraph repeated: %v", err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("graph is not deterministic: first=%+v second=%+v", first, second)
		}
		wantOrder := []string{"snap-full", "snap-tie-a", "snap-tie-b", "snap-child", "snap-null-label"}
		gotOrder := make([]string, len(first.Nodes))
		for i, node := range first.Nodes {
			gotOrder[i] = node.Snapshot.ID
		}
		if !reflect.DeepEqual(gotOrder, wantOrder) {
			t.Fatalf("node order: got %v want %v", gotOrder, wantOrder)
		}
		wantRoots := []string{"snap-full", "snap-tie-a", "snap-tie-b", "snap-null-label"}
		if !reflect.DeepEqual(first.RootIDs, wantRoots) {
			t.Fatalf("roots: got %v want %v", first.RootIDs, wantRoots)
		}
		if first.Nodes[0].ParentState != catalog.SnapshotParentNone || !reflect.DeepEqual(first.Nodes[0].ChildIDs, []string{"snap-child"}) {
			t.Fatalf("root relation: %+v", first.Nodes[0])
		}
		if first.Nodes[3].ParentState != catalog.SnapshotParentPresent || len(first.Nodes[3].ChildIDs) != 0 {
			t.Fatalf("child relation: %+v", first.Nodes[3])
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if graph, err := svc.LoadSnapshotGraph(cancelled); graph != nil || !catalog.IsCode(err, catalog.ErrorCancelled) || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled graph: graph=%+v err=%v", graph, err)
		}
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("graph read mutated state: before=%+v after=%+v", before, after)
		}
	})
}

// CAT-007 preserves the remaining deliberately deferred API boundary and proves
// those methods cannot return partial results or mutate the catalog.
func TestCatalogContractDeferredMethodsAcrossBackends(t *testing.T) {
	forEachCatalogBackend(t, func(t *testing.T, backend backendtest.Backend) {
		seedCatalogFixture(t, backend.DB)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)
		gcPlan, err := svc.LoadGCPlanMetadata(context.Background(), catalog.GCPlanInput{})
		assertDeferred(t, "LoadGCPlanMetadata", err, gcPlan)
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("deferred catalog methods mutated catalog state: before=%+v after=%+v", before, after)
		}
	})
}

func assertDeferred(t *testing.T, name string, err error, result any) {
	t.Helper()
	if !errors.Is(err, catalog.ErrNotImplemented) || !catalog.IsDeferred(err) {
		t.Errorf("%s: want catalog.ErrNotImplemented, got %v", name, err)
	}
	if !isNil(result) {
		t.Errorf("%s: want nil result, got %#v", name, result)
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// CAT-007 adds bounded portable cancelled-context assertions. Errors must not
// be converted to not-found results, and cancelled reads must not mutate state.
func assertCancelledCatalogErrors(t *testing.T, svc catalog.Catalog, dbconn *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := catalogStateCounts(t, dbconn)
	if ref, err := svc.FindLogicalFile(ctx, 1); err == nil || ref != nil {
		t.Errorf("cancelled FindLogicalFile: got (%+v, %v), want (nil, error)", ref, err)
	}
	if refs, err := svc.FindPhysicalFilesForLogicalFile(ctx, 1); err == nil || refs != nil {
		t.Errorf("cancelled FindPhysicalFilesForLogicalFile: got (%+v, %v), want (nil, error)", refs, err)
	}
	if ref, err := svc.FindSnapshot(ctx, "snap-full"); err == nil || ref != nil {
		t.Errorf("cancelled FindSnapshot: got (%+v, %v), want (nil, error)", ref, err)
	}
	if refs, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{}); err == nil || refs != nil {
		t.Errorf("cancelled ListSnapshots: got (%+v, %v), want (nil, error)", refs, err)
	}
	if roots, err := svc.LoadReachabilityRoots(ctx); err == nil || roots != nil {
		t.Errorf("cancelled LoadReachabilityRoots: got (%+v, %v), want (nil, error)", roots, err)
	}
	if after := catalogStateCounts(t, dbconn); after != before {
		t.Errorf("cancelled catalog reads mutated catalog state: before=%+v after=%+v", before, after)
	}
}

type catalogStateCount struct {
	logicalFiles  int
	physicalFiles int
	snapshots     int
	snapshotFiles int
}

func catalogStateCounts(t *testing.T, dbconn *sql.DB) catalogStateCount {
	t.Helper()
	var result catalogStateCount
	for _, count := range []struct {
		table string
		dest  *int
	}{
		{"logical_file", &result.logicalFiles},
		{"physical_file", &result.physicalFiles},
		{"snapshot", &result.snapshots},
		{"snapshot_file", &result.snapshotFiles},
	} {
		if err := dbconn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+count.table).Scan(count.dest); err != nil {
			t.Fatalf("count %s rows: %v", count.table, err)
		}
	}
	return result
}
