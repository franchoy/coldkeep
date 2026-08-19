package catalog_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/db"
)

const insertLogicalFileSQL = `
INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
VALUES (?, ?, ?, ?, ?, ?)`

// openTestDB opens an in-memory SQLite database with the coldkeep schema applied.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	return dbconn
}

type logicalFileFixture struct {
	ID           int64
	OriginalName string
	TotalSize    int64
	FileHash     string
	RefCount     int
	Status       string
}

func insertLogicalFileFixture(t *testing.T, dbconn *sql.DB, file logicalFileFixture) {
	t.Helper()
	_, err := dbconn.ExecContext(context.Background(), insertLogicalFileSQL,
		file.ID, file.OriginalName, file.TotalSize, file.FileHash, file.RefCount, file.Status)
	if err != nil {
		t.Fatalf("insert logical_file %d: %v", file.ID, err)
	}
}

func assertServiceLogicalFileRef(t *testing.T, got *catalog.LogicalFileRef, want logicalFileFixture) {
	t.Helper()
	if got == nil {
		t.Fatal("expected ref, got nil")
	}
	if got.ID != want.ID {
		t.Errorf("ID: got %d, want %d", got.ID, want.ID)
	}
	if got.OriginalName != want.OriginalName {
		t.Errorf("OriginalName: got %q, want %s", got.OriginalName, want.OriginalName)
	}
	if got.TotalSize != want.TotalSize {
		t.Errorf("TotalSize: got %d, want %d", got.TotalSize, want.TotalSize)
	}
	if got.FileHash != want.FileHash {
		t.Errorf("FileHash: got %q, want %s", got.FileHash, want.FileHash)
	}
	if got.RefCount != want.RefCount {
		t.Errorf("RefCount: got %d, want %d", got.RefCount, want.RefCount)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %q, want %s", got.Status, want.Status)
	}
}

type snapshotFixture struct {
	ID       string
	SnapType string
	Label    string
	Created  time.Time
}

func insertSnapshotFixture(t *testing.T, dbconn *sql.DB, snapshot snapshotFixture) {
	t.Helper()
	_, err := dbconn.ExecContext(context.Background(),
		`INSERT INTO snapshot (id, created_at, type, label) VALUES (?, ?, ?, ?)`,
		snapshot.ID, snapshot.Created.Format(time.RFC3339), snapshot.SnapType, snapshot.Label,
	)
	if err != nil {
		t.Fatalf("insert snapshot %s: %v", snapshot.ID, err)
	}
}

func assertSnapshotRef(t *testing.T, got *catalog.SnapshotRef, want snapshotFixture) {
	t.Helper()
	if got == nil {
		t.Fatal("expected ref, got nil")
	}
	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %s", got.ID, want.ID)
	}
	if got.Type != want.SnapType {
		t.Errorf("Type: got %q, want %s", got.Type, want.SnapType)
	}
	if got.Label != want.Label {
		t.Errorf("Label: got %q, want %s", got.Label, want.Label)
	}
	if got.ParentID != "" {
		t.Errorf("ParentID: got %q, want empty", got.ParentID)
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt: expected non-zero")
	}
}

func assertMissingSnapshot(t *testing.T, svc catalog.Catalog, id string) {
	t.Helper()
	ref, err := svc.FindSnapshot(context.Background(), id)
	if err != nil {
		t.Fatalf("FindSnapshot unexpected error: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil for missing snapshot, got %+v", ref)
	}
}

func seedListSnapshotFixtures(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	t1 := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	t2 := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	for _, snapshot := range []snapshotFixture{
		{ID: "snap-A", SnapType: "full", Label: "a", Created: t1},
		{ID: "snap-B", SnapType: "partial", Label: "b", Created: t2},
	} {
		insertSnapshotFixture(t, dbconn, snapshot)
	}
}

func assertListSnapshotsAll(t *testing.T, svc catalog.Catalog, ctx context.Context) {
	t.Helper()
	all, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{})
	if err != nil {
		t.Fatalf("ListSnapshots all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(all))
	}
	// Ordered newest first — snap-B should be first.
	if all[0].ID != "snap-B" {
		t.Errorf("first snapshot: got %q, want snap-B", all[0].ID)
	}
}

func assertListSnapshotsTypeFilter(t *testing.T, svc catalog.Catalog, ctx context.Context) {
	t.Helper()
	full, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Type: "full"})
	if err != nil {
		t.Fatalf("ListSnapshots type=full: %v", err)
	}
	if len(full) != 1 || full[0].ID != "snap-A" {
		t.Errorf("filtered type=full: got %v", full)
	}
}

func assertListSnapshotsLimit(t *testing.T, svc catalog.Catalog, ctx context.Context) {
	t.Helper()
	limited, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Limit: 1})
	if err != nil {
		t.Fatalf("ListSnapshots limit=1: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("limit=1: expected 1, got %d", len(limited))
	}
}

func seedReachabilityRootsFixture(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	seedReachabilityLogicalFiles(t, dbconn)
	seedReachabilityPhysicalFile(t, dbconn)
	seedReachabilitySnapshotFile(t, dbconn)
}

func seedReachabilityLogicalFiles(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	for _, file := range []logicalFileFixture{
		{ID: 1, OriginalName: "file1.txt", FileHash: "hash-file1", Status: "COMPLETED", RefCount: 1},
		{ID: 2, OriginalName: "file2.txt", FileHash: "hash-file2", Status: "COMPLETED", RefCount: 1},
	} {
		insertLogicalFileFixture(t, dbconn, file)
	}
}

func seedReachabilityPhysicalFile(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	_, err := dbconn.ExecContext(context.Background(), `
INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
VALUES ('/p/file1.txt', 1, 0)`)
	if err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}
}

func seedReachabilitySnapshotFile(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := dbconn.ExecContext(ctx,
		`INSERT INTO snapshot (id, created_at, type) VALUES ('s1', '2024-01-01T00:00:00Z', 'full')`); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.ExecContext(ctx,
		`INSERT INTO snapshot_path (id, path) VALUES (1, '/p/file2.txt')`); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.ExecContext(ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('s1', 1, 2)`); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}
}

func assertReachabilityRoots(t *testing.T, roots *catalog.ReachabilityRoots) {
	t.Helper()
	if roots == nil {
		t.Fatal("expected non-nil roots")
	}
	assertReachabilitySetContains(t, roots.Current, 1, "Current should contain logical file 1")
	assertReachabilitySetMissing(t, roots.Current, 2, "Current should NOT contain logical file 2 (only in snapshot_file)")
	assertReachabilitySetContains(t, roots.Snapshot, 2, "Snapshot should contain logical file 2")
	assertReachabilitySetMissing(t, roots.Snapshot, 1, "Snapshot should NOT contain logical file 1 (only in physical_file)")
}

func assertReachabilitySetContains(t *testing.T, set map[int64]struct{}, id int64, message string) {
	t.Helper()
	if _, ok := set[id]; !ok {
		t.Error(message)
	}
}

func assertReachabilitySetMissing(t *testing.T, set map[int64]struct{}, id int64, message string) {
	t.Helper()
	if _, ok := set[id]; ok {
		t.Error(message)
	}
}

// TestNewServiceAcceptsSQLDB verifies that *sql.DB satisfies the catalog.DB
// interface and that NewServiceFromSQL returns a non-nil *Service.
func TestNewServiceAcceptsSQLDB(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	if svc == nil {
		t.Fatal("NewServiceFromSQL returned nil")
	}
	// Verify it satisfies Catalog.
	var _ catalog.Catalog = svc
}

// TestServiceFindLogicalFileNotFound verifies that FindLogicalFile returns
// (nil, nil) when no matching row exists.
func TestServiceFindLogicalFileNotFound(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)

	ref, err := svc.FindLogicalFile(context.Background(), 999)
	if err != nil {
		t.Fatalf("FindLogicalFile unexpected error: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil ref for missing id, got %+v", ref)
	}
}

// TestServiceFindLogicalFile verifies that FindLogicalFile returns the correct
// row when the logical_file exists.
func TestServiceFindLogicalFile(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	want := logicalFileFixture{
		ID: 1, OriginalName: "hello.txt", TotalSize: 5,
		FileHash: "abc123", RefCount: 1, Status: "COMPLETED",
	}

	insertLogicalFileFixture(t, dbconn, want)
	ref, err := svc.FindLogicalFile(context.Background(), want.ID)
	if err != nil {
		t.Fatalf("FindLogicalFile: %v", err)
	}
	assertServiceLogicalFileRef(t, ref, want)
}

// TestServiceFindPhysicalFiles verifies FindPhysicalFilesForLogicalFile returns
// rows correctly (including the null-mtime case).
func TestServiceFindPhysicalFiles(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	ctx := context.Background()

	insertLogicalFileFixture(t, dbconn, logicalFileFixture{
		ID: 1, OriginalName: "a.txt", TotalSize: 3,
		FileHash: "aaa", RefCount: 1, Status: "COMPLETED",
	})

	_, err := dbconn.ExecContext(ctx, `
INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
VALUES ('/data/a.txt', 1, 0)`)
	if err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

	refs, err := svc.FindPhysicalFilesForLogicalFile(ctx, 1)
	if err != nil {
		t.Fatalf("FindPhysicalFilesForLogicalFile: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Path != "/data/a.txt" {
		t.Errorf("Path: got %q, want /data/a.txt", refs[0].Path)
	}
	if refs[0].LogicalFileID != 1 {
		t.Errorf("LogicalFileID: got %d, want 1", refs[0].LogicalFileID)
	}
	if refs[0].MTime != nil {
		t.Errorf("MTime: expected nil for NULL, got %v", refs[0].MTime)
	}
	if refs[0].IsMetadataComplete {
		t.Errorf("IsMetadataComplete: expected false, got true")
	}
}

// TestServiceFindPhysicalFilesEmpty verifies empty result for unknown ID.
func TestServiceFindPhysicalFilesEmpty(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)

	refs, err := svc.FindPhysicalFilesForLogicalFile(context.Background(), 999)
	if err != nil {
		t.Fatalf("FindPhysicalFilesForLogicalFile unexpected error: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("expected empty slice, got %d items", len(refs))
	}
}

// TestServiceFindSnapshot verifies FindSnapshot returns nil for missing ID and
// correct data for an existing snapshot.
func TestServiceFindSnapshot(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	want := snapshotFixture{
		ID: "snap-001", Created: time.Now().UTC().Truncate(time.Second),
		SnapType: "full", Label: "test-label",
	}

	assertMissingSnapshot(t, svc, "nonexistent-id")
	insertSnapshotFixture(t, dbconn, want)
	ref, err := svc.FindSnapshot(context.Background(), want.ID)
	if err != nil {
		t.Fatalf("FindSnapshot: %v", err)
	}
	assertSnapshotRef(t, ref, want)
}

// TestServiceListSnapshots verifies ListSnapshots returns all rows and respects
// the Type filter.
func TestServiceListSnapshots(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	ctx := context.Background()

	seedListSnapshotFixtures(t, dbconn)
	assertListSnapshotsAll(t, svc, ctx)
	assertListSnapshotsTypeFilter(t, svc, ctx)
	assertListSnapshotsLimit(t, svc, ctx)
}

// TestServiceLoadReachabilityRoots verifies that reachability roots are
// populated from physical_file and snapshot_file.
func TestServiceLoadReachabilityRoots(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	ctx := context.Background()

	seedReachabilityRootsFixture(t, dbconn)
	roots, err := svc.LoadReachabilityRoots(ctx)
	if err != nil {
		t.Fatalf("LoadReachabilityRoots: %v", err)
	}
	assertReachabilityRoots(t, roots)
}

// TestServiceDeferredMethodsReturnErrNotImplemented verifies the remaining
// restore/GC skeleton methods return ErrNotImplemented.
func TestServiceDeferredMethodsReturnErrNotImplemented(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	ctx := context.Background()
	before := countCatalogLogicalFiles(t, dbconn)

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

	after := countCatalogLogicalFiles(t, dbconn)
	if after != before {
		t.Fatalf("deferred catalog methods should not mutate logical_file rows: before=%d after=%d", before, after)
	}
}

func countCatalogLogicalFiles(t *testing.T, dbconn *sql.DB) int {
	t.Helper()

	var count int
	if err := dbconn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM logical_file`).Scan(&count); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	return count
}
