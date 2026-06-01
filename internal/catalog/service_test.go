package catalog_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/db"
)

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
	ctx := context.Background()

	// Insert a minimal logical_file row.
	_, err := dbconn.ExecContext(ctx, `
INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
VALUES (1, 'hello.txt', 5, 'abc123', 1, 'COMPLETED')`)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	ref, err := svc.FindLogicalFile(ctx, 1)
	if err != nil {
		t.Fatalf("FindLogicalFile: %v", err)
	}
	if ref == nil {
		t.Fatal("expected ref, got nil")
	}
	if ref.ID != 1 {
		t.Errorf("ID: got %d, want 1", ref.ID)
	}
	if ref.OriginalName != "hello.txt" {
		t.Errorf("OriginalName: got %q, want hello.txt", ref.OriginalName)
	}
	if ref.TotalSize != 5 {
		t.Errorf("TotalSize: got %d, want 5", ref.TotalSize)
	}
	if ref.FileHash != "abc123" {
		t.Errorf("FileHash: got %q, want abc123", ref.FileHash)
	}
	if ref.RefCount != 1 {
		t.Errorf("RefCount: got %d, want 1", ref.RefCount)
	}
	if ref.Status != "COMPLETED" {
		t.Errorf("Status: got %q, want COMPLETED", ref.Status)
	}
}

// TestServiceFindPhysicalFiles verifies FindPhysicalFilesForLogicalFile returns
// rows correctly (including the null-mtime case).
func TestServiceFindPhysicalFiles(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	ctx := context.Background()

	_, _ = dbconn.ExecContext(ctx, `
INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
VALUES (1, 'a.txt', 3, 'aaa', 1, 'COMPLETED')`)

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
	ctx := context.Background()

	// Missing snapshot.
	ref, err := svc.FindSnapshot(ctx, "nonexistent-id")
	if err != nil {
		t.Fatalf("FindSnapshot unexpected error: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil for missing snapshot, got %+v", ref)
	}

	// Insert a snapshot row.
	now := time.Now().UTC().Truncate(time.Second)
	_, err = dbconn.ExecContext(ctx,
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		"snap-001", now.Format(time.RFC3339), "full", "test-label",
	)
	if err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}

	ref, err = svc.FindSnapshot(ctx, "snap-001")
	if err != nil {
		t.Fatalf("FindSnapshot: %v", err)
	}
	if ref == nil {
		t.Fatal("expected ref, got nil")
	}
	if ref.ID != "snap-001" {
		t.Errorf("ID: got %q, want snap-001", ref.ID)
	}
	if ref.Type != "full" {
		t.Errorf("Type: got %q, want full", ref.Type)
	}
	if ref.Label != "test-label" {
		t.Errorf("Label: got %q, want test-label", ref.Label)
	}
	if ref.ParentID != "" {
		t.Errorf("ParentID: got %q, want empty", ref.ParentID)
	}
	if ref.CreatedAt.IsZero() {
		t.Error("CreatedAt: expected non-zero")
	}
}

// TestServiceListSnapshots verifies ListSnapshots returns all rows and respects
// the Type filter.
func TestServiceListSnapshots(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	ctx := context.Background()

	t1 := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	t2 := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)

	for _, row := range []struct {
		id, snapType, label string
		ts                  time.Time
	}{
		{"snap-A", "full", "a", t1},
		{"snap-B", "partial", "b", t2},
	} {
		_, err := dbconn.ExecContext(ctx,
			`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
			row.id, row.ts.Format(time.RFC3339), row.snapType, row.label,
		)
		if err != nil {
			t.Fatalf("insert snapshot %s: %v", row.id, err)
		}
	}

	all, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{})
	if err != nil {
		t.Fatalf("ListSnapshots all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(all))
	}
	// Ordered newest first — snap-B (t2 > t1) should be first.
	if all[0].ID != "snap-B" {
		t.Errorf("first snapshot: got %q, want snap-B", all[0].ID)
	}

	// Filter by type=full.
	full, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Type: "full"})
	if err != nil {
		t.Fatalf("ListSnapshots type=full: %v", err)
	}
	if len(full) != 1 || full[0].ID != "snap-A" {
		t.Errorf("filtered type=full: got %v", full)
	}

	// Limit=1 returns only one row.
	limited, err := svc.ListSnapshots(ctx, catalog.SnapshotFilter{Limit: 1})
	if err != nil {
		t.Fatalf("ListSnapshots limit=1: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("limit=1: expected 1, got %d", len(limited))
	}
}

// TestServiceLoadReachabilityRoots verifies that reachability roots are
// populated from physical_file and snapshot_file.
func TestServiceLoadReachabilityRoots(t *testing.T) {
	dbconn := openTestDB(t)
	svc := catalog.NewServiceFromSQL(dbconn)
	ctx := context.Background()

	// Two logical files.
	for _, row := range []struct {
		id   int
		name string
	}{
		{1, "file1.txt"}, {2, "file2.txt"},
	} {
		_, err := dbconn.ExecContext(ctx, `
INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status)
VALUES ($1, $2, 0, $3, 1, 'COMPLETED')`,
			row.id, row.name, "hash"+row.name)
		if err != nil {
			t.Fatalf("insert logical_file %d: %v", row.id, err)
		}
	}

	// physical_file references logical file 1 only.
	_, err := dbconn.ExecContext(ctx, `
INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
VALUES ('/p/file1.txt', 1, 0)`)
	if err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

	// snapshot_file references logical file 2 only.
	_, err = dbconn.ExecContext(ctx,
		`INSERT INTO snapshot (id, created_at, type) VALUES ('s1', '2024-01-01T00:00:00Z', 'full')`)
	if err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	_, err = dbconn.ExecContext(ctx,
		`INSERT INTO snapshot_path (id, path) VALUES (1, '/p/file2.txt')`)
	if err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	_, err = dbconn.ExecContext(ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('s1', 1, 2)`)
	if err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	roots, err := svc.LoadReachabilityRoots(ctx)
	if err != nil {
		t.Fatalf("LoadReachabilityRoots: %v", err)
	}
	if roots == nil {
		t.Fatal("expected non-nil roots")
	}
	if _, ok := roots.Current[1]; !ok {
		t.Error("Current should contain logical file 1")
	}
	if _, ok := roots.Current[2]; ok {
		t.Error("Current should NOT contain logical file 2 (only in snapshot_file)")
	}
	if _, ok := roots.Snapshot[2]; !ok {
		t.Error("Snapshot should contain logical file 2")
	}
	if _, ok := roots.Snapshot[1]; ok {
		t.Error("Snapshot should NOT contain logical file 1 (only in physical_file)")
	}
}

// TestServiceDeferredMethodsReturnErrNotImplemented verifies all skeleton
// methods return ErrNotImplemented and not nil or panic.
func TestServiceDeferredMethodsReturnErrNotImplemented(t *testing.T) {
	dbconn := openTestDB(t)
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
}
