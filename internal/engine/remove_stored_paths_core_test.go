package engine_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
)

func TestRemoveStoredPathsRejectsCancelledContext(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"cancelled.txt"}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := fixture.engine.RemoveStoredPaths(ctx, engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err == nil {
		t.Fatal("expected cancelled context error")
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("cancelled context must not classify as unsupported: %v", err)
	}
	if result.DryRun || result.ExecutionMode != "" || len(result.Items) != 0 || result.Summary != (engine.BatchSummary{}) {
		t.Fatalf("expected zero result on cancelled context, got %+v", result)
	}
}

func TestRemoveStoredPathsRejectsEmptyRequest(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"empty.txt"}, 1)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{})
	assertRemoveStoredPathsValidationError(t, result, err, "engine: remove stored paths requires at least one target")
}

func TestRemoveStoredPathsRejectsMissingDatabase(t *testing.T) {
	result, err := (&engine.DefaultEngine{}).RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{"/docs/a.txt"},
	})
	assertRemoveStoredPathsValidationError(t, result, err, "engine: remove stored paths database is required")
}

func TestRemoveStoredPathsPreservesInputOrder(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"ordered.txt"}, 1)
	other := addStoredPathMapping(t, fixture.db, fixture.logicalID, "ordered-b.txt")

	rawTargets := []string{"   ", fixture.storedPath, fixture.storedPath, "/missing/path", other}
	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: rawTargets,
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths preserve order: %v", err)
	}

	got := make([]string, 0, len(result.Items))
	for _, item := range result.Items {
		got = append(got, item.RawTarget)
	}
	if !reflect.DeepEqual(got, rawTargets) {
		t.Fatalf("raw target order mismatch: got %v want %v", got, rawTargets)
	}
}

func TestRemoveStoredPathsTrimsTargets(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"trimmed.txt"}, 1)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{"  " + fixture.storedPath + "  "},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths trims targets: %v", err)
	}
	if got := result.Items[0].StoredPath; got != fixture.storedPath {
		t.Fatalf("expected trimmed stored path %q, got %q", fixture.storedPath, got)
	}
}

func TestRemoveStoredPathsReportsBlankTargets(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"blank.txt"}, 1)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{"   "},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths blank target: %v", err)
	}
	item := result.Items[0]
	if item.RawTarget != "   " || item.StoredPath != "" || item.Status != engine.BatchItemFailed || item.Error != "stored path is required" || item.MappingRemoved {
		t.Fatalf("unexpected blank target item: %+v", item)
	}
	assertLogicalFileStillExists(t, fixture.db, fixture.logicalID)
}

func TestRemoveStoredPathsSkipsDuplicateTargets(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"duplicate.txt"}, 1)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath, "  " + fixture.storedPath + "  "},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths duplicate target: %v", err)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected two items, got %d", len(result.Items))
	}
	if result.Items[1].Status != engine.BatchItemSkipped || result.Items[1].Error != "duplicate target" || result.Items[1].StoredPath != fixture.storedPath {
		t.Fatalf("unexpected duplicate item: %+v", result.Items[1])
	}
}

func TestRemoveStoredPathsDryRunPlansExistingMapping(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"dry-run-existing.txt"}, 1)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths dry-run existing: %v", err)
	}
	item := result.Items[0]
	if item.Status != engine.BatchItemPlanned || item.LogicalFileID != fixture.logicalID || item.MappingRemoved || item.StoredPath != fixture.storedPath {
		t.Fatalf("unexpected dry-run item: %+v", item)
	}
}

func TestRemoveStoredPathsDryRunReportsMissingMapping(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"dry-run-missing.txt"}, 1)
	missingPath := filepath.Join(t.TempDir(), "missing.txt")

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{missingPath},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths dry-run missing: %v", err)
	}
	item := result.Items[0]
	if item.Status != engine.BatchItemFailed || !strings.Contains(item.Error, "not found (never stored)") || item.MappingRemoved {
		t.Fatalf("unexpected missing mapping item: %+v", item)
	}
}

func TestRemoveStoredPathsDryRunDoesNotMutateCatalog(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"dry-run-no-mutate.txt"}, 1)
	before := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths dry-run no mutate: %v", err)
	}
	if result.Items[0].Status != engine.BatchItemPlanned {
		t.Fatalf("expected planned status, got %+v", result.Items[0])
	}
	after := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	assertRemoveStoredPathStateEqual(t, before, after)
}

func TestRemoveStoredPathsDryRunPreservesSnapshotRetentionParityGap(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"dry-run-snapshot-gap.txt"}, 1)
	seedSnapshotRetentionReference(t, fixture.db, fixture.logicalID, fixture.storedPath)
	before := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)

	dryRunResult, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths dry-run retained: %v", err)
	}
	if dryRunResult.Items[0].Status != engine.BatchItemPlanned {
		t.Fatalf("expected planned dry-run item, got %+v", dryRunResult.Items[0])
	}
	afterDryRun := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	assertRemoveStoredPathStateEqual(t, before, afterDryRun)

	liveResult, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths live retained: %v", err)
	}
	item := liveResult.Items[0]
	if item.Status != engine.BatchItemFailed || item.InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked || item.RecommendedAction == "" {
		t.Fatalf("unexpected retained live item: %+v", item)
	}
	afterLive := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	assertRemoveStoredPathStateEqual(t, before, afterLive)
}

func TestRemoveStoredPathsRemovesOneOfMultipleMappings(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"multi-a.txt", "multi-b.txt"}, 2)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths one-of-many: %v", err)
	}
	item := result.Items[0]
	if item.Status != engine.BatchItemOK || !item.MappingRemoved || item.RemainingRefCount != 1 {
		t.Fatalf("unexpected one-of-many item: %+v", item)
	}

	state := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	if !state.logicalExists || state.refCount != 1 || state.physicalCount != 1 {
		t.Fatalf("unexpected one-of-many state: %+v", state)
	}
}

func TestRemoveStoredPathsRemovesLastMappingToZero(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "phase7-last-mapping.db")
	dbconn := openFileBackedSQLiteDB(t, dbPath)
	eng := newRemoveTestEngine(t, dbconn, t.TempDir())
	logicalID, paths := seedRemoveStoredPathFixture(t, dbconn, []string{"last-zero.txt"}, 1)

	assertLastStoredPathRemoved(t, removeStoredPathsWithRequest(t, eng, engine.RemoveStoredPathsRequest{StoredPaths: []string{paths[0]}}, "RemoveStoredPaths last mapping"))
	assertLastStoredPathZeroState(t, queryRemoveStoredPathState(t, dbconn, logicalID))
	reopened := reopenSQLiteRemoveStateDB(t, dbconn, dbPath)
	assertLastStoredPathReopenState(t, reopened, logicalID)
}

func assertLastStoredPathRemoved(t *testing.T, result engine.RemoveStoredPathsResult) {
	t.Helper()
	item := result.Items[0]
	if item.Status != engine.BatchItemOK || item.RemainingRefCount != 0 || !item.MappingRemoved {
		t.Fatalf("unexpected last-mapping item: %+v", item)
	}
}

func assertLastStoredPathZeroState(t *testing.T, state removeStoredPathState) {
	t.Helper()
	if !state.logicalExists || state.refCount != 0 || state.physicalCount != 0 {
		t.Fatalf("unexpected last-mapping state before rerun: %+v", state)
	}
}

func reopenSQLiteRemoveStateDB(t *testing.T, dbconn *sql.DB, dbPath string) *sql.DB {
	t.Helper()
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}
	reopened, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := dbpkg.RunMigrations(reopened); err != nil {
		t.Fatalf("rerun migrations: %v", err)
	}
	return reopened
}

func TestRemoveStoredPathsPreservesLogicalAndChunkGraph(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"graph-a.txt", "graph-b.txt"}, 2)
	before := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths preserve graph: %v", err)
	}
	if result.Items[0].Status != engine.BatchItemOK {
		t.Fatalf("expected success item, got %+v", result.Items[0])
	}

	after := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	if !after.logicalExists {
		t.Fatalf("logical file unexpectedly removed: %+v", after)
	}
	if before.originalName != after.originalName || before.fileChunkCount != after.fileChunkCount || !reflect.DeepEqual(before.chunkLiveRefs, after.chunkLiveRefs) || !reflect.DeepEqual(before.chunkPinCounts, after.chunkPinCounts) {
		t.Fatalf("logical/chunk graph changed unexpectedly: before=%+v after=%+v", before, after)
	}
}
