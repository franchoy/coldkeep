package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

type removeStoredPathFixture struct {
	db         *sql.DB
	engine     *engine.DefaultEngine
	logicalID  int64
	storedPath string
}

type removeStoredPathState struct {
	logicalExists  bool
	originalName   string
	refCount       int64
	physicalCount  int
	snapshotCount  int
	fileChunkCount int
	chunkLiveRefs  map[int64]int64
	chunkPinCounts map[int64]int64
}

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

	result, err := eng.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{paths[0]},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths last mapping: %v", err)
	}
	item := result.Items[0]
	if item.Status != engine.BatchItemOK || item.RemainingRefCount != 0 || !item.MappingRemoved {
		t.Fatalf("unexpected last-mapping item: %+v", item)
	}

	state := queryRemoveStoredPathState(t, dbconn, logicalID)
	if !state.logicalExists || state.refCount != 0 || state.physicalCount != 0 {
		t.Fatalf("unexpected last-mapping state before rerun: %+v", state)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}
	reopened, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if err := dbpkg.RunMigrations(reopened); err != nil {
		t.Fatalf("rerun migrations: %v", err)
	}

	var refCount int64
	if err := reopened.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read ref_count after reopen: %v", err)
	}
	if refCount != 0 {
		t.Fatalf("expected ref_count=0 after reopen, got %d", refCount)
	}
	var physicalCount int64
	if err := reopened.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical_file after reopen: %v", err)
	}
	if physicalCount != 0 {
		t.Fatalf("expected zero physical mappings after reopen, got %d", physicalCount)
	}
	var migratedCount int64
	if err := reopened.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path LIKE '/migrated/%' AND logical_file_id = $1`, logicalID).Scan(&migratedCount); err != nil {
		t.Fatalf("count migrated mappings after reopen: %v", err)
	}
	if migratedCount != 0 {
		t.Fatalf("expected no migrated mapping resurrection, got %d", migratedCount)
	}
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

func TestRemoveStoredPathsRefusesSnapshotRetainedMapping(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"snapshot-retained.txt"}, 1)
	seedSnapshotRetentionReference(t, fixture.db, fixture.logicalID, fixture.storedPath)
	before := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths snapshot retained: %v", err)
	}

	item := result.Items[0]
	if item.Status != engine.BatchItemFailed || item.InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked || item.RecommendedAction == "" || item.MappingRemoved {
		t.Fatalf("unexpected retained refusal item: %+v", item)
	}
	after := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	assertRemoveStoredPathStateEqual(t, before, after)
}

func TestRemoveStoredPathsReportsNeverStoredTarget(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"never-stored-anchor.txt"}, 1)
	missingPath := filepath.Join(t.TempDir(), "never-stored.txt")

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{missingPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths never stored: %v", err)
	}
	item := result.Items[0]
	if item.Status != engine.BatchItemFailed || !strings.Contains(item.Error, "not found (never stored)") {
		t.Fatalf("unexpected never-stored item: %+v", item)
	}
}

func TestRemoveStoredPathsRollsBackRefCountMismatch(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"rollback.txt"}, 5)
	before := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths ref-count mismatch: %v", err)
	}
	item := result.Items[0]
	if item.Status != engine.BatchItemFailed || !strings.Contains(item.Error, "ref_count invariant mismatch") || item.MappingRemoved {
		t.Fatalf("unexpected ref-count mismatch item: %+v", item)
	}
	after := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	assertRemoveStoredPathStateEqual(t, before, after)
}

func TestRemoveStoredPathsContinuesAfterExecutionFailureByDefault(t *testing.T) {
	first := newRemoveStoredPathFixture(t, []string{"continue-a.txt"}, 1)
	secondPath := addStandaloneStoredPathFixture(t, first.db, "continue-b.txt")
	missingPath := filepath.Join(t.TempDir(), "missing.txt")

	result, err := first.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{first.storedPath, missingPath, secondPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths continue after failure: %v", err)
	}
	assertStoredPathStatuses(t, result.Items, []engine.BatchItemStatus{
		engine.BatchItemOK,
		engine.BatchItemFailed,
		engine.BatchItemOK,
	})
	if result.Summary.OK != 2 || result.Summary.Failed != 1 || result.Summary.Skipped != 0 {
		t.Fatalf("unexpected summary: %+v", result.Summary)
	}
}

func TestRemoveStoredPathsFailFastStopsAfterExecutionFailure(t *testing.T) {
	first := newRemoveStoredPathFixture(t, []string{"failfast-a.txt"}, 1)
	secondPath := addStandaloneStoredPathFixture(t, first.db, "failfast-b.txt")
	missingPath := filepath.Join(t.TempDir(), "missing.txt")

	result, err := first.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{first.storedPath, missingPath, secondPath},
		FailFast:    true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths fail-fast: %v", err)
	}
	assertStoredPathStatuses(t, result.Items, []engine.BatchItemStatus{
		engine.BatchItemOK,
		engine.BatchItemFailed,
	})
	if result.Summary.OK != 1 || result.Summary.Failed != 1 || result.Summary.Skipped != 1 {
		t.Fatalf("unexpected fail-fast summary: %+v", result.Summary)
	}
}

func TestRemoveStoredPathsFailFastIgnoresBlankAndDuplicatePreparedItems(t *testing.T) {
	first := newRemoveStoredPathFixture(t, []string{"ignore-blank-dup.txt"}, 1)
	otherPath := addStandaloneStoredPathFixture(t, first.db, "ignore-blank-dup-other.txt")
	missingPath := filepath.Join(t.TempDir(), "missing.txt")

	result, err := first.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{"   ", first.storedPath, "  " + first.storedPath + "  ", missingPath, otherPath},
		FailFast:    true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths fail-fast ignores blank/dup: %v", err)
	}
	assertStoredPathStatuses(t, result.Items, []engine.BatchItemStatus{
		engine.BatchItemFailed,
		engine.BatchItemOK,
		engine.BatchItemSkipped,
		engine.BatchItemFailed,
	})
	if result.Summary.OK != 1 || result.Summary.Failed != 2 || result.Summary.Skipped != 2 {
		t.Fatalf("unexpected summary: %+v", result.Summary)
	}
}

func TestRemoveStoredPathsSummaryMatchesItemOutcomes(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"summary.txt"}, 1)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath, "", fixture.storedPath},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths summary: %v", err)
	}
	assertStoredPathStatuses(t, result.Items, []engine.BatchItemStatus{
		engine.BatchItemPlanned,
		engine.BatchItemFailed,
		engine.BatchItemSkipped,
	})
	if !result.DryRun || result.ExecutionMode != engine.ExecutionModeSequential {
		t.Fatalf("unexpected result envelope: %+v", result)
	}
	if result.Summary.OK != 1 || result.Summary.Failed != 1 || result.Summary.Skipped != 1 {
		t.Fatalf("unexpected summary: %+v", result.Summary)
	}
}

func TestRemoveStoredPathsProjectsSnapshotInvariantMetadata(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"snapshot-metadata.txt"}, 1)
	seedSnapshotRetentionReference(t, fixture.db, fixture.logicalID, fixture.storedPath)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths snapshot metadata: %v", err)
	}
	item := result.Items[0]
	if item.InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked || item.RecommendedAction != invariants.RecommendedActionForCode(invariants.CodeSnapshotRetainedDeleteBlocked) {
		t.Fatalf("unexpected snapshot invariant metadata: %+v", item)
	}
}

func TestRemoveStoredPathsProjectsRefCountInvariantMetadata(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"refcount-metadata.txt"}, 5)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths ref-count metadata: %v", err)
	}
	item := result.Items[0]
	if item.InvariantCode != invariants.CodePhysicalGraphRefCountMismatch || item.RecommendedAction != invariants.RecommendedActionForCode(invariants.CodePhysicalGraphRefCountMismatch) {
		t.Fatalf("unexpected ref-count invariant metadata: %+v", item)
	}
}

func TestRemoveStoredPathsPostgres(t *testing.T) {
	testgate.RequireDB(t)
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")

	dbconn := openTempPostgresEngineDatabase(t, "coldkeep_phase7_remove")
	if err := dbpkg.EnsurePostgresSchema(dbconn); err != nil {
		t.Fatalf("EnsurePostgresSchema: %v", err)
	}

	eng := newRemoveTestEngine(t, dbconn, t.TempDir())
	logicalID, paths := seedRemoveStoredPathFixture(t, dbconn, []string{"postgres-a.txt", "postgres-b.txt"}, 2)
	before := queryRemoveStoredPathState(t, dbconn, logicalID)

	firstResult, err := eng.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{paths[0]},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths postgres first unlink: %v", err)
	}
	if item := firstResult.Items[0]; item.Status != engine.BatchItemOK || item.RemainingRefCount != 1 || !item.MappingRemoved {
		t.Fatalf("unexpected postgres first unlink item: %+v", item)
	}
	afterFirst := queryRemoveStoredPathState(t, dbconn, logicalID)
	if !afterFirst.logicalExists || afterFirst.refCount != 1 || afterFirst.physicalCount != 1 {
		t.Fatalf("unexpected postgres first unlink state: before=%+v after=%+v", before, afterFirst)
	}
	if before.fileChunkCount != afterFirst.fileChunkCount || !reflect.DeepEqual(before.chunkLiveRefs, afterFirst.chunkLiveRefs) {
		t.Fatalf("unexpected postgres graph drift: before=%+v after=%+v", before, afterFirst)
	}

	secondResult, err := eng.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{paths[1]},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths postgres second unlink: %v", err)
	}
	if item := secondResult.Items[0]; item.Status != engine.BatchItemOK || item.RemainingRefCount != 0 || !item.MappingRemoved {
		t.Fatalf("unexpected postgres second unlink item: %+v", item)
	}

	if err := dbpkg.EnsurePostgresSchema(dbconn); err != nil {
		t.Fatalf("EnsurePostgresSchema rerun: %v", err)
	}
	afterSecond := queryRemoveStoredPathState(t, dbconn, logicalID)
	if !afterSecond.logicalExists || afterSecond.refCount != 0 || afterSecond.physicalCount != 0 {
		t.Fatalf("unexpected postgres last-mapping state: %+v", afterSecond)
	}
	var migratedCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1 AND path LIKE '/migrated/%'`, logicalID).Scan(&migratedCount); err != nil {
		t.Fatalf("count postgres migrated mappings: %v", err)
	}
	if migratedCount != 0 {
		t.Fatalf("expected no postgres migrated mapping resurrection, got %d", migratedCount)
	}
}

func newRemoveStoredPathFixture(t *testing.T, names []string, refCount int64) removeStoredPathFixture {
	t.Helper()

	dbconn := openSnapshotTestDB(t)
	logicalID, paths := seedRemoveStoredPathFixture(t, dbconn, names, refCount)
	return removeStoredPathFixture{
		db:         dbconn,
		engine:     newRemoveTestEngine(t, dbconn, t.TempDir()),
		logicalID:  logicalID,
		storedPath: paths[0],
	}
}

func addStandaloneStoredPathFixture(t *testing.T, dbconn *sql.DB, name string) string {
	t.Helper()

	_, paths := seedRemoveStoredPathFixture(t, dbconn, []string{name}, 1)
	return paths[0]
}

func addStoredPathMapping(t *testing.T, dbconn *sql.DB, logicalID int64, name string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
		path,
		logicalID,
		false,
	); err != nil {
		t.Fatalf("insert physical_file mapping: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = ref_count + 1 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("increment logical_file.ref_count: %v", err)
	}
	return path
}

func seedRemoveStoredPathFixture(t *testing.T, dbconn *sql.DB, names []string, refCount int64) (int64, []string) {
	t.Helper()

	var logicalID int64
	hash := fmt.Sprintf("phase7-remove-%d", time.Now().UnixNano())
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		names[0],
		int64(8),
		hash,
		"COMPLETED",
		refCount,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		hash+"-chunk",
		int64(8),
		"COMPLETED",
		1,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	paths := make([]string, 0, len(names))
	for _, name := range names {
		path := filepath.Join(t.TempDir(), name)
		paths = append(paths, path)
		if _, err := dbconn.Exec(
			`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
			path,
			logicalID,
			false,
		); err != nil {
			t.Fatalf("insert physical_file row: %v", err)
		}
	}
	return logicalID, paths
}

func openFileBackedSQLiteDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func queryRemoveStoredPathState(t *testing.T, dbconn *sql.DB, logicalID int64) removeStoredPathState {
	t.Helper()

	state := removeStoredPathState{
		chunkLiveRefs:  make(map[int64]int64),
		chunkPinCounts: make(map[int64]int64),
	}

	var logicalCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	state.logicalExists = logicalCount == 1
	if state.logicalExists {
		if err := dbconn.QueryRow(`SELECT original_name, ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&state.originalName, &state.refCount); err != nil {
			t.Fatalf("read logical_file state: %v", err)
		}
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID).Scan(&state.physicalCount); err != nil {
		t.Fatalf("count physical_file rows: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, logicalID).Scan(&state.snapshotCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, logicalID).Scan(&state.fileChunkCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}

	rows, err := dbconn.Query(`
		SELECT c.id, c.live_ref_count, c.pin_count
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, logicalID)
	if err != nil {
		t.Fatalf("query chunk state: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chunkID, liveRefCount, pinCount int64
		if err := rows.Scan(&chunkID, &liveRefCount, &pinCount); err != nil {
			t.Fatalf("scan chunk state: %v", err)
		}
		state.chunkLiveRefs[chunkID] = liveRefCount
		state.chunkPinCounts[chunkID] = pinCount
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk state: %v", err)
	}

	return state
}

func assertRemoveStoredPathStateEqual(t *testing.T, before, after removeStoredPathState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("catalog state changed: before=%+v after=%+v", before, after)
	}
}

func assertRemoveStoredPathsValidationError(t *testing.T, result engine.RemoveStoredPathsResult, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected validation error %q", want)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected validation error to remain non-unsupported: %v", err)
	}
	if err.Error() != want {
		t.Fatalf("expected validation error %q, got %q", want, err.Error())
	}
	if result.DryRun || result.ExecutionMode != "" || len(result.Items) != 0 || result.Summary != (engine.BatchSummary{}) {
		t.Fatalf("expected zero result on validation failure, got %+v", result)
	}
}

func assertStoredPathStatuses(t *testing.T, items []engine.RemoveStoredPathItemResult, want []engine.BatchItemStatus) {
	t.Helper()
	if len(items) != len(want) {
		t.Fatalf("status length mismatch: got %d want %d (%+v)", len(items), len(want), items)
	}
	for i, status := range want {
		if items[i].Status != status {
			t.Fatalf("item %d status mismatch: got %q want %q (%+v)", i, items[i].Status, status, items[i])
		}
	}
}
