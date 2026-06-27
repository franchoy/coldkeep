package engine_test

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/tests/utils/removestate"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

func TestRemoveStoredPathsMixedBatchDecrementsRefCountOncePerSuccessfulMapping(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"batch-a.txt", "batch-b.txt", "batch-c.txt", "batch-d.txt"}, 4)
	targetB := addStoredPathMapping(t, fixture.db, fixture.logicalID, "batch-extra-b.txt")
	targetC := addStoredPathMapping(t, fixture.db, fixture.logicalID, "batch-extra-c.txt")
	before := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{
			fixture.storedPath,
			fixture.storedPath,
			"   ",
			targetB,
			filepath.Join(t.TempDir(), "missing.txt"),
			targetC,
		},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths mixed batch accounting: %v", err)
	}

	assertStoredPathStatuses(t, result.Items, []engine.BatchItemStatus{
		engine.BatchItemOK,
		engine.BatchItemSkipped,
		engine.BatchItemFailed,
		engine.BatchItemOK,
		engine.BatchItemFailed,
		engine.BatchItemOK,
	})
	assertRemoveStoredPathsBatchSummary(t, result.Summary, 3, 2, 1)
	assertMixedRemoveStoredPathState(t, before, queryRemoveStoredPathState(t, fixture.db, fixture.logicalID), 3)
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

func TestRemoveStoredPathsSecondUnlinkReportsAlreadyRemovedWithoutFurtherMutation(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"repeat-a.txt", "repeat-b.txt"}, 2)

	first := removeStoredPathOnce(t, fixture, fixture.storedPath)
	assertStoredPathUnlinkSuccess(t, first, 1)
	afterFirst := removestate.Capture(t, fixture.db, fixture.logicalID, "")

	second := removeStoredPathOnce(t, fixture, fixture.storedPath)
	if item := second.Items[0]; item.Status != engine.BatchItemFailed || item.MappingRemoved {
		t.Fatalf("unexpected second unlink item: %+v", item)
	}
	if !strings.Contains(second.Items[0].Error, "already removed") && !strings.Contains(second.Items[0].Error, "not found (never stored)") {
		t.Fatalf("expected already-removed/never-stored meaning, got %+v", second.Items[0])
	}
	afterSecond := removestate.Capture(t, fixture.db, fixture.logicalID, "")
	removestate.AssertEqual(t, afterFirst, afterSecond)
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
	assertRemoveStoredPathsBatchSummary(t, result.Summary, 2, 1, 0)
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
	assertRemoveStoredPathsBatchSummary(t, result.Summary, 1, 1, 1)
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
	assertRemoveStoredPathsBatchSummary(t, result.Summary, 1, 2, 2)
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
	assertRemoveStoredPathsBatchSummary(t, result.Summary, 1, 1, 1)
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

	firstResult := removeStoredPathsWithRequest(t, eng, engine.RemoveStoredPathsRequest{StoredPaths: []string{paths[0]}}, "RemoveStoredPaths postgres first unlink")
	assertStoredPathUnlinkSuccess(t, firstResult, 1)
	afterFirst := queryRemoveStoredPathState(t, dbconn, logicalID)
	if !afterFirst.logicalExists || afterFirst.refCount != 1 || afterFirst.physicalCount != 1 {
		t.Fatalf("unexpected postgres first unlink state: before=%+v after=%+v", before, afterFirst)
	}
	if before.fileChunkCount != afterFirst.fileChunkCount || !reflect.DeepEqual(before.chunkLiveRefs, afterFirst.chunkLiveRefs) {
		t.Fatalf("unexpected postgres graph drift: before=%+v after=%+v", before, afterFirst)
	}

	secondResult := removeStoredPathsWithRequest(t, eng, engine.RemoveStoredPathsRequest{StoredPaths: []string{paths[1]}}, "RemoveStoredPaths postgres second unlink")
	assertStoredPathUnlinkSuccess(t, secondResult, 0)

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

func removeStoredPathOnce(t *testing.T, fixture removeStoredPathFixture, storedPath string) engine.RemoveStoredPathsResult {
	t.Helper()
	return removeStoredPathsWithRequest(t, fixture.engine, engine.RemoveStoredPathsRequest{
		StoredPaths: []string{storedPath},
	}, "RemoveStoredPaths")
}

func removeStoredPathsWithRequest(t *testing.T, eng *engine.DefaultEngine, req engine.RemoveStoredPathsRequest, callName string) engine.RemoveStoredPathsResult {
	t.Helper()
	result, err := eng.RemoveStoredPaths(context.Background(), req)
	if err != nil {
		t.Fatalf("%s: %v", callName, err)
	}
	return result
}

func assertStoredPathUnlinkSuccess(t *testing.T, result engine.RemoveStoredPathsResult, wantRemainingRefCount int64) {
	t.Helper()
	if item := result.Items[0]; item.Status != engine.BatchItemOK || item.RemainingRefCount != wantRemainingRefCount || !item.MappingRemoved {
		t.Fatalf("unexpected unlink item: %+v", item)
	}
}

func assertRemoveStoredPathsBatchSummary(t *testing.T, summary engine.BatchSummary, ok, failed, skipped int) {
	t.Helper()
	if summary.OK != ok || summary.Failed != failed || summary.Skipped != skipped {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func assertMixedRemoveStoredPathState(t *testing.T, before, after removeStoredPathState, removedCount int64) {
	t.Helper()
	if !after.logicalExists {
		t.Fatalf("logical file should remain after mixed batch: %+v", after)
	}
	if after.refCount != before.refCount-removedCount {
		t.Fatalf("expected ref_count to drop by %d, before=%d after=%d", removedCount, before.refCount, after.refCount)
	}
	if after.physicalCount != before.physicalCount-int(removedCount) {
		t.Fatalf("expected physical mapping count to drop by %d, before=%d after=%d", removedCount, before.physicalCount, after.physicalCount)
	}
	if after.physicalCount != int(after.refCount) {
		t.Fatalf("expected physical count to match ref_count, state=%+v", after)
	}
	if before.fileChunkCount != after.fileChunkCount || !reflect.DeepEqual(before.chunkLiveRefs, after.chunkLiveRefs) || !reflect.DeepEqual(before.chunkPinCounts, after.chunkPinCounts) {
		t.Fatalf("unexpected chunk graph drift: before=%+v after=%+v", before, after)
	}
}
