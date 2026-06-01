package engine_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/engine"
)

// allCandidateTypes returns one zero value per candidate request/result type so
// that the contract tests can exercise every Phase 2 contract uniformly.
func allCandidateTypes() []struct {
	name string
	val  any
} {
	return []struct {
		name string
		val  any
	}{
		{"OperationWarning", engine.OperationWarning{}},
		{"BatchSummary", engine.BatchSummary{}},
		{"SnapshotQuery", engine.SnapshotQuery{}},
		{"StoreRequest", engine.StoreRequest{}},
		{"StoreResult", engine.StoreResult{}},
		{"RestoreRequest", engine.RestoreRequest{}},
		{"RestoreItemResult", engine.RestoreItemResult{}},
		{"RestoreResult", engine.RestoreResult{}},
		{"RemoveRequest", engine.RemoveRequest{}},
		{"RemoveItemResult", engine.RemoveItemResult{}},
		{"RemoveResult", engine.RemoveResult{}},
		{"GarbageCollectRequest", engine.GarbageCollectRequest{}},
		{"GarbageCollectResult", engine.GarbageCollectResult{}},
		{"SnapshotMeta", engine.SnapshotMeta{}},
		{"SnapshotCreateRequest", engine.SnapshotCreateRequest{}},
		{"SnapshotCreateResult", engine.SnapshotCreateResult{}},
		{"SnapshotListRequest", engine.SnapshotListRequest{}},
		{"SnapshotListResult", engine.SnapshotListResult{}},
		{"SnapshotFile", engine.SnapshotFile{}},
		{"SnapshotShowRequest", engine.SnapshotShowRequest{}},
		{"SnapshotShowResult", engine.SnapshotShowResult{}},
		{"SnapshotStatsRequest", engine.SnapshotStatsRequest{}},
		{"SnapshotStatsResult", engine.SnapshotStatsResult{}},
		{"SnapshotDiffEntry", engine.SnapshotDiffEntry{}},
		{"SnapshotDiffRequest", engine.SnapshotDiffRequest{}},
		{"SnapshotDiffSummary", engine.SnapshotDiffSummary{}},
		{"SnapshotDiffResult", engine.SnapshotDiffResult{}},
		{"SnapshotRestoreRequest", engine.SnapshotRestoreRequest{}},
		{"SnapshotRestoreResult", engine.SnapshotRestoreResult{}},
		{"SnapshotDeleteRequest", engine.SnapshotDeleteRequest{}},
		{"SnapshotDeleteResult", engine.SnapshotDeleteResult{}},
		{"RepairRequest", engine.RepairRequest{}},
		{"RepairTargetResult", engine.RepairTargetResult{}},
		{"RepairResult", engine.RepairResult{}},
		{"RecoverRequest", engine.RecoverRequest{}},
		{"RecoverResult", engine.RecoverResult{}},
	}
}

// allowedFieldPackages lists the package import paths a contract field type may
// originate from. Contracts must stay backend- and renderer-neutral: only
// engine-local types and the standard time package are permitted. Anything else
// (database/sql, io, os, cobra, observability, storage, ...) would couple the
// contracts to a backend, the filesystem, or a renderer.
var allowedFieldPackages = map[string]bool{
	"":     true, // built-in types (string, int, bool, []byte, ...)
	"time": true,
	"github.com/franchoy/coldkeep/internal/engine": true,
}

// TestCandidateContractFieldTypesAreNeutral walks every candidate contract type
// and asserts that each field's underlying type comes only from an allowed
// package. This is stronger than the name-based check in candidates_test.go: it
// catches an io.Writer, *sql.DB, or *cobra.Command field even if the field were
// named innocuously.
func TestCandidateContractFieldTypesAreNeutral(t *testing.T) {
	for _, tc := range allCandidateTypes() {
		t.Run(tc.name, func(t *testing.T) {
			assertNeutralStruct(t, reflect.TypeOf(tc.val), tc.name, map[reflect.Type]bool{})
		})
	}
}

func assertNeutralStruct(t *testing.T, rt reflect.Type, path string, seen map[reflect.Type]bool) {
	t.Helper()
	if seen[rt] {
		return
	}
	seen[rt] = true
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		assertNeutralType(t, f.Type, path+"."+f.Name, seen)
	}
}

func assertNeutralType(t *testing.T, rt reflect.Type, path string, seen map[reflect.Type]bool) {
	t.Helper()
	switch rt.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Array:
		assertNeutralType(t, rt.Elem(), path, seen)
		return
	case reflect.Map:
		assertNeutralType(t, rt.Key(), path, seen)
		assertNeutralType(t, rt.Elem(), path, seen)
		return
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		t.Errorf("%s has non-neutral field kind %s", path, rt.Kind())
		return
	case reflect.Interface:
		// No interface fields are permitted in contracts; they could smuggle in
		// io.Writer or similar renderer/backend types.
		t.Errorf("%s is an interface type %q; contracts must use concrete neutral types", path, rt.String())
		return
	}

	if pkg := rt.PkgPath(); !allowedFieldPackages[pkg] {
		t.Errorf("%s has type %q from disallowed package %q", path, rt.String(), pkg)
		return
	}

	if rt.Kind() == reflect.Struct && rt.PkgPath() == "github.com/franchoy/coldkeep/internal/engine" {
		assertNeutralStruct(t, rt, path, seen)
	}
}

// TestRestoreContractRepresentsAllModes proves the restore contract can express
// file-ID batch restore and stored-path restore with each destination mode and
// the overwrite/strict/metadata options.
func TestRestoreContractRepresentsAllModes(t *testing.T) {
	byIDs := engine.RestoreRequest{
		Mode:      engine.RestoreModeFileIDs,
		FileIDs:   []int64{1, 2, 3},
		OutputDir: "/out",
		Overwrite: true,
		DryRun:    true,
		FailFast:  true,
		Workers:   4,
		Limit:     10,
		InputPath: "/batch.txt",
	}
	if byIDs.Mode != engine.RestoreModeFileIDs || len(byIDs.FileIDs) != 3 {
		t.Fatalf("file-ID restore not representable: %+v", byIDs)
	}

	for _, mode := range []engine.RestoreDestinationMode{
		engine.RestoreDestinationOriginal,
		engine.RestoreDestinationPrefix,
		engine.RestoreDestinationOverride,
	} {
		req := engine.RestoreRequest{
			Mode:            engine.RestoreModeStoredPath,
			StoredPath:      "a/b/c.txt",
			DestinationMode: mode,
			Destination:     "/dest",
			Overwrite:       true,
			Strict:          true,
			NoMetadata:      false,
		}
		if req.DestinationMode != mode {
			t.Errorf("stored-path restore mode %q not representable", mode)
		}
	}

	res := engine.RestoreResult{
		DryRun:        true,
		ExecutionMode: engine.ExecutionModeParallel,
		Items: []engine.RestoreItemResult{
			{FileID: 1, OutputPath: "/out/1", RestoredHash: "h", Status: engine.BatchItemOK},
			{StoredPath: "x", Status: engine.BatchItemFailed, Error: "boom"},
		},
		Summary: engine.BatchSummary{OK: 1, Failed: 1},
	}
	if len(res.Items) != 2 || res.Summary.OK != 1 || res.Summary.Failed != 1 {
		t.Fatalf("restore result not representable: %+v", res)
	}
}

// TestRemoveContractRepresentsAllModes proves remove can express ID batch,
// single stored-path, and stored-paths batch removal.
func TestRemoveContractRepresentsAllModes(t *testing.T) {
	cases := []engine.RemoveRequest{
		{Mode: engine.RemoveModeFileIDs, FileIDs: []int64{1, 2}, DryRun: true, FailFast: true},
		{Mode: engine.RemoveModeStoredPath, StoredPath: "a.txt"},
		{Mode: engine.RemoveModeStoredPaths, StoredPaths: []string{"a", "b"}, InputPath: "/in"},
	}
	for _, req := range cases {
		if req.Mode == "" {
			t.Errorf("remove mode not representable: %+v", req)
		}
	}

	res := engine.RemoveResult{
		Items: []engine.RemoveItemResult{
			{FileID: 1, RemainingRefCount: 0, Removed: true, Status: engine.BatchItemOK},
			{StoredPath: "a", RemainingRefCount: 2, Removed: false, Status: engine.BatchItemOK},
		},
		Summary: engine.BatchSummary{OK: 2},
	}
	if len(res.Items) != 2 {
		t.Fatalf("remove result not representable: %+v", res)
	}
}

// TestGarbageCollectContractRepresentsDryRunAndRetention proves GC can express
// dry-run/live and packed+legacy retention breakdowns.
func TestGarbageCollectContractRepresentsDryRunAndRetention(t *testing.T) {
	for _, dry := range []bool{true, false} {
		req := engine.GarbageCollectRequest{DryRun: dry, Workers: 2}
		if req.DryRun != dry {
			t.Errorf("gc dry-run %v not representable", dry)
		}
	}

	res := engine.GarbageCollectResult{
		DryRun:                           true,
		AffectedContainers:               3,
		ContainerFilenames:               []string{"c1", "c2", "c3"},
		SnapshotRetainedContainers:       1,
		SnapshotRetainedLogicalFiles:     2,
		CurrentOnlyRetainedLogicalFiles:  4,
		SnapshotOnlyRetainedLogicalFiles: 5,
		SharedRetainedLogicalFiles:       6,
		BytesReclaimed:                   1024,
	}
	if len(res.ContainerFilenames) != res.AffectedContainers {
		t.Fatalf("gc result not representable: %+v", res)
	}
}

// TestSnapshotContractsRepresentAllOperations proves the snapshot contracts can
// express create/list/show/stats/diff/delete/restore including query filters.
func TestSnapshotContractsRepresentAllOperations(t *testing.T) {
	min := int64(1)
	max := int64(100)
	after := time.Unix(0, 0)
	before := time.Unix(1000, 0)
	q := engine.SnapshotQuery{
		Path:           "p",
		Prefix:         "pre",
		Pattern:        "*.txt",
		Regex:          ".*",
		MinSize:        &min,
		MaxSize:        &max,
		ModifiedAfter:  &after,
		ModifiedBefore: &before,
		Limit:          50,
	}

	create := engine.SnapshotCreateRequest{ID: "s1", Label: "l", ParentID: "p0", Paths: []string{"a"}}
	if create.Paths == nil {
		t.Error("snapshot create (partial) not representable")
	}
	full := engine.SnapshotCreateRequest{Label: "full"}
	if len(full.Paths) != 0 {
		t.Error("snapshot create (full) not representable")
	}

	list := engine.SnapshotListRequest{Type: engine.SnapshotTypeFull, Label: "x", Since: &after, Until: &before, Limit: 10, Tree: true}
	if !list.Tree {
		t.Error("snapshot list not representable")
	}

	show := engine.SnapshotShowRequest{SnapshotID: "s1", Query: q}
	if show.Query.MinSize == nil {
		t.Error("snapshot show query not representable")
	}

	stats := engine.SnapshotStatsResult{SnapshotCount: 1, HasReuse: true, Reused: 3, New: 1, ReuseRatio: 75}
	if !stats.HasReuse {
		t.Error("snapshot stats reuse not representable")
	}

	for _, f := range []engine.SnapshotDiffFilter{
		engine.SnapshotDiffAll, engine.SnapshotDiffAdded,
		engine.SnapshotDiffRemoved, engine.SnapshotDiffModified,
	} {
		diff := engine.SnapshotDiffRequest{BaseID: "a", TargetID: "b", Summary: false, Filter: f, Query: q}
		if diff.Filter != f {
			t.Errorf("snapshot diff filter %q not representable", f)
		}
	}

	del := engine.SnapshotDeleteRequest{SnapshotID: "s1", DryRun: true}
	delForce := engine.SnapshotDeleteRequest{SnapshotID: "s1", Force: true}
	if del.DryRun == delForce.DryRun {
		t.Error("snapshot delete dry-run/force not distinguishable")
	}

	restore := engine.SnapshotRestoreRequest{
		SnapshotID:      "s1",
		Paths:           []string{"a"},
		DestinationMode: engine.RestoreDestinationPrefix,
		Destination:     "/dst",
		Overwrite:       true,
		Strict:          true,
		Query:           q,
	}
	if restore.DestinationMode != engine.RestoreDestinationPrefix {
		t.Error("snapshot restore not representable")
	}
}

// TestRepairContractRepresentsTargetsAndBatch proves repair can express the
// single targets and batch/fail-fast behavior.
func TestRepairContractRepresentsTargetsAndBatch(t *testing.T) {
	single := engine.RepairRequest{Target: engine.RepairTargetRefCounts}
	if single.Target != engine.RepairTargetRefCounts {
		t.Error("single ref-counts repair not representable")
	}
	batch := engine.RepairRequest{
		Batch:     true,
		Targets:   []engine.RepairTarget{engine.RepairTargetRefCounts, engine.RepairTargetChunkLiveRefCounts},
		FailFast:  true,
		InputPath: "/in",
	}
	if !batch.Batch || len(batch.Targets) != 2 {
		t.Error("batch repair not representable")
	}
	res := engine.RepairResult{
		Targets: []engine.RepairTargetResult{
			{Target: engine.RepairTargetRefCounts, ScannedRows: 10, UpdatedRows: 2, OrphanRows: 1, Status: engine.BatchItemOK},
			{Target: engine.RepairTargetChunkLiveRefCounts, ScannedRows: 5, UpdatedRows: 0, Status: engine.BatchItemOK},
		},
		Summary: engine.BatchSummary{OK: 2},
	}
	if len(res.Targets) != 2 {
		t.Fatalf("repair result not representable: %+v", res)
	}
}

// TestRecoverContractRepresentsCorrectiveReport proves the recovery contract
// models a corrective integrity report rather than a restore.
func TestRecoverContractRepresentsCorrectiveReport(t *testing.T) {
	req := engine.RecoverRequest{DryRun: true}
	if !req.DryRun {
		t.Error("recover dry-run not representable")
	}
	res := engine.RecoverResult{
		AbortedLogicalFiles:    1,
		AbortedChunks:          2,
		QuarantinedMissing:     3,
		QuarantinedCorruptTail: 4,
		QuarantinedOrphan:      5,
		SkippedDirEntries:      6,
		CheckedContainerRecord: 7,
		CheckedDiskFiles:       8,
		SealingCompleted:       9,
		SealingQuarantined:     10,
	}
	if res.AbortedLogicalFiles != 1 || res.SealingQuarantined != 10 {
		t.Fatalf("recovery report not representable: %+v", res)
	}
	// Guard: the recovery contract must not look like a restore.
	rt := reflect.TypeOf(req)
	for _, bad := range []string{"OutputPath", "SnapshotID", "Destination"} {
		if _, ok := rt.FieldByName(bad); ok {
			t.Errorf("RecoverRequest should not have restore-like field %q", bad)
		}
	}
}

// TestStoreContractRepresentsFileAndFolder proves store covers single file and
// recursive folder store with codec and workers.
func TestStoreContractRepresentsFileAndFolder(t *testing.T) {
	file := engine.StoreRequest{SourcePath: "f.txt", Codec: "aes-gcm"}
	folder := engine.StoreRequest{SourcePath: "dir", Recursive: true, Workers: 8, Codec: "plain"}
	if file.Recursive || !folder.Recursive || folder.Workers != 8 {
		t.Fatalf("store contract not representable: file=%+v folder=%+v", file, folder)
	}
	res := engine.StoreResult{
		SourcePath:    "f.txt",
		StoredPath:    "f.txt",
		LogicalFileID: 1,
		FileHash:      "h",
		AlreadyStored: true,
		ChunksReused:  3,
	}
	if !res.AlreadyStored || res.ChunksReused != 3 {
		t.Fatalf("store result not representable: %+v", res)
	}
}
