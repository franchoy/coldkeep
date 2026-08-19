package engine_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/engine"
)

// allEngineContractTypes returns one zero value per engine request/result type
// so that the contract tests can exercise every contract uniformly.
func allEngineContractTypes() []struct {
	name string
	val  any
} {
	types := append([]struct {
		name string
		val  any
	}{},
		activeCoreContractTypes()...,
	)
	types = append(types, activeSnapshotMutationContractTypes()...)
	return append(types, candidateCorrectiveContractTypes()...)
}

func activeCoreContractTypes() []struct {
	name string
	val  any
} {
	return []struct {
		name string
		val  any
	}{
		{"OperationWarning", engine.OperationWarning{}},
		{"Error", engine.Error{}},
		{"StatsRequest", engine.StatsRequest{}},
		{"StatsResult", engine.StatsResult{}},
		{"InspectRequest", engine.InspectRequest{}},
		{"InspectResult", engine.InspectResult{}},
		{"BatchSummary", engine.BatchSummary{}},
		{"SnapshotQuery", engine.SnapshotQuery{}},
		{"StoreRequest", engine.StoreRequest{}},
		{"StoreResult", engine.StoreResult{}},
		{"StoreFolderRequest", engine.StoreFolderRequest{}},
		{"StoreFolderResult", engine.StoreFolderResult{}},
		{"RestoreRequest", engine.RestoreRequest{}},
		{"RestoreItemResult", engine.RestoreItemResult{}},
		{"RestoreResult", engine.RestoreResult{}},
		{"RestoreStoredPathRequest", engine.RestoreStoredPathRequest{}},
		{"RestoreStoredPathResult", engine.RestoreStoredPathResult{}},
		{"RemoveRequest", engine.RemoveRequest{}},
		{"RemoveItemResult", engine.RemoveItemResult{}},
		{"RemoveResult", engine.RemoveResult{}},
		{"RemoveStoredPathsRequest", engine.RemoveStoredPathsRequest{}},
		{"RemoveStoredPathItemResult", engine.RemoveStoredPathItemResult{}},
		{"RemoveStoredPathsResult", engine.RemoveStoredPathsResult{}},
		{"GarbageCollectRequest", engine.GarbageCollectRequest{}},
		{"GarbageCollectResult", engine.GarbageCollectResult{}},
		{"VerifyRequest", engine.VerifyRequest{}},
		{"VerifyResult", engine.VerifyResult{}},
		{"SnapshotMeta", engine.SnapshotMeta{}},
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
	}
}

// TestActiveEngineContractNeutralityCoverage prevents a request/result pair
// from escaping the structural neutrality walk when a method is added.
func TestActiveEngineContractNeutralityCoverage(t *testing.T) {
	covered := make(map[reflect.Type]bool)
	for _, tc := range allEngineContractTypes() {
		covered[reflect.TypeOf(tc.val)] = true
	}

	interfaceType := reflect.TypeOf((*engine.Engine)(nil)).Elem()
	for i := 0; i < interfaceType.NumMethod(); i++ {
		method := interfaceType.Method(i)
		if method.Type.NumIn() != 2 || method.Type.NumOut() != 2 {
			t.Fatalf("Engine.%s has unexpected signature %s", method.Name, method.Type)
		}
		for _, contractType := range []reflect.Type{method.Type.In(1), method.Type.Out(0)} {
			if covered[contractType] {
				continue
			}
			t.Errorf("Engine.%s contract %s is absent from the neutrality walk", method.Name, contractType)
		}
	}
}

func activeSnapshotMutationContractTypes() []struct {
	name string
	val  any
} {
	return []struct {
		name string
		val  any
	}{
		{"SnapshotCreateRequest", engine.SnapshotCreateRequest{}},
		{"SnapshotCreateResult", engine.SnapshotCreateResult{}},
		{"SnapshotDeleteParent", engine.SnapshotDeleteParent{}},
		{"SnapshotDeletePreviewResult", engine.SnapshotDeletePreviewResult{}},
		{"SnapshotRestoreSelection", engine.SnapshotRestoreSelection{}},
		{"SnapshotRestoreDestination", engine.SnapshotRestoreDestination{}},
		{"SnapshotRestoreWarning", engine.SnapshotRestoreWarning{}},
		{"SnapshotRestoreRequest", engine.SnapshotRestoreRequest{}},
		{"SnapshotRestoreResult", engine.SnapshotRestoreResult{}},
		{"SnapshotDeleteRequest", engine.SnapshotDeleteRequest{}},
		{"SnapshotDeleteResult", engine.SnapshotDeleteResult{}},
	}
}

func candidateCorrectiveContractTypes() []struct {
	name string
	val  any
} {
	return []struct {
		name string
		val  any
	}{
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

// TestEngineContractFieldTypesAreNeutral walks every engine contract type
// and asserts that each field's underlying type comes only from an allowed
// package. This is stronger than the name-based check in candidates_test.go: it
// catches an io.Writer, *sql.DB, or *cobra.Command field even if the field were
// named innocuously.
func TestEngineContractFieldTypesAreNeutral(t *testing.T) {
	for _, tc := range allEngineContractTypes() {
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

// TestRestoreContractRepresentsByIDOnlySurface proves the active restore
// contract now represents only by-ID restore semantics.
func TestRestoreContractRepresentsByIDOnlySurface(t *testing.T) {
	req := engine.RestoreRequest{
		FileIDs:         []int64{1, 2, 3},
		DestinationRoot: "/out",
		Overwrite:       true,
		DryRun:          true,
		FailFast:        true,
	}
	if len(req.FileIDs) != 3 || req.DestinationRoot == "" {
		t.Fatalf("by-ID restore not representable: %+v", req)
	}

	res := engine.RestoreResult{
		DryRun:        true,
		ExecutionMode: engine.ExecutionModeSequential,
		Items: []engine.RestoreItemResult{
			{FileID: 1, DestinationPath: "/out/1", RestoredHash: "h", Status: engine.BatchItemOK},
			{FileID: 2, Status: engine.BatchItemFailed, Error: "boom"},
		},
		Summary: engine.BatchSummary{OK: 1, Failed: 1},
	}
	if len(res.Items) != 2 || res.Summary.OK != 1 || res.Summary.Failed != 1 {
		t.Fatalf("restore result not representable: %+v", res)
	}
}

func TestRestoreStoredPathRequestHasOnlyApprovedFields(t *testing.T) {
	assertStructFields(t, reflect.TypeOf(engine.RestoreStoredPathRequest{}), []string{
		"StoredPath",
		"DestinationMode",
		"DestinationRoot",
		"DestinationPath",
		"Overwrite",
		"StrictMetadata",
		"NoMetadata",
	})
}

func TestRestoreStoredPathResultHasOnlyApprovedFields(t *testing.T) {
	assertStructFields(t, reflect.TypeOf(engine.RestoreStoredPathResult{}), []string{
		"StoredPath",
		"FileID",
		"DestinationMode",
		"DestinationPath",
		"RestoredHash",
	})
}

func TestRestoreStoredPathContractRepresentsSingleStoredMappingRestore(t *testing.T) {
	cases := []engine.RestoreStoredPathRequest{
		{
			StoredPath: "/docs/original.txt",
			Overwrite:  true,
		},
		{
			StoredPath:      "/docs/prefix.txt",
			DestinationMode: engine.RestoreDestinationPrefix,
			DestinationRoot: "/tmp/out",
		},
		{
			StoredPath:      "/docs/override.txt",
			DestinationMode: engine.RestoreDestinationOverride,
			DestinationPath: "/tmp/out.txt",
			StrictMetadata:  true,
		},
	}
	for _, req := range cases {
		if req.StoredPath == "" {
			t.Fatalf("stored-path restore request not representable: %+v", req)
		}
	}

	res := engine.RestoreStoredPathResult{
		StoredPath:      "/docs/original.txt",
		FileID:          42,
		DestinationMode: engine.RestoreDestinationOriginal,
		DestinationPath: "/docs/original.txt",
		RestoredHash:    "hash",
	}
	if res.StoredPath == "" || res.FileID <= 0 || res.DestinationPath == "" {
		t.Fatalf("stored-path restore result not representable: %+v", res)
	}
}

// TestRemoveContractRepresentsByIDOnlySurface proves the active remove
// contract now represents only by-ID remove semantics.
func TestRemoveContractRepresentsByIDOnlySurface(t *testing.T) {
	req := engine.RemoveRequest{FileIDs: []int64{1, 2}, DryRun: true, FailFast: true}
	if len(req.FileIDs) != 2 {
		t.Fatalf("by-ID remove not representable: %+v", req)
	}

	res := engine.RemoveResult{
		Items: []engine.RemoveItemResult{
			{FileID: 1, LogicalFileRemoved: true, RemovedChunkAssociations: 2, Status: engine.BatchItemOK},
			{FileID: 2, Status: engine.BatchItemFailed, Error: "boom"},
		},
		Summary: engine.BatchSummary{OK: 1, Failed: 1},
	}
	if len(res.Items) != 2 || res.Summary.OK != 1 || res.Summary.Failed != 1 {
		t.Fatalf("remove result not representable: %+v", res)
	}
}

func TestRemoveStoredPathsRequestHasOnlyApprovedFields(t *testing.T) {
	assertStructFields(t, reflect.TypeOf(engine.RemoveStoredPathsRequest{}), []string{
		"StoredPaths",
		"DryRun",
		"FailFast",
	})
}

func TestRemoveStoredPathItemResultHasOnlyApprovedFields(t *testing.T) {
	assertStructFields(t, reflect.TypeOf(engine.RemoveStoredPathItemResult{}), []string{
		"RawTarget",
		"StoredPath",
		"LogicalFileID",
		"RemainingRefCount",
		"MappingRemoved",
		"Status",
		"Error",
		"InvariantCode",
		"RecommendedAction",
	})
}

func TestRemoveStoredPathsResultHasOnlyApprovedFields(t *testing.T) {
	assertStructFields(t, reflect.TypeOf(engine.RemoveStoredPathsResult{}), []string{
		"DryRun",
		"ExecutionMode",
		"Items",
		"Summary",
	})
}

func TestRemoveStoredPathsContractRepresentsBatchStoredMappingUnlink(t *testing.T) {
	req := engine.RemoveStoredPathsRequest{
		StoredPaths: []string{" /docs/a.txt ", "", "/docs/a.txt", "/docs/b.txt"},
		DryRun:      true,
		FailFast:    true,
	}
	if len(req.StoredPaths) != 4 || !req.DryRun || !req.FailFast {
		t.Fatalf("stored-path remove request not representable: %+v", req)
	}

	res := engine.RemoveStoredPathsResult{
		DryRun:        true,
		ExecutionMode: engine.ExecutionModeSequential,
		Items: []engine.RemoveStoredPathItemResult{
			{RawTarget: " /docs/a.txt ", StoredPath: "/docs/a.txt", LogicalFileID: 7, Status: engine.BatchItemPlanned},
			{RawTarget: "", Status: engine.BatchItemFailed, Error: "stored path is required"},
			{RawTarget: "/docs/a.txt", StoredPath: "/docs/a.txt", Status: engine.BatchItemSkipped, Error: "duplicate target"},
		},
		Summary: engine.BatchSummary{OK: 1, Failed: 1, Skipped: 1},
	}
	if len(res.Items) != 3 || res.Summary.OK != 1 || res.Summary.Failed != 1 || res.Summary.Skipped != 1 {
		t.Fatalf("stored-path remove result not representable: %+v", res)
	}
}

func assertStructFields(t *testing.T, rt reflect.Type, want []string) {
	t.Helper()

	got := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if field.IsExported() {
			got = append(got, field.Name)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s fields mismatch: got %v want %v", rt.Name(), got, want)
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
	q, after, before := snapshotContractQuery()
	assertSnapshotCreateContracts(t)
	assertSnapshotListContract(t, after, before)
	assertSnapshotShowContract(t, q)
	assertSnapshotStatsContract(t)
	assertSnapshotDiffContracts(t, q)
	assertSnapshotDeleteContract(t)
	assertSnapshotRestoreContract(t, after, before)
}

func snapshotContractQuery() (engine.SnapshotQuery, time.Time, time.Time) {
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
	return q, after, before
}

func assertSnapshotCreateContracts(t *testing.T) {
	t.Helper()
	create := engine.SnapshotCreateRequest{ID: "s1", Label: "l", ParentID: "p0", Paths: []string{"a"}}
	if create.Paths == nil {
		t.Error("snapshot create (partial) not representable")
	}
	full := engine.SnapshotCreateRequest{Label: "full"}
	if len(full.Paths) != 0 {
		t.Error("snapshot create (full) not representable")
	}
	result := engine.SnapshotCreateResult{
		SnapshotID:    "s1",
		Type:          engine.SnapshotTypePartial,
		PathsCount:    1,
		FilesInserted: 2,
		Label:         "l",
		ParentID:      "p0",
	}
	if result.SnapshotID == "" || result.Type == "" {
		t.Error("snapshot create result not representable")
	}
}

func assertSnapshotListContract(t *testing.T, after time.Time, before time.Time) {
	t.Helper()
	list := engine.SnapshotListRequest{
		Type:  engine.SnapshotTypeFull,
		Label: "x",
		Since: &after,
		Until: &before,
		Limit: 10,
		Tree:  true,
	}
	if !list.Tree {
		t.Error("snapshot list not representable")
	}
}

func assertSnapshotShowContract(t *testing.T, q engine.SnapshotQuery) {
	t.Helper()
	show := engine.SnapshotShowRequest{SnapshotID: "s1", Query: q}
	if show.Query.MinSize == nil {
		t.Error("snapshot show query not representable")
	}
}

func assertSnapshotStatsContract(t *testing.T) {
	t.Helper()
	stats := engine.SnapshotStatsResult{SnapshotCount: 1, HasReuse: true, Reused: 3, New: 1, ReuseRatio: 75}
	if !stats.HasReuse {
		t.Error("snapshot stats reuse not representable")
	}
}

func assertSnapshotDiffContracts(t *testing.T, q engine.SnapshotQuery) {
	t.Helper()
	for _, f := range snapshotDiffFilters() {
		diff := engine.SnapshotDiffRequest{BaseID: "a", TargetID: "b", Summary: false, Filter: f, Query: q}
		if diff.Filter != f {
			t.Errorf("snapshot diff filter %q not representable", f)
		}
	}
}

func snapshotDiffFilters() []engine.SnapshotDiffFilter {
	return []engine.SnapshotDiffFilter{
		engine.SnapshotDiffAll,
		engine.SnapshotDiffAdded,
		engine.SnapshotDiffRemoved,
		engine.SnapshotDiffModified,
	}
}

func assertSnapshotDeleteContract(t *testing.T) {
	t.Helper()
	previewReq := engine.SnapshotDeleteRequest{SnapshotID: "s1", Mode: engine.SnapshotDeleteModePreview}
	executeReq := engine.SnapshotDeleteRequest{SnapshotID: "s1", Mode: engine.SnapshotDeleteModeExecute}
	if previewReq.Mode == executeReq.Mode {
		t.Error("snapshot delete preview/execute modes not distinguishable")
	}

	preview := engine.SnapshotDeleteResult{
		SnapshotID: "s1",
		Mode:       engine.SnapshotDeleteModePreview,
		Deleted:    false,
		Preview: &engine.SnapshotDeletePreviewResult{
			Parent:      engine.SnapshotDeleteParent{ID: "p0", State: engine.SnapshotDeleteParentPresent},
			Children:    []string{"c1", "c2"},
			TotalFiles:  10,
			UniqueFiles: 4,
			SharedFiles: 6,
		},
	}
	if preview.Preview == nil || preview.Deleted {
		t.Error("snapshot delete preview result not representable")
	}

	execute := engine.SnapshotDeleteResult{
		SnapshotID: "s1",
		Mode:       engine.SnapshotDeleteModeExecute,
		Deleted:    true,
	}
	if !execute.Deleted || execute.Preview != nil {
		t.Error("snapshot delete execute result not representable")
	}
}

func assertSnapshotRestoreContract(t *testing.T, after time.Time, before time.Time) {
	t.Helper()
	restore := engine.SnapshotRestoreRequest{
		SnapshotID: "s1",
		Paths:      []string{"a", "dir/"},
		Selection: engine.SnapshotRestoreSelection{
			ExactPaths:     []string{"docs/a.txt", "docs/a.txt"},
			Prefixes:       []string{"docs/", "logs/"},
			Pattern:        "*.txt",
			Regex:          "^docs/",
			ModifiedAfter:  &after,
			ModifiedBefore: &before,
		},
		Destination: engine.SnapshotRestoreDestination{
			Mode: engine.SnapshotRestoreDestinationPrefix,
			Path: "/dst",
		},
		Overwrite: true,
		Metadata:  engine.SnapshotRestoreMetadataStrict,
	}
	if restore.Destination.Mode != engine.SnapshotRestoreDestinationPrefix {
		t.Error("snapshot restore not representable")
	}

	result := engine.SnapshotRestoreResult{
		SnapshotID:          "s1",
		DestinationMode:     engine.SnapshotRestoreDestinationPrefix,
		RequestedPathsCount: 2,
		RestoredFiles:       3,
		OutputTarget:        "/dst",
		OutputPaths:         []string{"/dst/docs/a.txt"},
		Warnings: []engine.SnapshotRestoreWarning{
			{
				Code:      engine.SnapshotRestoreWarningMetadata,
				Path:      "/dst/docs/a.txt",
				Operation: "chmod",
				Detail:    "permission denied",
			},
		},
	}
	if result.DestinationMode == "" || result.RestoredFiles == 0 {
		t.Error("snapshot restore result not representable")
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
	assertRecoverRequestIsCorrectiveOnly(t, req)
}

func assertRecoverRequestIsCorrectiveOnly(t *testing.T, req engine.RecoverRequest) {
	t.Helper()
	if !req.DryRun {
		t.Error("recover request should represent corrective dry-run mode")
	}
}

// TestStoreContractsSeparateFileAndFolder proves recursive traversal is a
// distinct operation rather than dormant fields on the single-file request.
func TestStoreContractsSeparateFileAndFolder(t *testing.T) {
	file := engine.StoreRequest{SourcePath: "f.txt", Codec: "aes-gcm"}
	folder := engine.StoreFolderRequest{SourcePath: "dir", Workers: 8, Codec: "plain"}
	if file.SourcePath != "f.txt" || folder.SourcePath != "dir" || folder.Workers != 8 {
		t.Fatalf("store contract not representable: file=%+v folder=%+v", file, folder)
	}
	fileResult := engine.StoreResult{
		SourcePath:    "f.txt",
		StoredPath:    "f.txt",
		LogicalFileID: 1,
		FileHash:      "h",
		AlreadyStored: true,
	}
	folderResult := engine.StoreFolderResult{
		SourcePath:   "dir",
		FilesStored:  3,
		BytesLogical: 42,
		WorkersUsed:  2,
	}
	if !fileResult.AlreadyStored || folderResult.FilesStored != 3 || folderResult.BytesLogical != 42 {
		t.Fatalf("store results not representable: file=%+v folder=%+v", fileResult, folderResult)
	}
}
