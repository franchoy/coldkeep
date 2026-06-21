package engine_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

// TestEngineActiveInterfaceApprovedMethods asserts that the active Engine
// interface contains exactly the currently supported engine methods and no
// accidentally activated future-only methods.
//
// This test is a guardrail: it must fail if a candidate method is
// accidentally added to the interface without explicit phase approval.
func TestEngineActiveInterfaceApprovedMethods(t *testing.T) {
	typ := reflect.TypeOf((*engine.Engine)(nil)).Elem()

	got := make(map[string]bool, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		got[typ.Method(i).Name] = true
	}

	want := []string{"Stats", "Inspect", "Verify", "SnapshotList", "SnapshotShow", "SnapshotStats", "SnapshotDiff", "GarbageCollect", "Store", "Remove", "RemoveStoredPaths", "Restore", "RestoreStoredPath"}
	for _, name := range want {
		if !got[name] {
			t.Errorf("Engine interface missing expected method %q", name)
		}
	}
	if len(got) != len(want) {
		names := make([]string, 0, len(got))
		for n := range got {
			names = append(names, n)
		}
		t.Errorf("Engine interface has %d methods, want exactly %d %v; got %v",
			len(got), len(want), want, names)
	}
}

// TestEngineActiveInterfaceExcludesCandidateOnlyOperations proves the active
// Engine interface still excludes future-only snapshot mutation and corrective
// integrity operations. Their request/result types are intentionally present as
// candidate contracts, but they are not active engine-owned methods.
func TestEngineActiveInterfaceExcludesCandidateOnlyOperations(t *testing.T) {
	typ := reflect.TypeOf((*engine.Engine)(nil)).Elem()

	got := make([]string, 0, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		got = append(got, typ.Method(i).Name)
	}

	for _, forbidden := range []string{
		"SnapshotCreate",
		"SnapshotDelete",
		"SnapshotRestore",
		"Repair",
		"Recover",
	} {
		if slices.Contains(got, forbidden) {
			t.Fatalf("Engine interface unexpectedly exposes candidate-only method %q; active methods=%v", forbidden, got)
		}
	}
}

// TestCandidateOnlyOperationContractsRemainOutsideActiveEngineOwnership
// documents which request/result pairs remain intentionally future-only and
// ties them to the approved active engine method set.
func TestCandidateOnlyOperationContractsRemainOutsideActiveEngineOwnership(t *testing.T) {
	activeMethods := activeEngineMethodSet()

	cases := []struct {
		name              string
		requestType       any
		resultType        any
		forbiddenMethod   string
		laterOwnerRelease string
	}{
		{
			name:              "snapshot create",
			requestType:       engine.SnapshotCreateRequest{},
			resultType:        engine.SnapshotCreateResult{},
			forbiddenMethod:   "SnapshotCreate",
			laterOwnerRelease: "v1.13.9",
		},
		{
			name:              "snapshot delete",
			requestType:       engine.SnapshotDeleteRequest{},
			resultType:        engine.SnapshotDeleteResult{},
			forbiddenMethod:   "SnapshotDelete",
			laterOwnerRelease: "v1.13.9",
		},
		{
			name:              "snapshot restore",
			requestType:       engine.SnapshotRestoreRequest{},
			resultType:        engine.SnapshotRestoreResult{},
			forbiddenMethod:   "SnapshotRestore",
			laterOwnerRelease: "v1.13.9",
		},
		{
			name:              "repair",
			requestType:       engine.RepairRequest{},
			resultType:        engine.RepairResult{},
			forbiddenMethod:   "Repair",
			laterOwnerRelease: "v1.13.10",
		},
		{
			name:              "recover",
			requestType:       engine.RecoverRequest{},
			resultType:        engine.RecoverResult{},
			forbiddenMethod:   "Recover",
			laterOwnerRelease: "v1.13.10",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if activeMethods[tc.forbiddenMethod] {
				t.Fatalf("candidate-only contract %q unexpectedly has active engine method %q", tc.name, tc.forbiddenMethod)
			}

			reqType := reflect.TypeOf(tc.requestType)
			resType := reflect.TypeOf(tc.resultType)
			if reqType.Name() == "" || resType.Name() == "" {
				t.Fatalf("candidate-only contract %q must remain a named request/result type", tc.name)
			}
			if reqType.PkgPath() != "github.com/franchoy/coldkeep/internal/engine" || resType.PkgPath() != "github.com/franchoy/coldkeep/internal/engine" {
				t.Fatalf("candidate-only contract %q moved outside engine package unexpectedly", tc.name)
			}
			if tc.laterOwnerRelease == "" {
				t.Fatalf("candidate-only contract %q must record a later owner release", tc.name)
			}
		})
	}
}

func activeEngineMethodSet() map[string]bool {
	typ := reflect.TypeOf((*engine.Engine)(nil)).Elem()
	methods := make(map[string]bool, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		methods[typ.Method(i).Name] = true
	}
	return methods
}

// TestCandidateContractsAreRendererNeutral verifies that mutating operation
// candidate request/result types do not expose renderer-specific or
// CLI-specific concepts (cobra, command, renderer, writer, stdout, stderr).
func TestCandidateContractsAreRendererNeutral(t *testing.T) {
	forbidden := []string{
		"cobra", "command", "renderer", "writer", "stdout", "stderr",
	}

	typesToCheck := []struct {
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

	for _, tc := range typesToCheck {
		t.Run(tc.name, func(t *testing.T) {
			rt := reflect.TypeOf(tc.val)
			for i := 0; i < rt.NumField(); i++ {
				field := rt.Field(i)
				if !field.IsExported() {
					continue
				}
				lower := strings.ToLower(field.Name)
				for _, term := range forbidden {
					if strings.Contains(lower, term) {
						t.Errorf("engine.%s has renderer-specific field %q (contains %q)",
							tc.name, field.Name, term)
					}
				}
			}
		})
	}
}
