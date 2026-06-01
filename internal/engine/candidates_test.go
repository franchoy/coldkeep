package engine_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

// TestEngineActiveInterfaceRemainsReadOriented asserts that the active Engine
// interface contains exactly the read-oriented methods approved for v1.12:
// Stats, Inspect, Verify (original), and the 4 read-side snapshot methods
// added in v1.12 Phase 5 (SnapshotList, SnapshotShow, SnapshotStats, SnapshotDiff).
//
// This test is a guardrail: it must fail if a mutating candidate method is
// accidentally added to the interface without explicit phase approval.
func TestEngineActiveInterfaceRemainsReadOriented(t *testing.T) {
	typ := reflect.TypeOf((*engine.Engine)(nil)).Elem()

	got := make(map[string]bool, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		got[typ.Method(i).Name] = true
	}

	want := []string{"Stats", "Inspect", "Verify", "SnapshotList", "SnapshotShow", "SnapshotStats", "SnapshotDiff"}
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
