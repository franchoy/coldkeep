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
// accidentally added to the interface without explicit contract activation,
// implementation, and guardrail update.
func TestEngineActiveInterfaceApprovedMethods(t *testing.T) {
	typ := reflect.TypeOf((*engine.Engine)(nil)).Elem()

	got := make(map[string]bool, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		got[typ.Method(i).Name] = true
	}

	want := []string{"Stats", "Inspect", "Verify", "SnapshotList", "SnapshotShow", "SnapshotStats", "SnapshotDiff", "SnapshotCreate", "SnapshotDelete", "SnapshotRestore", "GarbageCollect", "Store", "Remove", "RemoveStoredPaths", "Restore", "RestoreStoredPath"}
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

// TestEngineActiveInterfaceExcludesStillInactiveOperations proves the active
// Engine interface still excludes corrective integrity operations that remain
// future-only.
func TestEngineActiveInterfaceExcludesStillInactiveOperations(t *testing.T) {
	typ := reflect.TypeOf((*engine.Engine)(nil)).Elem()

	got := make([]string, 0, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		got = append(got, typ.Method(i).Name)
	}

	for _, forbidden := range []string{
		"Repair",
		"SnapshotRepair",
		"RepairPlan",
		"Recover",
		"SnapshotRecover",
		"RecoveryPlan",
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
	for _, tc := range candidateOnlyOperationCases() {
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
			if tc.futureDisposition == "" {
				t.Fatalf("candidate-only contract %q must record a future disposition", tc.name)
			}
		})
	}
}

type candidateOnlyOperationCase struct {
	name              string
	requestType       any
	resultType        any
	forbiddenMethod   string
	futureDisposition string
}

func candidateOnlyOperationCases() []candidateOnlyOperationCase {
	return []candidateOnlyOperationCase{
		{
			name:              "repair",
			requestType:       engine.RepairRequest{},
			resultType:        engine.RepairResult{},
			forbiddenMethod:   "Repair",
			futureDisposition: "early v2.0 activation-design decision",
		},
		{
			name:              "recover",
			requestType:       engine.RecoverRequest{},
			resultType:        engine.RecoverResult{},
			forbiddenMethod:   "Recover",
			futureDisposition: "early v2.0 activation-design decision",
		},
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

// TestEngineContractTypesAreRendererNeutral verifies that engine contract
// request/result types do not expose renderer-specific or
// CLI-specific concepts (cobra, command, renderer, writer, stdout, stderr).
func TestEngineContractTypesAreRendererNeutral(t *testing.T) {
	forbidden := []string{
		"cobra", "command", "renderer", "writer", "stdout", "stderr",
	}
	for _, tc := range allEngineContractTypes() {
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
