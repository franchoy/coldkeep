package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

var productionStatsEnginePhaseForTest = runObservabilityStatsPhase
var productionInspectEnginePhaseForTest = runObservabilityInspectPhase

func installObservabilityEngineStub(t *testing.T, stub engine.Engine) {
	t.Helper()
	dbconn := openSnapshotRoutingDB(t)
	originalLoad := loadDefaultStorageContextPhase
	originalFactory := newObservabilityCommandEngine
	originalStats := runObservabilityStatsPhase
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newObservabilityCommandEngine = originalFactory
		runObservabilityStatsPhase = originalStats
		runObservabilityInspectPhase = originalInspect
	})
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	newObservabilityCommandEngine = func(storage.StorageContext) (engine.Engine, error) { return stub, nil }
	runObservabilityStatsPhase = productionStatsEnginePhaseForTest
	runObservabilityInspectPhase = productionInspectEnginePhaseForTest
}

func TestStatsAndInspectRouteThroughTypedEngineResults(t *testing.T) {
	statsCalls, inspectCalls := 0, 0
	installObservabilityEngineStub(t, stubCommandEngine{
		statsFunc: func(_ context.Context, req engine.StatsRequest) (engine.StatsResult, error) {
			statsCalls++
			if !req.IncludeContainers || req.IncludeTrace {
				t.Fatalf("stats request = %+v", req)
			}
			return engine.StatsResult{Logical: engine.StatsLogical{TotalFiles: 7}}, nil
		},
		inspectFunc: func(_ context.Context, req engine.InspectRequest) (engine.InspectResult, error) {
			inspectCalls++
			wantOptions := engine.InspectOptions{Deep: true, Relations: true, Reverse: true, Limit: 9}
			if req.Entity != engine.InspectFile || req.EntityID != "42" || !reflect.DeepEqual(req.Options, wantOptions) {
				t.Fatalf("inspect request = %+v", req)
			}
			return engine.InspectResult{
				Entity: engine.InspectFile, EntityID: "42",
				Summary: map[string]engine.Value{"exact": {Kind: engine.ValueInteger, Integer: "9007199254740993"}},
			}, nil
		},
	})

	statsJSON := captureStdout(t, func() {
		if err := runStatsCommand(parsedCommandLine{method: "stats", flags: map[string][]string{"containers": {""}, "output": {"json"}}}, outputModeJSON); err != nil {
			t.Fatalf("stats: %v", err)
		}
	})
	if !strings.Contains(statsJSON, `"total_files":7`) {
		t.Fatalf("stats JSON = %s", statsJSON)
	}
	inspectJSON := captureStdout(t, func() {
		if err := runInspectCommand(parsedCommandLine{
			method: "inspect", positionals: []string{"file", "42"},
			flags: map[string][]string{"deep": {""}, "relations": {""}, "reverse": {""}, "limit": {"9"}, "output": {"json"}},
		}, outputModeJSON); err != nil {
			t.Fatalf("inspect: %v", err)
		}
	})
	if !strings.Contains(inspectJSON, `"exact":9007199254740993`) {
		t.Fatalf("inspect exact integer JSON = %s", inspectJSON)
	}
	if statsCalls != 1 || inspectCalls != 1 {
		t.Fatalf("calls stats=%d inspect=%d", statsCalls, inspectCalls)
	}
}

func TestInspectDecimalAdapterRetainsBoundedHumanProjection(t *testing.T) {
	installObservabilityEngineStub(t, stubCommandEngine{
		inspectFunc: func(context.Context, engine.InspectRequest) (engine.InspectResult, error) {
			return engine.InspectResult{
				Entity: engine.InspectRepository,
				Summary: map[string]engine.Value{
					"compression_factor": {Kind: engine.ValueDecimal, Decimal: "2.75"},
				},
			}, nil
		},
	})

	output := captureStdout(t, func() {
		if err := runInspectCommand(parsedCommandLine{
			method: "inspect", positionals: []string{"repository"}, flags: map[string][]string{},
		}, outputModeText); err != nil {
			t.Fatalf("inspect: %v", err)
		}
	})
	if !strings.Contains(output, "compression_factor: 2") {
		t.Fatalf("inspect decimal human output = %q", output)
	}
}

func TestVerifyUsesOneEngineOperationAndItsSummary(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	originalLoad := loadDefaultStorageContextPhase
	originalFactory := newVerifyCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newVerifyCommandEngine = originalFactory
	})
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	calls := 0
	newVerifyCommandEngine = func(storage.StorageContext) (engine.Engine, error) {
		return stubCommandEngine{verifyFunc: func(_ context.Context, req engine.VerifyRequest) (engine.VerifyResult, error) {
			calls++
			if req.Target != "file" || req.FileID != 42 || req.Level != "deep" {
				t.Fatalf("verify request = %+v", req)
			}
			return engine.VerifyResult{BlocksChecked: 5, PhysicalHashChecked: 4, CompressedHashChecked: 3, LogicalHashChecked: 2, CompressedBlocksChecked: 1}, nil
		}}, nil
	}

	output := captureStdout(t, func() {
		if err := runVerifyCommand(parsedCommandLine{
			method: "verify", positionals: []string{"file", "42"},
			flags: map[string][]string{"deep": {""}, "output": {"json"}},
		}, outputModeJSON); err != nil {
			t.Fatalf("verify: %v", err)
		}
	})
	if calls != 1 {
		t.Fatalf("Verify calls = %d, want 1", calls)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("verify JSON: %v (%s)", err, output)
	}
	if payload["blocks_checked"] != float64(5) || payload["compressed_blocks_checked"] != float64(1) {
		t.Fatalf("verify summary = %#v", payload)
	}
}

func TestVerifySummaryFailureRetainsGeneralExitClassification(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	originalLoad := loadDefaultStorageContextPhase
	originalFactory := newVerifyCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newVerifyCommandEngine = originalFactory
	})
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	newVerifyCommandEngine = func(storage.StorageContext) (engine.Engine, error) {
		return stubCommandEngine{verifyFunc: func(context.Context, engine.VerifyRequest) (engine.VerifyResult, error) {
			return engine.VerifyResult{}, engine.NewError(engine.ErrorOperationFailed, "verify", "collect verify summary: read failed", "", nil)
		}}, nil
	}
	err := runVerifyCommand(parsedCommandLine{method: "verify", positionals: []string{"system"}, flags: map[string][]string{}}, outputModeText)
	if err == nil || classifyExitCode(err) != exitGeneral {
		t.Fatalf("summary error = %v, exit=%d", err, classifyExitCode(err))
	}
}

func TestInspectEngineNotFoundPreservesCLIProjection(t *testing.T) {
	installObservabilityEngineStub(t, stubCommandEngine{
		inspectFunc: func(context.Context, engine.InspectRequest) (engine.InspectResult, error) {
			return engine.InspectResult{}, engine.NewError(engine.ErrorNotFound, "inspect", "missing", "", nil)
		},
	})
	err := runInspectCommand(parsedCommandLine{method: "inspect", positionals: []string{"snapshot", "missing"}, flags: map[string][]string{}}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "snapshot missing not found") {
		t.Fatalf("not-found projection = %v", err)
	}
	if got := publicErrorCode(err, classifyExitCode(err)); got != "NOT_FOUND" {
		t.Fatalf("public code = %q", got)
	}
}
