package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
)

func TestRunRepairBatchUsesEngineAndPreservesProjection(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	var calls int
	installRepairCommandStubs(t, dbconn, stubCommandEngine{repairFunc: func(_ context.Context, req engine.RepairRequest) (engine.RepairResult, error) {
		calls++
		if !req.FailFast || len(req.Targets) != 3 || req.Targets[0] != " ref-counts " || req.Targets[1] != "unknown" || req.Targets[2] != "ref-counts" {
			t.Fatalf("unexpected Repair request: %+v", req)
		}
		return engine.RepairResult{Targets: []engine.RepairTargetResult{
			{RawTarget: "ref-counts", Target: engine.RepairTargetRefCounts, Status: engine.BatchItemOK, Message: "repaired scanned_logical_files=1 updated_logical_files=1 orphan_physical_file_rows=0"},
			{RawTarget: "unknown", Status: engine.BatchItemFailed, Message: `unknown repair target "unknown"`},
			{RawTarget: "ref-counts", Target: engine.RepairTargetRefCounts, Status: engine.BatchItemSkipped, Message: "duplicate target"},
		}}, nil
	}})

	output := captureStdout(t, func() {
		if err := runRepairCommand(parsedCommandLine{method: "repair", positionals: []string{" ref-counts ", "unknown", "ref-counts"}, flags: map[string][]string{"batch": {"true"}, "fail-fast": {"true"}}}, outputModeJSON); err == nil {
			t.Fatal("mixed batch should retain its failed exit result")
		}
	})
	if calls != 1 {
		t.Fatalf("Repair calls=%d want 1", calls)
	}
	for _, want := range []string{`"command":"repair"`, `"execution_mode":"fail_fast"`, `"status":"failed"`, `"status":"skipped"`, `"message":"duplicate target"`} {
		if !strings.Contains(output, want) {
			t.Fatalf("batch JSON missing %q: %s", want, output)
		}
	}
}

func TestRunRepairSingleUsesEngineCompatibilityProjection(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installRepairCommandStubs(t, dbconn, stubCommandEngine{repairFunc: func(_ context.Context, req engine.RepairRequest) (engine.RepairResult, error) {
		if req.FailFast || len(req.Targets) != 1 || req.Targets[0] != "ref-counts" {
			t.Fatalf("unexpected Repair request: %+v", req)
		}
		return engine.RepairResult{Targets: []engine.RepairTargetResult{{RawTarget: "ref-counts", Target: engine.RepairTargetRefCounts, ScannedRows: 4, UpdatedRows: 2, OrphanRows: 0, Status: engine.BatchItemOK}}}, nil
	}})
	output := captureStdout(t, func() {
		if err := runRepairCommand(parsedCommandLine{method: "repair", positionals: []string{"ref-counts"}}, outputModeJSON); err != nil {
			t.Fatalf("runRepairCommand: %v", err)
		}
	})
	if strings.TrimSpace(output) != `{"command":"repair","data":{"orphan_physical_file_rows":0,"scanned_logical_files":4,"target":"ref-counts","updated_logical_files":2},"status":"ok"}` {
		t.Fatalf("unexpected JSON projection: %s", output)
	}
}

func TestRunRepairSinglePreservesInvariantExitClassification(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installRepairCommandStubs(t, dbconn, stubCommandEngine{repairFunc: func(context.Context, engine.RepairRequest) (engine.RepairResult, error) {
		return engine.RepairResult{Targets: []engine.RepairTargetResult{{
			RawTarget: "ref-counts", Target: engine.RepairTargetRefCounts, Status: engine.BatchItemFailed,
			Message: "repair refused", InvariantCode: invariants.CodeRepairRefusedOrphanRows,
		}}}, nil
	}})
	err := runRepairCommand(parsedCommandLine{method: "repair", positionals: []string{"ref-counts"}}, outputModeText)
	if err == nil || classifyExitCode(err) != exitVerify || !strings.Contains(err.Error(), "repair refused") {
		t.Fatalf("expected verify-class invariant error, got %v (exit=%d)", err, classifyExitCode(err))
	}
}

func installRepairCommandStubs(t *testing.T, dbconn *sql.DB, stub stubCommandEngine) {
	t.Helper()
	originalConnect := connectRepairDBPhase
	originalNewEngine := newRepairCommandEngine
	t.Cleanup(func() {
		connectRepairDBPhase = originalConnect
		newRepairCommandEngine = originalNewEngine
	})
	connectRepairDBPhase = func() (*sql.DB, error) {
		return dbconn, nil
	}
	newRepairCommandEngine = func(*sql.DB) (engine.Engine, error) {
		return stub, nil
	}
}
