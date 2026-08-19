package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestRunDoctorCommandUsesOneEngineOperationAndPreservesJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	calls := 0
	installDoctorCommandStubs(t, dbconn, stubCommandEngine{doctorFunc: func(_ context.Context, req engine.DoctorRequest) (engine.DoctorResult, error) {
		calls++
		if req.VerifyLevel != "full" {
			t.Fatalf("Doctor request=%+v", req)
		}
		return engine.DoctorResult{
			Recovery:    engine.RecoverResult{AbortedLogicalFiles: 1, SealingCompleted: 2},
			VerifyLevel: "full", SchemaVersion: 8,
			RecoveryStatus: "ok", SchemaStatus: "ok", VerifyStatus: "ok",
			PhysicalAudit: engine.DoctorPhysicalAudit{LogicalRefCountMismatches: 3},
			SnapshotAudit: engine.DoctorSnapshotAudit{SnapshotFileRows: 4},
		}, nil
	}})

	output := captureStdout(t, func() {
		if err := runDoctorCommand(parsedCommandLine{method: "doctor", flags: map[string][]string{"full": {"true"}}}, outputModeJSON); err != nil {
			t.Fatalf("runDoctorCommand: %v", err)
		}
	})
	if calls != 1 {
		t.Fatalf("Doctor calls=%d want 1", calls)
	}
	for _, want := range []string{`"command":"doctor"`, `"verify_level":"full"`, `"schema_version":8`, `"aborted_logical_files":1`, `"sealing_completed":2`} {
		if !strings.Contains(output, want) {
			t.Fatalf("Doctor JSON missing %q: %s", want, output)
		}
	}
	if strings.Contains(output, "physicalAudit") || strings.Contains(output, "snapshotAudit") {
		t.Fatalf("private audit summaries leaked into stable JSON: %s", output)
	}
}

func TestRunDoctorCommandPreservesStageExitClasses(t *testing.T) {
	tests := []struct {
		name  string
		stage engine.DoctorStage
		exit  int
	}{
		{"recovery", engine.DoctorStageRecovery, exitRecovery},
		{"schema", engine.DoctorStageSchema, exitGeneral},
		{"verify", engine.DoctorStageVerify, exitVerify},
		{"audit", engine.DoctorStageAudit, exitVerify},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbconn := openSnapshotRoutingDB(t)
			installDoctorCommandStubs(t, dbconn, stubCommandEngine{doctorFunc: func(context.Context, engine.DoctorRequest) (engine.DoctorResult, error) {
				return engine.DoctorResult{FailedStage: tc.stage}, errors.New("doctor " + tc.name + " phase failed")
			}})
			err := runDoctorCommand(parsedCommandLine{method: "doctor", flags: map[string][]string{}}, outputModeText)
			if err == nil || classifyExitCode(err) != tc.exit {
				t.Fatalf("stage %s error=%v exit=%d want %d", tc.stage, err, classifyExitCode(err), tc.exit)
			}
		})
	}
}

func installDoctorCommandStubs(t *testing.T, dbconn *sql.DB, stub stubCommandEngine) {
	t.Helper()
	originalConnect := connectDoctorDBPhase
	originalNewEngine := newDoctorCommandEngine
	t.Cleanup(func() {
		connectDoctorDBPhase = originalConnect
		newDoctorCommandEngine = originalNewEngine
	})
	connectDoctorDBPhase = func() (*sql.DB, error) { return dbconn, nil }
	newDoctorCommandEngine = func(*sql.DB, string) (engine.Engine, error) { return stub, nil }
}
