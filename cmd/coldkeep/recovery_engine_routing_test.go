package main

import (
	"context"
	"database/sql"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestStartupAndDoctorRecoveryUseSameEngineOperation(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	calls := 0
	installRecoveryCommandStubs(t, dbconn, stubCommandEngine{recoverFunc: func(_ context.Context, req engine.RecoverRequest) (engine.RecoverResult, error) {
		calls++
		if req != (engine.RecoverRequest{}) {
			t.Fatalf("unexpected Recover request: %+v", req)
		}
		return engine.RecoverResult{AbortedLogicalFiles: int64(calls), SealingCompleted: 2}, nil
	}})

	startup, err := startupRecoveryPhase("/isolated/startup")
	if err != nil || startup.AbortedLogicalFiles != 1 || startup.SealingCompleted != 2 {
		t.Fatalf("startup recovery=(%+v,%v)", startup, err)
	}
	explicit, err := doctorRecoveryPhase("/isolated/doctor")
	if err != nil || explicit.AbortedLogicalFiles != 2 || explicit.SealingCompleted != 2 {
		t.Fatalf("doctor recovery=(%+v,%v)", explicit, err)
	}
	if calls != 2 {
		t.Fatalf("Recover calls=%d want 2", calls)
	}
}

func installRecoveryCommandStubs(t *testing.T, dbconn *sql.DB, stub stubCommandEngine) {
	t.Helper()
	originalConnect := connectRecoveryDBPhase
	originalNewEngine := newRecoveryCommandEngine
	t.Cleanup(func() {
		connectRecoveryDBPhase = originalConnect
		newRecoveryCommandEngine = originalNewEngine
	})
	connectRecoveryDBPhase = func() (*sql.DB, error) { return dbconn, nil }
	newRecoveryCommandEngine = func(_ *sql.DB, containersDir string) (engine.Engine, error) {
		if containersDir != "/isolated/startup" && containersDir != "/isolated/doctor" {
			t.Fatalf("unexpected recovery container dir %q", containersDir)
		}
		return stub, nil
	}
}
