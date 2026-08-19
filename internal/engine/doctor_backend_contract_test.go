package engine_test

import (
	"context"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineDoctorAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		result, err := eng.Doctor(context.Background(), engine.DoctorRequest{})
		if err != nil {
			t.Fatalf("Doctor: %v", err)
		}
		if result.VerifyLevel != "standard" || result.RecoveryStatus != "ok" || result.SchemaStatus != "ok" || result.VerifyStatus != "ok" || result.SchemaVersion <= 0 || result.FailedStage != "" {
			t.Fatalf("Doctor result=%+v", result)
		}
		if result.PhysicalAudit != (engine.DoctorPhysicalAudit{}) || result.SnapshotAudit != (engine.DoctorSnapshotAudit{}) {
			t.Fatalf("empty repository audits=%+v %+v", result.PhysicalAudit, result.SnapshotAudit)
		}
	})
}
