package engine_test

import (
	"context"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineGarbageCollectionPlanAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		result, err := eng.PlanGarbageCollection(context.Background(), engine.GarbageCollectionPlanRequest{})
		if err != nil {
			t.Fatalf("PlanGarbageCollection: %v", err)
		}
		if result.Summary != (engine.GarbageCollectionPlanSummary{}) || len(result.Containers) != 0 || len(result.Warnings) != 0 {
			t.Fatalf("empty repository plan=%+v", result)
		}
	})
}
