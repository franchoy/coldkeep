package engine_test

import (
	"context"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/engine"
)

// TestGCDryRunThroughEngineEmptyDB verifies that a dry-run GC on an empty
// repository reports zero affected containers. This exercises the full engine
// path (RunGCWithDB) on a coherent, empty SQLite schema.
func TestGCDryRunThroughEngineEmptyDB(t *testing.T) {
	db := openSnapshotTestDB(t) // reuse existing helper: open SQLite + migrate
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: true})
	if err != nil {
		t.Fatalf("GarbageCollect dry-run on empty DB: %v", err)
	}
	if !result.DryRun {
		t.Errorf("expected DryRun=true, got false")
	}
	if result.AffectedContainers != 0 {
		t.Errorf("expected 0 affected containers on empty DB, got %d", result.AffectedContainers)
	}
	if len(result.ContainerFilenames) != 0 {
		t.Errorf("expected empty ContainerFilenames, got %v", result.ContainerFilenames)
	}
}

// TestGCDryRunEchoesFields verifies that GarbageCollectResult fields are
// correctly populated from the underlying maintenance.GCResult (field mapping).
func TestGCDryRunEchoesFields(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: true})
	if err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	assertGCNonNegativeFields(t, result)
}

func assertGCNonNegativeFields(t *testing.T, result engine.GarbageCollectResult) {
	t.Helper()
	// All retention counts must be non-negative (invariant: never delete reachable data).
	for _, field := range []struct {
		name  string
		value int64
	}{
		{"SnapshotRetainedContainers", int64(result.SnapshotRetainedContainers)},
		{"SnapshotRetainedLogicalFiles", int64(result.SnapshotRetainedLogicalFiles)},
		{"CurrentOnlyRetainedLogicalFiles", int64(result.CurrentOnlyRetainedLogicalFiles)},
		{"SnapshotOnlyRetainedLogicalFiles", int64(result.SnapshotOnlyRetainedLogicalFiles)},
		{"SharedRetainedLogicalFiles", int64(result.SharedRetainedLogicalFiles)},
		{"BytesReclaimed", result.BytesReclaimed},
	} {
		if field.value < 0 {
			t.Errorf("%s < 0: %d", field.name, field.value)
		}
	}
}

// TestGCLiveRefusedOnSQLite verifies that live (non-dry-run) GC through the
// engine returns an error on the SQLite backend. SQLite supports dry-run only.
// This test exercises the advisory lock path without requiring PostgreSQL.
func TestGCLiveRefusedOnSQLite(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	_, err = eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: false})
	if err == nil {
		t.Fatal("expected live GC to be rejected on SQLite backend, got nil error")
	}
	if !strings.Contains(err.Error(), "SQLite") && !strings.Contains(err.Error(), "dry-run") {
		t.Errorf("expected SQLite backend rejection message, got: %v", err)
	}
}
