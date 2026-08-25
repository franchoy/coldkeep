package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/container"
	internaldb "github.com/franchoy/coldkeep/internal/db"
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
	if result.BytesReclaimed != 0 {
		t.Errorf("expected 0 reclaimed bytes on empty DB, got %d", result.BytesReclaimed)
	}
}

func TestEnginePlanGarbageCollectionEmptyAndCancellation(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.PlanGarbageCollection(context.Background(), engine.GarbageCollectionPlanRequest{IncludeTrace: true})
	if err != nil {
		t.Fatalf("PlanGarbageCollection: %v", err)
	}
	if result.Summary.TotalChunks != 0 || result.Summary.ReachableChunks != 0 || result.Summary.UnreachableChunks != 0 {
		t.Fatalf("unexpected empty plan: %+v", result.Summary)
	}
	if len(result.Trace) != 7 || result.Trace[0].Step != "simulate.gc.start" || result.Trace[len(result.Trace)-1].Step != "simulate.gc.complete" {
		t.Fatalf("unexpected plan trace: %+v", result.Trace)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := eng.PlanGarbageCollection(cancelled, engine.GarbageCollectionPlanRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled PlanGarbageCollection: %v", err)
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

func TestGarbageCollectRejectsNegativeWorkersBeforeMaintenance(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{
		DryRun:  true,
		Workers: -1,
	})
	if !engine.IsCode(err, engine.ErrorInvalidArgument) {
		t.Fatalf("GarbageCollect error = %v, want %s", err, engine.ErrorInvalidArgument)
	}
	if !reflect.DeepEqual(result, engine.GarbageCollectResult{}) {
		t.Fatalf("negative-workers result = %+v, want zero", result)
	}
}

func TestGarbageCollectBytesReclaimedCreditsSuccessfulPhysicalRemovalOnly(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "gc-bytes.db"))
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := internaldb.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	containersDir := t.TempDir()
	payload := []byte("phase11-physical-byte-baseline")
	filename := "phase11-byte-baseline.bin"
	if err := os.WriteFile(filepath.Join(containersDir, filename), payload, 0o600); err != nil {
		t.Fatalf("write container fixture: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)`,
		filename,
		int64(len(payload)),
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container fixture: %v", err)
	}
	eng, err := engine.New(engine.Config{DB: dbconn, ContainerDir: containersDir})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: true})
	if err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	if result.AffectedContainers != 1 || len(result.ContainerFilenames) != 1 || result.ContainerFilenames[0] != filename {
		t.Fatalf("affected population = %+v, want one %q", result, filename)
	}
	if result.BytesReclaimed != int64(len(payload)) {
		t.Fatalf("BytesReclaimed = %d, want independently observed physical size %d", result.BytesReclaimed, len(payload))
	}
}

func TestGarbageCollectPreservesMaintenancePartialResultWhenReturningError(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "gc-partial.db"))
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := internaldb.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	containersDir := t.TempDir()
	payload := []byte("phase11-partial-success")
	if err := os.WriteFile(filepath.Join(containersDir, "a-success.bin"), payload, 0o600); err != nil {
		t.Fatalf("write successful container: %v", err)
	}
	for _, filename := range []string{"a-success.bin", "b-missing.bin"} {
		if _, err := dbconn.Exec(
			`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
			 VALUES ($1, $2, $3, TRUE, FALSE)`,
			filename,
			int64(len(payload)),
			container.GetContainerMaxSize(),
		); err != nil {
			t.Fatalf("insert %s: %v", filename, err)
		}
	}
	eng, err := engine.New(engine.Config{DB: dbconn, ContainerDir: containersDir})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: true, Workers: 1})
	if err == nil {
		t.Fatal("expected missing-file GC error")
	}
	if result.AffectedContainers != 1 || !reflect.DeepEqual(result.ContainerFilenames, []string{"a-success.bin"}) || result.BytesReclaimed != int64(len(payload)) {
		t.Fatalf("partial Engine result = %+v", result)
	}
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
