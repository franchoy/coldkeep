package engine_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
)

func TestEngineContainerDirDefaultIsConsistentAcrossOperations(t *testing.T) {
	originalDefault := container.ContainersDir
	t.Cleanup(func() { container.ContainersDir = originalDefault })

	t.Run("empty configuration uses the documented default", func(t *testing.T) {
		defaultRoot := t.TempDir()
		container.ContainersDir = defaultRoot
		dbconn := openPhase12EngineDB(t)
		fixture := newStoredPathRestoreFixtureFromDB(t, dbconn, []byte("phase12 default root"), defaultRoot)
		eng, err := engine.New(engine.Config{DB: fixture.db, StoreContext: fixture.sgctx})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}
		assertEngineContainerDirRoutes(t, eng, fixture)
	})

	t.Run("explicit configuration remains authoritative", func(t *testing.T) {
		explicitRoot := t.TempDir()
		container.ContainersDir = t.TempDir()
		dbconn := openPhase12EngineDB(t)
		fixture := newStoredPathRestoreFixtureFromDB(t, dbconn, []byte("phase12 explicit root"), explicitRoot)
		eng, err := engine.New(engine.Config{DB: fixture.db, ContainerDir: explicitRoot, StoreContext: fixture.sgctx})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}
		assertEngineContainerDirRoutes(t, eng, fixture)
	})
}

func openPhase12EngineDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "phase12.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	return dbconn
}

func assertEngineContainerDirRoutes(t *testing.T, eng *engine.DefaultEngine, fixture storedPathRestoreFixture) {
	t.Helper()
	ctx := context.Background()
	if _, err := eng.Verify(ctx, engine.VerifyRequest{Target: "system", Level: "standard"}); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if _, err := eng.GarbageCollect(ctx, engine.GarbageCollectRequest{DryRun: true}); err != nil {
		t.Fatalf("GarbageCollect dry-run: %v", err)
	}
	if _, err := eng.Recover(ctx, engine.RecoverRequest{}); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	restoreRoot := t.TempDir()
	restored, err := eng.Restore(ctx, engine.RestoreRequest{
		FileIDs:         []int64{fixture.stored.FileID},
		DestinationRoot: restoreRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if len(restored.Items) != 1 || restored.Items[0].Status != engine.BatchItemOK {
		t.Fatalf("Restore result = %+v, want one successful item", restored)
	}
	assertRestoredBytes(t, restored.Items[0].DestinationPath, fixture.payload)

	overridePath := filepath.Join(t.TempDir(), "restore-stored-path.txt")
	storedPathResult, err := eng.RestoreStoredPath(ctx, engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: overridePath,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath: %v", err)
	}
	if storedPathResult.DestinationPath != overridePath {
		t.Fatalf("RestoreStoredPath destination = %q, want %q", storedPathResult.DestinationPath, overridePath)
	}
	assertRestoredBytes(t, overridePath, fixture.payload)

	if _, err := os.Stat(overridePath); err != nil {
		t.Fatalf("stat restored path: %v", err)
	}
}
