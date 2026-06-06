package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestStoreRecursiveReturnsErrNotImplementedWithoutMutation(t *testing.T) {
	db := openSnapshotTestDB(t)
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewSimulatedWriter(1024 * 1024),
		ContainerDir: t.TempDir(),
	}
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: sgctx.ContainerDir, StoreContext: &sgctx})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	before := countLogicalFiles(t, db)
	_, err = eng.Store(context.Background(), engine.StoreRequest{
		SourcePath: t.TempDir(),
		Recursive:  true,
		Workers:    4,
		Codec:      "plain",
	})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for recursive store, got %v", err)
	}
	after := countLogicalFiles(t, db)
	if after != before {
		t.Fatalf("recursive unsupported mode should not mutate logical_file rows: before=%d after=%d", before, after)
	}
}

func TestRestoreStoredPathReturnsErrNotImplementedWithoutMutation(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng := newRestoreTestEngine(t, db)
	outputDir := t.TempDir()

	before := countLogicalFiles(t, db)
	_, err := eng.Restore(context.Background(), engine.RestoreRequest{
		Mode:       engine.RestoreModeStoredPath,
		StoredPath: "samples/hello.txt",
		OutputDir:  outputDir,
		DryRun:     true,
	})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for stored-path restore, got %v", err)
	}
	after := countLogicalFiles(t, db)
	if after != before {
		t.Fatalf("stored-path unsupported mode should not mutate logical_file rows: before=%d after=%d", before, after)
	}
	assertDirectoryEmpty(t, outputDir)
}

func TestRemoveStoredPathReturnsErrNotImplementedWithoutMutation(t *testing.T) {
	db, sgctx, stored := storeRemoveFixture(t, "remove-unsupported.txt", "phase2-remove-unsupported")
	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

	before := countLogicalFiles(t, db)
	_, err := eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:       engine.RemoveModeStoredPath,
		StoredPath: stored.Path,
	})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for stored-path remove, got %v", err)
	}
	after := countLogicalFiles(t, db)
	if after != before {
		t.Fatalf("stored-path unsupported mode should not mutate logical_file rows: before=%d after=%d", before, after)
	}
	assertLogicalFileStillExists(t, db, stored.FileID)
}

func TestRemoveStoredPathsReturnsErrNotImplementedWithoutMutation(t *testing.T) {
	db, sgctx, first := storeRemoveFixture(t, "remove-unsupported-batch-1.txt", "phase2-remove-unsupported-batch-1")
	secondPath := filepath.Join(t.TempDir(), "remove-unsupported-batch-2.txt")
	if err := os.WriteFile(secondPath, []byte("phase2-remove-unsupported-batch-2"), 0600); err != nil {
		t.Fatalf("write second input: %v", err)
	}
	second, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, secondPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store second fixture: %v", err)
	}

	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)
	before := countLogicalFiles(t, db)
	_, err = eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:        engine.RemoveModeStoredPaths,
		StoredPaths: []string{first.Path, second.Path},
	})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for stored-paths remove, got %v", err)
	}
	after := countLogicalFiles(t, db)
	if after != before {
		t.Fatalf("stored-paths unsupported mode should not mutate logical_file rows: before=%d after=%d", before, after)
	}
	assertLogicalFileStillExists(t, db, first.FileID)
	assertLogicalFileStillExists(t, db, second.FileID)
}

func countLogicalFiles(t *testing.T, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM logical_file`).Scan(&count); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	return count
}

func assertDirectoryEmpty(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read directory %q: %v", dir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected directory %q to remain empty, found %d entries", dir, len(entries))
	}
}
