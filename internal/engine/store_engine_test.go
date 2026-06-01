package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestStoreByFileThroughEngine(t *testing.T) {
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

	inPath := filepath.Join(t.TempDir(), "store.txt")
	if err := os.WriteFile(inPath, []byte("phase8-store"), 0644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	res, err := eng.Store(context.Background(), engine.StoreRequest{SourcePath: inPath, Codec: "plain"})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if res.LogicalFileID <= 0 {
		t.Fatalf("expected positive LogicalFileID, got %d", res.LogicalFileID)
	}
	if res.FileHash == "" {
		t.Fatalf("expected non-empty FileHash")
	}
	if res.StoredPath == "" {
		t.Fatalf("expected non-empty StoredPath")
	}
}

func TestStoreFolderDeferred(t *testing.T) {
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

	_, err = eng.Store(context.Background(), engine.StoreRequest{SourcePath: t.TempDir(), Recursive: true, Codec: "plain"})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for recursive store, got %v", err)
	}
}

func TestStoreRequiresInjectedStoreContext(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	_, err = eng.Store(context.Background(), engine.StoreRequest{SourcePath: "file.txt", Codec: "plain"})
	if err == nil {
		t.Fatalf("expected error when StoreContext is missing")
	}
}
