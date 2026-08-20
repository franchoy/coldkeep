package engine_test

import (
	"context"
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
	if err := os.WriteFile(inPath, []byte("phase8-store"), 0600); err != nil {
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

func TestStoreFolderThroughEngine(t *testing.T) {
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

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatalf("create nested directory: %v", err)
	}
	for path, payload := range map[string][]byte{
		filepath.Join(root, "a.txt"):           []byte("alpha"),
		filepath.Join(root, "nested", "b.txt"): []byte("bravo"),
	} {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatalf("write input %q: %v", path, err)
		}
	}

	res, err := eng.StoreFolder(context.Background(), engine.StoreFolderRequest{
		SourcePath: root,
		Codec:      "plain",
		Workers:    4,
	})
	if err != nil {
		t.Fatalf("StoreFolder: %v", err)
	}
	if res.SourcePath != root || res.FilesStored != 2 || res.BytesLogical != 10 || res.WorkersUsed != 1 {
		t.Fatalf("unexpected StoreFolder result: %+v", res)
	}
	if got := countLogicalFiles(t, db); got != 2 {
		t.Fatalf("logical file count: got %d want 2", got)
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
