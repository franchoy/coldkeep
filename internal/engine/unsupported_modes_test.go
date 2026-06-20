package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

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
	if !engine.IsUnsupported(err) {
		t.Fatalf("expected recursive store error to classify as unsupported, got %v", err)
	}
	after := countLogicalFiles(t, db)
	if after != before {
		t.Fatalf("recursive unsupported mode should not mutate logical_file rows: before=%d after=%d", before, after)
	}
}

func countLogicalFiles(t *testing.T, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM logical_file`).Scan(&count); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	return count
}
