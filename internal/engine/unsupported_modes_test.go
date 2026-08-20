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

func TestStoreFolderPreCancelledWithoutMutation(t *testing.T) {
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = eng.StoreFolder(ctx, engine.StoreFolderRequest{
		SourcePath: t.TempDir(),
		Workers:    4,
		Codec:      "plain",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	after := countLogicalFiles(t, db)
	if after != before {
		t.Fatalf("cancelled folder store should not mutate logical_file rows: before=%d after=%d", before, after)
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
