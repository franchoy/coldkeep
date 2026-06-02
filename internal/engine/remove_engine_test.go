package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRemoveByIDThroughEngine(t *testing.T) {
	db, sgctx, stored := storeRemoveFixture(t, "remove-engine.txt", "phase9-remove")
	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:     engine.RemoveModeFileIDs,
		FileIDs:  []int64{stored.FileID},
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	assertRemoveSuccess(t, res)
	assertLogicalFileRemoved(t, db, stored.FileID)
}

func TestRemoveByIDDryRunThroughEngine(t *testing.T) {
	db, sgctx, stored := storeRemoveFixture(t, "remove-dry-run.txt", "phase9-remove-dry-run")
	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:     engine.RemoveModeFileIDs,
		FileIDs:  []int64{stored.FileID},
		DryRun:   true,
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove dry-run: %v", err)
	}

	assertDryRunRemoveSuccess(t, res)
	assertLogicalFileStillExists(t, db, stored.FileID)
}

func TestRemoveByIDRetainedSnapshotFailsClosed(t *testing.T) {
	db := openSnapshotTestDB(t)
	const retainedID int64 = 701
	seedRetainedSnapshotFile(t, db, retainedID)

	eng := newRemoveTestEngine(t, db, t.TempDir())
	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:     engine.RemoveModeFileIDs,
		FileIDs:  []int64{retainedID},
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove retained: %v", err)
	}

	assertRetainedSnapshotRemoveFailed(t, res)
}

func TestRemoveStoredPathDeferredOnEngine(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	_, err = eng.Remove(context.Background(), engine.RemoveRequest{Mode: engine.RemoveModeStoredPath, StoredPath: "a.txt"})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for stored-path remove, got %v", err)
	}
}

func storeRemoveFixture(t *testing.T, filename, content string) (*sql.DB, storage.StorageContext, storage.StoreFileResult) {
	t.Helper()

	db := openSnapshotTestDB(t)
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewSimulatedWriter(1024 * 1024),
		ContainerDir: t.TempDir(),
	}

	inPath := filepath.Join(t.TempDir(), filename)
	if err := os.WriteFile(inPath, []byte(content), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store fixture: %v", err)
	}

	return db, sgctx, stored
}

func newRemoveTestEngine(t *testing.T, db *sql.DB, containerDir string) *engine.DefaultEngine {
	t.Helper()

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: containerDir})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return eng
}

func assertRemoveSuccess(t *testing.T, res engine.RemoveResult) {
	t.Helper()

	if res.Summary.OK != 1 || res.Summary.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", res.Summary)
	}
	if len(res.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(res.Items))
	}
	item := res.Items[0]
	if item.Status != engine.BatchItemOK {
		t.Fatalf("expected status ok, got %q", item.Status)
	}
	if item.RemovedMappings <= 0 {
		t.Fatalf("expected RemovedMappings > 0, got %d", item.RemovedMappings)
	}
	if !item.Removed {
		t.Fatalf("expected Removed=true")
	}
}

func assertDryRunRemoveSuccess(t *testing.T, res engine.RemoveResult) {
	t.Helper()

	if len(res.Items) != 1 || res.Items[0].Status != engine.BatchItemOK {
		t.Fatalf("unexpected item result: %+v", res.Items)
	}
}

func assertLogicalFileRemoved(t *testing.T, db *sql.DB, fileID int64) {
	t.Helper()

	_, err := storage.GetLogicalFileInfoWithDB(db, fileID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected removed logical file, got err=%v", err)
	}
}

func assertLogicalFileStillExists(t *testing.T, db *sql.DB, fileID int64) {
	t.Helper()

	_, err := storage.GetLogicalFileInfoWithDB(db, fileID)
	if err != nil {
		t.Fatalf("expected logical file to remain after dry-run, got %v", err)
	}
}

func seedRetainedSnapshotFile(t *testing.T, db *sql.DB, retainedID int64) {
	t.Helper()

	ctx := context.Background()
	execRetainedSnapshotSQL(t, db, ctx,
		`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1, $2, $3, $4, 1, 'COMPLETED')`,
		retainedID, "retained.txt", 10, "retained-hash")
	execRetainedSnapshotSQL(t, db, ctx,
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		"snap-retained", time.Now().UTC().Format(time.RFC3339), "full", "retained")
	execRetainedSnapshotSQL(t, db, ctx,
		`INSERT INTO snapshot_path (id, path) VALUES ($1, $2)`,
		int64(901), "retained.txt")
	execRetainedSnapshotSQL(t, db, ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ($1, $2, $3, $4)`,
		"snap-retained", int64(901), retainedID, int64(10))
}

func execRetainedSnapshotSQL(t *testing.T, db *sql.DB, ctx context.Context, query string, args ...any) {
	t.Helper()

	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("seed retained snapshot fixture: %v", err)
	}
}

func assertRetainedSnapshotRemoveFailed(t *testing.T, res engine.RemoveResult) {
	t.Helper()

	if len(res.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(res.Items))
	}
	item := res.Items[0]
	if item.Status != engine.BatchItemFailed {
		t.Fatalf("expected failed status, got %q", item.Status)
	}
	if item.InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked {
		t.Fatalf("expected invariant code %q, got %q", invariants.CodeSnapshotRetainedDeleteBlocked, item.InvariantCode)
	}
	if item.RecommendedAction == "" {
		t.Fatalf("expected recommended action for retained snapshot failure")
	}
}
