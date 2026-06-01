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
	db := openSnapshotTestDB(t)
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewSimulatedWriter(1024 * 1024),
		ContainerDir: t.TempDir(),
	}

	inPath := filepath.Join(t.TempDir(), "remove-engine.txt")
	if err := os.WriteFile(inPath, []byte("phase9-remove"), 0644); err != nil {
		t.Fatalf("write input: %v", err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store fixture: %v", err)
	}

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: sgctx.ContainerDir})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:     engine.RemoveModeFileIDs,
		FileIDs:  []int64{stored.FileID},
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if res.Summary.OK != 1 || res.Summary.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", res.Summary)
	}
	if len(res.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(res.Items))
	}
	if res.Items[0].Status != engine.BatchItemOK {
		t.Fatalf("expected status ok, got %q", res.Items[0].Status)
	}
	if res.Items[0].RemovedMappings <= 0 {
		t.Fatalf("expected RemovedMappings > 0, got %d", res.Items[0].RemovedMappings)
	}
	if !res.Items[0].Removed {
		t.Fatalf("expected Removed=true")
	}

	_, err = storage.GetLogicalFileInfoWithDB(db, stored.FileID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected removed logical file, got err=%v", err)
	}
}

func TestRemoveByIDDryRunThroughEngine(t *testing.T) {
	db := openSnapshotTestDB(t)
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewSimulatedWriter(1024 * 1024),
		ContainerDir: t.TempDir(),
	}

	inPath := filepath.Join(t.TempDir(), "remove-dry-run.txt")
	if err := os.WriteFile(inPath, []byte("phase9-remove-dry-run"), 0644); err != nil {
		t.Fatalf("write input: %v", err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store fixture: %v", err)
	}

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: sgctx.ContainerDir})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:     engine.RemoveModeFileIDs,
		FileIDs:  []int64{stored.FileID},
		DryRun:   true,
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove dry-run: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].Status != engine.BatchItemOK {
		t.Fatalf("unexpected item result: %+v", res.Items)
	}

	_, err = storage.GetLogicalFileInfoWithDB(db, stored.FileID)
	if err != nil {
		t.Fatalf("expected logical file to remain after dry-run, got %v", err)
	}
}

func TestRemoveByIDRetainedSnapshotFailsClosed(t *testing.T) {
	db := openSnapshotTestDB(t)

	const retainedID int64 = 701
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES ($1, $2, $3, $4, 1, 'COMPLETED')`,
		retainedID, "retained.txt", 10, "retained-hash")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		"snap-retained", time.Now().UTC().Format(time.RFC3339), "full", "retained")
	if err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	_, err = db.ExecContext(context.Background(), `INSERT INTO snapshot_path (id, path) VALUES ($1, $2)`, int64(901), "retained.txt")
	if err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ($1, $2, $3, $4)`,
		"snap-retained", int64(901), retainedID, int64(10))
	if err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{Mode: engine.RemoveModeFileIDs, FileIDs: []int64{retainedID}, FailFast: true})
	if err != nil {
		t.Fatalf("Remove retained: %v", err)
	}
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
