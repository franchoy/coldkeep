package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestRestoreDryRunByIDThroughEngine(t *testing.T) {
	db := openSnapshotTestDB(t)
	fileID := seedRestoreDryRunLogicalFile(t, db)
	eng := newRestoreTestEngine(t, db)

	outDir := t.TempDir()
	res, err := eng.Restore(context.Background(), engine.RestoreRequest{
		Mode:      engine.RestoreModeFileIDs,
		FileIDs:   []int64{fileID},
		OutputDir: outDir,
		DryRun:    true,
	})
	if err != nil {
		t.Fatalf("Restore dry-run: %v", err)
	}

	assertRestoreDryRunResult(t, res, filepath.Join(outDir, "phase7.txt"))
}

func TestRestoreStoredPathDeferred(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng := newRestoreTestEngine(t, db)

	_, err := eng.Restore(context.Background(), engine.RestoreRequest{
		Mode:       engine.RestoreModeStoredPath,
		StoredPath: "samples/hello.txt",
	})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for stored-path mode, got %v", err)
	}
}

func TestRestoreFailFastStopsOnFirstFailure(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng := newRestoreTestEngine(t, db)

	res, err := eng.Restore(context.Background(), engine.RestoreRequest{
		Mode:      engine.RestoreModeFileIDs,
		FileIDs:   []int64{-1, 2},
		OutputDir: t.TempDir(),
		FailFast:  true,
	})
	if err != nil {
		t.Fatalf("Restore fail-fast: %v", err)
	}
	if got := len(res.Items); got != 1 {
		t.Fatalf("expected fail-fast to stop at first item, got %d items", got)
	}
	if got := res.Items[0].Status; got != engine.BatchItemFailed {
		t.Fatalf("expected first item failed, got %q", got)
	}
}

func seedRestoreDryRunLogicalFile(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	ctx := context.Background()
	_, err := db.ExecContext(ctx,
		`INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status) VALUES ($1, $2, $3, 1, 'COMPLETED')`,
		"phase7.txt", int64(9), "phase7-hash")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var fileID int64
	if err := db.QueryRowContext(ctx, `SELECT id FROM logical_file WHERE file_hash = $1`, "phase7-hash").Scan(&fileID); err != nil {
		t.Fatalf("lookup file ID: %v", err)
	}
	return fileID
}

func newRestoreTestEngine(t *testing.T, db *sql.DB) *engine.DefaultEngine {
	t.Helper()

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return eng
}

func assertRestoreDryRunResult(t *testing.T, res engine.RestoreResult, wantOutputPath string) {
	t.Helper()

	if !res.DryRun {
		t.Fatalf("expected DryRun=true")
	}
	if got := len(res.Items); got != 1 {
		t.Fatalf("expected 1 item, got %d", got)
	}
	item := res.Items[0]
	if item.Status != engine.BatchItemOK {
		t.Fatalf("expected item status ok, got %q", item.Status)
	}
	if item.OutputPath != wantOutputPath {
		t.Fatalf("expected output path %q, got %q", wantOutputPath, item.OutputPath)
	}
	assertFileDoesNotExist(t, item.OutputPath)
}

func assertFileDoesNotExist(t *testing.T, path string) {
	t.Helper()

	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("dry-run should not create output file, stat err=%v", statErr)
	}
}
