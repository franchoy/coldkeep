package engine_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestRestoreDryRunByIDThroughEngine(t *testing.T) {
	db := openSnapshotTestDB(t)
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status) VALUES ($1, $2, $3, 1, 'COMPLETED')`,
		"phase7.txt", int64(9), "phase7-hash")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	var fileID int64
	if err := db.QueryRowContext(context.Background(), `SELECT id FROM logical_file WHERE file_hash = $1`, "phase7-hash").Scan(&fileID); err != nil {
		t.Fatalf("lookup file ID: %v", err)
	}

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

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
	if !res.DryRun {
		t.Fatalf("expected DryRun=true")
	}
	if got := len(res.Items); got != 1 {
		t.Fatalf("expected 1 item, got %d", got)
	}
	if got := res.Items[0].Status; got != engine.BatchItemOK {
		t.Fatalf("expected item status ok, got %q", got)
	}
	if want := filepath.Join(outDir, "phase7.txt"); res.Items[0].OutputPath != want {
		t.Fatalf("expected output path %q, got %q", want, res.Items[0].OutputPath)
	}
}

func TestRestoreStoredPathDeferred(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	_, err = eng.Restore(context.Background(), engine.RestoreRequest{
		Mode:       engine.RestoreModeStoredPath,
		StoredPath: "samples/hello.txt",
	})
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented for stored-path mode, got %v", err)
	}
}

func TestRestoreFailFastStopsOnFirstFailure(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

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
