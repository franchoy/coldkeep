package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestUnsupportedEngineModesRemainUnsupportedBoundaries(t *testing.T) {
	t.Run("recursive store", func(t *testing.T) {
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

		_, err = eng.Store(context.Background(), engine.StoreRequest{
			SourcePath: t.TempDir(),
			Recursive:  true,
			Workers:    2,
			Codec:      "plain",
		})
		assertUnsupportedBoundary(t, err, engine.ErrNotImplemented.Error())
	})

	t.Run("stored-path restore", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng := newRestoreTestEngine(t, db)

		_, err := eng.Restore(context.Background(), engine.RestoreRequest{
			Mode:       engine.RestoreModeStoredPath,
			StoredPath: "samples/hello.txt",
			OutputDir:  t.TempDir(),
			DryRun:     true,
		})
		assertUnsupportedBoundary(t, err, engine.ErrNotImplemented.Error())
	})

	t.Run("stored-path remove", func(t *testing.T) {
		db, sgctx, stored := storeRemoveFixture(t, "remove-boundary-unsupported.txt", "phase5-remove-boundary-unsupported")
		eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

		_, err := eng.Remove(context.Background(), engine.RemoveRequest{
			Mode:       engine.RemoveModeStoredPath,
			StoredPath: stored.Path,
		})
		assertUnsupportedBoundary(t, err, engine.ErrNotImplemented.Error())
	})

	t.Run("stored-paths remove", func(t *testing.T) {
		db, sgctx, first := storeRemoveFixture(t, "remove-boundary-unsupported-batch-1.txt", "phase5-remove-boundary-unsupported-batch-1")
		second, err := storeRemoveFixtureSecondPath(t, sgctx)
		if err != nil {
			t.Fatalf("second store fixture: %v", err)
		}

		eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)
		_, err = eng.Remove(context.Background(), engine.RemoveRequest{
			Mode:        engine.RemoveModeStoredPaths,
			StoredPaths: []string{first.Path, second.Path},
		})
		assertUnsupportedBoundary(t, err, engine.ErrNotImplemented.Error())
	})
}

func TestValidationErrorsRemainOutsideUnsupportedClassification(t *testing.T) {
	t.Run("store requires source path", func(t *testing.T) {
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

		_, err = eng.Store(context.Background(), engine.StoreRequest{SourcePath: "", Codec: "plain"})
		assertValidationBoundary(t, err, "engine: store source path is required")
	})

	t.Run("store requires injected StoreContext", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.Store(context.Background(), engine.StoreRequest{SourcePath: "file.txt", Codec: "plain"})
		assertValidationBoundary(t, err, "engine: store requires injected StoreContext")
	})

	t.Run("remove requires file IDs", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng := newRemoveTestEngine(t, db, t.TempDir())

		_, err := eng.Remove(context.Background(), engine.RemoveRequest{Mode: engine.RemoveModeFileIDs})
		assertValidationBoundary(t, err, "engine: remove requires at least one file ID")
	})

	t.Run("restore requires file IDs", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng := newRestoreTestEngine(t, db)

		_, err := eng.Restore(context.Background(), engine.RestoreRequest{
			Mode:      engine.RestoreModeFileIDs,
			OutputDir: t.TempDir(),
		})
		assertValidationBoundary(t, err, "engine: restore requires at least one file ID")
	})

	t.Run("restore requires output directory", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng := newRestoreTestEngine(t, db)

		_, err := eng.Restore(context.Background(), engine.RestoreRequest{
			Mode:    engine.RestoreModeFileIDs,
			FileIDs: []int64{42},
		})
		assertValidationBoundary(t, err, "engine: restore output directory is required")
	})
}

func TestPerItemExecutionFailuresRemainOutsideUnsupportedClassification(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng := newRemoveTestEngine(t, db, t.TempDir())

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		Mode:    engine.RemoveModeFileIDs,
		FileIDs: []int64{-1},
	})
	assertRemoveReturnedNonUnsupportedTopLevelSuccess(t, err)
	assertInvalidFileIDFailureItem(t, res)
	assertInvalidFileIDFailureSummary(t, res.Summary)
}

func assertRemoveReturnedNonUnsupportedTopLevelSuccess(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("Remove invalid file ID: %v", err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected top-level nil error to remain non-unsupported")
	}
}

func assertInvalidFileIDFailureItem(t *testing.T, res engine.RemoveResult) {
	t.Helper()

	if len(res.Items) != 1 {
		t.Fatalf("expected one item result, got %d", len(res.Items))
	}
	item := res.Items[0]
	if item.Status != engine.BatchItemFailed {
		t.Fatalf("expected invalid file ID to fail at item level, got %q", item.Status)
	}
	if item.Error != "invalid file ID -1" {
		t.Fatalf("expected invalid file ID item error, got %q", item.Error)
	}
	if item.InvariantCode != "" {
		t.Fatalf("expected no invariant code for invalid file ID, got %q", item.InvariantCode)
	}
}

func assertInvalidFileIDFailureSummary(t *testing.T, summary engine.BatchSummary) {
	t.Helper()

	if summary.Failed != 1 {
		t.Fatalf("expected one failed item, got summary: %+v", summary)
	}
	if summary.OK != 0 {
		t.Fatalf("expected zero OK items, got summary: %+v", summary)
	}
	if summary.Skipped != 0 {
		t.Fatalf("expected zero skipped items, got summary: %+v", summary)
	}
}

func assertUnsupportedBoundary(t *testing.T, err error, wantMessage string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected unsupported boundary error")
	}
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented-compatible unsupported error, got %v", err)
	}
	if !engine.IsUnsupported(err) {
		t.Fatalf("expected unsupported boundary to classify as unsupported, got %v", err)
	}
	if err.Error() != wantMessage {
		t.Fatalf("expected unsupported message %q, got %q", wantMessage, err.Error())
	}
}

func assertValidationBoundary(t *testing.T, err error, wantMessage string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected validation error")
	}
	if errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected validation error to remain distinct from ErrNotImplemented: %v", err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected validation error to remain non-unsupported: %v", err)
	}
	if err.Error() != wantMessage {
		t.Fatalf("expected validation message %q, got %q", wantMessage, err.Error())
	}
}
