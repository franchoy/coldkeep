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

		_, err := eng.Remove(context.Background(), engine.RemoveRequest{})
		assertValidationBoundary(t, err, "engine: remove requires at least one file ID")
	})

	t.Run("restore requires file IDs", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng := newRestoreTestEngine(t, db)

		_, err := eng.Restore(context.Background(), engine.RestoreRequest{
			DestinationRoot: t.TempDir(),
		})
		assertValidationBoundary(t, err, "engine: restore requires at least one file ID")
	})

	t.Run("restore requires output directory", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng := newRestoreTestEngine(t, db)

		_, err := eng.Restore(context.Background(), engine.RestoreRequest{
			FileIDs: []int64{42},
		})
		assertValidationBoundary(t, err, "engine: restore output directory is required")
	})

	t.Run("restore stored path requires stored path", func(t *testing.T) {
		db, sgctx, _ := storeRemoveFixture(t, "restore-stored-path-validation.txt", "restore-stored-path-validation")
		eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

		_, err := eng.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{})
		assertValidationBoundary(t, err, "engine: restore stored path is required")
	})

	t.Run("restore stored path rejects conflicting metadata modes", func(t *testing.T) {
		db, sgctx, stored := storeRemoveFixture(t, "restore-stored-path-metadata.txt", "restore-stored-path-metadata")
		eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

		_, err := eng.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
			StoredPath:     stored.Path,
			StrictMetadata: true,
			NoMetadata:     true,
		})
		assertValidationBoundary(t, err, "engine: restore stored path strict metadata and no metadata are mutually exclusive")
	})

	t.Run("remove stored paths requires targets", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng := newRemoveTestEngine(t, db, t.TempDir())

		_, err := eng.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{})
		assertValidationBoundary(t, err, "engine: remove stored paths requires at least one target")
	})

	t.Run("remove stored paths requires database", func(t *testing.T) {
		_, err := (&engine.DefaultEngine{}).RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
			StoredPaths: []string{"/docs/a.txt"},
		})
		assertValidationBoundary(t, err, "engine: remove stored paths database is required")
	})
}

func TestPerItemExecutionFailuresRemainOutsideUnsupportedClassification(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng := newRemoveTestEngine(t, db, t.TempDir())

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
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
