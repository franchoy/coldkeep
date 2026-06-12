package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/engine"
)

func TestReadSideStatsFailuresRemainOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	t.Run("engine construction requires db", func(t *testing.T) {
		_, err := engine.New(engine.Config{})
		if err == nil {
			t.Fatal("expected engine.New to fail with nil db")
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})

	t.Run("active success path stays outside unsupported and deferred", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.Stats(context.Background(), engine.StatsRequest{})
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})

	t.Run("runtime db failure stays outside unsupported and deferred", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}

		_, err = eng.Stats(context.Background(), engine.StatsRequest{})
		if err == nil {
			t.Fatal("expected Stats to fail after db close")
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})
}

func TestReadSideSnapshotShowFailuresRemainOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	t.Run("missing snapshot remains non-unsupported and non-deferred", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "no-such-snap"})
		if err == nil {
			t.Fatal("expected missing snapshot error")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Fatalf("expected not found error, got %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})

	t.Run("active success path stays outside unsupported and deferred", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		now := time.Now().UTC().Truncate(time.Second)
		insertTestSnapshot(t, db, "snap-show-boundary", "full", "boundary", "", now)
		insertTestSnapshotFile(t, db, "snap-show-boundary", "docs/a.txt", 100)

		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "snap-show-boundary"})
		if err != nil {
			t.Fatalf("SnapshotShow: %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})
}

func TestReadSideSnapshotDiffFailuresRemainOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	t.Run("missing base snapshot", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		now := time.Now().UTC().Truncate(time.Second)
		insertTestSnapshot(t, db, "diff-target-present", "full", "", "", now)

		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
			BaseID:   "missing-base",
			TargetID: "diff-target-present",
		})
		if err == nil {
			t.Fatal("expected missing base snapshot error")
		}
		if !strings.Contains(err.Error(), `snapshot "missing-base" not found`) {
			t.Fatalf("expected missing base snapshot error, got %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})

	t.Run("missing target snapshot", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		now := time.Now().UTC().Truncate(time.Second)
		insertTestSnapshot(t, db, "diff-base-present", "full", "", "", now)

		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
			BaseID:   "diff-base-present",
			TargetID: "missing-target",
		})
		if err == nil {
			t.Fatal("expected missing target snapshot error")
		}
		if !strings.Contains(err.Error(), `snapshot "missing-target" not found`) {
			t.Fatalf("expected missing target snapshot error, got %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})

	t.Run("summary fast path success stays outside unsupported and deferred", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		now := time.Now().UTC().Truncate(time.Second)
		insertTestSnapshot(t, db, "snap-diff-base-boundary", "full", "", "", now)
		insertTestSnapshot(t, db, "snap-diff-target-boundary", "full", "", "", now.Add(time.Second))
		insertTestSnapshotFile(t, db, "snap-diff-base-boundary", "common.txt", 100)
		insertTestSnapshotFile(t, db, "snap-diff-base-boundary", "removed.txt", 200)
		insertTestSnapshotFile(t, db, "snap-diff-target-boundary", "common.txt", 100)
		insertTestSnapshotFile(t, db, "snap-diff-target-boundary", "added.txt", 300)

		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
			BaseID:   "snap-diff-base-boundary",
			TargetID: "snap-diff-target-boundary",
			Summary:  true,
		})
		if err != nil {
			t.Fatalf("SnapshotDiff summary: %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})

	t.Run("detailed success stays outside unsupported and deferred", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		seedSnapshotDiffFullFixture(t, db)

		eng, err := engine.New(engine.Config{DB: db})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		_, err = eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
			BaseID:   "snap-df-base",
			TargetID: "snap-df-target",
		})
		if err != nil {
			t.Fatalf("SnapshotDiff detailed: %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})
}

func TestReadSideSnapshotDiffValidationFailuresRemainOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	t.Run("empty base id", func(t *testing.T) {
		_, err := eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
			BaseID:   "",
			TargetID: "target",
		})
		if err == nil {
			t.Fatal("expected empty base id error")
		}
		if !strings.Contains(err.Error(), "base snapshot id cannot be empty") {
			t.Fatalf("expected empty base id error, got %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})

	t.Run("empty target id", func(t *testing.T) {
		_, err := eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
			BaseID:   "base",
			TargetID: "",
		})
		if err == nil {
			t.Fatal("expected empty target id error")
		}
		if !strings.Contains(err.Error(), "target snapshot id cannot be empty") {
			t.Fatalf("expected empty target id error, got %v", err)
		}
		assertReadSideNonUnsupportedAndDeferred(t, err)
	})
}

func assertReadSideNonUnsupportedAndDeferred(t *testing.T, err error) {
	t.Helper()

	if engine.IsUnsupported(err) {
		t.Fatalf("expected read-side error to remain outside unsupported classification: %v", err)
	}
	if catalog.IsDeferred(err) {
		t.Fatalf("expected read-side error to remain outside deferred classification: %v", err)
	}
}
