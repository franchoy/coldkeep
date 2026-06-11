package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestReadSideCLIValidationErrorsRemainOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	t.Run("inspect unsupported entity", func(t *testing.T) {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"blob"},
		}, outputModeText)
		assertCLIReadSideBoundaryError(t, err, `unsupported inspect entity "blob"`)
	})

	t.Run("snapshot show empty id", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"show", "   "},
		}, outputModeText)
		assertCLIReadSideBoundaryError(t, err, "snapshotID cannot be empty")
	})

	t.Run("snapshot diff invalid filter", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "snap-1", "snap-2"},
			flags:       map[string][]string{"filter": {"invalid"}},
		}, outputModeText)
		assertCLIReadSideBoundaryError(t, err, "invalid --filter value")
	})

	t.Run("snapshot diff missing target argument", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "snap-1"},
		}, outputModeText)
		assertCLIReadSideBoundaryError(t, err, "Usage: coldkeep snapshot diff")
	})
}

func TestSnapshotDiffInvalidRegexRemainsOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"diff", "snap-1", "snap-2"},
		flags:       map[string][]string{"regex": {"[unclosed"}},
	}, outputModeText)
	assertCLIReadSideBoundaryError(t, err, "invalid --regex value")
}

func TestSnapshotShowMissingSnapshotRemainsOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalGet := getSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		getSnapshotPhase = originalGet
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	getSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.Snapshot, error) {
		return nil, fmt.Errorf("snapshot %q not found", snapshotID)
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"show", "missing-snap"},
	}, outputModeText)
	assertCLIReadSideBoundaryError(t, err, `snapshot "missing-snap" not found`)
}

func TestSnapshotDiffMissingSnapshotRemainsOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDiff := diffSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		diffSnapshotsPhase = originalDiff
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	diffSnapshotsPhase = func(_ context.Context, _ *sql.DB, baseID, targetID string, _ *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
		return nil, fmt.Errorf("snapshot %q not found", baseID)
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"diff", "missing-base", "target"},
	}, outputModeText)
	assertCLIReadSideBoundaryError(t, err, `snapshot "missing-base" not found`)
}

func assertCLIReadSideBoundaryError(t *testing.T, err error, wantSubstring string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected CLI error")
	}
	if wantSubstring != "" && !strings.Contains(err.Error(), wantSubstring) {
		t.Fatalf("expected error containing %q, got %v", wantSubstring, err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected CLI read-side error to remain outside unsupported classification: %v", err)
	}
	if catalog.IsDeferred(err) {
		t.Fatalf("expected CLI read-side error to remain outside deferred classification: %v", err)
	}
}
