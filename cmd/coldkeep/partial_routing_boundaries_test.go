package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRunInspectCommandPreservesDirectObservabilityBypass(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	called := false
	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		called = true
		if entity != observability.EntityFile {
			t.Fatalf("expected file entity, got %q", entity)
		}
		if id != "42" {
			t.Fatalf("expected file id 42, got %q", id)
		}
		return &observability.InspectResult{
			EntityType: observability.EntityFile,
			EntityID:   "42",
			Summary: map[string]any{
				"stored_path": "docs/routed.txt",
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"file", "42"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runInspectCommand: %v", err)
		}
	})

	if !called {
		t.Fatal("expected inspect command to preserve direct observability routing")
	}
	if !strings.Contains(output, "\"type\":\"inspect\"") {
		t.Fatalf("expected inspect JSON output, got %q", output)
	}
}

func TestRunSnapshotCommandShowPreservesMixedOwnershipSeams(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalGet := getSnapshotPhase
	originalListFiles := listSnapshotFilesPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		getSnapshotPhase = originalGet
		listSnapshotFilesPhase = originalListFiles
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	var got []string
	getSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.Snapshot, error) {
		got = append(got, "getSnapshotPhase:"+snapshotID)
		return &snapshot.Snapshot{ID: snapshotID, Type: "full", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)}, nil
	}
	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, snapshotID string, limit int, _ *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		got = append(got, "listSnapshotFilesPhase:"+snapshotID)
		return []snapshot.SnapshotFileEntry{{Path: "docs/a.txt"}}, nil
	}
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		got = append(got, "snapshotStatsPhase:"+snapshotID)
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 1}, nil
	}

	_ = captureStdout(t, func() {
		if err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"show", "snap-mixed-1"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON); err != nil {
			t.Fatalf("runSnapshotCommand show: %v", err)
		}
	})

	want := []string{
		"getSnapshotPhase:snap-mixed-1",
		"listSnapshotFilesPhase:snap-mixed-1",
		"snapshotStatsPhase:snap-mixed-1",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("expected mixed snapshot show seams %v, got %v", want, got)
	}
}

func TestDiffSnapshotsPhaseNarrowsPrefixesBeforeEngineSeam(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)

	now := time.Now().UTC().Truncate(time.Second)
	insertRoutingSnapshot(t, dbconn, "diff-narrow-base", "full", "", now)
	insertRoutingSnapshot(t, dbconn, "diff-narrow-target", "full", "", now.Add(time.Second))
	insertRoutingSnapshotFile(t, dbconn, "diff-narrow-base", "docs/removed.txt", 100)
	insertRoutingSnapshotFile(t, dbconn, "diff-narrow-base", "images/removed.png", 100)
	insertRoutingSnapshotFile(t, dbconn, "diff-narrow-target", "docs/added.txt", 100)
	insertRoutingSnapshotFile(t, dbconn, "diff-narrow-target", "images/added.png", 100)

	query := &snapshot.SnapshotQuery{
		Prefixes: []string{"docs/", "images/"},
	}

	result, err := diffSnapshotsPhase(context.Background(), dbconn, "diff-narrow-base", "diff-narrow-target", query)
	if err != nil {
		t.Fatalf("diffSnapshotsPhase: %v", err)
	}

	paths := make([]string, 0, len(result.Entries))
	for _, entry := range result.Entries {
		paths = append(paths, entry.Path)
		if strings.HasPrefix(entry.Path, "images/") {
			t.Fatalf("expected diff seam to narrow to first prefix only, got image entry %q", entry.Path)
		}
	}
	if len(paths) != 2 {
		t.Fatalf("expected only docs/ entries after narrowing, got %v", paths)
	}
}

func TestRestoreStoredPathPreservesDirectCLIStoragePath(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalRestoreByID := restoreByIDPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		restoreByIDPhase = originalRestoreByID
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{}, sql.ErrConnDone
	}

	calledByID := false
	restoreByIDPhase = func(_ *storage.StorageContext, _ int64, _ string, _ bool, _ bool) (storage.RestoreFileResult, error) {
		calledByID = true
		return storage.RestoreFileResult{}, nil
	}

	err := runRestoreCommand(parsedCommandLine{
		method:      "restore",
		positionals: nil,
		flags: map[string][]string{
			"stored-path": {"/docs/a.txt"},
			"mode":        {"override"},
			"destination": {"/tmp/out.txt"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "load storage context") {
		t.Fatalf("expected direct stored-path restore branch to load storage context, got %v", err)
	}
	if calledByID {
		t.Fatal("stored-path restore should not route through restoreByIDPhase")
	}
}

func TestRemoveStoredPathPreservesDirectCLIStoragePath(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalRemoveByID := removeByIDPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		removeByIDPhase = originalRemoveByID
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{}, sql.ErrConnDone
	}

	calledByID := false
	removeByIDPhase = func(_ *storage.StorageContext, _ int64, _ bool) batch.ItemResult {
		calledByID = true
		return batch.ItemResult{}
	}

	err := runRemoveCommand(parsedCommandLine{
		method:      "remove",
		positionals: nil,
		flags: map[string][]string{
			"stored-path": {"/docs/a.txt"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "load storage context") {
		t.Fatalf("expected direct stored-path remove branch to load storage context, got %v", err)
	}
	if calledByID {
		t.Fatal("stored-path remove should not route through removeByIDPhase")
	}
}

func TestRemoveStoredPathsPreservesDirectCLIStoredPathBatchPath(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalRemoveByID := removeByIDPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		removeByIDPhase = originalRemoveByID
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{}, sql.ErrConnDone
	}

	calledByID := false
	removeByIDPhase = func(_ *storage.StorageContext, _ int64, _ bool) batch.ItemResult {
		calledByID = true
		return batch.ItemResult{}
	}

	err := runRemoveCommand(parsedCommandLine{
		method:      "remove",
		positionals: []string{"/docs/a.txt", "/docs/b.txt"},
		flags: map[string][]string{
			"stored-paths": {""},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "load storage context") {
		t.Fatalf("expected direct stored-paths remove branch to load storage context, got %v", err)
	}
	if calledByID {
		t.Fatal("stored-paths remove should not route through removeByIDPhase")
	}
}

func TestSnapshotShowMixedOwnershipJSONEnvelopeStillStable(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalGet := getSnapshotPhase
	originalListFiles := listSnapshotFilesPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		getSnapshotPhase = originalGet
		listSnapshotFilesPhase = originalListFiles
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	getSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.Snapshot, error) {
		return &snapshot.Snapshot{ID: snapshotID, Type: "full", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)}, nil
	}
	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, _ string, _ int, _ *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		return []snapshot.SnapshotFileEntry{{Path: "docs/a.txt"}}, nil
	}
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 3, TotalSizeBytes: 123}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"show", "snap-show-envelope"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand show: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot show JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got := int(data["file_count"].(float64)); got != 1 {
		t.Fatalf("expected file_count=1, got %d", got)
	}
	if got := int(data["total_snapshot_file_count"].(float64)); got != 3 {
		t.Fatalf("expected total_snapshot_file_count=3, got %d", got)
	}
}
