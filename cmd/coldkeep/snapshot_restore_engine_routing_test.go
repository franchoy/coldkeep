package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRunSnapshotCommandRestoreValidationFailsBeforeEngineConstruction(t *testing.T) {
	tests := []struct {
		name        string
		positionals []string
		flags       map[string][]string
		wantErr     string
	}{
		{
			name:        "missing snapshot id",
			positionals: []string{"restore"},
			flags:       map[string][]string{},
			wantErr:     "Usage: coldkeep snapshot restore",
		},
		{
			name:        "blank snapshot id",
			positionals: []string{"restore", "   "},
			flags:       map[string][]string{},
			wantErr:     "snapshotID cannot be empty",
		},
		{
			name:        "conflicting metadata flags",
			positionals: []string{"restore", "snap-a"},
			flags:       map[string][]string{"strict": {""}, "no-metadata": {""}},
			wantErr:     "--strict and --no-metadata cannot be used together",
		},
		{
			name:        "invalid regex",
			positionals: []string{"restore", "snap-a"},
			flags:       map[string][]string{"regex": {"("}},
			wantErr:     `invalid --regex value "("`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalLoad := loadDefaultStorageContextPhase
			originalEngine := newSnapshotRestoreCommandEngine
			t.Cleanup(func() {
				loadDefaultStorageContextPhase = originalLoad
				newSnapshotRestoreCommandEngine = originalEngine
			})

			loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
				return storage.StorageContext{}, errors.New("storage init should not be called")
			}
			newSnapshotRestoreCommandEngine = func(storage.StorageContext) (engine.Engine, error) {
				return stubCommandEngine{}, errors.New("engine should not be constructed")
			}

			err := runSnapshotCommand(parsedCommandLine{
				method:      "snapshot",
				positionals: tc.positionals,
				flags:       tc.flags,
			}, outputModeText)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestRunSnapshotCommandRestoreRoutesThroughEngineWithExplicitOriginalRoot(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalEngine := newSnapshotRestoreCommandEngine
	originalCWD := currentWorkingDirectoryPhase
	originalRestore := restoreSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newSnapshotRestoreCommandEngine = originalEngine
		currentWorkingDirectoryPhase = originalCWD
		restoreSnapshotPhase = originalRestore
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	currentWorkingDirectoryPhase = func() (string, error) {
		return "/explicit/original/root", nil
	}
	restoreSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string, _ []string, _ snapshot.RestoreSnapshotOptions) (*snapshot.RestoreSnapshotResult, error) {
		t.Fatalf("restoreSnapshotPhase must not run in engine-routed snapshot restore path for %q", snapshotID)
		return nil, nil
	}

	called := false
	newSnapshotRestoreCommandEngine = func(_ storage.StorageContext) (engine.Engine, error) {
		return stubCommandEngine{
			snapshotRestoreFunc: func(_ context.Context, req engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error) {
				called = true
				if req.Destination.Mode != engine.SnapshotRestoreDestinationOriginal {
					t.Fatalf("expected original mode, got %+v", req.Destination)
				}
				if req.Destination.Path != "/explicit/original/root" {
					t.Fatalf("expected explicit lexical root, got %+v", req.Destination)
				}
				if req.Metadata != engine.SnapshotRestoreMetadataBestEffort {
					t.Fatalf("expected default metadata mode, got %+v", req)
				}
				return engine.SnapshotRestoreResult{
					SnapshotID:          req.SnapshotID,
					DestinationMode:     req.Destination.Mode,
					RequestedPathsCount: len(req.Paths),
					RestoredFiles:       1,
					OutputTarget:        req.Destination.Path,
				}, nil
			},
		}, nil
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"restore", "snap-original", "docs/a.txt"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand restore original: %v", err)
	}
	if !called {
		t.Fatal("expected engine-routed snapshot restore")
	}
}

func TestRunSnapshotCommandRestorePreservesSelectorSeparationAndRepetition(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalEngine := newSnapshotRestoreCommandEngine
	originalRestore := restoreSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newSnapshotRestoreCommandEngine = originalEngine
		restoreSnapshotPhase = originalRestore
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	restoreSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string, _ []string, _ snapshot.RestoreSnapshotOptions) (*snapshot.RestoreSnapshotResult, error) {
		t.Fatalf("restoreSnapshotPhase must not run in engine-routed snapshot restore path for %q", snapshotID)
		return nil, nil
	}

	newSnapshotRestoreCommandEngine = func(_ storage.StorageContext) (engine.Engine, error) {
		return stubCommandEngine{
			snapshotRestoreFunc: func(_ context.Context, req engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error) {
				if len(req.Paths) != 1 || req.Paths[0] != "docs/" {
					t.Fatalf("expected positional paths to remain separate, got %+v", req)
				}
				if len(req.Selection.ExactPaths) != 2 || req.Selection.ExactPaths[0] != "docs/a.txt" || req.Selection.ExactPaths[1] != "docs/a.txt" {
					t.Fatalf("expected repeated exact selectors preserved, got %+v", req.Selection)
				}
				if len(req.Selection.Prefixes) != 2 || req.Selection.Prefixes[0] != "docs/" || req.Selection.Prefixes[1] != "docs/" {
					t.Fatalf("expected repeated prefixes preserved, got %+v", req.Selection)
				}
				if req.Destination.Mode != engine.SnapshotRestoreDestinationOverride || req.Destination.Path != "/tmp/out.txt" {
					t.Fatalf("unexpected override destination mapping: %+v", req.Destination)
				}
				if req.Metadata != engine.SnapshotRestoreMetadataStrict || !req.Overwrite {
					t.Fatalf("unexpected overwrite/metadata mapping: %+v", req)
				}
				return engine.SnapshotRestoreResult{
					SnapshotID:          req.SnapshotID,
					DestinationMode:     req.Destination.Mode,
					RequestedPathsCount: len(req.Paths),
					RestoredFiles:       1,
					OutputTarget:        req.Destination.Path,
				}, nil
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-override", "docs/"},
			flags: map[string][]string{
				"mode":        {"override"},
				"destination": {"/tmp/out.txt"},
				"overwrite":   {""},
				"strict":      {""},
				"path":        {"docs/a.txt", "./docs/a.txt"},
				"prefix":      {"docs/", "./docs/"},
				"output":      {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand restore override: %v", err)
		}
	})

	if !strings.Contains(output, `"type":"partial_restore"`) {
		t.Fatalf("expected partial_restore JSON compatibility, got %s", output)
	}
	if !strings.Contains(output, `"output_root":"/tmp/out.txt"`) {
		t.Fatalf("expected output_root JSON compatibility, got %s", output)
	}
}
