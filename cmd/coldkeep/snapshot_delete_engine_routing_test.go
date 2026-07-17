package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

func installSnapshotDeleteCommandEngine(
	t *testing.T,
	handler func(context.Context, engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error),
) {
	t.Helper()

	originalLoad := loadDefaultStorageContextPhase
	originalEngine := newCommandEngine
	originalDelete := deleteSnapshotPhase
	originalPreview := snapshotDeleteLineagePreviewPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newCommandEngine = originalEngine
		deleteSnapshotPhase = originalDelete
		snapshotDeleteLineagePreviewPhase = originalPreview
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) error {
		t.Fatalf("deleteSnapshotPhase must not run in engine-routed snapshot delete path for %q", snapshotID)
		return nil
	}
	snapshotDeleteLineagePreviewPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
		t.Fatalf("snapshotDeleteLineagePreviewPhase must not run in engine-routed snapshot delete path for %q", snapshotID)
		return nil, nil
	}
	newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
		return stubCommandEngine{snapshotDeleteFunc: handler}, nil
	}
}

func TestRunSnapshotCommandDeleteDryRunAliasUsesPreviewMode(t *testing.T) {
	installSnapshotDeleteCommandEngine(t, func(_ context.Context, req engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error) {
		if req.Mode != engine.SnapshotDeleteModePreview || req.SnapshotID != "snap-alias" {
			t.Fatalf("unexpected snapshot delete request: %+v", req)
		}
		return engine.SnapshotDeleteResult{
			SnapshotID: req.SnapshotID,
			Mode:       engine.SnapshotDeleteModePreview,
			Preview: &engine.SnapshotDeletePreviewResult{
				Parent: engine.SnapshotDeleteParent{State: engine.SnapshotDeleteParentNone},
			},
		}, nil
	})

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"delete", "snap-alias"},
		flags: map[string][]string{
			"dryRun": {""},
		},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand delete with --dryRun: %v", err)
	}
}

func TestRunSnapshotCommandDeleteTrimsSnapshotIDBeforeEngine(t *testing.T) {
	installSnapshotDeleteCommandEngine(t, func(_ context.Context, req engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error) {
		if req.SnapshotID != "snap-trimmed" || req.Mode != engine.SnapshotDeleteModeExecute {
			t.Fatalf("unexpected snapshot delete request: %+v", req)
		}
		return engine.SnapshotDeleteResult{
			SnapshotID: req.SnapshotID,
			Mode:       engine.SnapshotDeleteModeExecute,
			Deleted:    true,
		}, nil
	})

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "  snap-trimmed  "},
			flags: map[string][]string{
				"force": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete trim case: %v", err)
		}
	})
	if !strings.Contains(output, "Snapshot deleted: id=snap-trimmed") {
		t.Fatalf("expected trimmed snapshot ID in output, got:\n%s", output)
	}
}

func TestRunSnapshotCommandDeleteValidationFailsBeforeEngineConstruction(t *testing.T) {
	tests := []struct {
		name        string
		positionals []string
		flags       map[string][]string
		wantErr     string
	}{
		{name: "missing snapshot id", positionals: []string{"delete"}, flags: map[string][]string{"force": {""}}, wantErr: "Usage: coldkeep snapshot delete"},
		{name: "blank snapshot id", positionals: []string{"delete", "   "}, flags: map[string][]string{"force": {""}}, wantErr: "snapshotID cannot be empty"},
		{name: "force false", positionals: []string{"delete", "snap-a"}, flags: map[string][]string{"force": {"false"}}, wantErr: "requires --force or --dry-run"},
		{name: "dry run false", positionals: []string{"delete", "snap-a"}, flags: map[string][]string{"dry-run": {"false"}}, wantErr: "requires --force or --dry-run"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalLoad := loadDefaultStorageContextPhase
			originalEngine := newCommandEngine
			t.Cleanup(func() {
				loadDefaultStorageContextPhase = originalLoad
				newCommandEngine = originalEngine
			})

			loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
				return storage.StorageContext{}, errors.New("storage init should not be called")
			}
			newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
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

func TestRunSnapshotCommandDeletePreviewMapsMissingParentCompatibility(t *testing.T) {
	installSnapshotDeleteCommandEngine(t, func(_ context.Context, req engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error) {
		return engine.SnapshotDeleteResult{
			SnapshotID: req.SnapshotID,
			Mode:       engine.SnapshotDeleteModePreview,
			Preview: &engine.SnapshotDeletePreviewResult{
				Parent:      engine.SnapshotDeleteParent{ID: "ghost-parent", State: engine.SnapshotDeleteParentMissing},
				TotalFiles:  1,
				UniqueFiles: 1,
			},
		}, nil
	})

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "child-snap"},
			flags: map[string][]string{
				"dry-run": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete preview missing-parent: %v", err)
		}
	})
	if !strings.Contains(output, "Parent: (missing)") {
		t.Fatalf("expected missing-parent output, got:\n%s", output)
	}
	if !strings.Contains(output, "Parent note: parent snapshot metadata is missing") {
		t.Fatalf("expected missing-parent note, got:\n%s", output)
	}
}

func TestRunSnapshotCommandDeleteExecuteJSONRemainsSparse(t *testing.T) {
	installSnapshotDeleteCommandEngine(t, func(_ context.Context, req engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error) {
		return engine.SnapshotDeleteResult{
			SnapshotID: req.SnapshotID,
			Mode:       engine.SnapshotDeleteModeExecute,
			Deleted:    true,
		}, nil
	})

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "snap-sparse"},
			flags: map[string][]string{
				"force":  {""},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete execute JSON: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse execute JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	assertSnapshotDeleteJSONHeader(t, data)
	assertSnapshotDeleteJSONSparseFields(t, data)
	assertSnapshotDeleteJSONZeroValues(t, data)
}

func assertSnapshotDeleteJSONHeader(t *testing.T, data map[string]any) {
	t.Helper()
	if data["action"] != "delete" || data["snapshot_id"] != "snap-sparse" || data["dry_run"] != false {
		t.Fatalf("unexpected execute JSON header: %v", data)
	}
}

func assertSnapshotDeleteJSONSparseFields(t *testing.T, data map[string]any) {
	t.Helper()
	if data["parent_id"] != nil || data["children"] != nil || data["warnings"] != nil {
		t.Fatalf("expected sparse execute JSON preview fields, got %v", data)
	}
}

func assertSnapshotDeleteJSONZeroValues(t *testing.T, data map[string]any) {
	t.Helper()
	if data["parent_missing"] != false || data["total_files"] != float64(0) || data["unique_files"] != float64(0) || data["shared_files"] != float64(0) {
		t.Fatalf("expected sparse execute JSON zero values, got %v", data)
	}
}
