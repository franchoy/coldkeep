package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRestoreByIDEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origPhase := restoreByIDPhase
	called := false
	restoreByIDPhase = func(_ *storage.StorageContext, fileID int64, outputDir string, overwrite bool, dryRun bool) (storage.RestoreFileResult, error) {
		called = true
		if fileID != 42 {
			t.Fatalf("expected fileID 42, got %d", fileID)
		}
		if overwrite {
			t.Fatalf("expected overwrite=false")
		}
		if dryRun {
			t.Fatalf("expected dryRun=false in live routing test")
		}
		out := filepath.Join(outputDir, "routed.txt")
		return storage.RestoreFileResult{FileID: fileID, OriginalName: "routed.txt", OutputPath: out, RestoredHash: "abc123"}, nil
	}
	t.Cleanup(func() { restoreByIDPhase = origPhase })

	outputDir := t.TempDir()
	output := captureStdout(t, func() {
		err := runRestoreCommand(parsedCommandLine{
			method:      "restore",
			positionals: []string{"42", outputDir},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRestoreCommand: %v", err)
		}
	})

	if !called {
		t.Fatalf("expected restoreByIDPhase to be called")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["command"]; got != "restore" {
		t.Fatalf("expected command=restore, got %v", got)
	}
	results, ok := payload["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("expected one JSON result item, got %T len=%d", payload["results"], len(results))
	}
	item, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("expected result item object, got %T", results[0])
	}
	if got := item["status"]; got != "success" {
		t.Fatalf("expected item status success, got %v", got)
	}
	if got := item["output_path"]; !strings.Contains(got.(string), "routed.txt") {
		t.Fatalf("expected output_path to include routed.txt, got %v", got)
	}
}

func TestRestoreByIDDryRunEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origPhase := restoreByIDPhase
	called := false
	restoreByIDPhase = func(_ *storage.StorageContext, fileID int64, outputDir string, overwrite bool, dryRun bool) (storage.RestoreFileResult, error) {
		called = true
		if fileID != 52 {
			t.Fatalf("expected fileID 52, got %d", fileID)
		}
		if overwrite {
			t.Fatalf("expected overwrite=false")
		}
		if !dryRun {
			t.Fatalf("expected dryRun=true in dry-run routing test")
		}
		out := filepath.Join(outputDir, "dry-run-routed.txt")
		return storage.RestoreFileResult{FileID: fileID, OriginalName: "dry-run-routed.txt", OutputPath: out}, nil
	}
	t.Cleanup(func() { restoreByIDPhase = origPhase })

	outputDir := t.TempDir()
	output := captureStdout(t, func() {
		err := runRestoreCommand(parsedCommandLine{
			method:      "restore",
			positionals: []string{"52", outputDir},
			flags:       map[string][]string{"output": {"json"}, "dry-run": {""}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRestoreCommand dry-run: %v", err)
		}
	})

	if !called {
		t.Fatalf("expected restoreByIDPhase to be called in dry-run path")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["command"]; got != "restore" {
		t.Fatalf("expected command=restore, got %v", got)
	}
	if got := payload["dry_run"]; got != true {
		t.Fatalf("expected dry_run=true, got %v", got)
	}
	results, ok := payload["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("expected one JSON result item, got %T len=%d", payload["results"], len(results))
	}
	item, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("expected result item object, got %T", results[0])
	}
	if got := item["status"]; got != "planned" {
		t.Fatalf("expected item status planned, got %v", got)
	}
	if got := item["message"]; !strings.Contains(got.(string), "would restore ->") {
		t.Fatalf("expected planned message, got %v", got)
	}
	if got := item["output_path"]; !strings.Contains(got.(string), "dry-run-routed.txt") {
		t.Fatalf("expected output_path to include dry-run-routed.txt, got %v", got)
	}
}
