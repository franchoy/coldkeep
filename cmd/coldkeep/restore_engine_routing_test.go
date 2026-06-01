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
	restoreByIDPhase = func(_ *storage.StorageContext, fileID int64, outputDir string, overwrite bool) (storage.RestoreFileResult, error) {
		called = true
		if fileID != 42 {
			t.Fatalf("expected fileID 42, got %d", fileID)
		}
		if overwrite {
			t.Fatalf("expected overwrite=false")
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
