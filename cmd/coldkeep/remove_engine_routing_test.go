package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRemoveByIDEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origPhase := removeByIDPhase
	called := false
	removeByIDPhase = func(_ *storage.StorageContext, fileID int64, dryRun bool) batch.ItemResult {
		called = true
		if fileID != 42 {
			t.Fatalf("expected fileID 42, got %d", fileID)
		}
		if dryRun {
			t.Fatalf("expected dryRun=false in live remove routing test")
		}
		return batch.ItemResult{ID: fileID, Status: batch.ResultSuccess, Message: "removed mappings=3"}
	}
	t.Cleanup(func() { removeByIDPhase = origPhase })

	output := captureStdout(t, func() {
		err := runRemoveCommand(parsedCommandLine{
			method:      "remove",
			positionals: []string{"42"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRemoveCommand: %v", err)
		}
	})
	if !called {
		t.Fatalf("expected removeByIDPhase to be called")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["command"]; got != "remove" {
		t.Fatalf("expected command=remove, got %v", got)
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
	if got := item["message"]; got != "removed mappings=3" {
		t.Fatalf("expected message removed mappings=3, got %v", got)
	}
}

func TestRemoveByIDDryRunEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origPhase := removeByIDPhase
	called := false
	removeByIDPhase = func(_ *storage.StorageContext, fileID int64, dryRun bool) batch.ItemResult {
		called = true
		if fileID != 52 {
			t.Fatalf("expected fileID 52, got %d", fileID)
		}
		if !dryRun {
			t.Fatalf("expected dryRun=true in dry-run remove routing test")
		}
		return batch.ItemResult{ID: fileID, Status: batch.ResultPlanned, Message: "would remove"}
	}
	t.Cleanup(func() { removeByIDPhase = origPhase })

	output := captureStdout(t, func() {
		err := runRemoveCommand(parsedCommandLine{
			method:      "remove",
			positionals: []string{"52"},
			flags:       map[string][]string{"output": {"json"}, "dry-run": {""}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRemoveCommand dry-run: %v", err)
		}
	})
	if !called {
		t.Fatalf("expected removeByIDPhase to be called in dry-run path")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["command"]; got != "remove" {
		t.Fatalf("expected command=remove, got %v", got)
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
	if got := item["message"]; got != "would remove" {
		t.Fatalf("expected message would remove, got %v", got)
	}
}

func TestRemoveByIDEngineRoutingText(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origPhase := removeByIDPhase
	called := false
	removeByIDPhase = func(_ *storage.StorageContext, fileID int64, dryRun bool) batch.ItemResult {
		called = true
		if fileID != 42 {
			t.Fatalf("expected fileID 42, got %d", fileID)
		}
		if dryRun {
			t.Fatalf("expected dryRun=false in live remove routing test")
		}
		return batch.ItemResult{ID: fileID, Status: batch.ResultSuccess, Message: "removed mappings=3"}
	}
	t.Cleanup(func() { removeByIDPhase = origPhase })

	output := captureStdout(t, func() {
		err := runRemoveCommand(parsedCommandLine{
			method:      "remove",
			positionals: []string{"42"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runRemoveCommand: %v", err)
		}
	})

	if !called {
		t.Fatalf("expected removeByIDPhase to be called")
	}
	if !strings.Contains(output, "removed mappings=3") {
		t.Fatalf("expected remove text output to contain routed message, got output:\n%s", output)
	}
}
