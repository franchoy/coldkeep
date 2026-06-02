package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRemoveByIDEngineRoutingJSON(t *testing.T) {
	injectRemoveRoutingDB(t)
	called := stubRemoveByIDPhase(t, 42, false, batch.ItemResult{
		ID:      42,
		Status:  batch.ResultSuccess,
		Message: "removed mappings=3",
	})

	payload := runRemoveJSON(t, []string{"42"}, map[string][]string{"output": {"json"}})

	assertRemovePhaseCalled(t, called, "expected removeByIDPhase to be called")
	assertRemoveJSONEnvelope(t, payload, false)
	assertRemoveJSONResult(t, payload, "success", "removed mappings=3")
}

func TestRemoveByIDDryRunEngineRoutingJSON(t *testing.T) {
	injectRemoveRoutingDB(t)
	called := stubRemoveByIDPhase(t, 52, true, batch.ItemResult{
		ID:      52,
		Status:  batch.ResultPlanned,
		Message: "would remove",
	})

	payload := runRemoveJSON(t, []string{"52"}, map[string][]string{
		"output":  {"json"},
		"dry-run": {""},
	})

	assertRemovePhaseCalled(t, called, "expected removeByIDPhase to be called in dry-run path")
	assertRemoveJSONEnvelope(t, payload, true)
	assertRemoveJSONResult(t, payload, "planned", "would remove")
}

func TestRemoveByIDEngineRoutingText(t *testing.T) {
	injectRemoveRoutingDB(t)
	called := stubRemoveByIDPhase(t, 42, false, batch.ItemResult{
		ID:      42,
		Status:  batch.ResultSuccess,
		Message: "removed mappings=3",
	})

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

	assertRemovePhaseCalled(t, called, "expected removeByIDPhase to be called")
	if !strings.Contains(output, "removed mappings=3") {
		t.Fatalf("expected remove text output to contain routed message, got output:\n%s", output)
	}
}

func injectRemoveRoutingDB(t *testing.T) {
	t.Helper()
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })
}

func stubRemoveByIDPhase(
	t *testing.T,
	expectedFileID int64,
	expectedDryRun bool,
	result batch.ItemResult,
) *bool {
	t.Helper()
	origPhase := removeByIDPhase
	called := false
	removeByIDPhase = func(_ *storage.StorageContext, fileID int64, dryRun bool) batch.ItemResult {
		called = true
		if fileID != expectedFileID {
			t.Fatalf("expected fileID %d, got %d", expectedFileID, fileID)
		}
		if dryRun != expectedDryRun {
			t.Fatalf("expected dryRun=%v, got %v", expectedDryRun, dryRun)
		}
		return result
	}
	t.Cleanup(func() { removeByIDPhase = origPhase })
	return &called
}

func runRemoveJSON(t *testing.T, positionals []string, flags map[string][]string) map[string]any {
	t.Helper()
	output := captureStdout(t, func() {
		err := runRemoveCommand(parsedCommandLine{
			method:      "remove",
			positionals: positionals,
			flags:       flags,
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRemoveCommand: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	return payload
}

func assertRemovePhaseCalled(t *testing.T, called *bool, message string) {
	t.Helper()
	if !*called {
		t.Fatalf("%s", message)
	}
}

func assertRemoveJSONEnvelope(t *testing.T, payload map[string]any, dryRun bool) {
	t.Helper()
	if got := payload["command"]; got != "remove" {
		t.Fatalf("expected command=remove, got %v", got)
	}
	if dryRun {
		assertRemoveJSONDryRun(t, payload)
	}
}

func assertRemoveJSONDryRun(t *testing.T, payload map[string]any) {
	t.Helper()
	if got := payload["dry_run"]; got != true {
		t.Fatalf("expected dry_run=true, got %v", got)
	}
}

func assertRemoveJSONResult(t *testing.T, payload map[string]any, status string, message string) {
	t.Helper()
	results := requireSingleRemoveJSONResult(t, payload)
	item, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("expected result item object, got %T", results[0])
	}
	if got := item["status"]; got != status {
		t.Fatalf("expected item status %s, got %v", status, got)
	}
	if got := item["message"]; got != message {
		t.Fatalf("expected message %s, got %v", message, got)
	}
}

func requireSingleRemoveJSONResult(t *testing.T, payload map[string]any) []any {
	t.Helper()
	results, ok := payload["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("expected one JSON result item, got %T len=%d", payload["results"], len(results))
	}
	return results
}
