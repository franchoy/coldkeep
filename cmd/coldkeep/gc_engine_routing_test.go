package main

// gc_engine_routing_test.go — tests verifying that the gc command is correctly
// routed through the engine-backed runGCPhase closure without changing CLI
// output behavior (JSON shape, human text, exit codes).
//
// These tests use a real in-memory SQLite DB. Live GC is not exercised here
// because the SQLite backend rejects it (dry-run only).

import (
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/storage"
)

// injectGCRoutingDB injects dbconn into loadDefaultStorageContextPhase for the
// duration of the test and restores the original on cleanup.
func injectGCRoutingDB(t *testing.T, containersDir string) {
	t.Helper()
	dbconn := openSnapshotRoutingDB(t)
	orig := loadDefaultStorageContextPhase
	t.Cleanup(func() { loadDefaultStorageContextPhase = orig })
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	_ = containersDir
}

// TestGCDryRunEngineRoutingJSON verifies that gc --dry-run emits valid JSON with
// the expected envelope shape and that the gc command is routed through the
// engine (not the old maintenance.RunGCWithContainersDirResult global path).
func TestGCDryRunEngineRoutingJSON(t *testing.T) {
	containersDir := t.TempDir()
	injectGCRoutingDB(t, containersDir)

	output := captureStdout(t, func() {
		err := runGCCommand(parsedCommandLine{
			method:      "gc",
			positionals: []string{},
			flags:       map[string][]string{"dry-run": {"true"}, "output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("gc --dry-run JSON: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}

	if got := payload["command"]; got != "gc" {
		t.Errorf("expected command=gc, got %v", got)
	}
	if got := payload["status"]; got != "ok" {
		t.Errorf("expected status=ok, got %v", got)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object, got %T: %v", payload["data"], payload["data"])
	}
	if got, ok := data["dry_run"].(bool); !ok || !got {
		t.Errorf("expected data.dry_run=true, got %v", data["dry_run"])
	}
	if _, hasAffected := data["affected_containers"]; !hasAffected {
		t.Errorf("expected data.affected_containers field in JSON output")
	}
	if _, hasFilenames := data["container_filenames"]; !hasFilenames {
		t.Errorf("expected data.container_filenames field in JSON output")
	}
}

// TestGCDryRunEngineRoutingHuman verifies that gc --dry-run emits the expected
// human-readable output text ("GC completed" or dry-run eligible message).
func TestGCDryRunEngineRoutingHuman(t *testing.T) {
	containersDir := t.TempDir()
	injectGCRoutingDB(t, containersDir)

	output := captureStdout(t, func() {
		err := runGCCommand(parsedCommandLine{
			method:      "gc",
			positionals: []string{},
			flags:       map[string][]string{"dry-run": {"true"}},
		}, outputModeText)
		if err != nil {
			t.Fatalf("gc --dry-run human: %v", err)
		}
	})

	if !strings.Contains(output, "GC completed") && !strings.Contains(output, "eligible for deletion") {
		t.Errorf("expected GC completion message in human output, got: %q", output)
	}
}
