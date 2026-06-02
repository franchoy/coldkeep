package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage"
)

type restoreRoutingOptions struct {
	fileID     int64
	dryRun     bool
	outputName string
	hash       string
}

func TestRestoreByIDEngineRoutingJSON(t *testing.T) {
	called := stubRestoreByIDRouting(t, restoreRoutingOptions{
		fileID:     42,
		outputName: "routed.txt",
		hash:       "abc123",
	})

	payload := runRestoreJSONCommand(t, "42", false)

	assertRestorePhaseCalled(t, called, "live path")
	assertRestoreJSONCommand(t, payload)
	item := singleRestoreJSONResult(t, payload)
	assertRestoreJSONItemStatus(t, item, "success")
	assertRestoreJSONFieldContains(t, item, "output_path", "routed.txt")
}

func TestRestoreByIDDryRunEngineRoutingJSON(t *testing.T) {
	called := stubRestoreByIDRouting(t, restoreRoutingOptions{
		fileID:     52,
		dryRun:     true,
		outputName: "dry-run-routed.txt",
	})

	payload := runRestoreJSONCommand(t, "52", true)

	assertRestorePhaseCalled(t, called, "dry-run path")
	assertRestoreJSONCommand(t, payload)
	assertRestoreJSONDryRun(t, payload)
	item := singleRestoreJSONResult(t, payload)
	assertRestoreJSONItemStatus(t, item, "planned")
	assertRestoreJSONFieldContains(t, item, "message", "would restore ->")
	assertRestoreJSONFieldContains(t, item, "output_path", "dry-run-routed.txt")
}

func TestRestoreByIDEngineRoutingText(t *testing.T) {
	called := stubRestoreByIDRouting(t, restoreRoutingOptions{
		fileID:     42,
		outputName: "routed.txt",
		hash:       "abc123",
	})

	outputDir := t.TempDir()
	output := captureStdout(t, func() {
		err := runRestoreCommand(parsedCommandLine{
			method:      "restore",
			positionals: []string{"42", outputDir},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runRestoreCommand: %v", err)
		}
	})

	assertRestorePhaseCalled(t, called, "text path")
	for _, want := range []string{"routed.txt", "Summary:", "Hint: " + doctorOperationalHint} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected restore text output to contain %q, got output:\n%s", want, output)
		}
	}
}

func stubRestoreByIDRouting(t *testing.T, opts restoreRoutingOptions) *bool {
	t.Helper()
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origPhase := restoreByIDPhase
	called := false
	restoreByIDPhase = func(_ *storage.StorageContext, fileID int64, outputDir string, overwrite bool, dryRun bool) (storage.RestoreFileResult, error) {
		t.Helper()
		called = true
		assertRestoreRoutingArgs(t, opts, fileID, overwrite, dryRun)

		out := filepath.Join(outputDir, opts.outputName)
		return storage.RestoreFileResult{
			FileID:       fileID,
			OriginalName: opts.outputName,
			OutputPath:   out,
			RestoredHash: opts.hash,
		}, nil
	}
	t.Cleanup(func() { restoreByIDPhase = origPhase })
	return &called
}

func assertRestoreRoutingArgs(t *testing.T, opts restoreRoutingOptions, fileID int64, overwrite bool, dryRun bool) {
	t.Helper()
	if fileID != opts.fileID {
		t.Fatalf("expected fileID %d, got %d", opts.fileID, fileID)
	}
	if overwrite {
		t.Fatalf("expected overwrite=false")
	}
	if dryRun != opts.dryRun {
		t.Fatalf("expected dryRun=%v, got %v", opts.dryRun, dryRun)
	}
}

func runRestoreJSONCommand(t *testing.T, fileID string, dryRun bool) map[string]any {
	t.Helper()
	outputDir := t.TempDir()
	flags := map[string][]string{"output": {"json"}}
	if dryRun {
		flags["dry-run"] = []string{""}
	}

	output := captureStdout(t, func() {
		err := runRestoreCommand(parsedCommandLine{
			method:      "restore",
			positionals: []string{fileID, outputDir},
			flags:       flags,
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRestoreCommand: %v", err)
		}
	})
	return parseRestoreJSONPayload(t, output)
}

func parseRestoreJSONPayload(t *testing.T, output string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	return payload
}

func assertRestorePhaseCalled(t *testing.T, called *bool, path string) {
	t.Helper()
	if !*called {
		t.Fatalf("expected restoreByIDPhase to be called in %s", path)
	}
}

func assertRestoreJSONCommand(t *testing.T, payload map[string]any) {
	t.Helper()
	if got := payload["command"]; got != "restore" {
		t.Fatalf("expected command=restore, got %v", got)
	}
}

func assertRestoreJSONDryRun(t *testing.T, payload map[string]any) {
	t.Helper()
	if got := payload["dry_run"]; got != true {
		t.Fatalf("expected dry_run=true, got %v", got)
	}
}

func singleRestoreJSONResult(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	results, ok := payload["results"].([]any)
	if !ok {
		t.Fatalf("expected JSON results array, got %T", payload["results"])
	}
	if len(results) != 1 {
		t.Fatalf("expected one JSON result item, got len=%d", len(results))
	}

	item, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("expected result item object, got %T", results[0])
	}
	return item
}

func assertRestoreJSONItemStatus(t *testing.T, item map[string]any, want string) {
	t.Helper()
	if got := item["status"]; got != want {
		t.Fatalf("expected item status %s, got %v", want, got)
	}
}

func assertRestoreJSONFieldContains(t *testing.T, item map[string]any, field string, want string) {
	t.Helper()
	got, ok := item[field].(string)
	if !ok {
		t.Fatalf("expected %s string field, got %T: %v", field, item[field], item[field])
	}
	if !strings.Contains(got, want) {
		t.Fatalf("expected %s to contain %q, got %v", field, want, got)
	}
}
