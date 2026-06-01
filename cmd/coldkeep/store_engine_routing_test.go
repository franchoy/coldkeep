package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage"
)

func TestStoreByFileEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origPhase := storeByFilePhase
	called := false
	storeByFilePhase = func(_ *storage.StorageContext, path, codecName string) (storage.StoreFileResult, error) {
		called = true
		if !strings.HasSuffix(path, ".txt") {
			t.Fatalf("expected .txt path, got %q", path)
		}
		if codecName != "" {
			t.Fatalf("expected default codec name empty, got %q", codecName)
		}
		return storage.StoreFileResult{
			FileID:        77,
			FileHash:      "deadbeef",
			Path:          path,
			AlreadyStored: false,
		}, nil
	}
	t.Cleanup(func() { storeByFilePhase = origPhase })

	inPath := filepath.Join(t.TempDir(), "routed.txt")
	output := captureStdout(t, func() {
		err := runStoreCommand(parsedCommandLine{method: "store", positionals: []string{inPath}}, outputModeJSON)
		if err != nil {
			t.Fatalf("runStoreCommand: %v", err)
		}
	})
	if !called {
		t.Fatalf("expected storeByFilePhase to be called")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["command"]; got != "store" {
		t.Fatalf("expected command=store, got %v", got)
	}
	if got := payload["status"]; got != "ok" {
		t.Fatalf("expected status=ok, got %v", got)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object, got %T", payload["data"])
	}
	if got := data["file_id"]; int(got.(float64)) != 77 {
		t.Fatalf("expected file_id=77, got %v", got)
	}
	if got := data["file_hash"]; got != "deadbeef" {
		t.Fatalf("expected file_hash deadbeef, got %v", got)
	}
}
