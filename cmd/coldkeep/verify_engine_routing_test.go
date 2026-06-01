package main

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

func TestVerifySystemEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)

	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origVerify := verifyCommandPhase
	verifyCalled := false
	verifyCommandPhase = func(_ *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
		verifyCalled = true
		if target != "system" {
			t.Fatalf("expected target system, got %q", target)
		}
		if fileID != 0 {
			t.Fatalf("expected fileID 0 for system verify, got %d", fileID)
		}
		if level != verify.VerifyFast {
			t.Fatalf("expected fast verify level, got %v", level)
		}
		return nil
	}
	t.Cleanup(func() { verifyCommandPhase = origVerify })

	origSummary := verifySummaryPhase
	summaryCalled := false
	verifySummaryPhase = func(_ *sql.DB, target string, fileID int64) (verifyOutputSummary, error) {
		summaryCalled = true
		if target != "system" {
			t.Fatalf("expected summary target system, got %q", target)
		}
		if fileID != 0 {
			t.Fatalf("expected summary fileID 0 for system verify, got %d", fileID)
		}
		return verifyOutputSummary{
			BlocksChecked:           10,
			PhysicalHashChecked:     7,
			CompressedHashChecked:   5,
			LogicalHashChecked:      10,
			CompressedBlocksChecked: 3,
		}, nil
	}
	t.Cleanup(func() { verifySummaryPhase = origSummary })

	output := captureStdout(t, func() {
		err := runVerifyCommand(parsedCommandLine{
			method:      "verify",
			positionals: []string{"system"},
			flags:       map[string][]string{"output": {"json"}, "fast": {""}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runVerifyCommand: %v", err)
		}
	})

	if !verifyCalled {
		t.Fatalf("expected verifyCommandPhase to be called")
	}
	if !summaryCalled {
		t.Fatalf("expected verifySummaryPhase to be called")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["command"]; got != "verify" {
		t.Fatalf("expected command=verify, got %v", got)
	}
	if got := payload["target"]; got != "system" {
		t.Fatalf("expected target system, got %v", got)
	}
	if got := payload["verify"]; got != "ok" {
		t.Fatalf("expected verify=ok, got %v", got)
	}
	if got := int(payload["blocks_checked"].(float64)); got != 10 {
		t.Fatalf("expected blocks_checked 10, got %d", got)
	}
}

func TestVerifyFileEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)

	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origVerify := verifyCommandPhase
	verifyCalled := false
	verifyCommandPhase = func(_ *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
		verifyCalled = true
		if target != "file" {
			t.Fatalf("expected target file, got %q", target)
		}
		if fileID != 42 {
			t.Fatalf("expected fileID 42 for file verify, got %d", fileID)
		}
		if level != verify.VerifyStandard {
			t.Fatalf("expected standard verify level, got %v", level)
		}
		return nil
	}
	t.Cleanup(func() { verifyCommandPhase = origVerify })

	origSummary := verifySummaryPhase
	summaryCalled := false
	verifySummaryPhase = func(_ *sql.DB, target string, fileID int64) (verifyOutputSummary, error) {
		summaryCalled = true
		if target != "file" {
			t.Fatalf("expected summary target file, got %q", target)
		}
		if fileID != 42 {
			t.Fatalf("expected summary fileID 42 for file verify, got %d", fileID)
		}
		return verifyOutputSummary{BlocksChecked: 1}, nil
	}
	t.Cleanup(func() { verifySummaryPhase = origSummary })

	output := captureStdout(t, func() {
		err := runVerifyCommand(parsedCommandLine{
			method:      "verify",
			positionals: []string{"file", "42"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runVerifyCommand: %v", err)
		}
	})

	if !verifyCalled {
		t.Fatalf("expected verifyCommandPhase to be called")
	}
	if !summaryCalled {
		t.Fatalf("expected verifySummaryPhase to be called")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["target"]; got != "file" {
		t.Fatalf("expected target file, got %v", got)
	}
	if got := int(payload["file_id"].(float64)); got != 42 {
		t.Fatalf("expected file_id 42, got %d", got)
	}
}
