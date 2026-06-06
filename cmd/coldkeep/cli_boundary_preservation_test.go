package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

func TestRunSnapshotCommandCreatePreservesDirectSnapshotOwnership(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalCreate := createSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		createSnapshotPhase = originalCreate
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	called := false
	createSnapshotPhase = func(_ context.Context, _ *sql.DB, opts snapshot.SnapshotCreateOptions) error {
		called = true
		if opts.ID != "snap-cli-owned" {
			t.Fatalf("expected forwarded snapshot ID, got %q", opts.ID)
		}
		if opts.Type != "partial" {
			t.Fatalf("expected partial snapshot type, got %q", opts.Type)
		}
		if len(opts.Paths) != 1 || opts.Paths[0] != "docs/" {
			t.Fatalf("expected direct snapshot create paths, got %v", opts.Paths)
		}
		return nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"create", "docs/"},
			flags: map[string][]string{
				"id":     {"snap-cli-owned"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand create: %v", err)
		}
	})

	if !called {
		t.Fatal("expected snapshot create to preserve direct CLI/snapshot ownership")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse create JSON output: %v output=%q", err, output)
	}
	if got, _ := payload["command"].(string); got != "snapshot" {
		t.Fatalf("expected snapshot command payload, got %v", payload)
	}
	data, _ := payload["data"].(map[string]any)
	if got, _ := data["snapshot_id"].(string); got != "snap-cli-owned" {
		t.Fatalf("expected snapshot_id=snap-cli-owned, got %v", data)
	}
	if got, _ := data["type"].(string); got != "partial" {
		t.Fatalf("expected type=partial, got %v", data)
	}
}

func TestRunSnapshotCommandDeletePreservesDirectSnapshotOwnership(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDelete := deleteSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		deleteSnapshotPhase = originalDelete
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	called := false
	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) error {
		called = true
		if snapshotID != "snap-delete-owned" {
			t.Fatalf("expected forwarded snapshot ID, got %q", snapshotID)
		}
		return nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "snap-delete-owned"},
			flags: map[string][]string{
				"force":  {""},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete: %v", err)
		}
	})

	if !called {
		t.Fatal("expected snapshot delete to preserve direct CLI/snapshot ownership")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse delete JSON output: %v output=%q", err, output)
	}
	data, _ := payload["data"].(map[string]any)
	if got, _ := data["action"].(string); got != "delete" {
		t.Fatalf("expected action=delete, got %v", data)
	}
	if got, _ := data["dry_run"].(bool); got {
		t.Fatalf("expected dry_run=false, got %v", data)
	}
}

func TestRunSnapshotCommandRestorePreservesDirectSnapshotOwnership(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalRestore := restoreSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		restoreSnapshotPhase = originalRestore
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	called := false
	restoreSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string, paths []string, opts snapshot.RestoreSnapshotOptions) (*snapshot.RestoreSnapshotResult, error) {
		called = true
		if snapshotID != "snap-restore-owned" {
			t.Fatalf("expected forwarded snapshot ID, got %q", snapshotID)
		}
		if len(paths) != 1 || paths[0] != "docs/" {
			t.Fatalf("expected direct snapshot restore paths, got %v", paths)
		}
		if opts.Query == nil || len(opts.Query.ExactPaths) != 1 {
			t.Fatalf("expected snapshot restore query to stay CLI/snapshot-owned, got %+v", opts.Query)
		}
		if _, ok := opts.Query.ExactPaths["docs/a.txt"]; !ok {
			t.Fatalf("expected normalized exact path docs/a.txt, got %+v", opts.Query.ExactPaths)
		}
		return &snapshot.RestoreSnapshotResult{
			SnapshotID:     snapshotID,
			RestoredFiles:  1,
			RequestedPaths: int64(len(paths)),
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-restore-owned", "docs/"},
			flags: map[string][]string{
				"path":   {"./docs/a.txt"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand restore: %v", err)
		}
	})

	if !called {
		t.Fatal("expected snapshot restore to preserve direct CLI/snapshot ownership")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse restore JSON output: %v output=%q", err, output)
	}
	data, _ := payload["data"].(map[string]any)
	if got, _ := data["action"].(string); got != "restore" {
		t.Fatalf("expected action=restore, got %v", data)
	}
}

func TestRunRepairCommandPreservesDirectMaintenanceOwnership(t *testing.T) {
	originalLogical := repairLogicalRefCountsPhase
	originalChunk := repairChunkLiveRefCountsPhase
	t.Cleanup(func() {
		repairLogicalRefCountsPhase = originalLogical
		repairChunkLiveRefCountsPhase = originalChunk
	})

	logicalCalled := false
	chunkCalled := false
	repairLogicalRefCountsPhase = func() (maintenance.RepairLogicalRefCountsResult, error) {
		logicalCalled = true
		return maintenance.RepairLogicalRefCountsResult{
			ScannedLogicalFiles: 4,
			UpdatedLogicalFiles: 1,
		}, nil
	}
	repairChunkLiveRefCountsPhase = func() (maintenance.RepairChunkLiveRefCountsResult, error) {
		chunkCalled = true
		return maintenance.RepairChunkLiveRefCountsResult{}, nil
	}

	output := captureStdout(t, func() {
		err := runRepairCommand(parsedCommandLine{
			method:      "repair",
			positionals: []string{"ref-counts"},
			flags: map[string][]string{
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRepairCommand: %v", err)
		}
	})

	if !logicalCalled {
		t.Fatal("expected repair ref-counts to preserve direct maintenance ownership")
	}
	if chunkCalled {
		t.Fatal("repair ref-counts should not invoke chunk-live-ref-count maintenance phase")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse repair JSON output: %v output=%q", err, output)
	}
	data, _ := payload["data"].(map[string]any)
	if got, _ := data["target"].(string); got != "ref-counts" {
		t.Fatalf("expected target=ref-counts, got %v", data)
	}
}

func TestRunDoctorCommandPreservesDirectRecoveryOwnership(t *testing.T) {
	originalRecovery := doctorRecoveryPhase
	originalSchema := doctorSchemaVersionPhase
	originalVerify := doctorVerifyPhase
	t.Cleanup(func() {
		doctorRecoveryPhase = originalRecovery
		doctorSchemaVersionPhase = originalSchema
		doctorVerifyPhase = originalVerify
	})

	recoveryCalled := false
	schemaCalled := false
	verifyCalled := false
	doctorRecoveryPhase = func(string) (recovery.Report, error) {
		recoveryCalled = true
		return recovery.Report{}, errors.New("recovery unavailable")
	}
	doctorSchemaVersionPhase = func() (int64, error) {
		schemaCalled = true
		return 5, nil
	}
	doctorVerifyPhase = func(string, string, int, verify.VerifyLevel) error {
		verifyCalled = true
		return nil
	}

	err := runDoctorCommand(parsedCommandLine{method: "doctor", flags: map[string][]string{}}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "doctor recovery phase failed") {
		t.Fatalf("expected doctor recovery failure, got %v", err)
	}
	if got := classifyExitCode(err); got != exitRecovery {
		t.Fatalf("expected recovery exit code %d, got %d", exitRecovery, got)
	}
	if !recoveryCalled {
		t.Fatal("expected doctor to preserve direct recovery ownership")
	}
	if schemaCalled {
		t.Fatal("schema phase should not run after recovery failure")
	}
	if verifyCalled {
		t.Fatal("verify phase should not run after recovery failure")
	}
}
