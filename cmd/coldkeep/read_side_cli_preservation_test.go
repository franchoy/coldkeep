package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestReadSideCLIJSONDoesNotExposeTaxonomyFields(t *testing.T) {
	t.Run("stats success", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"stats", "--json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertJSONEnvelopeShape(t, payload, "stats")
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("inspect success", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"inspect", "chunk", "7", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertJSONEnvelopeShape(t, payload, "inspect")
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("snapshot show success", func(t *testing.T) {
		installSnapshotShowCLISuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "show", "snap-preserve", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertPayloadString(t, payload, "status", "ok")
		assertPayloadString(t, payload, "command", "snapshot")
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected snapshot show data object, got %T", payload["data"])
		}
		assertPayloadString(t, data, "action", "show")
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("snapshot diff success", func(t *testing.T) {
		installSnapshotDiffCLIDetailedSuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "diff", "base-preserve", "target-preserve", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertPayloadString(t, payload, "status", "ok")
		assertPayloadString(t, payload, "command", "snapshot diff")
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected snapshot diff data object, got %T", payload["data"])
		}
		if _, ok := data["entries"].([]any); !ok {
			t.Fatalf("expected detailed diff entries, got %T", data["entries"])
		}
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("stats validation error", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"stats", "--json", "--output", "human"})
		if code != exitUsage {
			t.Fatalf("expected exitUsage, got %d stderr=%q", code, stderr)
		}
		if strings.TrimSpace(stdout) != "" {
			t.Fatalf("expected empty stdout for JSON usage error, got %q", stdout)
		}

		payload := lastJSONStderrPayload(t, stderr)
		message, _ := payload["message"].(string)
		assertNoTaxonomyLeak(t, payload, message)
	})
}

func TestReadSideInspectCLIErrorPreservation(t *testing.T) {
	t.Run("unsupported entity remains usage error", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"inspect", "blob", "--json"})
		if code != exitUsage {
			t.Fatalf("expected exitUsage, got %d stderr=%q", code, stderr)
		}
		if strings.TrimSpace(stdout) != "" {
			t.Fatalf("expected empty stdout, got %q", stdout)
		}

		payload := lastJSONStderrPayload(t, stderr)
		assertPayloadString(t, payload, "status", "error")
		assertPayloadString(t, payload, "error_class", "USAGE")
		assertPayloadExitCode(t, payload, exitUsage)
		if got, _ := payload["message"].(string); !strings.Contains(got, `unsupported inspect entity "blob"`) {
			t.Fatalf("expected unsupported inspect entity message, got payload=%v", payload)
		}
		errorNode := assertPayloadErrorNode(t, payload)
		assertNestedErrorString(t, payload, errorNode, "code", "INVALID_ARGUMENT")
		message, _ := payload["message"].(string)
		assertNoTaxonomyLeak(t, payload, message)
	})

	t.Run("missing valid entity remains not found", func(t *testing.T) {
		installNoopStartupRecovery(t)

		originalInspect := runObservabilityInspectPhase
		t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })
		runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
			return nil, fmt.Errorf("lookup chunk %s: %w", id, observability.ErrNotFound)
		}

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"inspect", "chunk", "999", "--json"})
		if code != exitGeneral {
			t.Fatalf("expected exitGeneral, got %d stderr=%q", code, stderr)
		}
		if strings.TrimSpace(stdout) != "" {
			t.Fatalf("expected empty stdout, got %q", stdout)
		}

		payload := lastJSONStderrPayload(t, stderr)
		assertPayloadString(t, payload, "status", "error")
		assertPayloadString(t, payload, "error_class", "GENERAL")
		assertPayloadExitCode(t, payload, exitGeneral)
		errorNode := assertPayloadErrorNode(t, payload)
		assertNestedErrorString(t, payload, errorNode, "code", "NOT_FOUND")
		assertNestedErrorString(t, payload, errorNode, "message", "chunk 999 not found")
		assertPayloadString(t, payload, "message", "chunk 999 not found")
		message, _ := payload["message"].(string)
		assertNoTaxonomyLeak(t, payload, message)
	})
}

func TestReadSideSnapshotShowCLIPreservation(t *testing.T) {
	t.Run("json success preserves legacy envelope and counts", func(t *testing.T) {
		installSnapshotShowCLISuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "show", "snap-preserve", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertPayloadString(t, payload, "status", "ok")
		assertPayloadString(t, payload, "command", "snapshot")
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %T", payload["data"])
		}
		assertPayloadString(t, data, "action", "show")
		assertJSONNumber(t, data, "file_count", 1)
		assertJSONNumber(t, data, "matched_file_count", 1)
		assertJSONNumber(t, data, "total_snapshot_file_count", 3)
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("missing snapshot id remains usage error", func(t *testing.T) {
		installNoopStartupRecovery(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "show", "--output", "json"})
		if code != exitUsage {
			t.Fatalf("expected exitUsage, got %d stderr=%q", code, stderr)
		}
		if strings.TrimSpace(stdout) != "" {
			t.Fatalf("expected empty stdout, got %q", stdout)
		}

		payload := lastJSONStderrPayload(t, stderr)
		assertPayloadString(t, payload, "status", "error")
		assertPayloadString(t, payload, "error_class", "USAGE")
		assertPayloadExitCode(t, payload, exitUsage)
		message, _ := payload["message"].(string)
		if !strings.Contains(message, "Usage: coldkeep snapshot show") {
			t.Fatalf("expected snapshot show usage message, got payload=%v", payload)
		}
		assertNoTaxonomyLeak(t, payload, message)
	})

	t.Run("missing valid snapshot remains domain/general error", func(t *testing.T) {
		installSnapshotShowCLIMissingSnapshotStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "show", "missing-snap", "--output", "json"})
		if code != exitGeneral {
			t.Fatalf("expected exitGeneral, got %d stderr=%q", code, stderr)
		}
		if strings.TrimSpace(stdout) != "" {
			t.Fatalf("expected empty stdout, got %q", stdout)
		}

		payload := lastJSONStderrPayload(t, stderr)
		message := assertDomainErrorEnvelope(t, payload, `snapshot "missing-snap" not found`)
		assertNoTaxonomyLeak(t, payload, message)
	})
}

func TestReadSideSnapshotDiffCLIPreservation(t *testing.T) {
	t.Run("detailed json preserves envelope", func(t *testing.T) {
		installSnapshotDiffCLIDetailedSuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "diff", "base-preserve", "target-preserve", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertPayloadString(t, payload, "status", "ok")
		assertPayloadString(t, payload, "command", "snapshot diff")
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %T", payload["data"])
		}
		if _, ok := data["entries"].([]any); !ok {
			t.Fatalf("expected entries array, got %T", data["entries"])
		}
		assertJSONNumber(t, data, "entry_count", 2)
		assertJSONNumber(t, data, "matched_entry_count", 2)
		assertJSONNumber(t, data, "total_diff_entry_count", 2)
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("summary json omits entries", func(t *testing.T) {
		installSnapshotDiffCLISummarySuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "diff", "base-preserve", "target-preserve", "--summary", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %T", payload["data"])
		}
		if _, exists := data["entries"]; exists {
			t.Fatalf("did not expect entries in summary mode, got data=%v", data)
		}
		if summaryMode, ok := data["summary_mode"].(bool); !ok || !summaryMode {
			t.Fatalf("expected summary_mode=true, got data=%v", data)
		}
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("invalid regex remains usage error", func(t *testing.T) {
		installNoopStartupRecovery(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "diff", "base", "target", "--regex", "[unclosed", "--output", "json"})
		if code != exitUsage {
			t.Fatalf("expected exitUsage, got %d stderr=%q", code, stderr)
		}
		if strings.TrimSpace(stdout) != "" {
			t.Fatalf("expected empty stdout, got %q", stdout)
		}

		payload := lastJSONStderrPayload(t, stderr)
		assertPayloadString(t, payload, "status", "error")
		assertPayloadString(t, payload, "error_class", "USAGE")
		assertPayloadExitCode(t, payload, exitUsage)
		message, _ := payload["message"].(string)
		if !strings.Contains(message, "invalid --regex value") {
			t.Fatalf("expected invalid regex message, got payload=%v", payload)
		}
		errorNode := assertPayloadErrorNode(t, payload)
		assertNestedErrorString(t, payload, errorNode, "code", "INVALID_ARGUMENT")
		assertNoTaxonomyLeak(t, payload, message)
	})
}

func installNoopStartupRecovery(t *testing.T) {
	t.Helper()

	originalStartupRecovery := startupRecoveryPhase
	t.Cleanup(func() { startupRecoveryPhase = originalStartupRecovery })
	startupRecoveryPhase = func(string) (recovery.Report, error) {
		return recovery.Report{}, nil
	}
}

func installSnapshotShowCLISuccessStubs(t *testing.T) {
	t.Helper()

	installNoopStartupRecovery(t)

	originalLoad := loadDefaultStorageContextPhase
	originalGet := getSnapshotPhase
	originalListFiles := listSnapshotFilesPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		getSnapshotPhase = originalGet
		listSnapshotFilesPhase = originalListFiles
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	getSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.Snapshot, error) {
		return &snapshot.Snapshot{
			ID:        snapshotID,
			Type:      "full",
			Label:     sql.NullString{String: "preserve-me", Valid: true},
			CreatedAt: time.Date(2026, time.April, 10, 12, 0, 0, 0, time.UTC),
		}, nil
	}
	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, _ string, _ int, _ *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		return []snapshot.SnapshotFileEntry{{Path: "docs/a.txt"}}, nil
	}
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{
			SnapshotID:       snapshotID,
			SnapshotCount:    1,
			SnapshotFileCount: 3,
			TotalSizeBytes:   123,
		}, nil
	}
}

func installSnapshotShowCLIMissingSnapshotStubs(t *testing.T) {
	t.Helper()

	installNoopStartupRecovery(t)

	originalLoad := loadDefaultStorageContextPhase
	originalGet := getSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		getSnapshotPhase = originalGet
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	getSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.Snapshot, error) {
		return nil, fmt.Errorf("snapshot %q not found", snapshotID)
	}
}

func installSnapshotDiffCLIDetailedSuccessStubs(t *testing.T) {
	t.Helper()

	installNoopStartupRecovery(t)

	originalLoad := loadDefaultStorageContextPhase
	originalDiff := diffSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		diffSnapshotsPhase = originalDiff
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	diffSnapshotsPhase = func(_ context.Context, _ *sql.DB, baseID, targetID string, _ *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
		return &snapshot.SnapshotDiffResult{
			BaseSnapshotID:   baseID,
			TargetSnapshotID: targetID,
			Entries: []snapshot.SnapshotDiffEntry{
				{Path: "docs/added.txt", Type: snapshot.DiffAdded},
				{Path: "docs/removed.txt", Type: snapshot.DiffRemoved},
			},
			Summary: snapshot.SnapshotDiffSummary{Added: 1, Removed: 1, Modified: 0},
		}, nil
	}
}

func installSnapshotDiffCLISummarySuccessStubs(t *testing.T) {
	t.Helper()

	installNoopStartupRecovery(t)

	originalLoad := loadDefaultStorageContextPhase
	originalDiff := diffSnapshotsPhase
	originalSummary := diffSnapshotSummaryPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		diffSnapshotsPhase = originalDiff
		diffSnapshotSummaryPhase = originalSummary
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}
	diffSnapshotsPhase = func(_ context.Context, _ *sql.DB, baseID, targetID string, _ *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
		return &snapshot.SnapshotDiffResult{
			BaseSnapshotID:   baseID,
			TargetSnapshotID: targetID,
			Entries: []snapshot.SnapshotDiffEntry{
				{Path: "docs/ignored.txt", Type: snapshot.DiffAdded},
			},
			Summary: snapshot.SnapshotDiffSummary{Added: 1, Removed: 0, Modified: 0},
		}, nil
	}
	diffSnapshotSummaryPhase = func(_ context.Context, _ *sql.DB, _, _ string) (*snapshot.SnapshotDiffSummary, error) {
		return &snapshot.SnapshotDiffSummary{Added: 1, Removed: 1, Modified: 0}, nil
	}
}

func lastJSONStderrPayload(t *testing.T, stderr string) map[string]any {
	t.Helper()

	payloads := assertEveryLineIsJSONObject(t, stderr)
	if len(payloads) == 0 {
		t.Fatal("expected at least one JSON object on stderr")
	}
	return payloads[len(payloads)-1]
}

func assertReadSideSuccessPayloadNoTaxonomyLeak(t *testing.T, payload map[string]any) {
	t.Helper()

	assertNoTaxonomyLeakKeys(t, payload)
	assertNoTaxonomyLeakEncodedPayload(t, payload)
}
