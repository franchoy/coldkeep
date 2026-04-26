package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

type singleChunkV2CLITestChunker struct{}

func (singleChunkV2CLITestChunker) Version() chunk.Version {
	return chunk.VersionV2FastCDC
}

func (singleChunkV2CLITestChunker) ChunkFile(path string) ([]chunk.Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []chunk.Result{{Data: data}}, nil
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	originalStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stderr pipe: %v", err)
	}
	os.Stderr = w

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}
	os.Stderr = originalStderr

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stderr output: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close read pipe: %v", err)
	}

	return buf.String()
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	originalStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	os.Stdout = w

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}
	os.Stdout = originalStdout

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stdout output: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close read pipe: %v", err)
	}

	return buf.String()
}

func TestEmitStartupRecoveryReportJSONSuccessSchema(t *testing.T) {
	report := recovery.Report{
		AbortedLogicalFiles:    2,
		AbortedChunks:          3,
		QuarantinedMissing:     4,
		QuarantinedCorruptTail: 5,
		QuarantinedOrphan:      6,
		CheckedContainerRecord: 7,
		CheckedDiskFiles:       11,
		SkippedDirEntries:      13,
	}

	output := captureStderr(t, func() {
		emitStartupRecoveryReport(outputModeJSON, report, nil)
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", err, output)
	}

	if got, ok := payload["event"].(string); !ok || got != "startup_recovery" {
		t.Fatalf("event mismatch: got=%v", payload["event"])
	}
	if got, ok := payload["status"].(string); !ok || got != "ok" {
		t.Fatalf("status mismatch: got=%v", payload["status"])
	}
	if _, exists := payload["message"]; exists {
		t.Fatalf("unexpected message field in success payload: %v", payload["message"])
	}

	assertJSONNumber(t, payload, "aborted_logical_files", 2)
	assertJSONNumber(t, payload, "aborted_chunks", 3)
	assertJSONNumber(t, payload, "quarantined_missing_containers", 4)
	assertJSONNumber(t, payload, "quarantined_corrupt_tail_containers", 5)
	assertJSONNumber(t, payload, "quarantined_orphan_containers", 6)
	assertJSONNumber(t, payload, "checked_container_records", 7)
	assertJSONNumber(t, payload, "checked_disk_files", 11)
	assertJSONNumber(t, payload, "skipped_dir_entries", 13)
}

func TestEmitStartupRecoveryReportJSONErrorSchema(t *testing.T) {
	report := recovery.Report{}
	recoveryErr := errors.New("db unavailable")

	output := captureStderr(t, func() {
		emitStartupRecoveryReport(outputModeJSON, report, recoveryErr)
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", err, output)
	}

	if got, ok := payload["event"].(string); !ok || got != "startup_recovery" {
		t.Fatalf("event mismatch: got=%v", payload["event"])
	}
	if got, ok := payload["status"].(string); !ok || got != "error" {
		t.Fatalf("status mismatch: got=%v", payload["status"])
	}
	if got, ok := payload["message"].(string); !ok || got != "db unavailable" {
		t.Fatalf("message mismatch: got=%v", payload["message"])
	}

	assertJSONNumber(t, payload, "aborted_logical_files", 0)
	assertJSONNumber(t, payload, "aborted_chunks", 0)
	assertJSONNumber(t, payload, "quarantined_missing_containers", 0)
	assertJSONNumber(t, payload, "quarantined_corrupt_tail_containers", 0)
	assertJSONNumber(t, payload, "quarantined_orphan_containers", 0)
	assertJSONNumber(t, payload, "checked_container_records", 0)
	assertJSONNumber(t, payload, "checked_disk_files", 0)
	assertJSONNumber(t, payload, "skipped_dir_entries", 0)
}

func assertJSONNumber(t *testing.T, payload map[string]any, key string, expected int64) {
	t.Helper()

	raw, exists := payload[key]
	if !exists {
		t.Fatalf("missing key: %s", key)
	}

	got, ok := raw.(float64)
	if !ok {
		t.Fatalf("key %s has non-number value: %T (%v)", key, raw, raw)
	}

	if int64(got) != expected {
		t.Fatalf("key %s mismatch: got=%d expected=%d", key, int64(got), expected)
	}
}

func TestDoctorJSONFailureUsesGenericCLIErrorPayload(t *testing.T) {
	err := verifyError(errors.New("doctor verify phase failed: chunk mismatch"))

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeJSON)
		if code != exitVerify {
			t.Fatalf("expected verify exit code %d, got %d", exitVerify, code)
		}
	})

	var payload map[string]any
	if parseErr := json.Unmarshal([]byte(output), &payload); parseErr != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", parseErr, output)
	}

	if got, ok := payload["status"].(string); !ok || got != "error" {
		t.Fatalf("status mismatch: got=%v", payload["status"])
	}
	if got, ok := payload["error_class"].(string); !ok || got != "VERIFY" {
		t.Fatalf("error_class mismatch: got=%v", payload["error_class"])
	}
	if got, ok := payload["message"].(string); !ok || !strings.Contains(got, "doctor verify phase failed") {
		t.Fatalf("message mismatch: got=%v", payload["message"])
	}
	if _, exists := payload["command"]; exists {
		t.Fatalf("unexpected command field in doctor failure payload: %v", payload["command"])
	}
	if _, exists := payload["data"]; exists {
		t.Fatalf("unexpected data field in doctor failure payload: %v", payload["data"])
	}
	if _, exists := payload["invariant_code"]; exists {
		t.Fatalf("unexpected invariant_code field for generic verify error: %v", payload["invariant_code"])
	}
	if _, exists := payload["recommended_action"]; exists {
		t.Fatalf("unexpected recommended_action field for generic verify error: %v", payload["recommended_action"])
	}
}

func TestDoctorJSONFailureIncludesInvariantCodeAndActionWhenAvailable(t *testing.T) {
	err := verifyError(
		fmt.Errorf(
			"doctor verify phase failed: %w",
			invariants.New(invariants.CodePhysicalGraphRefCountMismatch, "logical ref_count mismatches=1", nil),
		),
	)

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeJSON)
		if code != exitVerify {
			t.Fatalf("expected verify exit code %d, got %d", exitVerify, code)
		}
	})

	var payload map[string]any
	if parseErr := json.Unmarshal([]byte(output), &payload); parseErr != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", parseErr, output)
	}

	if got, _ := payload["invariant_code"].(string); got != invariants.CodePhysicalGraphRefCountMismatch {
		t.Fatalf("invariant_code mismatch: got=%v payload=%v", payload["invariant_code"], payload)
	}
	action, _ := payload["recommended_action"].(string)
	if !strings.Contains(action, "repair ref-counts") {
		t.Fatalf("recommended_action should mention repair ref-counts: payload=%v", payload)
	}
}

func TestDoctorTextFailureIncludesInvariantCodeAndActionWhenAvailable(t *testing.T) {
	err := verifyError(
		fmt.Errorf(
			"doctor verify phase failed: %w",
			invariants.New(invariants.CodePhysicalGraphOrphan, "orphan physical_file rows=1", nil),
		),
	)

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeText)
		if code != exitVerify {
			t.Fatalf("expected verify exit code %d, got %d", exitVerify, code)
		}
	})

	if !strings.Contains(output, "INVARIANT_CODE: PHYSICAL_GRAPH_ORPHAN") {
		t.Fatalf("expected invariant code line in text output, got: %q", output)
	}
	if !strings.Contains(output, "Recommended action:") {
		t.Fatalf("expected recommended action line in text output, got: %q", output)
	}
}

func TestPrintCLIErrorTextIncludesLocalDBSetupHintWhenEnvMissing(t *testing.T) {
	t.Setenv("DB_HOST", "")
	t.Setenv("DB_PORT", "")
	t.Setenv("DB_USER", "")
	t.Setenv("DB_PASSWORD", "")
	t.Setenv("DB_NAME", "")
	t.Setenv("DB_SSLMODE", "")

	err := errors.New("load storage context: failed to connect to local DB: dial tcp: lookup port=: no such host")

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeText)
		if code != exitGeneral {
			t.Fatalf("expected general exit code %d, got %d", exitGeneral, code)
		}
	})

	if !strings.Contains(output, "DB setup hint: local mode requires PostgreSQL connection env vars") {
		t.Fatalf("expected DB setup hint header in text output, got: %q", output)
	}
	if !strings.Contains(output, "Missing/empty: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_SSLMODE") {
		t.Fatalf("expected missing env list in text output, got: %q", output)
	}
	if !strings.Contains(output, "export DB_HOST=127.0.0.1") {
		t.Fatalf("expected export snippet in text output, got: %q", output)
	}
}

func TestPrintCLIErrorJSONDoesNotAddDBHintFields(t *testing.T) {
	err := errors.New("load storage context: failed to connect to local DB: dial tcp: lookup port=: no such host")

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeJSON)
		if code != exitGeneral {
			t.Fatalf("expected general exit code %d, got %d", exitGeneral, code)
		}
	})

	var payload map[string]any
	if parseErr := json.Unmarshal([]byte(output), &payload); parseErr != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", parseErr, output)
	}

	if got, ok := payload["message"].(string); !ok || got != err.Error() {
		t.Fatalf("message mismatch: got=%v", payload["message"])
	}
	if _, exists := payload["hint"]; exists {
		t.Fatalf("unexpected hint field in JSON payload: %v", payload)
	}
}

func TestRunCLIRepairJSONFailureIncludesInvariantMetadata(t *testing.T) {
	originalRepairPhase := repairLogicalRefCountsPhase
	t.Cleanup(func() { repairLogicalRefCountsPhase = originalRepairPhase })

	repairLogicalRefCountsPhase = func() (maintenance.RepairLogicalRefCountsResult, error) {
		return maintenance.RepairLogicalRefCountsResult{}, invariants.New(
			invariants.CodeRepairRefusedOrphanRows,
			"ref_count repair refused: orphan physical_file rows=1",
			nil,
		)
	}

	stderr := captureStderr(t, func() {
		code := runCLI([]string{"repair", "ref-counts", "--output", "json"})
		if code != exitVerify {
			t.Fatalf("expected exit code %d, got %d", exitVerify, code)
		}
	})

	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	lastLine := lines[len(lines)-1]

	var payload map[string]any
	if err := json.Unmarshal([]byte(lastLine), &payload); err != nil {
		t.Fatalf("parse JSON payload: %v output=%q", err, stderr)
	}

	if got, _ := payload["error_class"].(string); got != "VERIFY" {
		t.Fatalf("error_class mismatch: got=%v payload=%v", payload["error_class"], payload)
	}
	if got, _ := payload["invariant_code"].(string); got != invariants.CodeRepairRefusedOrphanRows {
		t.Fatalf("invariant_code mismatch: got=%v payload=%v", payload["invariant_code"], payload)
	}
	action, _ := payload["recommended_action"].(string)
	if !strings.Contains(action, "orphan physical_file") {
		t.Fatalf("recommended_action should mention orphan physical_file handling: payload=%v", payload)
	}
}

func TestDoctorJSONRecoveryFailureUsesRecoveryErrorClass(t *testing.T) {
	err := recoveryError(errors.New("doctor recovery phase failed: db unavailable"))

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeJSON)
		if code != exitRecovery {
			t.Fatalf("expected recovery exit code %d, got %d", exitRecovery, code)
		}
	})

	var payload map[string]any
	if parseErr := json.Unmarshal([]byte(output), &payload); parseErr != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", parseErr, output)
	}

	if got, ok := payload["status"].(string); !ok || got != "error" {
		t.Fatalf("status mismatch: got=%v", payload["status"])
	}
	if got, ok := payload["error_class"].(string); !ok || got != "RECOVERY" {
		t.Fatalf("error_class mismatch: got=%v", payload["error_class"])
	}
	if got, ok := payload["message"].(string); !ok || !strings.Contains(got, "doctor recovery phase failed") {
		t.Fatalf("message mismatch: got=%v", payload["message"])
	}
}

func TestRunCLIDoctorJSONParseFailureEmitsSingleJSONError(t *testing.T) {
	output := captureStderr(t, func() {
		code := runCLI([]string{"doctor", "--output", "json", "--limit"})
		if code != exitUsage {
			t.Fatalf("expected usage exit code %d, got %d", exitUsage, code)
		}
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	nonEmpty := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			nonEmpty = append(nonEmpty, trimmed)
		}
	}

	if len(nonEmpty) != 1 {
		t.Fatalf("expected exactly one non-empty output line, got %d\noutput=%q", len(nonEmpty), output)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(nonEmpty[0]), &payload); err != nil {
		t.Fatalf("expected single JSON object line, parse error: %v\nline=%q", err, nonEmpty[0])
	}

	if got, _ := payload["status"].(string); got != "error" {
		t.Fatalf("status mismatch: got=%v payload=%v", payload["status"], payload)
	}
	if got, _ := payload["error_class"].(string); got != "USAGE" {
		t.Fatalf("error_class mismatch: got=%v payload=%v", payload["error_class"], payload)
	}
	if got, _ := payload["exit_code"].(float64); int(got) != exitUsage {
		t.Fatalf("exit_code mismatch: got=%v payload=%v", payload["exit_code"], payload)
	}
	if message, _ := payload["message"].(string); !strings.Contains(message, "missing value for --limit") {
		t.Fatalf("message mismatch: payload=%v", payload)
	}
}

func TestRunCLIStoreJSONEmitsStartupRecoveryAndCrossVersionReuseSuccess(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	originalLoad := loadDefaultStorageContextPhase
	originalStartupRecovery := startupRecoveryPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		startupRecoveryPhase = originalStartupRecovery
	})

	startupRecoveryPhase = func(string) (recovery.Report, error) {
		return recovery.Report{}, nil
	}

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	payload := []byte("runcli-store-cross-version-collision")
	hash := sha256.Sum256(payload)
	chunkHash := hex.EncodeToString(hash[:])

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES (?, ?, ?, ?, ?)`,
		chunkHash,
		int64(len(payload)),
		"COMPLETED",
		int64(0),
		string(chunk.VersionV1SimpleRolling),
	); err != nil {
		t.Fatalf("insert existing v1 chunk row: %v", err)
	}

	containersDir := t.TempDir()
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{
			DB:           dbconn,
			Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
			ContainerDir: containersDir,
			Chunker:      singleChunkV2CLITestChunker{},
		}, nil
	}

	inPath := filepath.Join(t.TempDir(), "runcli-cross-version.bin")
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	var stdout string
	stderr := captureStderr(t, func() {
		stdout = captureStdout(t, func() {
			code := runCLI([]string{"store", "--output", "json", inPath})
			if code != exitSuccess {
				t.Fatalf("expected exit code %d, got %d", exitSuccess, code)
			}
		})
	})

	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one stderr JSON line (startup event only), got %d output=%q", len(lines), stderr)
	}

	var startupPayload map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &startupPayload); err != nil {
		t.Fatalf("parse startup JSON payload: %v line=%q", err, lines[0])
	}
	if got, _ := startupPayload["event"].(string); got != "startup_recovery" {
		t.Fatalf("startup event mismatch: payload=%v", startupPayload)
	}
	if got, _ := startupPayload["status"].(string); got != "ok" {
		t.Fatalf("startup status mismatch: payload=%v", startupPayload)
	}

	var successPayload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &successPayload); err != nil {
		t.Fatalf("parse command success JSON payload: %v output=%q", err, stdout)
	}
	if got, _ := successPayload["status"].(string); got != "ok" {
		t.Fatalf("success payload status mismatch: payload=%v", successPayload)
	}
	if got, _ := successPayload["command"].(string); got != "store" {
		t.Fatalf("command mismatch: payload=%v", successPayload)
	}
	data, _ := successPayload["data"].(map[string]any)
	if data == nil {
		t.Fatalf("expected data object in success payload, got=%v", successPayload)
	}
	if got, _ := data["file_id"].(float64); int(got) == 0 {
		t.Fatalf("expected non-zero file_id in success payload, got=%v payload=%v", data["file_id"], successPayload)
	}
}

func TestPrintBatchHumanReportSymbolsAndAlignment(t *testing.T) {
	report := batch.Report{
		Operation: batch.OperationRestore,
		Summary:   batch.Summary{Total: 5, Success: 2, Failed: 2, Skipped: 1},
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultSuccess, OutputPath: "./out/report.pdf"},
			{ID: 18, Status: batch.ResultSuccess, OutputPath: "./out/archive.zip"},
			{Status: batch.ResultFailed, RawValue: "abc", Message: "invalid file ID \"abc\""},
			{ID: 24, Status: batch.ResultFailed, Message: "file not found"},
			{ID: 18, Status: batch.ResultSkipped, Message: "duplicate target"},
		},
	}

	output := captureStdout(t, func() {
		printBatchHumanReport("RESTORE", report)
	})

	if !strings.Contains(output, "[RESTORE]\n") {
		t.Fatalf("missing restore header: %q", output)
	}
	if !strings.Contains(output, "✔ id=12     -> ./out/report.pdf") {
		t.Fatalf("missing aligned success line: %q", output)
	}
	if !strings.Contains(output, "✖ id=24     error=file not found") {
		t.Fatalf("missing failed symbol line: %q", output)
	}
	if !strings.Contains(output, "✖ input=\"abc\" error=invalid file ID \"abc\"") {
		t.Fatalf("missing invalid raw-input failure line: %q", output)
	}
	if !strings.Contains(output, "↷ id=18     skipped duplicate target") {
		t.Fatalf("missing skipped symbol line: %q", output)
	}
}

func TestPrintBatchHumanReportDryRunPlannedNoIcon(t *testing.T) {
	report := batch.Report{
		Operation: batch.OperationRestore,
		DryRun:    true,
		Summary:   batch.Summary{Total: 2, Planned: 1, Failed: 1},
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultPlanned, Message: "would restore -> ./out/report.pdf"},
			{ID: 99, Status: batch.ResultFailed, Message: "file not found"},
		},
	}

	output := captureStdout(t, func() {
		printBatchHumanReport("RESTORE", report)
	})

	if !strings.Contains(output, "[RESTORE DRY-RUN]\n") {
		t.Fatalf("missing dry-run header: %q", output)
	}
	if !strings.Contains(output, "  id=12     would restore -> ./out/report.pdf") {
		t.Fatalf("planned line should not have icon: %q", output)
	}
	if !strings.Contains(output, "  planned: 1") {
		t.Fatalf("missing dry-run planned summary: %q", output)
	}
	if !strings.Contains(output, "  skipped: 0") {
		t.Fatalf("missing dry-run skipped summary: %q", output)
	}
}

func TestEmitBatchCommandReportJSONSchema(t *testing.T) {
	report := batch.Report{
		Operation: batch.OperationRestore,
		Summary:   batch.Summary{Total: 4, Success: 2, Failed: 2},
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultSuccess, OutputPath: "./out/a.txt", OriginalName: "a.txt"},
			{Status: batch.ResultFailed, RawValue: "abc", Message: "invalid file ID \"abc\""},
			{ID: 18, Status: batch.ResultFailed, Message: "file not found"},
			{ID: 22, Status: batch.ResultSuccess},
		},
	}

	output := captureStdout(t, func() {
		err := emitBatchCommandReport("restore", report, outputModeJSON)
		if err == nil {
			t.Fatalf("expected non-nil error when report has failures")
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse batch JSON: %v output=%q", err, output)
	}

	if got, _ := payload["status"].(string); got != "partial_failure" {
		t.Fatalf("status mismatch: got=%v payload=%v", payload["status"], payload)
	}
	if got, _ := payload["command"].(string); got != "restore" {
		t.Fatalf("command mismatch: payload=%v", payload)
	}
	if got, _ := payload["execution_mode"].(string); got != string(batch.ExecutionModeContinueOnError) {
		t.Fatalf("execution_mode mismatch: payload=%v", payload)
	}
	if _, hasData := payload["data"]; hasData {
		t.Fatalf("batch payload should not include legacy data field: %v", payload)
	}

	results, ok := payload["results"].([]any)
	if !ok || len(results) != 4 {
		t.Fatalf("results mismatch: payload=%v", payload)
	}
	invalidItem, ok := results[1].(map[string]any)
	if !ok {
		t.Fatalf("invalid item should be an object: %T", results[1])
	}
	if _, hasID := invalidItem["id"]; hasID {
		t.Fatalf("invalid parse item should not expose id field: %v", invalidItem)
	}
	if got, _ := invalidItem["raw_value"].(string); got != "abc" {
		t.Fatalf("invalid item raw_value mismatch: item=%v", invalidItem)
	}
	failedItem, ok := results[2].(map[string]any)
	if !ok {
		t.Fatalf("failed item should be an object: %T", results[2])
	}
	if got, _ := failedItem["error"].(string); got != "file not found" {
		t.Fatalf("failed item error mismatch: item=%v", failedItem)
	}
	if _, hasMessage := failedItem["message"]; hasMessage {
		t.Fatalf("failed item should expose error field, not message: %v", failedItem)
	}
}

func TestEmitBatchCommandReportJSONIncludesNonFailureMessages(t *testing.T) {
	report := batch.Report{
		Operation:     batch.OperationRemove,
		ExecutionMode: batch.ExecutionModeFailFast,
		DryRun:        true,
		Summary:       batch.Summary{Total: 4, Planned: 1, Success: 1, Failed: 1, Skipped: 1},
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultPlanned, Message: "would remove"},
			{ID: 18, Status: batch.ResultSkipped, Message: "duplicate target"},
			{ID: 24, Status: batch.ResultSuccess, Message: "removed mappings=3"},
			{ID: 99, Status: batch.ResultFailed, Message: "file not found", InvariantCode: invariants.CodeGCRefusedIntegrity, RecommendedAction: "coldkeep repair ref-counts; then rerun coldkeep verify"},
		},
	}

	output := captureStdout(t, func() {
		err := emitBatchCommandReport("remove", report, outputModeJSON)
		if err == nil {
			t.Fatalf("expected non-nil error when report has failures")
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse batch JSON: %v output=%q", err, output)
	}

	results, ok := payload["results"].([]any)
	if !ok || len(results) != 4 {
		t.Fatalf("results mismatch: payload=%v", payload)
	}
	if got, _ := payload["execution_mode"].(string); got != string(batch.ExecutionModeFailFast) {
		t.Fatalf("execution_mode mismatch: payload=%v", payload)
	}

	plannedItem, _ := results[0].(map[string]any)
	if got, _ := plannedItem["message"].(string); got != "would remove" {
		t.Fatalf("planned item should expose message, got item=%v", plannedItem)
	}

	skippedItem, _ := results[1].(map[string]any)
	if got, _ := skippedItem["message"].(string); got != "duplicate target" {
		t.Fatalf("skipped item should expose message, got item=%v", skippedItem)
	}

	successItem, _ := results[2].(map[string]any)
	if got, _ := successItem["message"].(string); got != "removed mappings=3" {
		t.Fatalf("success item should expose message, got item=%v", successItem)
	}

	failedItem, _ := results[3].(map[string]any)
	if got, _ := failedItem["error"].(string); got != "file not found" {
		t.Fatalf("failed item error mismatch: item=%v", failedItem)
	}
	if got, _ := failedItem["invariant_code"].(string); got != invariants.CodeGCRefusedIntegrity {
		t.Fatalf("failed item invariant_code mismatch: item=%v", failedItem)
	}
	if got, _ := failedItem["recommended_action"].(string); !strings.Contains(got, "repair ref-counts") {
		t.Fatalf("failed item recommended_action mismatch: item=%v", failedItem)
	}
	if _, hasMessage := failedItem["message"]; hasMessage {
		t.Fatalf("failed item should not expose message when error is present: %v", failedItem)
	}
}

func TestEmitBatchCommandReportJSONSkippedMessageFallback(t *testing.T) {
	report := batch.Report{
		Operation: batch.OperationRemove,
		DryRun:    false,
		Summary:   batch.Summary{Total: 1, Success: 0, Failed: 0, Skipped: 1},
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultSkipped, Message: "   "},
		},
	}

	output := captureStdout(t, func() {
		err := emitBatchCommandReport("remove", report, outputModeJSON)
		if err != nil {
			t.Fatalf("expected nil error when report has no failures, got %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse batch JSON: %v output=%q", err, output)
	}

	results, ok := payload["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("results mismatch: payload=%v", payload)
	}

	skippedItem, _ := results[0].(map[string]any)
	if got, _ := skippedItem["message"].(string); got != "skipped" {
		t.Fatalf("skipped item should expose fallback message, got item=%v", skippedItem)
	}
}

func TestExecuteBatchPreservesInputOrder(t *testing.T) {
	raw := []batch.RawTarget{
		{Value: "12", Source: "args"},
		{Value: "abc", Source: "args"},
		{Value: "18", Source: "args"},
		{Value: "12", Source: "args"},
	}

	targets := batch.PrepareTargets(raw)
	report := batch.ExecutePrepared(batch.OperationRemove, false, false, targets, func(id int64) batch.ItemResult {
		return batch.ItemResult{ID: id, Status: batch.ResultSuccess, Message: "ok"}
	})

	if len(report.Results) != 4 {
		t.Fatalf("result length mismatch: got=%d", len(report.Results))
	}

	if report.Results[0].ID != 12 || report.Results[0].Status != batch.ResultSuccess {
		t.Fatalf("unexpected first result: %v", report.Results[0])
	}
	if report.Results[1].Status != batch.ResultFailed || !strings.Contains(report.Results[1].Message, "invalid file ID") {
		t.Fatalf("unexpected second result: %v", report.Results[1])
	}
	if report.Results[1].RawValue != "abc" {
		t.Fatalf("expected second result raw_value=abc, got: %v", report.Results[1])
	}
	if report.Results[2].ID != 18 || report.Results[2].Status != batch.ResultSuccess {
		t.Fatalf("unexpected third result: %v", report.Results[2])
	}
	if report.Results[3].ID != 12 || report.Results[3].Status != batch.ResultSkipped {
		t.Fatalf("unexpected fourth result: %v", report.Results[3])
	}
}

func TestRunRemoveCommandAllInvalidTargetsEmitsBatchJSONReport(t *testing.T) {
	err := error(nil)
	output := captureStdout(t, func() {
		err = runRemoveCommand(parsedCommandLine{
			method:      "remove",
			positionals: []string{"abc", "def", "ghi"},
			flags:       map[string][]string{},
		}, outputModeJSON)
	})

	if err == nil {
		t.Fatal("expected non-nil error for all-invalid remove targets")
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}

	var payload map[string]any
	if decodeErr := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); decodeErr != nil {
		t.Fatalf("parse batch JSON: %v output=%q", decodeErr, output)
	}

	if got, _ := payload["status"].(string); got != "error" {
		t.Fatalf("status mismatch: got=%v payload=%v", payload["status"], payload)
	}
	if got, _ := payload["command"].(string); got != "remove" {
		t.Fatalf("command mismatch: payload=%v", payload)
	}

	summary, ok := payload["summary"].(map[string]any)
	if !ok {
		t.Fatalf("summary should be object: payload=%v", payload)
	}
	if total, _ := summary["total"].(float64); int(total) != 3 {
		t.Fatalf("summary.total mismatch: summary=%v", summary)
	}
	if failed, _ := summary["failed"].(float64); int(failed) != 3 {
		t.Fatalf("summary.failed mismatch: summary=%v", summary)
	}

	results, ok := payload["results"].([]any)
	if !ok || len(results) != 3 {
		t.Fatalf("results mismatch: payload=%v", payload)
	}
	for _, raw := range results {
		item, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("result should be object: %T", raw)
		}
		if got, _ := item["status"].(string); got != string(batch.ResultFailed) {
			t.Fatalf("expected failed item status, got item=%v", item)
		}
		if _, hasID := item["id"]; hasID {
			t.Fatalf("invalid item should not include id: item=%v", item)
		}
		if got, _ := item["error"].(string); !strings.Contains(got, "invalid file ID") {
			t.Fatalf("invalid item error mismatch: item=%v", item)
		}
	}
}

func TestRunRemoveCommandStoredPathRejectsPositionals(t *testing.T) {
	err := runRemoveCommand(parsedCommandLine{
		method:      "remove",
		positionals: []string{"1"},
		flags: map[string][]string{
			"stored-path": {"/tmp/file.txt"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Usage: coldkeep remove --stored-path") {
		t.Fatalf("expected remove stored-path usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRemoveCommandStoredPathRejectsInput(t *testing.T) {
	err := runRemoveCommand(parsedCommandLine{
		method:      "remove",
		positionals: nil,
		flags: map[string][]string{
			"stored-path": {"/tmp/file.txt"},
			"input":       {"ids.txt"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "--input is not supported with --stored-path") {
		t.Fatalf("expected remove stored-path input usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRemoveCommandStoredPathRejectsDryRunAndFailFast(t *testing.T) {
	err := runRemoveCommand(parsedCommandLine{
		method:      "remove",
		positionals: nil,
		flags: map[string][]string{
			"stored-path": {"/tmp/file.txt"},
			"dry-run":     {""},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "--dry-run and --fail-fast are not supported with --stored-path") {
		t.Fatalf("expected remove stored-path dry-run/fail-fast usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRestoreCommandAllInvalidTargetsEmitsBatchJSONReport(t *testing.T) {
	err := error(nil)
	output := captureStdout(t, func() {
		err = runRestoreCommand(parsedCommandLine{
			method:      "restore",
			positionals: []string{"abc", "def", "ghi", "./out"},
			flags:       map[string][]string{},
		}, outputModeJSON)
	})

	if err == nil {
		t.Fatal("expected non-nil error for all-invalid restore targets")
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}

	var payload map[string]any
	if decodeErr := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); decodeErr != nil {
		t.Fatalf("parse batch JSON: %v output=%q", decodeErr, output)
	}

	if got, _ := payload["status"].(string); got != "error" {
		t.Fatalf("status mismatch: got=%v payload=%v", payload["status"], payload)
	}
	if got, _ := payload["command"].(string); got != "restore" {
		t.Fatalf("command mismatch: payload=%v", payload)
	}

	summary, ok := payload["summary"].(map[string]any)
	if !ok {
		t.Fatalf("summary should be object: payload=%v", payload)
	}
	if total, _ := summary["total"].(float64); int(total) != 3 {
		t.Fatalf("summary.total mismatch: summary=%v", summary)
	}
	if failed, _ := summary["failed"].(float64); int(failed) != 3 {
		t.Fatalf("summary.failed mismatch: summary=%v", summary)
	}

	results, ok := payload["results"].([]any)
	if !ok || len(results) != 3 {
		t.Fatalf("results mismatch: payload=%v", payload)
	}
	for _, raw := range results {
		item, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("result should be object: %T", raw)
		}
		if got, _ := item["status"].(string); got != string(batch.ResultFailed) {
			t.Fatalf("expected failed item status, got item=%v", item)
		}
		if _, hasID := item["id"]; hasID {
			t.Fatalf("invalid item should not include id: item=%v", item)
		}
		if got, _ := item["error"].(string); !strings.Contains(got, "invalid file ID") {
			t.Fatalf("invalid item error mismatch: item=%v", item)
		}
	}
}

func TestRunRestoreCommandStoredPathRejectsInvalidModeClassifiesAsUsage(t *testing.T) {
	err := runRestoreCommand(parsedCommandLine{
		method:      "restore",
		positionals: nil,
		flags: map[string][]string{
			"stored-path": {"/tmp/file.txt"},
			"mode":        {"unsupported"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "invalid --mode value") {
		t.Fatalf("expected invalid mode usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRestoreCommandInvalidFileIDIncludesDidYouMeanHint(t *testing.T) {
	err := runRestoreCommand(parsedCommandLine{
		method:      "restore",
		positionals: []string{"hello.txt", "./out"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected usage error for non-numeric restore target")
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
	if !strings.Contains(err.Error(), "Invalid fileID: hello.txt") {
		t.Fatalf("expected invalid fileID in error message, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Did you mean: coldkeep restore --stored-path <path>") {
		t.Fatalf("expected did-you-mean restore hint in error message, got: %v", err)
	}
}

func TestRunRestoreCommandStoredPathRequiresDestinationForPrefixMode(t *testing.T) {
	err := runRestoreCommand(parsedCommandLine{
		method:      "restore",
		positionals: nil,
		flags: map[string][]string{
			"stored-path": {"/tmp/file.txt"},
			"mode":        {"prefix"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "--destination is required with --mode prefix") {
		t.Fatalf("expected missing destination usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRestoreCommandStoredPathRejectsPositionals(t *testing.T) {
	err := runRestoreCommand(parsedCommandLine{
		method:      "restore",
		positionals: []string{"123", "./out"},
		flags: map[string][]string{
			"stored-path": {"/tmp/file.txt"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Usage: coldkeep restore --stored-path") {
		t.Fatalf("expected stored-path usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRestoreCommandStoredPathRejectsStrictAndNoMetadataTogether(t *testing.T) {
	err := runRestoreCommand(parsedCommandLine{
		method:      "restore",
		positionals: nil,
		flags: map[string][]string{
			"stored-path": {"/tmp/file.txt"},
			"strict":      {""},
			"no-metadata": {""},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "--strict and --no-metadata cannot be used together") {
		t.Fatalf("expected strict/no-metadata conflict usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRestoreCommandRejectsStrictWithoutStoredPath(t *testing.T) {
	err := runRestoreCommand(parsedCommandLine{
		method:      "restore",
		positionals: []string{"1", "/tmp/out"},
		flags: map[string][]string{
			"strict": {""},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "--strict and --no-metadata are only supported with --stored-path") {
		t.Fatalf("expected strict unsupported usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunDoctorCommandShortCircuitsAfterRecoveryFailure(t *testing.T) {
	originalRecovery := doctorRecoveryPhase
	originalSchema := doctorSchemaVersionPhase
	originalVerify := doctorVerifyPhase
	t.Cleanup(func() {
		doctorRecoveryPhase = originalRecovery
		doctorSchemaVersionPhase = originalSchema
		doctorVerifyPhase = originalVerify
	})

	schemaCalled := false
	verifyCalled := false

	doctorRecoveryPhase = func(string) (recovery.Report, error) {
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
		t.Fatalf("expected doctor recovery phase failure, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitRecovery {
		t.Fatalf("expected recovery exit code %d, got %d", exitRecovery, got)
	}
	if schemaCalled {
		t.Fatal("schema phase should not run after recovery failure")
	}
	if verifyCalled {
		t.Fatal("verify phase should not run after recovery failure")
	}
}

func TestRunDoctorCommandShortCircuitsAfterSchemaFailure(t *testing.T) {
	originalRecovery := doctorRecoveryPhase
	originalSchema := doctorSchemaVersionPhase
	originalVerify := doctorVerifyPhase
	t.Cleanup(func() {
		doctorRecoveryPhase = originalRecovery
		doctorSchemaVersionPhase = originalSchema
		doctorVerifyPhase = originalVerify
	})

	verifyCalled := false

	doctorRecoveryPhase = func(string) (recovery.Report, error) {
		return recovery.Report{}, nil
	}
	doctorSchemaVersionPhase = func() (int64, error) {
		return 0, errors.New("schema query failed")
	}
	doctorVerifyPhase = func(string, string, int, verify.VerifyLevel) error {
		verifyCalled = true
		return nil
	}

	err := runDoctorCommand(parsedCommandLine{method: "doctor", flags: map[string][]string{}}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "doctor schema/version check failed") {
		t.Fatalf("expected doctor schema/version check failure, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitGeneral {
		t.Fatalf("expected general exit code %d, got %d", exitGeneral, got)
	}
	if verifyCalled {
		t.Fatal("verify phase should not run after schema failure")
	}
}

func TestFormatDoctorTextReportGoldenHealthy(t *testing.T) {
	report := doctorReport{
		Recovery: recovery.Report{
			AbortedLogicalFiles:    0,
			AbortedChunks:          0,
			QuarantinedMissing:     0,
			QuarantinedCorruptTail: 0,
			QuarantinedOrphan:      0,
		},
		VerifyLevel:    "full",
		SchemaVersion:  5,
		RecoveryStatus: "ok",
		VerifyStatus:   "ok",
		SchemaStatus:   "ok",
	}

	got := formatDoctorTextReport(report)
	want := "Doctor health report\n" +
		"  Overall status:      ok\n" +
		"  Verify level:        full\n" +
		"  Phase 1 - Recovery:  ok\n" +
		"  Phase 2 - Verify:    ok\n" +
		"  Phase 3 - Schema:    ok (version=5)\n" +
		"  Note: Recovery phase may have modified metadata\n" +
		"  Recovery summary: aborted_logical_files=0 aborted_chunks=0 quarantined_missing_containers=0 quarantined_corrupt_tail_containers=0 quarantined_orphan_containers=0\n" +
		"  Physical mapping integrity: orphan_physical_file_rows=0 logical_ref_count_mismatches=0 negative_logical_ref_count_rows=0\n" +
		"  Snapshot retention integrity: snapshot_file_rows=0 snapshot_referenced_logical_files=0 snapshot_only_logical_files=0 shared_logical_files=0 orphan_snapshot_logical_refs=0 invalid_snapshot_lifecycle_states=0 retained_missing_chunk_graph=0\n" +
		"  Recommended next step: none\n"

	if got != want {
		t.Fatalf("doctor text output mismatch\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestFormatDoctorTextReportGoldenDegraded(t *testing.T) {
	report := doctorReport{
		Recovery: recovery.Report{
			AbortedLogicalFiles:    1,
			AbortedChunks:          2,
			QuarantinedMissing:     3,
			QuarantinedCorruptTail: 4,
			QuarantinedOrphan:      5,
		},
		VerifyLevel:    "deep",
		SchemaVersion:  0,
		RecoveryStatus: "ok",
		VerifyStatus:   "error",
		SchemaStatus:   "error",
	}

	got := formatDoctorTextReport(report)
	want := "Doctor health report\n" +
		"  Overall status:      error\n" +
		"  Verify level:        deep\n" +
		"  Phase 1 - Recovery:  ok\n" +
		"  Phase 2 - Verify:    error\n" +
		"  Phase 3 - Schema:    error\n" +
		"  Note: Recovery phase may have modified metadata\n" +
		"  Recovery summary: aborted_logical_files=1 aborted_chunks=2 quarantined_missing_containers=3 quarantined_corrupt_tail_containers=4 quarantined_orphan_containers=5\n" +
		"  Physical mapping integrity: orphan_physical_file_rows=0 logical_ref_count_mismatches=0 negative_logical_ref_count_rows=0\n" +
		"  Snapshot retention integrity: snapshot_file_rows=0 snapshot_referenced_logical_files=0 snapshot_only_logical_files=0 shared_logical_files=0 orphan_snapshot_logical_refs=0 invalid_snapshot_lifecycle_states=0 retained_missing_chunk_graph=0\n" +
		"  Recommended next step: inspect stderr / doctor output\n"

	if got != want {
		t.Fatalf("doctor text output mismatch\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestClassifyExitCodeTypedUsageError(t *testing.T) {
	err := usageErrorf("Usage: coldkeep store <filePath>")
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestBatchFailureExitCodeClassification(t *testing.T) {
	validationOnly := batch.Report{
		Results: []batch.ItemResult{
			{Status: batch.ResultFailed, RawValue: "abc", Message: "invalid file ID \"abc\""},
		},
	}
	if got := deriveBatchFailureExitCode(validationOnly); got != exitUsage {
		t.Fatalf("expected usage exit code for validation-only failures %d, got %d", exitUsage, got)
	}

	executionOnly := batch.Report{
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultFailed, Message: "file ID 12 not found"},
		},
	}
	if got := deriveBatchFailureExitCode(executionOnly); got != exitGeneral {
		t.Fatalf("expected general exit code for execution failures %d, got %d", exitGeneral, got)
	}

	mixed := batch.Report{
		Results: []batch.ItemResult{
			{Status: batch.ResultFailed, RawValue: "abc", Message: "invalid file ID \"abc\""},
			{ID: 99, Status: batch.ResultFailed, Message: "file ID 99 not found"},
		},
	}
	if got := deriveBatchFailureExitCode(mixed); got != exitGeneral {
		t.Fatalf("expected execution failure precedence exit code %d, got %d", exitGeneral, got)
	}
}

func TestClassifyExitCodeTypedVerifyError(t *testing.T) {
	err := verifyError(errors.New("verification failed: chunk mismatch"))
	if got := classifyExitCode(err); got != exitVerify {
		t.Fatalf("expected verify exit code %d, got %d", exitVerify, got)
	}
}

func TestClassifyExitCodeTypedRecoveryError(t *testing.T) {
	err := recoveryError(errors.New("doctor recovery phase failed: db unavailable"))
	if got := classifyExitCode(err); got != exitRecovery {
		t.Fatalf("expected recovery exit code %d, got %d", exitRecovery, got)
	}
}

func TestClassifyExitCodeFallbackStringMatch(t *testing.T) {
	err := errors.New("unknown command: nope")
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code from fallback matching %d, got %d", exitUsage, got)
	}
}

func TestClassifyExitCodeNoValidFileIDsIsUsage(t *testing.T) {
	err := errors.New("no valid file IDs after parsing input")
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestClassifyExitCodeFallbackVerifyMessage(t *testing.T) {
	err := errors.New("doctor verify phase failed: chunk mismatch")
	if got := classifyExitCode(err); got != exitVerify {
		t.Fatalf("expected verify exit code from fallback matching %d, got %d", exitVerify, got)
	}
}

func TestClassifyExitCodeUnknownVerifyLevelClassifiesAsUsage(t *testing.T) {
	err := errors.New("unknown verify level: ultra")
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestClassifyExitCodeFallbackDoesNotOvermatchVerifyWord(t *testing.T) {
	err := errors.New("could not verify credentials for DB user")
	if got := classifyExitCode(err); got != exitGeneral {
		t.Fatalf("expected general exit code %d, got %d", exitGeneral, got)
	}
}

func TestExitErrorClassLabelKnownCodes(t *testing.T) {
	if got := exitErrorClassLabel(exitUsage); got != "USAGE" {
		t.Fatalf("expected USAGE label, got %q", got)
	}
	if got := exitErrorClassLabel(exitVerify); got != "VERIFY" {
		t.Fatalf("expected VERIFY label, got %q", got)
	}
	if got := exitErrorClassLabel(exitRecovery); got != "RECOVERY" {
		t.Fatalf("expected RECOVERY label, got %q", got)
	}
}

func TestExitErrorClassLabelSuccessDefaultsToGeneral(t *testing.T) {
	if got := exitErrorClassLabel(exitSuccess); got != "GENERAL" {
		t.Fatalf("expected GENERAL label for non-error code, got %q", got)
	}
}

func TestResolveOutputModeInvalidValueClassifiesAsUsage(t *testing.T) {
	parsed := parsedCommandLine{
		method: "stats",
		flags: map[string][]string{
			"output": {"yaml"},
		},
	}

	_, err := resolveOutputMode(parsed)
	if err == nil || !strings.Contains(err.Error(), "invalid --output value") {
		t.Fatalf("expected invalid output mode error, got: %v", err)
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestResolveOutputModeSupportsJSONShorthand(t *testing.T) {
	mode, err := resolveOutputMode(parsedCommandLine{
		method: "stats",
		flags:  map[string][]string{"json": {""}},
	})
	if err != nil {
		t.Fatalf("expected no error for --json shorthand, got %v", err)
	}
	if mode != outputModeJSON {
		t.Fatalf("expected json output mode for --json shorthand, got %q", mode)
	}
}

func TestResolveOutputModeSupportsHumanAlias(t *testing.T) {
	mode, err := resolveOutputMode(parsedCommandLine{
		method: "stats",
		flags:  map[string][]string{"output": {"human"}},
	})
	if err != nil {
		t.Fatalf("expected no error for --output human, got %v", err)
	}
	if mode != outputModeText {
		t.Fatalf("expected text output mode for --output human, got %q", mode)
	}
}

func TestResolveOutputModeRejectsJSONConflictWithHumanOutput(t *testing.T) {
	_, err := resolveOutputMode(parsedCommandLine{
		method: "stats",
		flags: map[string][]string{
			"json":   {""},
			"output": {"human"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot combine --json with --output human") {
		t.Fatalf("expected conflict error for --json with --output human, got %v", err)
	}
}

func TestRunSimulateCommandMissingArgsClassifiesAsUsage(t *testing.T) {
	err := runSimulateCommand(parsedCommandLine{
		method:      "simulate",
		positionals: []string{"store"},
		flags:       map[string][]string{},
	}, outputModeText)

	if err == nil || !strings.Contains(err.Error(), "Usage: coldkeep simulate") {
		t.Fatalf("expected simulate usage error, got: %v", err)
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunSimulateCommandUnknownSubcommandClassifiesAsUsage(t *testing.T) {
	err := runSimulateCommand(parsedCommandLine{
		method:      "simulate",
		positionals: []string{"noop", "target"},
		flags:       map[string][]string{},
	}, outputModeText)

	if err == nil || !strings.Contains(err.Error(), "unknown simulate subcommand") {
		t.Fatalf("expected unknown simulate subcommand error, got: %v", err)
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunBenchmarkCommandMissingArgsClassifiesAsUsage(t *testing.T) {
	err := runBenchmarkCommand(parsedCommandLine{
		method: "benchmark",
		flags:  map[string][]string{},
	}, outputModeText)

	if err == nil || !strings.Contains(err.Error(), "Usage: coldkeep benchmark") {
		t.Fatalf("expected benchmark usage error, got: %v", err)
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunBenchmarkCommandUnknownSubcommandClassifiesAsUsage(t *testing.T) {
	err := runBenchmarkCommand(parsedCommandLine{
		method:      "benchmark",
		positionals: []string{"noop"},
		flags:       map[string][]string{},
	}, outputModeText)

	if err == nil || !strings.Contains(err.Error(), "unknown benchmark subcommand") {
		t.Fatalf("expected unknown benchmark subcommand error, got: %v", err)
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunBenchmarkCommandJSONOutputSchema(t *testing.T) {
	originalPhase := runChunkerBenchmarkPhase
	t.Cleanup(func() { runChunkerBenchmarkPhase = originalPhase })

	runChunkerBenchmarkPhase = func() (BenchmarkChunkersReport, error) {
		return BenchmarkChunkersReport{
			GeneratedAtUTC: "2026-04-25T00:00:00Z",
			Rows: []BenchmarkChunkersReportRecord{
				{
					Dataset:       "slight-modifications",
					Metric:        "reuse-after-small-edit",
					V1SimplePct:   10,
					V2FastCDCPct:  20,
					DeltaPct:      10,
					WinnerVersion: string(chunk.VersionV2FastCDC),
				},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runBenchmarkCommand(parsedCommandLine{
			method:      "benchmark",
			positionals: []string{"chunkers"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runBenchmarkCommand returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse benchmark JSON payload: %v output=%q", err, output)
	}
	if got, _ := payload["status"].(string); got != "ok" {
		t.Fatalf("status mismatch: got=%v payload=%v", payload["status"], payload)
	}
	if got, _ := payload["command"].(string); got != "benchmark" {
		t.Fatalf("command mismatch: got=%v payload=%v", payload["command"], payload)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object in benchmark payload, got=%v", payload)
	}
	rows, ok := data["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("expected one row in benchmark payload, got=%v", data["rows"])
	}
}

func TestRunBenchmarkCommandTextOutputIncludesRows(t *testing.T) {
	originalPhase := runChunkerBenchmarkPhase
	t.Cleanup(func() { runChunkerBenchmarkPhase = originalPhase })

	runChunkerBenchmarkPhase = func() (BenchmarkChunkersReport, error) {
		return BenchmarkChunkersReport{
			GeneratedAtUTC: "2026-04-25T00:00:00Z",
			Rows: []BenchmarkChunkersReportRecord{
				{
					Dataset:       "slight-modifications",
					Metric:        "reuse-after-small-edit",
					V1SimplePct:   12.34,
					V2FastCDCPct:  56.78,
					DeltaPct:      44.44,
					WinnerVersion: string(chunk.VersionV2FastCDC),
				},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runBenchmarkCommand(parsedCommandLine{
			method:      "benchmark",
			positionals: []string{"chunkers"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runBenchmarkCommand returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Chunker benchmark") {
		t.Fatalf("expected benchmark heading in text output, got=%q", output)
	}
	if !strings.Contains(output, "slight-modifications") {
		t.Fatalf("expected dataset row in text output, got=%q", output)
	}
	if !strings.Contains(output, "reuse-after-small-edit") {
		t.Fatalf("expected metric in text output, got=%q", output)
	}
	if !strings.Contains(output, "Typical outcomes") {
		t.Fatalf("expected interpretation guidance in text output, got=%q", output)
	}
}

func TestRunListCommandInvalidLimitClassifiesAsUsage(t *testing.T) {
	err := runListCommand(parsedCommandLine{
		method: "list",
		flags: map[string][]string{
			"limit": {"-1"},
		},
	}, outputModeText)

	if err == nil || !strings.Contains(err.Error(), "invalid --limit") {
		t.Fatalf("expected invalid list limit error, got: %v", err)
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunSearchCommandInvalidOffsetClassifiesAsUsage(t *testing.T) {
	err := runSearchCommand(parsedCommandLine{
		method: "search",
		flags: map[string][]string{
			"offset": {"-3"},
		},
	}, outputModeText)

	if err == nil || !strings.Contains(err.Error(), "invalid --offset") {
		t.Fatalf("expected invalid search offset error, got: %v", err)
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestSearchArgsIncludesPaginationFlags(t *testing.T) {
	args := searchArgs(parsedCommandLine{
		method: "search",
		flags: map[string][]string{
			"name":   {"report"},
			"limit":  {"25", "50"},
			"offset": {"100"},
		},
		positionals: []string{"ignored-positional"},
	})

	encoded := strings.Join(args, " ")
	if !strings.Contains(encoded, "--limit 50") {
		t.Fatalf("expected last limit value to be forwarded, got %q", encoded)
	}
	if strings.Contains(encoded, "--limit 25") {
		t.Fatalf("expected earlier limit values to be ignored, got %q", encoded)
	}
	if !strings.Contains(encoded, "--offset 100") {
		t.Fatalf("expected offset value to be forwarded, got %q", encoded)
	}
}

func TestValidateNonNegativeIntegerFlagUsesLastValue(t *testing.T) {
	err := validateNonNegativeIntegerFlag(parsedCommandLine{
		method: "search",
		flags: map[string][]string{
			"limit": {"invalid", "25"},
		},
	}, "limit")
	if err != nil {
		t.Fatalf("expected final limit value to be used, got %v", err)
	}

	err = validateNonNegativeIntegerFlag(parsedCommandLine{
		method: "search",
		flags: map[string][]string{
			"limit": {"25", "invalid"},
		},
	}, "limit")
	if err == nil || !strings.Contains(err.Error(), "invalid --limit") {
		t.Fatalf("expected invalid limit value error, got: %v", err)
	}
}

func TestValidateNonNegativeIntegerFlagRejectsLimitAboveMaximum(t *testing.T) {
	err := validateNonNegativeIntegerFlag(parsedCommandLine{
		method: "search",
		flags: map[string][]string{
			"limit": {"10001"},
		},
	}, "limit")
	if err == nil || !strings.Contains(err.Error(), "must be <= 10000") {
		t.Fatalf("expected limit-above-maximum error containing \"must be <= 10000\", got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestShouldRunStartupRecoveryForStorageCommands(t *testing.T) {
	commands := []string{"store", "store-folder", "restore", "remove", "repair", "gc", "stats", "inspect", "list", "search", "verify", "snapshot"}

	for _, command := range commands {
		if !shouldRunStartupRecovery(command) {
			t.Fatalf("expected startup recovery to run for command %q", command)
		}
	}
}

func TestShouldNotRunStartupRecoveryForNonStorageCommands(t *testing.T) {
	commands := []string{"help", "version", "init", "simulate", "benchmark", "doctor", "config", "-h", "--help", "-v", "--version", "unknown"}

	for _, command := range commands {
		if shouldRunStartupRecovery(command) {
			t.Fatalf("expected startup recovery to be skipped for command %q", command)
		}
	}
}

func TestInferOutputModeFromArgsSupportsDoctorJSON(t *testing.T) {
	mode := inferOutputModeFromArgs([]string{"doctor", "--output", "json"})
	if mode != outputModeJSON {
		t.Fatalf("expected doctor --output json to infer json mode, got %q", mode)
	}

	mode = inferOutputModeFromArgs([]string{"doctor", "--output=json"})
	if mode != outputModeJSON {
		t.Fatalf("expected doctor --output=json to infer json mode, got %q", mode)
	}
}

func TestInferOutputModeFromArgsSupportsSnapshotJSON(t *testing.T) {
	mode := inferOutputModeFromArgs([]string{"snapshot", "create", "--output", "json"})
	if mode != outputModeJSON {
		t.Fatalf("expected snapshot --output json to infer json mode, got %q", mode)
	}

	mode = inferOutputModeFromArgs([]string{"snapshot", "create", "--output=json"})
	if mode != outputModeJSON {
		t.Fatalf("expected snapshot --output=json to infer json mode, got %q", mode)
	}
}

func TestInferOutputModeFromArgsSupportsBenchmarkJSON(t *testing.T) {
	mode := inferOutputModeFromArgs([]string{"benchmark", "chunkers", "--output", "json"})
	if mode != outputModeJSON {
		t.Fatalf("expected benchmark --output json to infer json mode, got %q", mode)
	}

	mode = inferOutputModeFromArgs([]string{"benchmark", "chunkers", "--output=json"})
	if mode != outputModeJSON {
		t.Fatalf("expected benchmark --output=json to infer json mode, got %q", mode)
	}
}

func TestInferOutputModeFromArgsSupportsStatsJSONShorthand(t *testing.T) {
	mode := inferOutputModeFromArgs([]string{"stats", "--json"})
	if mode != outputModeJSON {
		t.Fatalf("expected stats --json to infer json mode, got %q", mode)
	}
}

func TestParseDoctorVerifyLevelDefaultsToStandard(t *testing.T) {
	level, err := parseDoctorVerifyLevel(parsedCommandLine{
		method: "doctor",
		flags:  map[string][]string{},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if level != verify.VerifyStandard {
		t.Fatalf("expected default doctor verify level standard, got %v", level)
	}
}

func TestParseDoctorVerifyLevelUsesExplicitFlag(t *testing.T) {
	level, err := parseDoctorVerifyLevel(parsedCommandLine{
		method: "doctor",
		flags: map[string][]string{
			"full": {""},
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if level != verify.VerifyFull {
		t.Fatalf("expected explicit doctor verify level full, got %v", level)
	}
}

func TestPrintCLISuccessJSONCommandPolicy(t *testing.T) {
	selfEmittingJSONCommands := []string{"store", "store-folder", "restore", "remove", "repair", "gc", "list", "search", "stats", "inspect", "simulate", "benchmark", "doctor", "snapshot", "config", "version", "-v", "--version"}

	for _, command := range selfEmittingJSONCommands {
		output := captureStdout(t, func() {
			printCLISuccess(parsedCommandLine{method: command}, outputModeJSON)
		})
		if strings.TrimSpace(output) != "" {
			t.Fatalf("expected no generic success JSON for self-emitting command %q, got %q", command, output)
		}
	}

	genericSuccessCommands := []string{"verify", "help", "init"}

	for _, command := range genericSuccessCommands {
		output := captureStdout(t, func() {
			printCLISuccess(parsedCommandLine{method: command}, outputModeJSON)
		})

		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
			t.Fatalf("expected JSON payload for command %q: %v, output=%q", command, err, output)
		}
		if got, ok := payload["status"].(string); !ok || got != "ok" {
			t.Fatalf("status mismatch for command %q: got=%v", command, payload["status"])
		}
		if got, ok := payload["command"].(string); !ok || got != command {
			t.Fatalf("command mismatch for command %q: got=%v", command, payload["command"])
		}
	}
}

func TestRunVerifyCommandNoTargetIncludesDidYouMeanHint(t *testing.T) {
	err := runVerifyCommand(parsedCommandLine{
		method:      "verify",
		positionals: []string{},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected usage error for verify without target")
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}

	msg := err.Error()
	if !strings.Contains(msg, "Usage: coldkeep verify <system|file <fileID>>") {
		t.Fatalf("expected verify usage in error message, got: %q", msg)
	}
	if !strings.Contains(msg, "Did you mean: coldkeep verify system --standard") {
		t.Fatalf("expected did-you-mean hint in error message, got: %q", msg)
	}
}

func TestRunConfigCommandSetAndGetDefaultChunker(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
	})

	dbPath := filepath.Join(t.TempDir(), "config_set_get.sqlite")
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return storage.StorageContext{}, err
		}
		if err := dbpkg.RunMigrations(dbconn); err != nil {
			_ = dbconn.Close()
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	setOut := captureStdout(t, func() {
		err := runConfigCommand(parsedCommandLine{
			method:      "config",
			positionals: []string{"set", "default-chunker", string(chunk.VersionV2FastCDC)},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runConfigCommand set returned error: %v", err)
		}
	})
	if !strings.Contains(setOut, "default-chunker set to v2-fastcdc") {
		t.Fatalf("expected confirmation output, got: %q", setOut)
	}

	getOut := captureStdout(t, func() {
		err := runConfigCommand(parsedCommandLine{
			method:      "config",
			positionals: []string{"get", "default-chunker"},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runConfigCommand get returned error: %v", err)
		}
	})
	if strings.TrimSpace(getOut) != string(chunk.VersionV2FastCDC) {
		t.Fatalf("expected get output %q, got %q", chunk.VersionV2FastCDC, strings.TrimSpace(getOut))
	}
}

func TestRunInspectCommandFileTextShowsChunkerAndChunkSummary(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() {
		runObservabilityInspectPhase = originalInspect
	})

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		if entity != observability.EntityFile {
			t.Fatalf("unexpected entity: %s", entity)
		}
		if id != "42" {
			t.Fatalf("unexpected id: %s", id)
		}
		return &observability.InspectResult{
			EntityType: observability.EntityLogicalFile,
			EntityID:   "42",
			Summary: map[string]any{
				"original_name":        "photo.jpg",
				"chunker_version":      "v2-fastcdc",
				"chunk_count":          int64(142),
				"avg_chunk_size_bytes": 58 * 1024.0,
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"file", "42"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runInspectCommand text returned error: %v", err)
		}
	})

	for _, want := range []string{
		"Logical file 42",
		"Summary",
		"name:",
		"photo.jpg",
		"chunks:",
		"142",
		"chunker version:",
		"v2-fastcdc",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected inspect text output to contain %q, got output:\n%s", want, output)
		}
	}
}

func TestRunInspectCommandFileJSONShowsChunkerAndChunkSummary(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() {
		runObservabilityInspectPhase = originalInspect
	})

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		if entity != observability.EntityFile {
			t.Fatalf("unexpected entity: %s", entity)
		}
		if id != "42" {
			t.Fatalf("unexpected id: %s", id)
		}
		return &observability.InspectResult{
			EntityType: observability.EntityChunk,
			EntityID:   "123",
			Summary: map[string]any{
				"size_bytes":           int64(4096),
				"chunker_version":      "v2-fastcdc",
				"chunk_count":          int64(142),
				"avg_chunk_size_bytes": 58 * 1024.0,
			},
			Relations: []observability.Relation{
				{
					Type:       "referenced_by",
					Direction:  observability.RelationIncoming,
					TargetType: observability.EntityLogicalFile,
					TargetID:   "45",
				},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"file", "42"},
			flags: map[string][]string{
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runInspectCommand json returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse inspect JSON output: %v output=%q", err, output)
	}
	if got, _ := payload["entity_type"].(string); got != "chunk" {
		t.Fatalf("expected entity_type=chunk, got %v", payload["entity_type"])
	}
	if got, _ := payload["entity_id"].(string); got != "123" {
		t.Fatalf("expected entity_id=123, got %v", payload["entity_id"])
	}
	summary, ok := payload["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary object, got %T", payload["summary"])
	}
	if got, _ := summary["chunker_version"].(string); got != "v2-fastcdc" {
		t.Fatalf("expected chunker_version=v2-fastcdc, got %v", summary["chunker_version"])
	}
	assertJSONNumber(t, summary, "size_bytes", 4096)
	relations, ok := payload["relations"].([]any)
	if !ok || len(relations) != 1 {
		t.Fatalf("expected 1 relation, got %T (%v)", payload["relations"], payload["relations"])
	}
	rel, ok := relations[0].(map[string]any)
	if !ok {
		t.Fatalf("expected relation object, got %T", relations[0])
	}
	if got, _ := rel["type"].(string); got != "referenced_by" {
		t.Fatalf("expected relation type referenced_by, got %v", rel["type"])
	}
	if got, _ := rel["direction"].(string); got != "incoming" {
		t.Fatalf("expected relation direction incoming, got %v", rel["direction"])
	}
	if got, _ := rel["target_type"].(string); got != "logical_file" {
		t.Fatalf("expected relation target_type logical_file, got %v", rel["target_type"])
	}
	if got, _ := rel["target_id"].(string); got != "45" {
		t.Fatalf("expected relation target_id 45, got %v", rel["target_id"])
	}
}

func TestRunInspectCommandRejectsInvalidUsage(t *testing.T) {
	// unknown entity type
	err := runInspectCommand(parsedCommandLine{
		method:      "inspect",
		positionals: []string{"blob", "1"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Usage: coldkeep inspect (file|snapshot|chunk|container) <id>") {
		t.Fatalf("expected inspect usage error for unknown entity, got: %v", err)
	}

	// wrong number of positional args
	err = runInspectCommand(parsedCommandLine{
		method:      "inspect",
		positionals: []string{"file"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Usage: coldkeep inspect (file|snapshot|chunk|container) <id>") {
		t.Fatalf("expected inspect usage error for missing id, got: %v", err)
	}

	// invalid numeric id for file
	err = runInspectCommand(parsedCommandLine{
		method:      "inspect",
		positionals: []string{"file", "notanumber"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Invalid file id") {
		t.Fatalf("expected invalid file id error, got: %v", err)
	}

	// invalid numeric id for chunk
	err = runInspectCommand(parsedCommandLine{
		method:      "inspect",
		positionals: []string{"chunk", "abc"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Invalid chunk id") {
		t.Fatalf("expected invalid chunk id error, got: %v", err)
	}

	// invalid limit flag
	err = runInspectCommand(parsedCommandLine{
		method:      "inspect",
		positionals: []string{"snapshot", "snap-1"},
		flags:       map[string][]string{"limit": {"bad"}},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Invalid --limit value") {
		t.Fatalf("expected invalid limit error, got: %v", err)
	}
}

func TestRunInspectCommandSnapshotTextNewEntity(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		if entity != observability.EntitySnapshot {
			t.Fatalf("unexpected entity: %s", entity)
		}
		if id != "snap-99" {
			t.Fatalf("unexpected id: %s", id)
		}
		if !opts.Relations {
			t.Fatalf("expected relations=true")
		}
		return &observability.InspectResult{
			EntityType: observability.EntitySnapshot,
			EntityID:   "snap-99",
			Summary: map[string]any{
				"type":               "full",
				"logical_file_count": int64(3),
				"total_size_bytes":   int64(1024),
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"snapshot", "snap-99"},
			flags:       map[string][]string{"relations": {""}},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runInspectCommand snapshot text returned error: %v", err)
		}
	})

	for _, want := range []string{"Snapshot snap-99", "Summary"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestRunInspectCommandChunkTextNewEntity(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		if entity != observability.EntityChunk {
			t.Fatalf("unexpected entity: %s", entity)
		}
		if id != "77" {
			t.Fatalf("unexpected id: %s", id)
		}
		return &observability.InspectResult{
			EntityType: observability.EntityChunk,
			EntityID:   "77",
			Summary: map[string]any{
				"size_bytes":      int64(2048),
				"chunker_version": "v2-fastcdc",
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"chunk", "77"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runInspectCommand chunk text returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Chunk 77") {
		t.Fatalf("expected output to contain chunk header, got:\n%s", output)
	}
}

func TestRunInspectCommandContainerTextNewEntity(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		if entity != observability.EntityContainer {
			t.Fatalf("unexpected entity: %s", entity)
		}
		if id != "5" {
			t.Fatalf("unexpected id: %s", id)
		}
		return &observability.InspectResult{
			EntityType: observability.EntityContainer,
			EntityID:   "5",
			Summary: map[string]any{
				"filename":    "ctr_5.bin",
				"size_bytes":  int64(4096),
				"chunk_count": int64(12),
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"container", "5"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runInspectCommand container text returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Container 5") {
		t.Fatalf("expected output to contain container header, got:\n%s", output)
	}
}

func TestRunInspectCommandFlagsPassedThrough(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	var capturedOpts observability.InspectOptions
	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		capturedOpts = opts
		return &observability.InspectResult{
			EntityType: observability.EntityLogicalFile,
			EntityID:   id,
			Summary:    map[string]any{},
		}, nil
	}

	_ = runInspectCommand(parsedCommandLine{
		method:      "inspect",
		positionals: []string{"file", "1"},
		flags:       map[string][]string{"relations": {""}, "reverse": {""}, "deep": {""}, "limit": {"7"}},
	}, outputModeText)

	if !capturedOpts.Relations {
		t.Fatal("expected Relations=true from --relations flag")
	}
	if !capturedOpts.Reverse {
		t.Fatal("expected Reverse=true from --reverse flag")
	}
	if !capturedOpts.Deep {
		t.Fatal("expected Deep=true from --deep flag")
	}
	if capturedOpts.Limit != 7 {
		t.Fatalf("expected Limit=7 from --limit 7, got %d", capturedOpts.Limit)
	}
}

func TestRunInspectCommandNotFoundError(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		return nil, fmt.Errorf("%w: chunk %s", observability.ErrNotFound, id)
	}

	err := runInspectCommand(parsedCommandLine{
		method:      "inspect",
		positionals: []string{"chunk", "9999"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got: %v", err)
	}
}

func TestRunConfigCommandSetRejectsUnknownVersion(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
	})

	dbPath := filepath.Join(t.TempDir(), "config_unknown.sqlite")
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return storage.StorageContext{}, err
		}
		if err := dbpkg.RunMigrations(dbconn); err != nil {
			_ = dbconn.Close()
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	err := runConfigCommand(parsedCommandLine{
		method:      "config",
		positionals: []string{"set", "default-chunker", "v9-future-cdc"},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "unknown chunker version") {
		t.Fatalf("expected unknown-version error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunConfigCommandSetRejectsDeprecatedVersion(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDeprecation := isDeprecatedChunkerVersionPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		isDeprecatedChunkerVersionPhase = originalDeprecation
	})

	dbPath := filepath.Join(t.TempDir(), "config_deprecated.sqlite")
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return storage.StorageContext{}, err
		}
		if err := dbpkg.RunMigrations(dbconn); err != nil {
			_ = dbconn.Close()
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	isDeprecatedChunkerVersionPhase = func(v chunk.Version) (bool, string) {
		if v == chunk.VersionV2FastCDC {
			return true, "scheduled removal"
		}
		return false, ""
	}

	err := runConfigCommand(parsedCommandLine{
		method:      "config",
		positionals: []string{"set", "default-chunker", string(chunk.VersionV2FastCDC)},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "deprecated chunker version") {
		t.Fatalf("expected deprecated-version error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "scheduled removal") {
		t.Fatalf("expected deprecated-version reason in error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunConfigCommandGetJSON(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
	})

	dbPath := filepath.Join(t.TempDir(), "config_get_json.sqlite")
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return storage.StorageContext{}, err
		}
		if err := dbpkg.RunMigrations(dbconn); err != nil {
			_ = dbconn.Close()
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	output := captureStdout(t, func() {
		err := runConfigCommand(parsedCommandLine{
			method:      "config",
			positionals: []string{"get", "default-chunker"},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runConfigCommand get json returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse json payload: %v; output=%q", err, output)
	}
	if got, _ := payload["command"].(string); got != "config get" {
		t.Fatalf("command mismatch: payload=%v", payload)
	}
	data, _ := payload["data"].(map[string]any)
	if got, _ := data["key"].(string); got != "default-chunker" {
		t.Fatalf("key mismatch: payload=%v", payload)
	}
	if got, _ := data["value"].(string); got != string(chunk.VersionV2FastCDC) {
		t.Fatalf("value mismatch: payload=%v", payload)
	}
}

func TestRunConfigCommandSetWarnsOnlyOnActualSwitch(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
	})

	dbPath := filepath.Join(t.TempDir(), "config_set_warn_switch.sqlite")
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return storage.StorageContext{}, err
		}
		if err := dbpkg.RunMigrations(dbconn); err != nil {
			_ = dbconn.Close()
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	switchOutput := captureStdout(t, func() {
		err := runConfigCommand(parsedCommandLine{
			method:      "config",
			positionals: []string{"set", "default-chunker", string(chunk.VersionV1SimpleRolling)},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runConfigCommand set returned error: %v", err)
		}
	})
	if !strings.Contains(switchOutput, "Warning: This affects only new stored data.") {
		t.Fatalf("expected switch warning in output, got: %q", switchOutput)
	}
	if !strings.Contains(switchOutput, "Existing data remains unchanged.") {
		t.Fatalf("expected unchanged-data warning in output, got: %q", switchOutput)
	}

	noSwitchOutput := captureStdout(t, func() {
		err := runConfigCommand(parsedCommandLine{
			method:      "config",
			positionals: []string{"set", "default-chunker", string(chunk.VersionV1SimpleRolling)},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runConfigCommand set same value returned error: %v", err)
		}
	})
	if strings.Contains(noSwitchOutput, "Warning: This affects only new stored data.") {
		t.Fatalf("did not expect warning when value does not change, got: %q", noSwitchOutput)
	}
}

func TestRunConfigCommandSetDoesNotModifyExistingData(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
	})

	dbPath := filepath.Join(t.TempDir(), "config_set_safety_guard.sqlite")

	seedDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = seedDB.Close() }()
	if err := dbpkg.RunMigrations(seedDB); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var logicalID int64
	if err := seedDB.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES (?, ?, ?, ?, ?) RETURNING id`,
		"safety.bin", int64(11), "safety-logical-hash", "COMPLETED", string(chunk.VersionV1SimpleRolling),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var chunkID int64
	if err := seedDB.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES (?, ?, ?, ?, ?) RETURNING id`,
		"safety-chunk-hash", int64(11), "COMPLETED", int64(1), string(chunk.VersionV1SimpleRolling),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return storage.StorageContext{}, err
		}
		if err := dbpkg.RunMigrations(dbconn); err != nil {
			_ = dbconn.Close()
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	if err := runConfigCommand(parsedCommandLine{
		method:      "config",
		positionals: []string{"set", "default-chunker", string(chunk.VersionV2FastCDC)},
	}, outputModeText); err != nil {
		t.Fatalf("runConfigCommand set returned error: %v", err)
	}

	verifyDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = verifyDB.Close() }()

	var logicalVersion string
	if err := verifyDB.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = ?`, logicalID).Scan(&logicalVersion); err != nil {
		t.Fatalf("read logical_file chunker_version: %v", err)
	}
	if logicalVersion != string(chunk.VersionV1SimpleRolling) {
		t.Fatalf("existing logical_file chunker_version changed: got %q want %q", logicalVersion, chunk.VersionV1SimpleRolling)
	}

	var persistedChunkVersion string
	if err := verifyDB.QueryRow(`SELECT chunker_version FROM chunk WHERE id = ?`, chunkID).Scan(&persistedChunkVersion); err != nil {
		t.Fatalf("read chunk chunker_version: %v", err)
	}
	if persistedChunkVersion != string(chunk.VersionV1SimpleRolling) {
		t.Fatalf("existing chunk chunker_version changed: got %q want %q", persistedChunkVersion, chunk.VersionV1SimpleRolling)
	}

	var configVersion string
	if err := verifyDB.QueryRow(`SELECT value FROM repository_config WHERE key = 'default_chunker'`).Scan(&configVersion); err != nil {
		t.Fatalf("read repository_config.default_chunker: %v", err)
	}
	if configVersion != string(chunk.VersionV2FastCDC) {
		t.Fatalf("expected repository default to be updated to %q, got %q", chunk.VersionV2FastCDC, configVersion)
	}
}

func TestRunStoreCommandAllowsCrossVersionReuseText(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
	})

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	payload := []byte("cli-cross-version-collision")
	hash := sha256.Sum256(payload)
	chunkHash := hex.EncodeToString(hash[:])

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES (?, ?, ?, ?, ?)`,
		chunkHash,
		int64(len(payload)),
		"COMPLETED",
		int64(0),
		string(chunk.VersionV1SimpleRolling),
	); err != nil {
		t.Fatalf("insert existing v1 chunk row: %v", err)
	}

	containersDir := t.TempDir()
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{
			DB:           dbconn,
			Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
			ContainerDir: containersDir,
			Chunker:      singleChunkV2CLITestChunker{},
		}, nil
	}

	inPath := filepath.Join(t.TempDir(), "cli-cross-version.bin")
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	err = runStoreCommand(parsedCommandLine{method: "store", positionals: []string{inPath}}, outputModeText)
	if err != nil {
		t.Fatalf("expected cross-version reuse store to succeed, got: %v", err)
	}
}

func TestRunStoreCommandAllowsCrossVersionReuseJSON(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
	})

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	payload := []byte("cli-cross-version-collision-json")
	hash := sha256.Sum256(payload)
	chunkHash := hex.EncodeToString(hash[:])

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES (?, ?, ?, ?, ?)`,
		chunkHash,
		int64(len(payload)),
		"COMPLETED",
		int64(0),
		string(chunk.VersionV1SimpleRolling),
	); err != nil {
		t.Fatalf("insert existing v1 chunk row: %v", err)
	}

	containersDir := t.TempDir()
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{
			DB:           dbconn,
			Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
			ContainerDir: containersDir,
			Chunker:      singleChunkV2CLITestChunker{},
		}, nil
	}

	inPath := filepath.Join(t.TempDir(), "cli-cross-version-json.bin")
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	stdout := captureStdout(t, func() {
		storeErr := runStoreCommand(parsedCommandLine{method: "store", positionals: []string{inPath}}, outputModeJSON)
		if storeErr != nil {
			t.Fatalf("expected store command to succeed, got: %v", storeErr)
		}
	})

	var payloadJSON map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &payloadJSON); err != nil {
		t.Fatalf("parse JSON payload: %v output=%q", err, stdout)
	}
	if got, _ := payloadJSON["status"].(string); got != "ok" {
		t.Fatalf("status mismatch: got=%v payload=%v", payloadJSON["status"], payloadJSON)
	}
	if got, _ := payloadJSON["command"].(string); got != "store" {
		t.Fatalf("command mismatch: got=%v payload=%v", payloadJSON["command"], payloadJSON)
	}
	data, _ := payloadJSON["data"].(map[string]any)
	if data == nil {
		t.Fatalf("expected data object in JSON payload, got=%v", payloadJSON)
	}
	if got, _ := data["file_id"].(float64); int(got) == 0 {
		t.Fatalf("expected non-zero file_id in JSON payload, got=%v payload=%v", data["file_id"], payloadJSON)
	}
}

func TestRunSnapshotCommandCreateForwardsPartialInputs(t *testing.T) {
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

	var (
		called       bool
		gotID        string
		gotType      string
		gotLabel     *string
		gotParentID  *string
		gotPaths     []string
		gotDBNonNil  bool
		gotCtxNonNil bool
	)
	createSnapshotPhase = func(ctx context.Context, db *sql.DB, opts snapshot.SnapshotCreateOptions) error {
		called = true
		gotID = opts.ID
		gotType = opts.Type
		gotLabel = opts.Label
		gotParentID = opts.ParentID
		gotPaths = append([]string(nil), opts.Paths...)
		gotDBNonNil = db != nil
		gotCtxNonNil = ctx != nil
		return nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"create", "docs/", "a.txt"},
			flags: map[string][]string{
				"id":     {"snap-phase2"},
				"label":  {"release-candidate"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand returned error: %v", err)
		}
	})

	if !called {
		t.Fatal("expected createSnapshotPhase to be called")
	}
	if !gotCtxNonNil || !gotDBNonNil {
		t.Fatalf("expected non-nil ctx and db, got ctx=%v db=%v", gotCtxNonNil, gotDBNonNil)
	}
	if gotID != "snap-phase2" {
		t.Fatalf("snapshot ID mismatch: got=%q", gotID)
	}
	if gotType != "partial" {
		t.Fatalf("snapshot type mismatch: got=%q", gotType)
	}
	if gotLabel == nil || *gotLabel != "release-candidate" {
		t.Fatalf("snapshot label mismatch: got=%v", gotLabel)
	}
	if gotParentID != nil {
		t.Fatalf("expected nil parentID when --from is not provided, got=%v", gotParentID)
	}
	if len(gotPaths) != 2 || gotPaths[0] != "docs/" || gotPaths[1] != "a.txt" {
		t.Fatalf("snapshot paths mismatch: got=%v", gotPaths)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot JSON output: %v output=%q", err, output)
	}
	if got, _ := payload["command"].(string); got != "snapshot" {
		t.Fatalf("expected command snapshot in JSON payload, got=%v", payload["command"])
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object in snapshot payload, got=%v", payload)
	}
	if _, ok := data["duration_ms"]; !ok {
		t.Fatalf("expected duration_ms in snapshot payload data, got=%v", data)
	}
}

func TestRunSnapshotCommandCreateInfersFullWhenNoPaths(t *testing.T) {
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

	gotType := ""
	gotPathCount := -1
	createSnapshotPhase = func(_ context.Context, _ *sql.DB, opts snapshot.SnapshotCreateOptions) error {
		gotType = opts.Type
		gotPathCount = len(opts.Paths)
		return nil
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"create"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand returned error: %v", err)
	}

	if gotType != "full" {
		t.Fatalf("expected full snapshot type, got %q", gotType)
	}
	if gotPathCount != 0 {
		t.Fatalf("expected zero paths for full snapshot, got %d", gotPathCount)
	}
}

func TestRunSnapshotCommandCreateForwardsFromParentID(t *testing.T) {
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

	var gotParentID *string
	createSnapshotPhase = func(_ context.Context, _ *sql.DB, opts snapshot.SnapshotCreateOptions) error {
		gotParentID = opts.ParentID
		return nil
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"create"},
		flags: map[string][]string{
			"id":   {"snap-child"},
			"from": {"snap-parent"},
		},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand returned error: %v", err)
	}

	if gotParentID == nil || *gotParentID != "snap-parent" {
		t.Fatalf("expected forwarded parentID snap-parent, got=%v", gotParentID)
	}
}

func TestRunSnapshotCommandCreateRejectsEmptyFrom(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"create"},
		flags: map[string][]string{
			"from": {"   "},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "--from cannot be empty") {
		t.Fatalf("expected empty --from usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestParseCommandLineTreatsFromAsValueFlag(t *testing.T) {
	parsed, err := parseCommandLine([]string{"snapshot", "create", "--id", "snap-child", "--from", "snap-parent"}, flagsWithValues)
	if err != nil {
		t.Fatalf("parseCommandLine returned error: %v", err)
	}

	from, ok := parsed.lastFlagValue("from")
	if !ok {
		t.Fatalf("expected --from to be present in parsed flags: %+v", parsed.flags)
	}
	if from != "snap-parent" {
		t.Fatalf("expected --from value snap-parent, got %q", from)
	}
}

func TestRunSnapshotCommandRejectsUnknownSubcommand(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"wat"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "unknown snapshot subcommand") {
		t.Fatalf("expected unknown snapshot subcommand usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunSnapshotCommandRestoreForwardsInputs(t *testing.T) {
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

	var (
		called        bool
		gotSnapshotID string
		gotPaths      []string
		gotOpts       snapshot.RestoreSnapshotOptions
	)
	restoreSnapshotPhase = func(ctx context.Context, db *sql.DB, snapshotID string, paths []string, opts snapshot.RestoreSnapshotOptions) (*snapshot.RestoreSnapshotResult, error) {
		called = true
		_ = ctx
		_ = db
		gotSnapshotID = snapshotID
		gotPaths = append([]string(nil), paths...)
		gotOpts = opts
		return &snapshot.RestoreSnapshotResult{SnapshotID: snapshotID, RestoredFiles: 2, RequestedPaths: int64(len(paths))}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-restore-1", "docs/", "a.txt"},
			flags: map[string][]string{
				"mode":        {"prefix"},
				"destination": {"./restored"},
				"overwrite":   {""},
				"output":      {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand restore returned error: %v", err)
		}
	})

	if !called {
		t.Fatal("expected restoreSnapshotPhase to be called")
	}
	if gotSnapshotID != "snap-restore-1" {
		t.Fatalf("snapshot ID mismatch: got=%q", gotSnapshotID)
	}
	if len(gotPaths) != 2 || gotPaths[0] != "docs/" || gotPaths[1] != "a.txt" {
		t.Fatalf("snapshot restore paths mismatch: got=%v", gotPaths)
	}
	if gotOpts.DestinationMode != storage.RestoreDestinationPrefix {
		t.Fatalf("destination mode mismatch: got=%q", gotOpts.DestinationMode)
	}
	if gotOpts.Destination != "./restored" {
		t.Fatalf("destination mismatch: got=%q", gotOpts.Destination)
	}
	if !gotOpts.Overwrite {
		t.Fatal("expected overwrite=true")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot restore JSON output: %v output=%q", err, output)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object in snapshot restore payload, got=%v", payload)
	}
	if got, _ := data["action"].(string); got != "restore" {
		t.Fatalf("expected action=restore, got=%v", data)
	}
}

func TestRunSnapshotCommandRestoreInfersFullWhenNoPaths(t *testing.T) {
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

	gotPathCount := -1
	restoreSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string, paths []string, _ snapshot.RestoreSnapshotOptions) (*snapshot.RestoreSnapshotResult, error) {
		gotPathCount = len(paths)
		return &snapshot.RestoreSnapshotResult{SnapshotID: "snap-full-restore", RestoredFiles: 0}, nil
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"restore", "snap-full-restore"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand restore full returned error: %v", err)
	}

	if gotPathCount != 0 {
		t.Fatalf("expected zero restore paths for full snapshot restore, got %d", gotPathCount)
	}
}

func TestRunSnapshotCommandRestoreForwardsSnapshotQuery(t *testing.T) {
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

	var gotQuery *snapshot.SnapshotQuery
	restoreSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string, _ []string, opts snapshot.RestoreSnapshotOptions) (*snapshot.RestoreSnapshotResult, error) {
		gotQuery = opts.Query
		return &snapshot.RestoreSnapshotResult{SnapshotID: "snap-query", RestoredFiles: 1}, nil
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"restore", "snap-query"},
		flags: map[string][]string{
			"path":            {"./docs/a.txt", "docs/b.txt"},
			"prefix":          {"./docs//", "images/"},
			"pattern":         {"*.txt"},
			"regex":           {"^docs/"},
			"min-size":        {"10"},
			"max-size":        {"20"},
			"modified-after":  {"2026-01-01"},
			"modified-before": {"2026-01-31"},
		},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand restore returned error: %v", err)
	}
	if gotQuery == nil {
		t.Fatal("expected restore query to be forwarded")
	}
	if len(gotQuery.ExactPaths) != 2 {
		t.Fatalf("expected 2 exact paths, got %#v", gotQuery.ExactPaths)
	}
	if _, ok := gotQuery.ExactPaths["docs/a.txt"]; !ok {
		t.Fatalf("expected normalized path docs/a.txt, got %#v", gotQuery.ExactPaths)
	}
	if _, ok := gotQuery.ExactPaths["docs/b.txt"]; !ok {
		t.Fatalf("expected exact path docs/b.txt, got %#v", gotQuery.ExactPaths)
	}
	if len(gotQuery.Prefixes) != 2 || gotQuery.Prefixes[0] != "docs/" || gotQuery.Prefixes[1] != "images/" {
		t.Fatalf("expected normalized prefixes [docs/ images/], got %#v", gotQuery.Prefixes)
	}
	if gotQuery.Pattern != "*.txt" {
		t.Fatalf("expected pattern '*.txt', got %q", gotQuery.Pattern)
	}
	if gotQuery.Regex == nil || gotQuery.Regex.String() != "^docs/" {
		t.Fatalf("expected regex '^docs/', got %#v", gotQuery.Regex)
	}
	if gotQuery.MinSize == nil || *gotQuery.MinSize != 10 {
		t.Fatalf("expected min size 10, got %#v", gotQuery.MinSize)
	}
	if gotQuery.MaxSize == nil || *gotQuery.MaxSize != 20 {
		t.Fatalf("expected max size 20, got %#v", gotQuery.MaxSize)
	}
	if gotQuery.ModifiedAfter == nil || gotQuery.ModifiedAfter.UTC().Format("2006-01-02") != "2026-01-01" {
		t.Fatalf("expected modified-after date, got %#v", gotQuery.ModifiedAfter)
	}
	if gotQuery.ModifiedBefore == nil || gotQuery.ModifiedBefore.UTC().Format("2006-01-02") != "2026-01-31" {
		t.Fatalf("expected modified-before date, got %#v", gotQuery.ModifiedBefore)
	}
}

func TestRunSnapshotCommandRestoreRejectsInvalidQueryRanges(t *testing.T) {
	t.Run("size range", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-1"},
			flags: map[string][]string{
				"min-size": {"20"},
				"max-size": {"10"},
			},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), "--min-size must be <= --max-size") {
			t.Fatalf("expected min/max validation error, got: %v", err)
		}
	})

	t.Run("time range", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-1"},
			flags: map[string][]string{
				"modified-after":  {"2026-02-01"},
				"modified-before": {"2026-01-01"},
			},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), "--modified-after must be <= --modified-before") {
			t.Fatalf("expected modified range validation error, got: %v", err)
		}
	})

	t.Run("prefix format", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-1"},
			flags: map[string][]string{
				"prefix": {"docs"},
			},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), "must end with '/'") {
			t.Fatalf("expected prefix validation error, got: %v", err)
		}
	})

	t.Run("invalid modified-after timestamp", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-1"},
			flags: map[string][]string{
				"modified-after": {"not-a-date"},
			},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), "invalid --modified-after value") {
			t.Fatalf("expected invalid-timestamp error, got: %v", err)
		}
		if got := classifyExitCode(err); got != exitUsage {
			t.Fatalf("expected exitUsage=%d, got %d", exitUsage, got)
		}
	})

	t.Run("invalid modified-before timestamp", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-1"},
			flags: map[string][]string{
				"modified-before": {"not-a-date"},
			},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), "invalid --modified-before value") {
			t.Fatalf("expected invalid-timestamp error, got: %v", err)
		}
		if got := classifyExitCode(err); got != exitUsage {
			t.Fatalf("expected exitUsage=%d, got %d", exitUsage, got)
		}
	})
}

func TestRunSnapshotCommandRestoreRejectsInvalidQueryFlagValues(t *testing.T) {
	t.Run("invalid regex", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-1"},
			flags: map[string][]string{
				"regex": {"[unclosed"},
			},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), "invalid --regex value") {
			t.Fatalf("expected invalid-regex error, got: %v", err)
		}
		if got := classifyExitCode(err); got != exitUsage {
			t.Fatalf("expected exitUsage=%d, got %d", exitUsage, got)
		}
	})

	t.Run("invalid pattern", func(t *testing.T) {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"restore", "snap-1"},
			flags: map[string][]string{
				"pattern": {"[unclosed"},
			},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), "invalid --pattern value") {
			t.Fatalf("expected invalid-pattern error, got: %v", err)
		}
		if got := classifyExitCode(err); got != exitUsage {
			t.Fatalf("expected exitUsage=%d, got %d", exitUsage, got)
		}
	})
}

func TestRunSnapshotCommandShowRejectsInvalidQueryFlagValues(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"show", "snap-1"},
		flags: map[string][]string{
			"regex": {"[unclosed"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "invalid --regex value") {
		t.Fatalf("expected invalid-regex error from show command, got: %v", err)
	}
}

func TestRunSnapshotCommandDiffRejectsInvalidQueryFlagValues(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"diff", "snap-1", "snap-2"},
		flags: map[string][]string{
			"pattern": {"[unclosed"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "invalid --pattern value") {
		t.Fatalf("expected invalid-pattern error from diff command, got: %v", err)
	}
}

func TestRunSnapshotCommandRestoreRejectsInvalidModeCombination(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"restore", "snap-1"},
		flags: map[string][]string{
			"mode": {"override"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "--destination is required") {
		t.Fatalf("expected destination-required error for override restore mode, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunSnapshotCommandListForwardsFilters(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	var gotFilter snapshot.SnapshotListFilter
	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, filter snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		gotFilter = filter
		return []snapshot.Snapshot{{ID: "snap-list-1", Type: "full", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)}}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"type":   {"full"},
				"label":  {"backup"},
				"since":  {"2026-01-01"},
				"until":  {"2026-12-31"},
				"limit":  {"10"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand list returned error: %v", err)
		}
	})

	if gotFilter.Type == nil || *gotFilter.Type != "full" {
		t.Fatalf("expected type filter full, got %+v", gotFilter)
	}
	if gotFilter.Label == nil || *gotFilter.Label != "backup" {
		t.Fatalf("expected label filter backup, got %+v", gotFilter)
	}
	if gotFilter.Limit != 10 || gotFilter.Since == nil || gotFilter.Until == nil {
		t.Fatalf("expected limit and date filters to be parsed, got %+v", gotFilter)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot list JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got, _ := data["action"].(string); got != "list" {
		t.Fatalf("expected action=list, got %v", data)
	}
	if got := int(data["count"].(float64)); got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}
}

func TestRunSnapshotCommandListRejectsUnsupportedFilterFlag(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"list"},
		flags: map[string][]string{
			"tree":   {""},
			"filter": {"label:day"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "unknown flag(s) for snapshot") || !strings.Contains(err.Error(), "filter") {
		t.Fatalf("expected unsupported --filter usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunSnapshotCommandListTreeFlagSwitchesTextOutputMode(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "root", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "child", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "partial", ParentID: sql.NullString{String: "root", Valid: true}},
		}, nil
	}

	flatOutput := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list returned error: %v", err)
		}
	})

	if !strings.Contains(flatOutput, "  root  full  2026-04-10T10:00:00Z") {
		t.Fatalf("expected default list output to include flat root row, got:\n%s", flatOutput)
	}
	if !strings.Contains(flatOutput, "  child  partial  2026-04-10T11:00:00Z") {
		t.Fatalf("expected default list output to include flat child row, got:\n%s", flatOutput)
	}
	if strings.Contains(flatOutput, "├──") || strings.Contains(flatOutput, "└──") {
		t.Fatalf("expected default list output to remain flat, got:\n%s", flatOutput)
	}

	treeOutput := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list --tree returned error: %v", err)
		}
	})

	if !strings.Contains(treeOutput, "root") || !strings.Contains(treeOutput, "└── child") {
		t.Fatalf("expected --tree output to include rendered tree, got:\n%s", treeOutput)
	}
	if strings.Contains(treeOutput, "Snapshots:") {
		t.Fatalf("expected --tree output to contain tree only, got:\n%s", treeOutput)
	}
	if strings.Contains(treeOutput, "  root  full  2026-04-10T10:00:00Z") || strings.Contains(treeOutput, "  child  partial  2026-04-10T11:00:00Z") {
		t.Fatalf("expected --tree output to omit flat list rows, got:\n%s", treeOutput)
	}
}

func TestRunSnapshotCommandListTreeSingleSnapshot(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"}}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list --tree returned error: %v", err)
		}
	})

	if !strings.HasPrefix(output, "day1\n") {
		t.Fatalf("expected single-snapshot tree output to start with day1, got:\n%s", output)
	}
}

func TestRunSnapshotCommandListTreeNoSnapshotsFound(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return nil, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list --tree returned error: %v", err)
		}
	})

	if !strings.HasPrefix(output, "no snapshots found\n") {
		t.Fatalf("expected no snapshots found message, got:\n%s", output)
	}
}

func TestRunSnapshotCommandListTreeSeparatesMultipleRoots(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "backup1", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "backup2", CreatedAt: time.Date(2026, 4, 10, 13, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "backup1", Valid: true}},
			{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "day2", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day1", Valid: true}},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list --tree returned error: %v", err)
		}
	})

	if !strings.Contains(output, "day1\n└── day2\n\nbackup1\n└── backup2\n") {
		t.Fatalf("expected separate root trees, got:\n%s", output)
	}
}

func TestRunSnapshotCommandListTreeTextRendersHierarchy(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "experiment2", CreatedAt: time.Date(2026, 4, 10, 13, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "experiment1", Valid: true}},
			{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "day3", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day2", Valid: true}},
			{ID: "experiment1", CreatedAt: time.Date(2026, 4, 10, 11, 30, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day1", Valid: true}},
			{ID: "day2", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day1", Valid: true}},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list tree returned error: %v", err)
		}
	})

	for _, want := range []string{
		"day1",
		"├── day2",
		"│   └── day3",
		"└── experiment1",
		"    └── experiment2",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected tree output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestRunSnapshotCommandListTreeJSONIncludesParentMetadata(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "day2", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day1", Valid: true}},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree":   {""},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand list tree json returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot list JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if treeMode, ok := data["tree_mode"].(bool); !ok || !treeMode {
		t.Fatalf("expected tree_mode=true, got data=%v", data)
	}
	snapshots := data["snapshots"].([]any)
	if len(snapshots) != 2 {
		t.Fatalf("expected two snapshots, got data=%v", data)
	}
	second := snapshots[1].(map[string]any)
	if got, _ := second["parent_id"].(string); got != "day1" {
		t.Fatalf("expected parent_id=day1 in snapshot JSON, got snapshot=%v", second)
	}
	treeLines := data["tree_lines"].([]any)
	if len(treeLines) == 0 {
		t.Fatalf("expected tree_lines in tree JSON output, got data=%v", data)
	}
	for _, forbidden := range []string{"graph", "nodes", "edges", "stats", "size"} {
		if _, exists := data[forbidden]; exists {
			t.Fatalf("expected no %q field in tree list JSON output, got data=%v", forbidden, data)
		}
	}
}

func TestRunSnapshotCommandListTreeTreatsMissingParentAsRoot(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "orphan", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "missing-parent", Valid: true}},
			{ID: "child", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "orphan", Valid: true}},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list tree returned error: %v", err)
		}
	})

	if !strings.Contains(output, "day1\n\norphan\n└── child\n") {
		t.Fatalf("expected missing parent to be treated as a separate root tree, got:\n%s", output)
	}
}

func TestRunSnapshotCommandListTreeTreatsNullParentAsRoot(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "day2", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{}},
			{ID: "day3", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day2", Valid: true}},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list tree returned error: %v", err)
		}
	})

	if !strings.Contains(output, "day1\n\nday2\n└── day3\n") {
		t.Fatalf("expected NULL parent to be treated as root, got:\n%s", output)
	}
}

func TestRunSnapshotCommandListTreeIsReadOnlyAndSnapshotOnly(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	originalListFiles := listSnapshotFilesPhase
	originalCreate := createSnapshotPhase
	originalDelete := deleteSnapshotPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
		listSnapshotFilesPhase = originalListFiles
		createSnapshotPhase = originalCreate
		deleteSnapshotPhase = originalDelete
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "root", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "child", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "root", Valid: true}},
		}, nil
	}

	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, _ string, _ int, _ *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		t.Fatal("listSnapshotFilesPhase should not be called by snapshot list --tree")
		return nil, nil
	}
	createSnapshotPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotCreateOptions) error {
		t.Fatal("createSnapshotPhase should not be called by snapshot list --tree")
		return nil
	}
	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string) error {
		t.Fatal("deleteSnapshotPhase should not be called by snapshot list --tree")
		return nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list tree returned error: %v", err)
		}
	})

	if !strings.Contains(output, "root\n└── child\n") {
		t.Fatalf("expected metadata-only tree output, got:\n%s", output)
	}
}

func TestRunSnapshotCommandListTreeSortsChildrenByCreatedAtAscending(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalList := listSnapshotsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		listSnapshotsPhase = originalList
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	listSnapshotsPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
		return []snapshot.Snapshot{
			{ID: "child-late", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "root", Valid: true}},
			{ID: "root", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
			{ID: "child-early", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "root", Valid: true}},
			{ID: "child-middle", CreatedAt: time.Date(2026, 4, 10, 11, 30, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "root", Valid: true}},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags: map[string][]string{
				"tree": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand list tree returned error: %v", err)
		}
	})

	earlyIdx := strings.Index(output, "├── child-early")
	middleIdx := strings.Index(output, "├── child-middle")
	lateIdx := strings.Index(output, "└── child-late")
	if earlyIdx == -1 || middleIdx == -1 || lateIdx == -1 {
		t.Fatalf("expected ordered child entries in output, got:\n%s", output)
	}
	if earlyIdx >= middleIdx || middleIdx >= lateIdx {
		t.Fatalf("expected child order early -> middle -> late, got:\n%s", output)
	}
}

func TestRenderSnapshotTreeLinesBreaksCyclesAndLogsWarning(t *testing.T) {
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
	})

	lines := renderSnapshotTreeLines([]snapshot.Snapshot{
		{ID: "day2", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day3", Valid: true}},
		{ID: "day3", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day2", Valid: true}},
	})

	if strings.Join(lines, "\n") != "day2\n└── day3" {
		t.Fatalf("expected cycle to be broken into a finite tree, got: %#v", lines)
	}
	if !strings.Contains(logBuffer.String(), "snapshot tree cycle detected") {
		t.Fatalf("expected cycle warning log, got: %q", logBuffer.String())
	}
}

func TestRenderSnapshotTreeLinesLinearChain(t *testing.T) {
	lines := renderSnapshotTreeLines([]snapshot.Snapshot{
		{ID: "day3", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day2", Valid: true}},
		{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
		{ID: "day2", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day1", Valid: true}},
	})

	if got := strings.Join(lines, "\n"); got != "day1\n└── day2\n    └── day3" {
		t.Fatalf("expected linear chain day1 -> day2 -> day3, got: %q", got)
	}
}

func TestRenderSnapshotTreeLinesBranching(t *testing.T) {
	lines := renderSnapshotTreeLines([]snapshot.Snapshot{
		{ID: "exp1", CreatedAt: time.Date(2026, 4, 10, 11, 30, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day1", Valid: true}},
		{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
		{ID: "day2", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "day1", Valid: true}},
	})

	if got := strings.Join(lines, "\n"); got != "day1\n├── day2\n└── exp1" {
		t.Fatalf("expected branching tree under day1, got: %q", got)
	}
}

func TestRenderSnapshotTreeLinesDeterministicAcrossCalls(t *testing.T) {
	items := []snapshot.Snapshot{
		{ID: "child-b", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "root", Valid: true}},
		{ID: "root", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC), Type: "full"},
		{ID: "child-a", CreatedAt: time.Date(2026, 4, 10, 11, 0, 0, 0, time.UTC), Type: "full", ParentID: sql.NullString{String: "root", Valid: true}},
		{ID: "day1", CreatedAt: time.Date(2026, 4, 10, 9, 0, 0, 0, time.UTC), Type: "full"},
	}

	first := strings.Join(renderSnapshotTreeLines(items), "\n")
	second := strings.Join(renderSnapshotTreeLines(items), "\n")
	if first != second {
		t.Fatalf("expected deterministic tree output across calls, first=%q second=%q", first, second)
	}
	if first != "day1\n\nroot\n├── child-a\n└── child-b" {
		t.Fatalf("unexpected deterministic tree output, got: %q", first)
	}
}

func TestRunSnapshotCommandShowReturnsSnapshotAndFiles(t *testing.T) {
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
		return &snapshot.Snapshot{ID: snapshotID, Type: "full", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)}, nil
	}
	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, snapshotID string, limit int, _ *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		if snapshotID != "snap-show-1" || limit != 50 {
			t.Fatalf("unexpected show args snapshotID=%q limit=%d", snapshotID, limit)
		}
		return []snapshot.SnapshotFileEntry{{Path: "docs/a.txt"}, {Path: "img/x.png"}}, nil
	}
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 2, TotalSizeBytes: 123}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"show", "snap-show-1"},
			flags: map[string][]string{
				"limit":  {"50"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand show returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot show JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got, _ := data["action"].(string); got != "show" {
		t.Fatalf("expected action=show, got %v", data)
	}
	files := data["files"].([]any)
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
}

func TestRunSnapshotCommandShowForwardsSnapshotQuery(t *testing.T) {
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
		return &snapshot.Snapshot{ID: snapshotID, Type: "full", CreatedAt: time.Now().UTC()}, nil
	}

	var gotQuery *snapshot.SnapshotQuery
	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, _ string, _ int, query *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		gotQuery = query
		return nil, nil
	}
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 0}, nil
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"show", "snap-show-query"},
		flags: map[string][]string{
			"path":   {"./docs/a.txt"},
			"prefix": {"./docs//"},
		},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand show returned error: %v", err)
	}
	if gotQuery == nil {
		t.Fatal("expected show query to be forwarded")
	}
	if _, ok := gotQuery.ExactPaths["docs/a.txt"]; !ok {
		t.Fatalf("expected normalized path docs/a.txt, got %#v", gotQuery.ExactPaths)
	}
	if len(gotQuery.Prefixes) != 1 || gotQuery.Prefixes[0] != "docs/" {
		t.Fatalf("expected normalized prefix docs/, got %#v", gotQuery.Prefixes)
	}
}

func TestRunSnapshotCommandShowJSONUsesFilteredFileCount(t *testing.T) {
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
		return &snapshot.Snapshot{ID: snapshotID, Type: "full", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)}, nil
	}
	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, _ string, _ int, _ *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		return []snapshot.SnapshotFileEntry{{Path: "docs/a.txt"}}, nil
	}
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 3, TotalSizeBytes: 123}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"show", "snap-show-filtered-json"},
			flags: map[string][]string{
				"prefix": {"docs/"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand show returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot show JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got := int(data["file_count"].(float64)); got != 1 {
		t.Fatalf("expected file_count=1 (filtered), got %d", got)
	}
	if got := int(data["matched_file_count"].(float64)); got != 1 {
		t.Fatalf("expected matched_file_count=1, got %d", got)
	}
	if got := int(data["total_snapshot_file_count"].(float64)); got != 3 {
		t.Fatalf("expected total_snapshot_file_count=3, got %d", got)
	}
}

func TestRunSnapshotCommandShowTextDisplaysMatchedAndTotalCounts(t *testing.T) {
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
		return &snapshot.Snapshot{ID: snapshotID, Type: "full", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)}, nil
	}
	listSnapshotFilesPhase = func(_ context.Context, _ *sql.DB, _ string, _ int, _ *snapshot.SnapshotQuery) ([]snapshot.SnapshotFileEntry, error) {
		return []snapshot.SnapshotFileEntry{{Path: "docs/a.txt"}}, nil
	}
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 3, TotalSizeBytes: 123}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"show", "snap-show-filtered-text"},
			flags: map[string][]string{
				"prefix": {"docs/"},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand show returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Files (matched): 1") {
		t.Fatalf("expected text output to include matched file count, got: %q", output)
	}
	if !strings.Contains(output, "Files (total): 3") {
		t.Fatalf("expected text output to include total file count, got: %q", output)
	}
}

func TestRunSnapshotCommandStatsSupportsGlobalAndPerSnapshot(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	calledWith := ""
	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		calledWith = snapshotID
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 2, SnapshotFileCount: 4, TotalSizeBytes: 2048}, nil
	}

	if err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"stats", "snap-stats-1"},
		flags:       map[string][]string{},
	}, outputModeText); err != nil {
		t.Fatalf("runSnapshotCommand stats returned error: %v", err)
	}
	if calledWith != "snap-stats-1" {
		t.Fatalf("expected per-snapshot stats call, got snapshotID=%q", calledWith)
	}
}

func TestRunSnapshotCommandStatsTextShowsLineageBreakdownWhenParentExists(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{
			SnapshotID:        snapshotID,
			SnapshotCount:     1,
			SnapshotFileCount: 100,
			TotalSizeBytes:    2048,
			ParentSnapshotID:  sql.NullString{String: "snap-parent", Valid: true},
			ReusedFileCount:   sql.NullInt64{Int64: 99, Valid: true},
			NewFileCount:      sql.NullInt64{Int64: 1, Valid: true},
			ReuseRatioPct:     sql.NullFloat64{Float64: 99.0, Valid: true},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"stats", "snap-child"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand stats returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Reused: 99") {
		t.Fatalf("expected reused count in text output, got: %q", output)
	}
	if !strings.Contains(output, "New: 1") {
		t.Fatalf("expected new count in text output, got: %q", output)
	}
	if !strings.Contains(output, "Reuse ratio: 99.0%") {
		t.Fatalf("expected reuse ratio in text output, got: %q", output)
	}
}

func TestRunSnapshotCommandStatsTextShowsNoParentFallback(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 4, TotalSizeBytes: 2048}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"stats", "snap-root"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand stats returned error: %v", err)
		}
	})

	if !strings.Contains(output, "(Reused/New not available -- no parent snapshot metadata)") {
		t.Fatalf("expected no-parent fallback line in text output, got: %q", output)
	}
}

func TestRunSnapshotCommandStatsTextShowsMissingParentFallback(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{
			SnapshotID:        snapshotID,
			SnapshotCount:     1,
			SnapshotFileCount: 4,
			TotalSizeBytes:    2048,
			LineageStatus:     snapshot.SnapshotLineageStatusParentMissing,
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"stats", "snap-child-missing-parent"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand stats returned error: %v", err)
		}
	})

	if !strings.Contains(output, "(Reused/New not available -- parent snapshot metadata exists but parent snapshot is missing)") {
		t.Fatalf("expected missing-parent fallback line in text output, got: %q", output)
	}
}

func TestRunSnapshotCommandStatsTextShowsSkippedLineageFallback(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{
			SnapshotID:        snapshotID,
			SnapshotCount:     1,
			SnapshotFileCount: 4,
			TotalSizeBytes:    2048,
			LineageStatus:     snapshot.SnapshotLineageStatusSkipped,
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"stats", "snap-child-skipped-lineage"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand stats returned error: %v", err)
		}
	})

	if !strings.Contains(output, "(Reused/New not available -- lineage analysis was intentionally skipped for this snapshot scope)") {
		t.Fatalf("expected skipped-lineage fallback line in text output, got: %q", output)
	}
}

func TestRunSnapshotCommandStatsJSONIncludesLineageFieldsWhenParentExists(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{
			SnapshotID:        snapshotID,
			SnapshotCount:     1,
			SnapshotFileCount: 10,
			TotalSizeBytes:    512,
			ParentSnapshotID:  sql.NullString{String: "snap-parent-json", Valid: true},
			ReusedFileCount:   sql.NullInt64{Int64: 8, Valid: true},
			NewFileCount:      sql.NullInt64{Int64: 2, Valid: true},
			ReuseRatioPct:     sql.NullFloat64{Float64: 80.0, Valid: true},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"stats", "snap-child-json"},
			flags: map[string][]string{
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand stats returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot stats JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got := int64(data["reused"].(float64)); got != 8 {
		t.Fatalf("expected reused=8, got %d data=%v", got, data)
	}
	if got := int64(data["new"].(float64)); got != 2 {
		t.Fatalf("expected new=2, got %d data=%v", got, data)
	}
	if got := data["reuse_ratio"].(float64); got != 80.0 {
		t.Fatalf("expected reuse_ratio=80.0, got %v data=%v", got, data)
	}
	if _, exists := data["parent_snapshot_id"]; exists {
		t.Fatalf("did not expect parent_snapshot_id field in stats output, got data=%v", data)
	}
}

func TestRunSnapshotCommandStatsJSONOmitsLineageFieldsWhenNoParent(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalStats := snapshotStatsPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		snapshotStatsPhase = originalStats
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	snapshotStatsPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
		return &snapshot.SnapshotStats{SnapshotID: snapshotID, SnapshotCount: 1, SnapshotFileCount: 5, TotalSizeBytes: 1024}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"stats", "snap-no-parent-json"},
			flags: map[string][]string{
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand stats returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot stats JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if _, exists := data["reused"]; exists {
		t.Fatalf("did not expect reused field without parent, got data=%v", data)
	}
	if _, exists := data["new"]; exists {
		t.Fatalf("did not expect new field without parent, got data=%v", data)
	}
	if _, exists := data["reuse_ratio"]; exists {
		t.Fatalf("did not expect reuse_ratio field without parent, got data=%v", data)
	}
}

func TestRunSnapshotCommandDeleteRequiresForceAndForwards(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"delete", "snap-del-1"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "requires --force or --dry-run") {
		t.Fatalf("expected --force/--dry-run requirement error, got: %v", err)
	}

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

	calledWith := ""
	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) error {
		calledWith = snapshotID
		return nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "snap-del-1"},
			flags: map[string][]string{
				"force":  {""},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete returned error: %v", err)
		}
	})
	if calledWith != "snap-del-1" {
		t.Fatalf("expected delete to be called with snap-del-1, got %q", calledWith)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot delete JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got, _ := data["action"].(string); got != "delete" {
		t.Fatalf("expected action=delete, got %v", data)
	}
	if got, _ := data["dry_run"].(bool); got {
		t.Fatalf("expected dry_run=false for normal delete, got %v", data)
	}
}

func TestRunSnapshotCommandDeleteRejectsDeleteUnusedFlag(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"delete", "snap-del-1"},
		flags: map[string][]string{
			"dry-run":       {""},
			"delete-unused": {""},
		},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected error when using unsupported --delete-unused flag")
	}
	if !strings.Contains(err.Error(), "delete-unused") {
		t.Fatalf("expected error mentioning delete-unused, got: %v", err)
	}
}

func TestFormatNumberWithCommas(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{10, "10"},
		{100, "100"},
		{1000, "1,000"},
		{10000, "10,000"},
		{100000, "100,000"},
		{1000000, "1,000,000"},
		{99500, "99,500"},
		{500, "500"},
		{-1000, "-1,000"},
	}

	for _, tt := range tests {
		got := formatNumberWithCommas(tt.input)
		if got != tt.expected {
			t.Fatalf("formatNumberWithCommas(%d) = %q, expected %q", tt.input, got, tt.expected)
		}
	}
}

func TestFormatSnapshotDeleteDryRunOutput(t *testing.T) {
	preview := &snapshotDeleteLineagePreview{
		SnapshotID:       "snapshot-123",
		ParentID:         sql.NullString{String: "parent-snap", Valid: true},
		ChildSnapshotIDs: []string{"child-1", "child-2"},
		TotalFiles:       100000,
		UniqueFiles:      500,
		SharedFiles:      99500,
	}

	output := formatSnapshotDeleteDryRunOutput("snapshot-123", preview)

	// Verify structure
	if !strings.Contains(output, "Snapshot: snapshot-123") {
		t.Fatalf("missing snapshot header in output:\n%s", output)
	}
	if !strings.Contains(output, "Files:") {
		t.Fatalf("missing Files section in output:\n%s", output)
	}
	if !strings.Contains(output, "100,000") {
		t.Fatalf("missing formatted total files in output:\n%s", output)
	}
	if !strings.Contains(output, "Lineage:") {
		t.Fatalf("missing Lineage section in output:\n%s", output)
	}
	if !strings.Contains(output, "parent-snap") {
		t.Fatalf("missing parent in output:\n%s", output)
	}
	if !strings.Contains(output, "Impact:") {
		t.Fatalf("missing Impact section in output:\n%s", output)
	}
	if !strings.Contains(output, "Dry run: no changes applied.") {
		t.Fatalf("missing dry-run notice in output:\n%s", output)
	}
}

func TestFormatSnapshotDeleteDryRunOutputWithNoParentNoChildren(t *testing.T) {
	preview := &snapshotDeleteLineagePreview{
		SnapshotID:       "root-snap",
		ParentID:         sql.NullString{Valid: false},
		ChildSnapshotIDs: []string{},
		TotalFiles:       50,
		UniqueFiles:      50,
		SharedFiles:      0,
	}

	output := formatSnapshotDeleteDryRunOutput("root-snap", preview)

	if !strings.Contains(output, "Parent: none") {
		t.Fatalf("expected 'Parent: none' for null parent, got:\n%s", output)
	}
	if !strings.Contains(output, "Children: none") {
		t.Fatalf("expected 'Children: none' for empty children, got:\n%s", output)
	}
	if strings.Contains(output, "Warning:") {
		t.Fatalf("expected no Warning section when there are no children, got:\n%s", output)
	}
}

func TestFormatSnapshotDeleteDryRunOutputWithMissingParent(t *testing.T) {
	preview := &snapshotDeleteLineagePreview{
		SnapshotID:       "child-snap",
		ParentID:         sql.NullString{String: "missing-parent", Valid: true},
		ParentMissing:    true,
		ChildSnapshotIDs: []string{},
		TotalFiles:       0,
		UniqueFiles:      0,
		SharedFiles:      0,
	}

	output := formatSnapshotDeleteDryRunOutput("child-snap", preview)

	if !strings.Contains(output, "Parent: (missing)") {
		t.Fatalf("expected missing-parent marker in output, got:\n%s", output)
	}
}

func TestFormatSnapshotDeleteDryRunOutputIncludesWarningWhenHasChildren(t *testing.T) {
	preview := &snapshotDeleteLineagePreview{
		SnapshotID:       "day1",
		ParentID:         sql.NullString{Valid: false},
		ChildSnapshotIDs: []string{"day2", "exp1"},
		TotalFiles:       1000,
		UniqueFiles:      100,
		SharedFiles:      900,
	}

	output := formatSnapshotDeleteDryRunOutput("day1", preview)

	if !strings.Contains(output, "Warning:") {
		t.Fatalf("expected 'Warning:' section when there are children, got:\n%s", output)
	}
	if !strings.Contains(output, "This snapshot is parent of:") {
		t.Fatalf("expected 'This snapshot is parent of:' in warning, got:\n%s", output)
	}
	if !strings.Contains(output, "- day2") || !strings.Contains(output, "- exp1") {
		t.Fatalf("expected children listed in warning, got:\n%s", output)
	}
	if !strings.Contains(output, "Deleting it will break lineage visualization") {
		t.Fatalf("expected lineage warning text, got:\n%s", output)
	}
	if !strings.Contains(output, "snapshots remain fully usable") {
		t.Fatalf("expected usability note, got:\n%s", output)
	}
}

func TestPreviewWarningsIncludesLineageBreakageWhenHasChildren(t *testing.T) {
	preview := &snapshotDeleteLineagePreview{
		SnapshotID:       "snap1",
		ChildSnapshotIDs: []string{"child1", "child2"},
	}

	warnings := previewWarnings(preview)
	if len(warnings) == 0 {
		t.Fatalf("expected warnings when there are children, got none")
	}

	warning := warnings[0]
	if warningType, ok := warning["type"].(string); !ok || warningType != "lineage_breakage" {
		t.Fatalf("expected type='lineage_breakage', got %v", warning["type"])
	}

	if msg, ok := warning["message"].(string); !ok || !strings.Contains(msg, "break lineage visualization") {
		t.Fatalf("expected lineage breakage message, got %v", warning["message"])
	}

	details := warning["details"].(map[string]any)
	affected := details["affected_snapshots"].([]string)
	if len(affected) != 2 || affected[0] != "child1" || affected[1] != "child2" {
		t.Fatalf("expected affected_snapshots=['child1', 'child2'], got %v", affected)
	}
}

func TestPreviewWarningsEmptyWhenNoChildren(t *testing.T) {
	preview := &snapshotDeleteLineagePreview{
		SnapshotID:       "snap1",
		ChildSnapshotIDs: []string{},
	}

	warnings := previewWarnings(preview)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings when there are no children, got %d warnings", len(warnings))
	}

	// Also test with nil preview
	warnings = previewWarnings(nil)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings for nil preview, got %d warnings", len(warnings))
	}
}

func TestRunSnapshotCommandDeleteDryRunIsReadOnly(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDelete := deleteSnapshotPhase
	originalPreview := snapshotDeleteLineagePreviewPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		deleteSnapshotPhase = originalDelete
		snapshotDeleteLineagePreviewPhase = originalPreview
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string) error {
		t.Fatal("deleteSnapshotPhase must not be called in --dry-run mode")
		return nil
	}
	snapshotDeleteLineagePreviewPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
		return &snapshotDeleteLineagePreview{
			SnapshotID:       snapshotID,
			ParentID:         sql.NullString{String: "snap-parent", Valid: true},
			ChildSnapshotIDs: []string{"snap-child-1", "snap-child-2"},
			TotalFiles:       10,
			UniqueFiles:      3,
			SharedFiles:      7,
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "snap-del-2"},
			flags: map[string][]string{
				"dry-run": {""},
				"output":  {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete --dry-run returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot delete dry-run JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got, _ := data["action"].(string); got != "delete_dry_run" {
		t.Fatalf("expected action=delete_dry_run, got %v", data)
	}
	if got, _ := data["dry_run"].(bool); !got {
		t.Fatalf("expected dry_run=true for dry-run delete, got %v", data)
	}
	if got, _ := data["parent_id"].(string); got != "snap-parent" {
		t.Fatalf("expected parent_id=snap-parent, got %v", data)
	}
	children, ok := data["children"].([]any)
	if !ok || len(children) != 2 {
		t.Fatalf("expected two child snapshots in preview, got %v", data)
	}
	if totalFiles, _ := data["total_files"].(float64); totalFiles != 10 {
		t.Fatalf("expected total_files=10, got %v", data["total_files"])
	}
	if uniqueFiles, _ := data["unique_files"].(float64); uniqueFiles != 3 {
		t.Fatalf("expected unique_files=3, got %v", data["unique_files"])
	}
	if sharedFiles, _ := data["shared_files"].(float64); sharedFiles != 7 {
		t.Fatalf("expected shared_files=7, got %v", data["shared_files"])
	}
	if parentMissing, _ := data["parent_missing"].(bool); parentMissing {
		t.Fatalf("expected parent_missing=false, got true")
	}

	// Check warnings field (should have lineage_breakage warning since has children)
	warnings, ok := data["warnings"].([]any)
	if !ok || len(warnings) != 1 {
		t.Fatalf("expected one warning for snapshot with children, got %v", data["warnings"])
	}
	warningObj := warnings[0].(map[string]any)
	if warningType, _ := warningObj["type"].(string); warningType != "lineage_breakage" {
		t.Fatalf("expected warning type='lineage_breakage', got %v", warningObj["type"])
	}

	// v1 non-goal lock: no GC simulation / disk impact metrics in snapshot delete dry-run.
	for _, forbidden := range []string{"bytes_freed", "disk_space_freed", "chunks_affected", "containers_affected", "gc_plan", "gc_simulation"} {
		if _, exists := data[forbidden]; exists {
			t.Fatalf("did not expect GC simulation field %q in delete dry-run output: %v", forbidden, data)
		}
	}
}

func TestRunSnapshotCommandDeleteDryRunJSONNoChildrenHasNoWarnings(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDelete := deleteSnapshotPhase
	originalPreview := snapshotDeleteLineagePreviewPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		deleteSnapshotPhase = originalDelete
		snapshotDeleteLineagePreviewPhase = originalPreview
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string) error {
		t.Fatal("deleteSnapshotPhase must not be called in --dry-run mode")
		return nil
	}
	snapshotDeleteLineagePreviewPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
		return &snapshotDeleteLineagePreview{
			SnapshotID:       snapshotID,
			ParentID:         sql.NullString{Valid: false},
			ChildSnapshotIDs: []string{},
			TotalFiles:       0,
			UniqueFiles:      0,
			SharedFiles:      0,
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "root-snap"},
			flags: map[string][]string{
				"dry-run": {""},
				"output":  {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete --dry-run returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot delete dry-run JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if warnings, exists := data["warnings"]; exists && warnings != nil {
		if arr, ok := warnings.([]any); ok && len(arr) > 0 {
			t.Fatalf("expected no warnings for snapshot without children, got %v", warnings)
		}
	}
}

func TestRunSnapshotCommandDeleteDryRunSnapshotNotFound(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDelete := deleteSnapshotPhase
	originalPreview := snapshotDeleteLineagePreviewPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		deleteSnapshotPhase = originalDelete
		snapshotDeleteLineagePreviewPhase = originalPreview
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string) error {
		t.Fatal("deleteSnapshotPhase must not be called when preview fails")
		return nil
	}
	snapshotDeleteLineagePreviewPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
		return nil, fmt.Errorf("snapshot %q not found", snapshotID)
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"delete", "missing-snap"},
		flags: map[string][]string{
			"dry-run": {""},
		},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected not found error, got nil")
	}
	if got := err.Error(); got != `snapshot "missing-snap" not found` {
		t.Fatalf("expected exact not-found error, got %q", got)
	}
}

func TestRunSnapshotCommandDeleteDryRunTextOutput(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDelete := deleteSnapshotPhase
	originalPreview := snapshotDeleteLineagePreviewPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		deleteSnapshotPhase = originalDelete
		snapshotDeleteLineagePreviewPhase = originalPreview
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string) error {
		t.Fatal("deleteSnapshotPhase must not be called in --dry-run mode")
		return nil
	}
	snapshotDeleteLineagePreviewPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
		return &snapshotDeleteLineagePreview{
			SnapshotID:       snapshotID,
			ParentID:         sql.NullString{String: "day1-parent", Valid: true},
			ChildSnapshotIDs: []string{"day2", "exp1"},
			TotalFiles:       100000,
			UniqueFiles:      500,
			SharedFiles:      99500,
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "day1"},
			flags: map[string][]string{
				"dry-run": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete --dry-run returned error: %v", err)
		}
	})

	// Verify key output elements are present
	if !strings.Contains(output, "Snapshot: day1") {
		t.Fatalf("expected 'Snapshot: day1' header in output:\n%s", output)
	}
	if !strings.Contains(output, "Files:") {
		t.Fatalf("expected 'Files:' section in output:\n%s", output)
	}
	if !strings.Contains(output, "100,000") {
		t.Fatalf("expected '100,000' total files in output:\n%s", output)
	}
	if !strings.Contains(output, "500") {
		t.Fatalf("expected '500' unique files in output:\n%s", output)
	}
	if !strings.Contains(output, "99,500") {
		t.Fatalf("expected '99,500' shared files in output:\n%s", output)
	}
	if !strings.Contains(output, "Lineage:") {
		t.Fatalf("expected 'Lineage:' section in output:\n%s", output)
	}
	if !strings.Contains(output, "day1-parent") {
		t.Fatalf("expected parent 'day1-parent' in output:\n%s", output)
	}
	if !strings.Contains(output, "- day2") {
		t.Fatalf("expected child '- day2' in output:\n%s", output)
	}
	if !strings.Contains(output, "- exp1") {
		t.Fatalf("expected child '- exp1' in output:\n%s", output)
	}
	if !strings.Contains(output, "Impact:") {
		t.Fatalf("expected 'Impact:' section in output:\n%s", output)
	}
	if !strings.Contains(output, "remove snapshot metadata") {
		t.Fatalf("expected 'remove snapshot metadata' in output:\n%s", output)
	}
	if !strings.Contains(output, "NOT delete shared data") {
		t.Fatalf("expected 'NOT delete shared data' in output:\n%s", output)
	}
	if !strings.Contains(output, "remove 500 unique snapshot file reference") {
		t.Fatalf("expected 'remove 500 unique snapshot file reference' in output:\n%s", output)
	}
	if !strings.Contains(output, "reference impact only; does not guarantee reclaimed disk space") {
		t.Fatalf("expected explicit reference-impact note in output:\n%s", output)
	}
	if !strings.Contains(output, "Dry run: no changes applied.") {
		t.Fatalf("expected 'Dry run: no changes applied.' in output:\n%s", output)
	}

	// Most importantly, verify the warning section is present
	if !strings.Contains(output, "Warning:") {
		t.Fatalf("expected 'Warning:' section in text output when has children:\n%s", output)
	}
	if !strings.Contains(output, "This snapshot is parent of:") {
		t.Fatalf("expected 'This snapshot is parent of:' in warning:\n%s", output)
	}
	if !strings.Contains(output, "Deleting it will break lineage visualization") {
		t.Fatalf("expected lineage warning text:\n%s", output)
	}
	if !strings.Contains(output, "snapshots remain fully usable") {
		t.Fatalf("expected usability note:\n%s", output)
	}
}

func TestRunSnapshotCommandDeleteDryRunTextOutputNoChildren(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalDelete := deleteSnapshotPhase
	originalPreview := snapshotDeleteLineagePreviewPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		deleteSnapshotPhase = originalDelete
		snapshotDeleteLineagePreviewPhase = originalPreview
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return storage.StorageContext{}, err
		}
		return storage.StorageContext{DB: dbconn}, nil
	}

	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, _ string) error {
		t.Fatal("deleteSnapshotPhase must not be called in --dry-run mode")
		return nil
	}
	snapshotDeleteLineagePreviewPhase = func(_ context.Context, _ *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
		return &snapshotDeleteLineagePreview{
			SnapshotID:       snapshotID,
			ParentID:         sql.NullString{Valid: false},
			ChildSnapshotIDs: []string{}, // No children
			TotalFiles:       50,
			UniqueFiles:      50,
			SharedFiles:      0,
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "root-snap"},
			flags: map[string][]string{
				"dry-run": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete --dry-run returned error: %v", err)
		}
	})

	// Verify warning section is NOT present when no children
	if strings.Contains(output, "Warning:") {
		t.Fatalf("expected NO 'Warning:' section when there are no children:\n%s", output)
	}

	// Verify other sections are still present
	if !strings.Contains(output, "Snapshot: root-snap") {
		t.Fatalf("expected snapshot header:\n%s", output)
	}
	if !strings.Contains(output, "Parent: none") {
		t.Fatalf("expected 'Parent: none':\n%s", output)
	}
	if !strings.Contains(output, "Children: none") {
		t.Fatalf("expected 'Children: none':\n%s", output)
	}
}

func TestLoadSnapshotDeleteLineagePreviewLoadsSnapshotAndChildren(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if _, err := dbconn.Exec(`CREATE TABLE snapshot (
		id TEXT PRIMARY KEY,
		parent_id TEXT NULL
	)`); err != nil {
		t.Fatalf("create snapshot table: %v", err)
	}
	if _, err := dbconn.Exec(`CREATE TABLE snapshot_file (
		snapshot_id TEXT NOT NULL,
		path_id INTEGER NOT NULL,
		logical_file_id TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create snapshot_file table: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, parent_id) VALUES
		('day1', NULL),
		('day2', 'day1'),
		('day3', 'day2'),
		('experiment1', 'day1')`); err != nil {
		t.Fatalf("seed snapshots: %v", err)
	}

	// day1: 4 files (3 unique to day1, 1 shared with day2)
	// day2: 3 files (1 shared with day1, 1 shared with day3, 1 unique to day2)
	// day3: 2 files (1 shared with day2, 1 unique to day3)
	// experiment1: 2 files (1 shared with day1, 1 unique to experiment1)
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file(snapshot_id, path_id, logical_file_id) VALUES
		('day1', 1, 'file1'),
		('day1', 2, 'file2'),
		('day1', 3, 'file3'),
		('day1', 4, 'file4'),
		('day2', 4, 'file4'),
		('day2', 5, 'file5'),
		('day2', 6, 'file6'),
		('day3', 6, 'file6'),
		('day3', 7, 'file7'),
		('experiment1', 1, 'file1'),
		('experiment1', 8, 'file8')`); err != nil {
		t.Fatalf("seed snapshot_file: %v", err)
	}

	preview, err := loadSnapshotDeleteLineagePreview(context.Background(), dbconn, "day1")
	if err != nil {
		t.Fatalf("loadSnapshotDeleteLineagePreview returned error: %v", err)
	}
	if preview.SnapshotID != "day1" {
		t.Fatalf("expected snapshot id day1, got %q", preview.SnapshotID)
	}
	if preview.ParentID.Valid {
		t.Fatalf("expected parent_id NULL for root snapshot, got %+v", preview.ParentID)
	}
	if len(preview.ChildSnapshotIDs) != 2 || preview.ChildSnapshotIDs[0] != "day2" || preview.ChildSnapshotIDs[1] != "experiment1" {
		t.Fatalf("expected ordered child IDs [day2 experiment1], got %#v", preview.ChildSnapshotIDs)
	}

	// day1 has 4 total files:
	// file1 (path_id=1): shared with experiment1 (shared)
	// file2 (path_id=2): unique to day1
	// file3 (path_id=3): unique to day1
	// file4 (path_id=4): shared with day2 (shared)
	// Expected: 4 total, 2 unique, 2 shared
	if preview.TotalFiles != 4 {
		t.Fatalf("expected 4 total files, got %d", preview.TotalFiles)
	}
	if preview.UniqueFiles != 2 {
		t.Fatalf("expected 2 unique files, got %d", preview.UniqueFiles)
	}
	if preview.SharedFiles != 2 {
		t.Fatalf("expected 2 shared files, got %d", preview.SharedFiles)
	}
}

func TestLoadSnapshotDeleteLineagePreviewNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if _, err := dbconn.Exec(`CREATE TABLE snapshot (
		id TEXT PRIMARY KEY,
		parent_id TEXT NULL
	)`); err != nil {
		t.Fatalf("create snapshot table: %v", err)
	}
	if _, err := dbconn.Exec(`CREATE TABLE snapshot_file (
		snapshot_id TEXT NOT NULL,
		path_id INTEGER NOT NULL,
		logical_file_id TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create snapshot_file table: %v", err)
	}

	_, err = loadSnapshotDeleteLineagePreview(context.Background(), dbconn, "X")
	if err == nil {
		t.Fatal("expected not found error, got nil")
	}
	if got := err.Error(); got != `snapshot "X" not found` {
		t.Fatalf("expected exact error %q, got %q", `snapshot "X" not found`, got)
	}
}

func TestLoadSnapshotDeleteLineagePreviewEdgeCaseStats(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if _, err := dbconn.Exec(`CREATE TABLE snapshot (
		id TEXT PRIMARY KEY,
		parent_id TEXT NULL
	)`); err != nil {
		t.Fatalf("create snapshot table: %v", err)
	}
	if _, err := dbconn.Exec(`CREATE TABLE snapshot_file (
		snapshot_id TEXT NOT NULL,
		path_id INTEGER NOT NULL,
		logical_file_id TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create snapshot_file table: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, parent_id) VALUES
		('empty', NULL),
		('shared-a', NULL),
		('shared-b', NULL),
		('unique-a', NULL)`); err != nil {
		t.Fatalf("seed snapshots: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file(snapshot_id, path_id, logical_file_id) VALUES
		('shared-a', 10, 'f10'),
		('shared-a', 11, 'f11'),
		('shared-b', 10, 'f10'),
		('shared-b', 11, 'f11'),
		('unique-a', 20, 'f20'),
		('unique-a', 21, 'f21')`); err != nil {
		t.Fatalf("seed snapshot_file: %v", err)
	}

	t.Run("empty snapshot totals are zero", func(t *testing.T) {
		preview, err := loadSnapshotDeleteLineagePreview(context.Background(), dbconn, "empty")
		if err != nil {
			t.Fatalf("load preview: %v", err)
		}
		if preview.TotalFiles != 0 || preview.UniqueFiles != 0 || preview.SharedFiles != 0 {
			t.Fatalf("expected empty stats 0/0/0, got total=%d unique=%d shared=%d", preview.TotalFiles, preview.UniqueFiles, preview.SharedFiles)
		}
	})

	t.Run("all files shared means unique is zero", func(t *testing.T) {
		preview, err := loadSnapshotDeleteLineagePreview(context.Background(), dbconn, "shared-a")
		if err != nil {
			t.Fatalf("load preview: %v", err)
		}
		if preview.TotalFiles != 2 || preview.UniqueFiles != 0 || preview.SharedFiles != 2 {
			t.Fatalf("expected shared stats total=2 unique=0 shared=2, got total=%d unique=%d shared=%d", preview.TotalFiles, preview.UniqueFiles, preview.SharedFiles)
		}
	})

	t.Run("all files unique means shared is zero", func(t *testing.T) {
		preview, err := loadSnapshotDeleteLineagePreview(context.Background(), dbconn, "unique-a")
		if err != nil {
			t.Fatalf("load preview: %v", err)
		}
		if preview.TotalFiles != 2 || preview.UniqueFiles != 2 || preview.SharedFiles != 0 {
			t.Fatalf("expected unique stats total=2 unique=2 shared=0, got total=%d unique=%d shared=%d", preview.TotalFiles, preview.UniqueFiles, preview.SharedFiles)
		}
	})

	t.Run("same path but different logical_file_id is NOT shared", func(t *testing.T) {
		if _, err := dbconn.Exec(`INSERT INTO snapshot(id, parent_id) VALUES
			('same-path-a', NULL),
			('same-path-b', NULL)`); err != nil {
			t.Fatalf("seed same-path snapshots: %v", err)
		}

		// Same path_id (30), different logical_file_id values. These must NOT be treated as shared.
		if _, err := dbconn.Exec(`INSERT INTO snapshot_file(snapshot_id, path_id, logical_file_id) VALUES
			('same-path-a', 30, 'L1'),
			('same-path-b', 30, 'L2')`); err != nil {
			t.Fatalf("seed same-path different-logical rows: %v", err)
		}

		preview, err := loadSnapshotDeleteLineagePreview(context.Background(), dbconn, "same-path-a")
		if err != nil {
			t.Fatalf("load preview: %v", err)
		}
		if preview.TotalFiles != 1 || preview.UniqueFiles != 1 || preview.SharedFiles != 0 {
			t.Fatalf("expected same-path/different-logical to be unique: total=1 unique=1 shared=0, got total=%d unique=%d shared=%d", preview.TotalFiles, preview.UniqueFiles, preview.SharedFiles)
		}
	})
}

func TestLoadSnapshotDeleteLineagePreviewDetectsMissingParent(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if _, err := dbconn.Exec(`CREATE TABLE snapshot (
		id TEXT PRIMARY KEY,
		parent_id TEXT NULL
	)`); err != nil {
		t.Fatalf("create snapshot table: %v", err)
	}
	if _, err := dbconn.Exec(`CREATE TABLE snapshot_file (
		snapshot_id TEXT NOT NULL,
		path_id INTEGER NOT NULL,
		logical_file_id TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create snapshot_file table: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot(id, parent_id) VALUES ('child', 'ghost-parent')`); err != nil {
		t.Fatalf("seed snapshot with missing parent: %v", err)
	}

	preview, err := loadSnapshotDeleteLineagePreview(context.Background(), dbconn, "child")
	if err != nil {
		t.Fatalf("loadSnapshotDeleteLineagePreview returned error: %v", err)
	}
	if !preview.ParentID.Valid || preview.ParentID.String != "ghost-parent" {
		t.Fatalf("expected parent_id ghost-parent, got %+v", preview.ParentID)
	}
	if !preview.ParentMissing {
		t.Fatal("expected ParentMissing=true when parent row does not exist")
	}

	output := formatSnapshotDeleteDryRunOutput("child", preview)
	if !strings.Contains(output, "Parent: (missing)") {
		t.Fatalf("expected formatted output to show Parent: (missing), got:\n%s", output)
	}
	if !strings.Contains(output, "Parent note: parent snapshot metadata is missing") {
		t.Fatalf("expected formatted output to include missing-parent note, got:\n%s", output)
	}
}

func TestPrintHelpSnapshotFlagDocs(t *testing.T) {
	output := captureStdout(t, func() {
		printHelp()
	})

	for _, expected := range []string{
		"--from records lineage metadata only",
		"--tree renders metadata lineage only",
		"--summary returns count-only diff output",
		"--dry-run performs a read-only preview and never writes data",
		"missing lineage parent metadata is shown as Parent: (missing)",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected help output to include %q, got:\n%s", expected, output)
		}
	}
}

func TestPrintHelpConfigDefaultChunkerSafetyNote(t *testing.T) {
	output := captureStdout(t, func() {
		printHelp()
	})

	if !strings.Contains(output, "config set default-chunker") {
		t.Fatalf("expected help output to include config set default-chunker row, got:\n%s", output)
	}
	if !strings.Contains(output, "Affects only new stored data. Existing data is not modified.") {
		t.Fatalf("expected help output to include default-chunker safety note, got:\n%s", output)
	}
}

func TestRunSnapshotCommandDeleteWithForceExecutesImmediately(t *testing.T) {
	// CRITICAL BEHAVIOR LOCK (v1 contract):
	// --force must execute deletion immediately without confirmation or blocking.
	// This test ensures Phase 7 dry-run feature doesn't change actual delete behavior.

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

	// Track if delete was called and when
	deleteCallOrder := []string{}
	deleteCalled := false
	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) error {
		deleteCallOrder = append(deleteCallOrder, "delete:"+snapshotID)
		deleteCalled = true
		return nil
	}

	// Execute delete with --force (no --dry-run)
	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "parent-snap"},
			flags: map[string][]string{
				"force":  {""},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand delete --force returned error: %v", err)
		}
	})

	// VERIFY: Delete must be called immediately without any prompts or delays
	if !deleteCalled {
		t.Fatalf("expected deleteSnapshotPhase to be called with --force, but it wasn't")
	}

	// VERIFY: Delete must be the only operation
	if len(deleteCallOrder) != 1 || deleteCallOrder[0] != "delete:parent-snap" {
		t.Fatalf("expected single delete operation, got: %v", deleteCallOrder)
	}

	// VERIFY: Output must confirm deletion (not simulation/preview)
	if strings.Contains(output, "dry-run") || strings.Contains(output, "simulation") {
		t.Fatalf("--force output should not mention dry-run or simulation, got: %s", output)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot delete JSON output: %v", err)
	}
	data := payload["data"].(map[string]any)

	// VERIFY: action must be "delete" not "delete_dry_run"
	if action, _ := data["action"].(string); action != "delete" {
		t.Fatalf("expected action=delete for --force, got %v", action)
	}

	// VERIFY: dry_run must be false
	if dryRun, _ := data["dry_run"].(bool); dryRun {
		t.Fatalf("expected dry_run=false for --force, got true")
	}
}

func TestRunSnapshotCommandDeleteWithForceNoCascadeDelete(t *testing.T) {
	// CRITICAL BEHAVIOR LOCK (v1 contract):
	// Deleting a parent snapshot must NOT cascade-delete children.
	// Children remain fully usable with null parent_id.

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

	// Track what gets deleted
	deletedSnapshots := []string{}
	deleteSnapshotPhase = func(_ context.Context, _ *sql.DB, snapshotID string) error {
		deletedSnapshots = append(deletedSnapshots, snapshotID)
		return nil
	}

	// Execute delete of parent snapshot with --force
	_ = captureStdout(t, func() {
		_ = runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"delete", "parent-snap"},
			flags: map[string][]string{
				"force": {""},
			},
		}, outputModeText)
	})

	// CRITICAL: Only the specified snapshot should be deleted, NOT children
	if len(deletedSnapshots) != 1 {
		t.Fatalf("expected exactly 1 snapshot deleted (only parent), got %d: %v", len(deletedSnapshots), deletedSnapshots)
	}
	if deletedSnapshots[0] != "parent-snap" {
		t.Fatalf("expected parent-snap deleted, got: %v", deletedSnapshots)
	}
}

func TestRunGCCommandJSONIncludesSnapshotRetainedLogicalFiles(t *testing.T) {
	originalRunGC := runGCPhase
	t.Cleanup(func() { runGCPhase = originalRunGC })

	runGCPhase = func(_ bool, _ string) (maintenance.GCResult, error) {
		return maintenance.GCResult{
			DryRun:                       false,
			AffectedContainers:           2,
			ContainerFilenames:           []string{"a.bin", "b.bin"},
			SnapshotRetainedContainers:   1,
			SnapshotRetainedLogicalFiles: 4,
			RetainedCurrentOnlyLogical:   2,
			RetainedSnapshotOnlyLogical:  1,
			RetainedSharedLogical:        3,
		}, nil
	}

	output := captureStdout(t, func() {
		if err := runGCCommand(parsedCommandLine{method: "gc", flags: map[string][]string{}}, outputModeJSON); err != nil {
			t.Fatalf("runGCCommand JSON returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", err, output)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data object in payload: %v", payload)
	}
	assertJSONNumber(t, data, "snapshot_retained_logical_files", 4)
	assertJSONNumber(t, data, "snapshot_retained_containers", 1)
	assertJSONNumber(t, data, "retained_current_only_logical_files", 2)
	assertJSONNumber(t, data, "retained_snapshot_only_logical_files", 1)
	assertJSONNumber(t, data, "retained_shared_logical_files", 3)
	assertJSONNumber(t, data, "affected_containers", 2)
}

func TestRunGCCommandTextOutputIncludesSnapshotRetainedLogicalFiles(t *testing.T) {
	originalRunGC := runGCPhase
	t.Cleanup(func() { runGCPhase = originalRunGC })

	t.Run("appears in non-dry-run output when non-zero", func(t *testing.T) {
		runGCPhase = func(_ bool, _ string) (maintenance.GCResult, error) {
			return maintenance.GCResult{
				DryRun:                       false,
				AffectedContainers:           1,
				ContainerFilenames:           []string{"c.bin"},
				SnapshotRetainedLogicalFiles: 3,
				RetainedCurrentOnlyLogical:   4,
				RetainedSnapshotOnlyLogical:  1,
				RetainedSharedLogical:        2,
			}, nil
		}
		output := captureStdout(t, func() {
			if err := runGCCommand(parsedCommandLine{method: "gc", flags: map[string][]string{}}, outputModeText); err != nil {
				t.Fatalf("runGCCommand text returned error: %v", err)
			}
		})
		if !strings.Contains(output, "GC retained snapshot-protected logical files: 3") {
			t.Fatalf("expected snapshot-retained logical files line in output:\n%s", output)
		}
		if !strings.Contains(output, "GC retention roots (logical files): current_only=4 snapshot_only=1 shared=2") {
			t.Fatalf("expected retention roots line in output:\n%s", output)
		}
	})

	t.Run("appears in dry-run output when non-zero", func(t *testing.T) {
		runGCPhase = func(_ bool, _ string) (maintenance.GCResult, error) {
			return maintenance.GCResult{
				DryRun:                       true,
				AffectedContainers:           1,
				ContainerFilenames:           []string{"d.bin"},
				SnapshotRetainedLogicalFiles: 5,
				RetainedCurrentOnlyLogical:   1,
				RetainedSnapshotOnlyLogical:  5,
				RetainedSharedLogical:        0,
			}, nil
		}
		output := captureStdout(t, func() {
			if err := runGCCommand(parsedCommandLine{method: "gc", flags: map[string][]string{"dry-run": {"true"}}}, outputModeText); err != nil {
				t.Fatalf("runGCCommand dry-run text returned error: %v", err)
			}
		})
		if !strings.Contains(output, "GC retained snapshot-protected logical files: 5") {
			t.Fatalf("expected snapshot-retained logical files line in dry-run output:\n%s", output)
		}
		if !strings.Contains(output, "GC retention roots (logical files): current_only=1 snapshot_only=5 shared=0") {
			t.Fatalf("expected retention roots line in dry-run output:\n%s", output)
		}
	})

	t.Run("absent when zero", func(t *testing.T) {
		runGCPhase = func(_ bool, _ string) (maintenance.GCResult, error) {
			return maintenance.GCResult{
				DryRun:                       false,
				AffectedContainers:           1,
				ContainerFilenames:           []string{"e.bin"},
				SnapshotRetainedLogicalFiles: 0,
				RetainedCurrentOnlyLogical:   0,
				RetainedSnapshotOnlyLogical:  0,
				RetainedSharedLogical:        0,
			}, nil
		}
		output := captureStdout(t, func() {
			if err := runGCCommand(parsedCommandLine{method: "gc", flags: map[string][]string{}}, outputModeText); err != nil {
				t.Fatalf("runGCCommand text returned error: %v", err)
			}
		})
		if strings.Contains(output, "snapshot-protected") {
			t.Fatalf("expected snapshot-retained line to be absent when zero, got output:\n%s", output)
		}
		if strings.Contains(output, "GC retention roots (logical files):") {
			t.Fatalf("expected retention roots line to be absent when all buckets are zero, got output:\n%s", output)
		}
	})
}

func TestRunStatsCommandJSONIncludesSnapshotRetention(t *testing.T) {
	originalRunStats := runObservabilityStatsPhase
	t.Cleanup(func() {
		runObservabilityStatsPhase = originalRunStats
	})

	runObservabilityStatsPhase = func(opts observability.StatsOptions) (*observability.StatsResult, error) {
		records := []observability.ContainerStatRecord{}
		if opts.IncludeContainers {
			records = append(records, observability.ContainerStatRecord{ID: 10, Filename: "ctr-10.bin", TotalBytes: 99})
		}

		return &observability.StatsResult{
			Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
			Logical: observability.LogicalStats{
				TotalFiles:             7,
				EstimatedDedupRatioPct: 25,
			},
			Chunks: observability.ChunkStats{
				TotalReferences:  10000,
				UniqueReferenced: 7500,
				CountsByVersion: map[string]int64{
					"v1-simple-rolling": 3,
					"v2-fastcdc":        2,
					"unknown":           1,
				},
				BytesByVersion: map[string]int64{
					"v1-simple-rolling": 30,
					"v2-fastcdc":        20,
					"unknown":           9,
				},
				ChunkerVersions: []observability.VersionStat{
					{Version: "unknown", Chunks: 1, Bytes: 9},
					{Version: "v1-simple-rolling", Chunks: 3, Bytes: 30},
					{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
				},
			},
			Containers: observability.ContainerStats{Records: records},
			Retention: observability.RetentionStats{
				CurrentOnlyLogicalFiles:        2,
				CurrentOnlyBytes:               256,
				SnapshotReferencedLogicalFiles: 3,
				SnapshotReferencedBytes:        768,
				SnapshotOnlyLogicalFiles:       1,
				SnapshotOnlyBytes:              128,
				SharedLogicalFiles:             2,
				SharedBytes:                    640,
			},
		}, nil
	}

	output := captureStdout(t, func() {
		if err := runStatsCommand(parsedCommandLine{method: "stats", flags: map[string][]string{"containers": {""}}}, outputModeJSON); err != nil {
			t.Fatalf("runStatsCommand JSON returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("parse JSON payload: %v\noutput=%q", err, output)
	}
	data := payload
	retentionData, ok := data["retention"].(map[string]any)
	if !ok {
		t.Fatalf("missing retention object in payload: %v", data)
	}
	chunksData, ok := data["chunks"].(map[string]any)
	if !ok {
		t.Fatalf("missing chunks object in payload: %v", data)
	}
	chunkerVersions, ok := chunksData["chunker_versions"].([]any)
	if !ok {
		t.Fatalf("missing chunks.chunker_versions array in payload: %v", chunksData)
	}
	if len(chunkerVersions) != 3 {
		t.Fatalf("expected 3 chunker_versions entries, got %d", len(chunkerVersions))
	}

	entry0, ok := chunkerVersions[0].(map[string]any)
	if !ok {
		t.Fatalf("expected chunker_versions[0] object, got %T", chunkerVersions[0])
	}
	entry1, ok := chunkerVersions[1].(map[string]any)
	if !ok {
		t.Fatalf("expected chunker_versions[1] object, got %T", chunkerVersions[1])
	}
	entry2, ok := chunkerVersions[2].(map[string]any)
	if !ok {
		t.Fatalf("expected chunker_versions[2] object, got %T", chunkerVersions[2])
	}

	if got, _ := entry0["version"].(string); got != "unknown" {
		t.Fatalf("expected chunker_versions[0].version=unknown, got %q", got)
	}
	assertJSONNumber(t, entry0, "chunks", 1)
	assertJSONNumber(t, entry0, "bytes", 9)
	if got, _ := entry1["version"].(string); got != "v1-simple-rolling" {
		t.Fatalf("expected chunker_versions[1].version=v1-simple-rolling, got %q", got)
	}
	assertJSONNumber(t, entry1, "chunks", 3)
	assertJSONNumber(t, entry1, "bytes", 30)
	if got, _ := entry2["version"].(string); got != "v2-fastcdc" {
		t.Fatalf("expected chunker_versions[2].version=v2-fastcdc, got %q", got)
	}
	assertJSONNumber(t, entry2, "chunks", 2)
	assertJSONNumber(t, entry2, "bytes", 20)
	repositoryData, ok := data["repository"].(map[string]any)
	if !ok {
		t.Fatalf("missing repository object in payload: %v", data)
	}
	logicalData, ok := data["logical"].(map[string]any)
	if !ok {
		t.Fatalf("missing logical object in payload: %v", data)
	}
	if raw, ok := repositoryData["active_write_chunker"].(string); !ok || raw != "v2-fastcdc" {
		t.Fatalf("active_write_chunker mismatch: got=%v payload=%v", repositoryData["active_write_chunker"], data)
	}
	assertJSONNumber(t, chunksData, "total_references", 10000)
	assertJSONNumber(t, chunksData, "unique_referenced", 7500)
	if raw, ok := logicalData["estimated_dedup_ratio_pct"].(float64); !ok || raw != 25 {
		t.Fatalf("estimated_dedup_ratio_pct mismatch: got=%v payload=%v", logicalData["estimated_dedup_ratio_pct"], data)
	}
	assertJSONNumber(t, retentionData, "current_only_logical_files", 2)
	assertJSONNumber(t, retentionData, "snapshot_referenced_logical_files", 3)
	assertJSONNumber(t, retentionData, "snapshot_only_logical_files", 1)
	assertJSONNumber(t, retentionData, "shared_logical_files", 2)
	assertJSONNumber(t, retentionData, "snapshot_referenced_bytes", 768)
	assertJSONNumber(t, retentionData, "snapshot_only_bytes", 128)
	assertJSONNumber(t, retentionData, "shared_bytes", 640)
	assertJSONNumber(t, retentionData, "current_only_bytes", 256)

	containersData, ok := data["containers"].(map[string]any)
	if !ok {
		t.Fatalf("missing containers object in payload: %v", data)
	}
	records, ok := containersData["records"].([]any)
	if !ok {
		t.Fatalf("expected containers.records array when --containers is set: %v", containersData)
	}
	if len(records) != 1 {
		t.Fatalf("expected one container record, got %d", len(records))
	}
}

func TestStatsCommandHuman(t *testing.T) {
	originalRunStats := runObservabilityStatsPhase
	t.Cleanup(func() { runObservabilityStatsPhase = originalRunStats })

	runObservabilityStatsPhase = func(opts observability.StatsOptions) (*observability.StatsResult, error) {
		return &observability.StatsResult{
			Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
			Logical:    observability.LogicalStats{TotalFiles: 1, CompletedFiles: 1, TotalSizeBytes: 1024, CompletedSizeBytes: 1024},
			Chunks:     observability.ChunkStats{TotalChunks: 1, CompletedChunks: 1, CompletedBytes: 512},
		}, nil
	}

	output := captureStdout(t, func() {
		if err := runStatsCommand(parsedCommandLine{method: "stats", flags: map[string][]string{}}, outputModeText); err != nil {
			t.Fatalf("runStatsCommand human returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Coldkeep repository stats") {
		t.Fatalf("expected human stats output header, got:\n%s", output)
	}
}

func TestStatsCommandJSON(t *testing.T) {
	originalRunStats := runObservabilityStatsPhase
	t.Cleanup(func() { runObservabilityStatsPhase = originalRunStats })

	runObservabilityStatsPhase = func(opts observability.StatsOptions) (*observability.StatsResult, error) {
		return &observability.StatsResult{
			Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
			Logical:    observability.LogicalStats{TotalFiles: 1},
		}, nil
	}

	output := captureStdout(t, func() {
		if err := runStatsCommand(parsedCommandLine{method: "stats", flags: map[string][]string{}}, outputModeJSON); err != nil {
			t.Fatalf("runStatsCommand json returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("parse json payload: %v\noutput=%q", err, output)
	}
	repo, ok := payload["repository"].(map[string]any)
	if !ok {
		t.Fatalf("missing repository object: %v", payload)
	}
	if got, _ := repo["active_write_chunker"].(string); got != "v2-fastcdc" {
		t.Fatalf("active_write_chunker mismatch: got=%q", got)
	}
}

func TestStatsCommandJSONShorthand(t *testing.T) {
	parsed := parsedCommandLine{method: "stats", flags: map[string][]string{"json": {""}}}
	mode, err := resolveOutputMode(parsed)
	if err != nil {
		t.Fatalf("resolveOutputMode: %v", err)
	}
	if mode != outputModeJSON {
		t.Fatalf("expected outputModeJSON for --json shorthand, got %q", mode)
	}
}

func TestStatsCommandContainers(t *testing.T) {
	originalRunStats := runObservabilityStatsPhase
	t.Cleanup(func() { runObservabilityStatsPhase = originalRunStats })

	var includeContainers bool
	runObservabilityStatsPhase = func(opts observability.StatsOptions) (*observability.StatsResult, error) {
		includeContainers = opts.IncludeContainers
		return &observability.StatsResult{}, nil
	}

	if err := runStatsCommand(parsedCommandLine{method: "stats", flags: map[string][]string{"containers": {""}}}, outputModeText); err != nil {
		t.Fatalf("runStatsCommand with --containers returned error: %v", err)
	}
	if !includeContainers {
		t.Fatal("expected IncludeContainers=true when --containers is set")
	}
}

func TestStatsCommandConflictingOutputFlags(t *testing.T) {
	_, err := resolveOutputMode(parsedCommandLine{
		method: "stats",
		flags: map[string][]string{
			"json":   {""},
			"output": {"human"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot combine --json with --output human") {
		t.Fatalf("expected output flag conflict error, got %v", err)
	}
}

func TestPrintStatsReportIncludesSnapshotRetention(t *testing.T) {
	output := captureStdout(t, func() {
		printStatsReport(&maintenance.StatsResult{
			ActiveWriteChunker:     "v2-fastcdc",
			TotalChunkReferences:   10000,
			UniqueReferencedChunks: 7500,
			EstimatedDedupRatioPct: 25,
			LogicalFileCountsByVersion: map[string]int64{
				"v1-simple-rolling": 6,
				"v2-fastcdc":        1,
				"unknown":           2,
			},
			ChunkCountsByVersion: map[string]int64{
				"v1-simple-rolling": 4,
				"v2-fastcdc":        1,
				"unknown":           2,
			},
			ChunkBytesByVersion: map[string]int64{
				"v1-simple-rolling": 2 * 1024 * 1024,
				"v2-fastcdc":        1 * 1024 * 1024,
				"unknown":           512,
			},
			SnapshotRetention: maintenance.SnapshotRetentionStats{
				CurrentOnlyLogicalFiles:        4,
				CurrentOnlyBytes:               2 * 1024 * 1024,
				SnapshotReferencedLogicalFiles: 3,
				SnapshotReferencedBytes:        5 * 1024 * 1024,
				SnapshotOnlyLogicalFiles:       1,
				SnapshotOnlyBytes:              1 * 1024 * 1024,
				SharedLogicalFiles:             2,
				SharedBytes:                    4 * 1024 * 1024,
			},
		})
	})

	for _, want := range []string{
		"Active chunker (new writes):     v2-fastcdc",
		"Snapshot retention:",
		"Chunker Distribution:",
		"v1-simple-rolling:     4 chunks",
		"v2-fastcdc:            1 chunks",
		"unknown:               2 chunks",
		"Stored Data by Chunker:",
		"v1-simple-rolling:     0.00 GB",
		"v2-fastcdc:            0.00 GB",
		"unknown:               0.00 GB",
		"Logical Files by Chunker:",
		"v1-simple-rolling:     6 files",
		"v2-fastcdc:            1 files",
		"unknown:               2 files",
		"⚠ Repository contains multiple chunker versions.",
		"This is expected after upgrades or configuration changes.",
		"Dedup Signal:",
		"Total chunk references:  10000",
		"Unique referenced chunks:7500",
		"Estimated dedup ratio:   25.00%",
		"Current-only logical files:    4 (2.00 MB)",
		"Snapshot-referenced files:     3 (5.00 MB)",
		"Snapshot-only logical files:   1 (1.00 MB)",
		"Shared logical files:          2 (4.00 MB)",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected stats report to contain %q, got output:\n%s", want, output)
		}
	}
}

func TestPrintStatsReportOmitsMixedChunkerWarningWhenSingleKnownVersion(t *testing.T) {
	output := captureStdout(t, func() {
		printStatsReport(&maintenance.StatsResult{
			ActiveWriteChunker: "v2-fastcdc",
			ChunkCountsByVersion: map[string]int64{
				"v2-fastcdc": 10,
				"unknown":    2,
			},
			LogicalFileCountsByVersion: map[string]int64{
				"v2-fastcdc": 4,
				"unknown":    1,
			},
		})
	})

	if strings.Contains(output, "Repository contains multiple chunker versions") {
		t.Fatalf("expected no mixed-chunker warning for single known version, got output:\n%s", output)
	}
}

func TestRunSnapshotCommandDiffForwardsAndFormatsJSON(t *testing.T) {
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

	var gotBase, gotTarget string
	diffSnapshotsPhase = func(_ context.Context, _ *sql.DB, baseID, targetID string, _ *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
		gotBase = baseID
		gotTarget = targetID
		return &snapshot.SnapshotDiffResult{
			BaseSnapshotID:   baseID,
			TargetSnapshotID: targetID,
			Entries: []snapshot.SnapshotDiffEntry{
				{Path: "docs/new.txt", Type: "added", TargetLogicalID: sql.NullInt64{Int64: 2, Valid: true}},
				{Path: "docs/old.txt", Type: "removed", BaseLogicalID: sql.NullInt64{Int64: 1, Valid: true}},
				{Path: "docs/config.yaml", Type: "modified", BaseLogicalID: sql.NullInt64{Int64: 3, Valid: true}, TargetLogicalID: sql.NullInt64{Int64: 4, Valid: true}},
			},
			Summary: snapshot.SnapshotDiffSummary{Added: 1, Removed: 1, Modified: 1},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "snap-1", "snap-2"},
			flags: map[string][]string{
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand diff returned error: %v", err)
		}
	})

	if gotBase != "snap-1" || gotTarget != "snap-2" {
		t.Fatalf("expected diff args snap-1/snap-2, got %q/%q", gotBase, gotTarget)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot diff JSON output: %v output=%q", err, output)
	}
	if got, _ := payload["command"].(string); got != "snapshot diff" {
		t.Fatalf("expected command=snapshot diff, got payload=%v", payload)
	}
	data := payload["data"].(map[string]any)
	if got := int(data["entry_count"].(float64)); got != 3 {
		t.Fatalf("expected entry_count=3, got %d", got)
	}
	if got := int(data["matched_entry_count"].(float64)); got != 3 {
		t.Fatalf("expected matched_entry_count=3, got %d", got)
	}
	if got := int(data["total_diff_entry_count"].(float64)); got != 3 {
		t.Fatalf("expected total_diff_entry_count=3, got %d", got)
	}
	summary := data["summary"].(map[string]any)
	if int64(summary["added"].(float64)) != 1 || int64(summary["removed"].(float64)) != 1 || int64(summary["modified"].(float64)) != 1 {
		t.Fatalf("unexpected diff summary payload: %v", data)
	}
}

func TestRunSnapshotCommandDiffFilterJSONShowsMatchedAndTotalCounts(t *testing.T) {
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
				{Path: "docs/new.txt", Type: snapshot.DiffAdded, TargetLogicalID: sql.NullInt64{Int64: 2, Valid: true}},
				{Path: "docs/old.txt", Type: snapshot.DiffRemoved, BaseLogicalID: sql.NullInt64{Int64: 1, Valid: true}},
				{Path: "docs/config.yaml", Type: snapshot.DiffModified, BaseLogicalID: sql.NullInt64{Int64: 3, Valid: true}, TargetLogicalID: sql.NullInt64{Int64: 4, Valid: true}},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "snap-1", "snap-2"},
			flags: map[string][]string{
				"filter": {"added"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand diff returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot diff JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got := int(data["entry_count"].(float64)); got != 1 {
		t.Fatalf("expected filtered entry_count=1, got %d", got)
	}
	if got := int(data["matched_entry_count"].(float64)); got != 1 {
		t.Fatalf("expected matched_entry_count=1, got %d", got)
	}
	if got := int(data["total_diff_entry_count"].(float64)); got != 3 {
		t.Fatalf("expected total_diff_entry_count=3, got %d", got)
	}
	summary := data["summary"].(map[string]any)
	if int(summary["added"].(float64)) != 1 || int(summary["removed"].(float64)) != 0 || int(summary["modified"].(float64)) != 0 {
		t.Fatalf("unexpected filtered summary: %v", summary)
	}
}

func TestRunSnapshotCommandDiffTextShowsMatchedAndTotalCounts(t *testing.T) {
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
				{Path: "docs/new.txt", Type: snapshot.DiffAdded, TargetLogicalID: sql.NullInt64{Int64: 2, Valid: true}},
				{Path: "docs/old.txt", Type: snapshot.DiffRemoved, BaseLogicalID: sql.NullInt64{Int64: 1, Valid: true}},
				{Path: "docs/config.yaml", Type: snapshot.DiffModified, BaseLogicalID: sql.NullInt64{Int64: 3, Valid: true}, TargetLogicalID: sql.NullInt64{Int64: 4, Valid: true}},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "snap-1", "snap-2"},
			flags: map[string][]string{
				"filter": {"added"},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand diff returned error: %v", err)
		}
	})

	if !strings.Contains(output, "entries (matched): 1") {
		t.Fatalf("expected text output to include matched entry count, got: %q", output)
	}
	if !strings.Contains(output, "entries (total): 3") {
		t.Fatalf("expected text output to include total entry count, got: %q", output)
	}
}

func TestRunSnapshotCommandDiffSummaryTextShowsOnlySummary(t *testing.T) {
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
				{Path: "docs/new.txt", Type: snapshot.DiffAdded, TargetLogicalID: sql.NullInt64{Int64: 2, Valid: true}},
				{Path: "docs/old.txt", Type: snapshot.DiffRemoved, BaseLogicalID: sql.NullInt64{Int64: 1, Valid: true}},
				{Path: "docs/config.yaml", Type: snapshot.DiffModified, BaseLogicalID: sql.NullInt64{Int64: 3, Valid: true}, TargetLogicalID: sql.NullInt64{Int64: 4, Valid: true}},
			},
		}, nil
	}
	diffSnapshotSummaryPhase = func(_ context.Context, _ *sql.DB, _, _ string) (*snapshot.SnapshotDiffSummary, error) {
		return &snapshot.SnapshotDiffSummary{Added: 1, Removed: 1, Modified: 1}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "day1", "day2"},
			flags: map[string][]string{
				"summary": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand diff summary returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Snapshot diff: day1 -> day2") {
		t.Fatalf("expected summary header, got: %q", output)
	}
	if !strings.Contains(output, "Added:     1 files") || !strings.Contains(output, "Removed:   1 files") || !strings.Contains(output, "Modified:  1 files") {
		t.Fatalf("expected summary counters, got: %q", output)
	}
	if !strings.Contains(output, "Total changes: 3") {
		t.Fatalf("expected total changes line, got: %q", output)
	}
	if strings.Contains(output, "+ docs/new.txt") || strings.Contains(output, "- docs/old.txt") || strings.Contains(output, "~ docs/config.yaml") {
		t.Fatalf("did not expect detailed entries in summary mode, got: %q", output)
	}
}

func TestRunSnapshotCommandDiffSummaryJSONOmitsEntries(t *testing.T) {
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
				{Path: "docs/new.txt", Type: snapshot.DiffAdded, TargetLogicalID: sql.NullInt64{Int64: 2, Valid: true}},
				{Path: "docs/old.txt", Type: snapshot.DiffRemoved, BaseLogicalID: sql.NullInt64{Int64: 1, Valid: true}},
			},
		}, nil
	}
	diffSnapshotSummaryPhase = func(_ context.Context, _ *sql.DB, _, _ string) (*snapshot.SnapshotDiffSummary, error) {
		return &snapshot.SnapshotDiffSummary{Added: 1, Removed: 1, Modified: 0}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "day1", "day2"},
			flags: map[string][]string{
				"summary": {""},
				"output":  {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand diff summary returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot diff JSON output: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if summaryMode, ok := data["summary_mode"].(bool); !ok || !summaryMode {
		t.Fatalf("expected summary_mode=true, got data=%v", data)
	}
	if _, exists := data["entries"]; exists {
		t.Fatalf("did not expect entries in summary mode JSON, got data=%v", data)
	}
	summary := data["summary"].(map[string]any)
	if int64(summary["added"].(float64)) != 1 || int64(summary["removed"].(float64)) != 1 || int64(summary["modified"].(float64)) != 0 {
		t.Fatalf("unexpected summary payload: %v", summary)
	}
}

func TestRunSnapshotCommandDiffSummaryUsesSQLFastPathWithoutFilters(t *testing.T) {
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

	calledSummary := false
	calledDetailed := false
	diffSnapshotSummaryPhase = func(_ context.Context, _ *sql.DB, _, _ string) (*snapshot.SnapshotDiffSummary, error) {
		calledSummary = true
		return &snapshot.SnapshotDiffSummary{Added: 2, Removed: 1, Modified: 3}, nil
	}
	diffSnapshotsPhase = func(_ context.Context, _ *sql.DB, _, _ string, _ *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
		calledDetailed = true
		return &snapshot.SnapshotDiffResult{}, nil
	}

	if err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"diff", "base", "target"},
		flags: map[string][]string{
			"summary": {""},
		},
	}, outputModeText); err != nil {
		t.Fatalf("runSnapshotCommand diff summary returned error: %v", err)
	}

	if !calledSummary {
		t.Fatal("expected SQL summary fast path to be called")
	}
	if calledDetailed {
		t.Fatal("did not expect detailed diff path to be called in unfiltered summary mode")
	}
}

func TestRunSnapshotCommandDiffSummaryWithFilterUsesDetailedPath(t *testing.T) {
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

	calledSummary := false
	calledDetailed := false
	diffSnapshotSummaryPhase = func(_ context.Context, _ *sql.DB, _, _ string) (*snapshot.SnapshotDiffSummary, error) {
		calledSummary = true
		return &snapshot.SnapshotDiffSummary{}, nil
	}
	diffSnapshotsPhase = func(_ context.Context, _ *sql.DB, baseID, targetID string, _ *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
		calledDetailed = true
		return &snapshot.SnapshotDiffResult{
			BaseSnapshotID:   baseID,
			TargetSnapshotID: targetID,
			Entries: []snapshot.SnapshotDiffEntry{
				{Path: "docs/new.txt", Type: snapshot.DiffAdded},
				{Path: "docs/config.yaml", Type: snapshot.DiffModified},
			},
		}, nil
	}

	if err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"diff", "base", "target"},
		flags: map[string][]string{
			"summary": {""},
			"filter":  {"modified"},
		},
	}, outputModeText); err != nil {
		t.Fatalf("runSnapshotCommand diff summary/filter returned error: %v", err)
	}

	if calledSummary {
		t.Fatal("did not expect SQL summary fast path when filter is present")
	}
	if !calledDetailed {
		t.Fatal("expected detailed diff path when filter is present")
	}
}

func TestRunSnapshotCommandDiffForwardsSnapshotQuery(t *testing.T) {
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

	var gotQuery *snapshot.SnapshotQuery
	diffSnapshotsPhase = func(_ context.Context, _ *sql.DB, _, _ string, query *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
		gotQuery = query
		return &snapshot.SnapshotDiffResult{}, nil
	}

	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"diff", "snap-1", "snap-2"},
		flags: map[string][]string{
			"path":   {"./docs/a.txt", "docs/b.txt"},
			"prefix": {"./docs//", "images/"},
		},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSnapshotCommand diff returned error: %v", err)
	}
	if gotQuery == nil {
		t.Fatal("expected diff query to be forwarded")
	}
	if len(gotQuery.ExactPaths) != 2 {
		t.Fatalf("expected 2 exact paths, got %#v", gotQuery.ExactPaths)
	}
	if len(gotQuery.Prefixes) != 2 || gotQuery.Prefixes[0] != "docs/" || gotQuery.Prefixes[1] != "images/" {
		t.Fatalf("expected normalized prefixes [docs/ images/], got %#v", gotQuery.Prefixes)
	}
}

func TestRunSnapshotCommandDiffFilterValidation(t *testing.T) {
	err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"diff", "snap-1", "snap-2"},
		flags: map[string][]string{
			"filter": {"invalid"},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "invalid --filter value") {
		t.Fatalf("expected invalid filter usage error, got: %v", err)
	}
}

func TestRunRepairCommandRejectsUnknownTarget(t *testing.T) {
	err := runRepairCommand(parsedCommandLine{
		method:      "repair",
		positionals: []string{"wrong-target"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "Usage: coldkeep repair ref-counts") {
		t.Fatalf("expected repair usage error, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestRunRepairCommandJSONSuccess(t *testing.T) {
	originalRepair := repairLogicalRefCountsPhase
	defer func() {
		repairLogicalRefCountsPhase = originalRepair
	}()

	repairLogicalRefCountsPhase = func() (maintenance.RepairLogicalRefCountsResult, error) {
		return maintenance.RepairLogicalRefCountsResult{
			ScannedLogicalFiles:    4,
			UpdatedLogicalFiles:    2,
			OrphanPhysicalFileRows: 0,
		}, nil
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
			t.Fatalf("runRepairCommand json failed: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse repair JSON output: %v output=%q", err, output)
	}
	if got, _ := payload["command"].(string); got != "repair" {
		t.Fatalf("expected command=repair, got payload=%v", payload)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("repair JSON missing data object: payload=%v", payload)
	}
	if got, _ := data["target"].(string); got != "ref-counts" {
		t.Fatalf("expected target=ref-counts, got payload=%v", payload)
	}
	if got := int64(data["updated_logical_files"].(float64)); got != 2 {
		t.Fatalf("expected updated_logical_files=2, got payload=%v", payload)
	}
}

func TestRunRepairCommandIntegrityFailureUsesVerifyExitClass(t *testing.T) {
	originalRepair := repairLogicalRefCountsPhase
	defer func() {
		repairLogicalRefCountsPhase = originalRepair
	}()

	repairLogicalRefCountsPhase = func() (maintenance.RepairLogicalRefCountsResult, error) {
		return maintenance.RepairLogicalRefCountsResult{}, errors.New("ref_count repair refused: orphan physical_file rows=1")
	}

	err := runRepairCommand(parsedCommandLine{
		method:      "repair",
		positionals: []string{"ref-counts"},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "repair ref-counts failed") {
		t.Fatalf("expected repair integrity failure, got: %v", err)
	}
	if got := classifyExitCode(err); got != exitVerify {
		t.Fatalf("expected verify exit code %d, got %d", exitVerify, got)
	}
}

func TestRunRepairCommandBatchJSONPartialFailure(t *testing.T) {
	originalRepair := repairLogicalRefCountsPhase
	defer func() {
		repairLogicalRefCountsPhase = originalRepair
	}()

	repairLogicalRefCountsPhase = func() (maintenance.RepairLogicalRefCountsResult, error) {
		return maintenance.RepairLogicalRefCountsResult{
			ScannedLogicalFiles:    4,
			UpdatedLogicalFiles:    1,
			OrphanPhysicalFileRows: 0,
		}, nil
	}

	var runErr error
	output := captureStdout(t, func() {
		runErr = runRepairCommand(parsedCommandLine{
			method:      "repair",
			positionals: []string{"ref-counts", "unknown-target", "ref-counts"},
			flags: map[string][]string{
				"batch":  {""},
				"output": {"json"},
			},
		}, outputModeJSON)
	})

	if runErr == nil {
		t.Fatal("expected non-nil error for batch partial failure")
	}
	if got := classifyExitCode(runErr); got != exitUsage {
		t.Fatalf("expected usage exit code %d for mixed valid/invalid targets, got %d", exitUsage, got)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse batch repair JSON output: %v output=%q", err, output)
	}
	if got, _ := payload["status"].(string); got != "partial_failure" {
		t.Fatalf("expected status=partial_failure, got payload=%v", payload)
	}
	if got, _ := payload["command"].(string); got != "repair" {
		t.Fatalf("expected command=repair, got payload=%v", payload)
	}

	summary, ok := payload["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary object, got payload=%v", payload)
	}
	if int(summary["success"].(float64)) != 1 || int(summary["failed"].(float64)) != 1 || int(summary["skipped"].(float64)) != 1 {
		t.Fatalf("unexpected summary for repair batch partial failure: %v", summary)
	}
}

func TestRunRepairCommandBatchInvariantFailureUsesVerifyExitAndMetadata(t *testing.T) {
	originalRepair := repairLogicalRefCountsPhase
	defer func() {
		repairLogicalRefCountsPhase = originalRepair
	}()

	repairLogicalRefCountsPhase = func() (maintenance.RepairLogicalRefCountsResult, error) {
		return maintenance.RepairLogicalRefCountsResult{}, invariants.New(
			invariants.CodeRepairRefusedOrphanRows,
			"ref_count repair refused: orphan physical_file rows=1",
			nil,
		)
	}

	var runErr error
	output := captureStdout(t, func() {
		runErr = runRepairCommand(parsedCommandLine{
			method:      "repair",
			positionals: []string{"ref-counts"},
			flags: map[string][]string{
				"batch":  {""},
				"output": {"json"},
			},
		}, outputModeJSON)
	})

	if runErr == nil {
		t.Fatal("expected non-nil error for invariant-coded batch repair failure")
	}
	if got := classifyExitCode(runErr); got != exitVerify {
		t.Fatalf("expected verify exit code %d, got %d", exitVerify, got)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse batch repair JSON output: %v output=%q", err, output)
	}
	results, ok := payload["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("expected one result in batch repair payload, got=%v payload=%v", results, payload)
	}
	first, _ := results[0].(map[string]any)
	if got, _ := first["invariant_code"].(string); got != invariants.CodeRepairRefusedOrphanRows {
		t.Fatalf("expected invariant_code=%s, got first=%v", invariants.CodeRepairRefusedOrphanRows, first)
	}
	if action, _ := first["recommended_action"].(string); !strings.Contains(action, "orphan physical_file") {
		t.Fatalf("expected recommended_action to mention orphan physical_file handling, got first=%v", first)
	}
}

func TestRunSimulateGCCommandJSONNestedSchema(t *testing.T) {
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() { runObservabilitySimulateGCPhase = originalSimulate })

	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		return &observability.SimulationResult{
			GeneratedAtUTC: time.Date(2026, time.April, 26, 10, 0, 0, 0, time.UTC),
			Kind:           observability.SimulationKindGC,
			Exact:          true,
			Mutated:        false,
			GC: &observability.GCSimulationResult{
				GeneratedAtUTC: time.Date(2026, time.April, 26, 10, 0, 0, 0, time.UTC),
				Kind:           observability.SimulationKindGC,
				Exact:          true,
				Mutated:        false,
				Assumptions: observability.GCSimulationAssumptions{
					DeletedSnapshots: opts.AssumeDeletedSnapshots,
				},
				Summary: observability.GCSimulationSummary{
					ReachableChunks:            10,
					UnreachableChunks:          2,
					LogicallyReclaimableBytes:  200,
					PhysicallyReclaimableBytes: 100,
					FullyReclaimableContainers: 1,
					PartiallyDeadContainers:    1,
				},
				Containers: []observability.ContainerSimulationImpact{{
					ContainerID:        7,
					Filename:           "c007.bin",
					TotalBytes:         100,
					LiveBytesAfterGC:   0,
					ReclaimableBytes:   100,
					ReclaimableChunks:  1,
					TotalChunks:        1,
					FullyReclaimable:   true,
					RequiresCompaction: false,
				}},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSimulateGCCommand(parsedCommandLine{
			method:      "simulate",
			positionals: []string{"gc"},
			flags: map[string][]string{
				"json":            {""},
				"delete-snapshot": {"s1"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSimulateGCCommand JSON returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse simulate gc JSON: %v output=%q", err, output)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data object: %v", payload)
	}
	gcNode, ok := data["gc"].(map[string]any)
	if !ok {
		t.Fatalf("missing gc object: %v", data)
	}
	if got, _ := gcNode["kind"].(string); got != "gc" {
		t.Fatalf("expected gc.kind=gc, got %v", gcNode["kind"])
	}
	if got, _ := gcNode["exact"].(bool); !got {
		t.Fatalf("expected gc.exact=true, got %v", gcNode["exact"])
	}
	if got, _ := gcNode["mutated"].(bool); got {
		t.Fatalf("expected gc.mutated=false, got %v", gcNode["mutated"])
	}
	assumptions, ok := gcNode["assumptions"].(map[string]any)
	if !ok {
		t.Fatalf("missing gc.assumptions object: %v", gcNode)
	}
	deleted, ok := assumptions["deleted_snapshots"].([]any)
	if !ok || len(deleted) != 1 || deleted[0] != "s1" {
		t.Fatalf("unexpected deleted_snapshots: %v", assumptions["deleted_snapshots"])
	}
	summary, ok := gcNode["summary"].(map[string]any)
	if !ok {
		t.Fatalf("missing gc.summary object: %v", gcNode)
	}
	assertJSONNumber(t, summary, "reachable_chunks", 10)
	assertJSONNumber(t, summary, "unreachable_chunks", 2)
	assertJSONNumber(t, summary, "logically_reclaimable_bytes", 200)
	assertJSONNumber(t, summary, "physically_reclaimable_bytes", 100)
	assertJSONNumber(t, summary, "fully_reclaimable_containers", 1)
	assertJSONNumber(t, summary, "partially_dead_containers", 1)
	if _, exists := gcNode["containers"]; exists {
		t.Fatalf("expected gc.containers omitted without --containers flag, got %v", gcNode["containers"])
	}
}

func TestRunSimulateGCCommandTextOutputFromNestedSummary(t *testing.T) {
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() { runObservabilitySimulateGCPhase = originalSimulate })

	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		return &observability.SimulationResult{
			Kind:    observability.SimulationKindGC,
			Exact:   true,
			Mutated: false,
			GC: &observability.GCSimulationResult{
				Kind:    observability.SimulationKindGC,
				Exact:   true,
				Mutated: false,
				Summary: observability.GCSimulationSummary{
					ReachableChunks:            3,
					UnreachableChunks:          2,
					LogicallyReclaimableBytes:  150 * 1024 * 1024,
					PhysicallyReclaimableBytes: 100 * 1024 * 1024,
					FullyReclaimableContainers: 1,
					PartiallyDeadContainers:    1,
				},
				Containers: []observability.ContainerSimulationImpact{
					{ContainerID: 1, Filename: "full.bin", ReclaimableBytes: 100, LiveBytesAfterGC: 0, ReclaimableChunks: 2, TotalChunks: 2, FullyReclaimable: true},
					{ContainerID: 2, Filename: "partial.bin", ReclaimableBytes: 50, LiveBytesAfterGC: 50, ReclaimableChunks: 1, TotalChunks: 2, FullyReclaimable: false, RequiresCompaction: true},
				},
				Assumptions: observability.GCSimulationAssumptions{DeletedSnapshots: opts.AssumeDeletedSnapshots},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSimulateGCCommand(parsedCommandLine{
			method:      "simulate",
			positionals: []string{"gc"},
			flags: map[string][]string{
				"containers":      {""},
				"delete-snapshot": {"s1", "s2"},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSimulateGCCommand text returned error: %v", err)
		}
	})

	if !strings.Contains(output, "GC simulation") {
		t.Fatalf("expected gc simulation header, got:\n%s", output)
	}
	if !strings.Contains(output, "Mode") || !strings.Contains(output, "exact:       true") || !strings.Contains(output, "mutated:     false") {
		t.Fatalf("expected mode section, got:\n%s", output)
	}
	if !strings.Contains(output, "Reachability") || !strings.Contains(output, "reachable chunks:       3") || !strings.Contains(output, "unreachable chunks:     2") {
		t.Fatalf("expected reachability section, got:\n%s", output)
	}
	if !strings.Contains(output, "Reclaimable") || !strings.Contains(output, "logical bytes:          150 MiB") || !strings.Contains(output, "physical bytes now:     100 MiB") {
		t.Fatalf("expected reclaimable section with MiB formatting, got:\n%s", output)
	}
	if !strings.Contains(output, "Containers") || !strings.Contains(output, "fully reclaimable:      1") || !strings.Contains(output, "partially dead:         1") {
		t.Fatalf("expected containers summary section, got:\n%s", output)
	}
	if !strings.Contains(output, "Assumptions") || !strings.Contains(output, "snapshot treated as deleted: s1") || !strings.Contains(output, "snapshot treated as deleted: s2") {
		t.Fatalf("expected assumptions section, got:\n%s", output)
	}
	if !strings.Contains(output, "[fully reclaimable now]") {
		t.Fatalf("expected fully reclaimable container marker, got:\n%s", output)
	}
	if !strings.Contains(output, "[requires compaction]") {
		t.Fatalf("expected requires compaction marker, got:\n%s", output)
	}
	if !strings.Contains(output, "No state was changed.") {
		t.Fatalf("expected no state changed footer, got:\n%s", output)
	}
}

func TestRunSimulateGCCommandJSONIncludesContainersWhenRequested(t *testing.T) {
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() { runObservabilitySimulateGCPhase = originalSimulate })

	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		return &observability.SimulationResult{
			Kind:    observability.SimulationKindGC,
			Exact:   true,
			Mutated: false,
			GC: &observability.GCSimulationResult{
				Kind:    observability.SimulationKindGC,
				Exact:   true,
				Mutated: false,
				Summary: observability.GCSimulationSummary{},
				Containers: []observability.ContainerSimulationImpact{{
					ContainerID: 42,
					Filename:    "c042.bin",
				}},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSimulateGCCommand(parsedCommandLine{
			method:      "simulate",
			positionals: []string{"gc"},
			flags: map[string][]string{
				"containers": {""},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSimulateGCCommand JSON returned error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse simulate gc JSON: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	gcNode := data["gc"].(map[string]any)
	containers, ok := gcNode["containers"].([]any)
	if !ok || len(containers) != 1 {
		t.Fatalf("expected one container in gc.containers when --containers is set, got %v", gcNode["containers"])
	}
}

func TestRunSimulateGCCommandDeleteSnapshotFlagPassThrough(t *testing.T) {
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() { runObservabilitySimulateGCPhase = originalSimulate })

	var captured observability.SimulationOptions
	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		captured = opts
		return &observability.SimulationResult{
			Kind:    observability.SimulationKindGC,
			Exact:   true,
			Mutated: false,
			GC: &observability.GCSimulationResult{
				Kind:    observability.SimulationKindGC,
				Exact:   true,
				Mutated: false,
				Summary: observability.GCSimulationSummary{},
			},
		}, nil
	}

	err := runSimulateGCCommand(parsedCommandLine{
		method:      "simulate",
		positionals: []string{"gc"},
		flags: map[string][]string{
			"delete-snapshot": {"s3", "s4"},
		},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runSimulateGCCommand returned error: %v", err)
	}

	if captured.Kind != observability.SimulationKindGC {
		t.Fatalf("captured.Kind = %q, want %q", captured.Kind, observability.SimulationKindGC)
	}
	if len(captured.AssumeDeletedSnapshots) != 2 || captured.AssumeDeletedSnapshots[0] != "s3" || captured.AssumeDeletedSnapshots[1] != "s4" {
		t.Fatalf("captured.AssumeDeletedSnapshots = %v, want [s3 s4]", captured.AssumeDeletedSnapshots)
	}
}

func TestRunSimulateGCCommandRejectsMissingSnapshot(t *testing.T) {
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() { runObservabilitySimulateGCPhase = originalSimulate })

	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		return nil, fmt.Errorf(`gc simulation: build plan: gc.BuildPlan: validate assumed-deleted snapshots: snapshot %q does not exist`, opts.AssumeDeletedSnapshots[0])
	}

	err := runSimulateGCCommand(parsedCommandLine{
		method:      "simulate",
		positionals: []string{"gc"},
		flags: map[string][]string{
			"delete-snapshot": {"missing-snapshot"},
		},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `simulate gc: gc simulation: build plan: gc.BuildPlan: validate assumed-deleted snapshots: snapshot "missing-snapshot" does not exist`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunSimulateGCCommandTextOutputIncludesWarnings(t *testing.T) {
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() { runObservabilitySimulateGCPhase = originalSimulate })

	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		return &observability.SimulationResult{
			Kind:    observability.SimulationKindGC,
			Exact:   true,
			Mutated: false,
			GC: &observability.GCSimulationResult{
				Kind:    observability.SimulationKindGC,
				Exact:   true,
				Mutated: false,
				Summary: observability.GCSimulationSummary{},
				Warnings: []observability.ObservationWarning{{
					Code:    "QUARANTINED_CONTAINER",
					Message: "quarantined containers are excluded from physical reclaim calculation",
				}},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSimulateGCCommand(parsedCommandLine{
			method:      "simulate",
			positionals: []string{"gc"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSimulateGCCommand returned error: %v", err)
		}
	})

	if !strings.Contains(output, "Warnings") {
		t.Fatalf("expected warnings section, got:\n%s", output)
	}
	if !strings.Contains(output, "[QUARANTINED_CONTAINER] quarantined containers are excluded from physical reclaim calculation") {
		t.Fatalf("expected warning message, got:\n%s", output)
	}
	if !strings.Contains(output, "No state was changed.") {
		t.Fatalf("expected no state changed footer, got:\n%s", output)
	}
}

func TestRunCLISimulateSkipsStartupRecovery(t *testing.T) {
	originalStartupRecovery := startupRecoveryPhase
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() {
		startupRecoveryPhase = originalStartupRecovery
		runObservabilitySimulateGCPhase = originalSimulate
	})

	startupCalls := 0
	startupRecoveryPhase = func(string) (recovery.Report, error) {
		startupCalls++
		return recovery.Report{}, nil
	}
	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		return &observability.SimulationResult{
			Kind:    observability.SimulationKindGC,
			Exact:   true,
			Mutated: false,
			GC: &observability.GCSimulationResult{
				Kind:    observability.SimulationKindGC,
				Exact:   true,
				Mutated: false,
				Summary: observability.GCSimulationSummary{},
			},
		}, nil
	}

	stderrOutput := captureStderr(t, func() {
		stdoutOutput := captureStdout(t, func() {
			code := runCLI([]string{"simulate", "gc"})
			if code != exitSuccess {
				t.Fatalf("expected exitSuccess, got %d", code)
			}
		})
		if !strings.Contains(stdoutOutput, "GC simulation") {
			t.Fatalf("expected simulate output, got %q", stdoutOutput)
		}
	})
	if stderrOutput != "" {
		t.Fatalf("expected no stderr output, got %q", stderrOutput)
	}
	if startupCalls != 0 {
		t.Fatalf("expected startup recovery to be skipped for simulate, got %d calls", startupCalls)
	}
}

func TestRunSimulateGCCommandTextAndJSONStayConsistent(t *testing.T) {
	originalSimulate := runObservabilitySimulateGCPhase
	t.Cleanup(func() { runObservabilitySimulateGCPhase = originalSimulate })

	result := &observability.SimulationResult{
		Kind:    observability.SimulationKindGC,
		Exact:   true,
		Mutated: false,
		GC: &observability.GCSimulationResult{
			Kind:    observability.SimulationKindGC,
			Exact:   true,
			Mutated: false,
			Assumptions: observability.GCSimulationAssumptions{
				DeletedSnapshots: []string{"snap-a"},
			},
			Summary: observability.GCSimulationSummary{
				ReachableChunks:            11,
				UnreachableChunks:          2,
				LogicallyReclaimableBytes:  200 * 1024 * 1024,
				PhysicallyReclaimableBytes: 100 * 1024 * 1024,
				FullyReclaimableContainers: 1,
				PartiallyDeadContainers:    1,
			},
			Warnings: []observability.ObservationWarning{{
				Code:    "PARTIAL_RECLAIM_REQUIRES_COMPACTION",
				Message: "some dead bytes are in partially live containers and are not physically reclaimable yet",
			}},
		},
	}
	runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
		return result, nil
	}

	textOutput := captureStdout(t, func() {
		err := runSimulateGCCommand(parsedCommandLine{method: "simulate", positionals: []string{"gc"}}, outputModeText)
		if err != nil {
			t.Fatalf("text simulate: %v", err)
		}
	})
	jsonOutput := captureStdout(t, func() {
		err := runSimulateGCCommand(parsedCommandLine{method: "simulate", positionals: []string{"gc"}, flags: map[string][]string{"json": {""}}}, outputModeJSON)
		if err != nil {
			t.Fatalf("json simulate: %v", err)
		}
	})

	if !strings.Contains(textOutput, "reachable chunks:       11") || !strings.Contains(textOutput, "unreachable chunks:     2") {
		t.Fatalf("text output missing reachability summary: %s", textOutput)
	}
	if !strings.Contains(textOutput, "logical bytes:          200 MiB") || !strings.Contains(textOutput, "physical bytes now:     100 MiB") {
		t.Fatalf("text output missing reclaimable summary: %s", textOutput)
	}
	if !strings.Contains(textOutput, "snapshot treated as deleted: snap-a") {
		t.Fatalf("text output missing assumptions: %s", textOutput)
	}
	if !strings.Contains(textOutput, "[PARTIAL_RECLAIM_REQUIRES_COMPACTION] some dead bytes are in partially live containers and are not physically reclaimable yet") {
		t.Fatalf("text output missing warning: %s", textOutput)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(jsonOutput)), &payload); err != nil {
		t.Fatalf("parse simulate gc JSON: %v output=%q", err, jsonOutput)
	}
	data := payload["data"].(map[string]any)
	gcNode := data["gc"].(map[string]any)
	summary := gcNode["summary"].(map[string]any)
	assertJSONNumber(t, summary, "reachable_chunks", 11)
	assertJSONNumber(t, summary, "unreachable_chunks", 2)
	assertJSONNumber(t, summary, "logically_reclaimable_bytes", 200*1024*1024)
	assertJSONNumber(t, summary, "physically_reclaimable_bytes", 100*1024*1024)
	assumptions := gcNode["assumptions"].(map[string]any)
	deleted := assumptions["deleted_snapshots"].([]any)
	if len(deleted) != 1 || deleted[0] != "snap-a" {
		t.Fatalf("unexpected deleted_snapshots: %v", assumptions["deleted_snapshots"])
	}
	warnings := gcNode["warnings"].([]any)
	if len(warnings) != 1 {
		t.Fatalf("expected one warning, got %v", warnings)
	}
}
