package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/verify"
)

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
		Operation: batch.OperationRemove,
		DryRun:    true,
		Summary:   batch.Summary{Total: 4, Planned: 1, Success: 1, Failed: 1, Skipped: 1},
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultPlanned, Message: "would remove"},
			{ID: 18, Status: batch.ResultSkipped, Message: "duplicate target"},
			{ID: 24, Status: batch.ResultSuccess, Message: "removed mappings=3"},
			{ID: 99, Status: batch.ResultFailed, Message: "file not found"},
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
	if got := batchFailureExitCode(validationOnly); got != exitUsage {
		t.Fatalf("expected usage exit code for validation-only failures %d, got %d", exitUsage, got)
	}

	executionOnly := batch.Report{
		Results: []batch.ItemResult{
			{ID: 12, Status: batch.ResultFailed, Message: "file ID 12 not found"},
		},
	}
	if got := batchFailureExitCode(executionOnly); got != exitGeneral {
		t.Fatalf("expected general exit code for execution failures %d, got %d", exitGeneral, got)
	}

	mixed := batch.Report{
		Results: []batch.ItemResult{
			{Status: batch.ResultFailed, RawValue: "abc", Message: "invalid file ID \"abc\""},
			{ID: 99, Status: batch.ResultFailed, Message: "file ID 99 not found"},
		},
	}
	if got := batchFailureExitCode(mixed); got != exitGeneral {
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
	commands := []string{"store", "store-folder", "restore", "remove", "gc", "stats", "list", "search", "verify"}

	for _, command := range commands {
		if !shouldRunStartupRecovery(command) {
			t.Fatalf("expected startup recovery to run for command %q", command)
		}
	}
}

func TestShouldNotRunStartupRecoveryForNonStorageCommands(t *testing.T) {
	commands := []string{"help", "version", "init", "simulate", "doctor", "-h", "--help", "-v", "--version", "unknown"}

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
	selfEmittingJSONCommands := []string{"store", "store-folder", "restore", "remove", "gc", "list", "search", "stats", "simulate", "doctor", "version", "-v", "--version"}

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
