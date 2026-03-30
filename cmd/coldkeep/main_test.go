package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/franchoy/coldkeep/internal/recovery"
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

func TestEmitStartupRecoveryReportJSONSuccessSchema(t *testing.T) {
	report := recovery.Report{
		AbortedLogicalFiles:    2,
		AbortedChunks:          3,
		QuarantinedMissing:     4,
		QuarantinedOrphan:      5,
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
	assertJSONNumber(t, payload, "quarantined_orphan_containers", 5)
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

func TestClassifyExitCodeTypedUsageError(t *testing.T) {
	err := usageErrorf("Usage: coldkeep store <filePath>")
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}

func TestClassifyExitCodeTypedVerifyError(t *testing.T) {
	err := verifyError(errors.New("verification failed: chunk mismatch"))
	if got := classifyExitCode(err); got != exitVerify {
		t.Fatalf("expected verify exit code %d, got %d", exitVerify, got)
	}
}

func TestClassifyExitCodeFallbackStringMatch(t *testing.T) {
	err := errors.New("unknown command: nope")
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code from fallback matching %d, got %d", exitUsage, got)
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
	if err == nil {
		t.Fatal("expected error for invalid output mode")
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

	if err == nil {
		t.Fatal("expected error for missing simulate args")
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

	if err == nil {
		t.Fatal("expected error for unknown simulate subcommand")
	}

	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
}
