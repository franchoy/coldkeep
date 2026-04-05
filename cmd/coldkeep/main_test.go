package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

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
