package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/invariants"
)

func phase13README(t *testing.T) string {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatalf("read root README: %v", err)
	}
	return string(contents)
}

func phase13PublicContractText(value string) string {
	return strings.ReplaceAll(value, "`", "")
}

func phase13AssertContainsAll(t *testing.T, surfaceName string, surface string, required ...string) {
	t.Helper()

	for _, text := range required {
		if !strings.Contains(surface, text) {
			t.Errorf("%s missing required public contract %q", surfaceName, text)
		}
	}
}

type phase13InitResult struct {
	err       error
	env       string
	mode      os.FileMode
	envExists bool
}

func phase13RunInitInTemp(t *testing.T, parsed parsedCommandLine) phase13InitResult {
	t.Helper()

	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get test working directory: %v", err)
	}
	testDir := t.TempDir()
	if err := os.Chdir(testDir); err != nil {
		t.Fatalf("enter test-owned init directory: %v", err)
	}
	defer func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Fatalf("restore test working directory: %v", err)
		}
	}()

	result := phase13InitResult{}
	_ = captureStdout(t, func() {
		result.err = initCommand(parsed, outputModeText)
	})
	contents, readErr := os.ReadFile(".env")
	if errors.Is(readErr, os.ErrNotExist) {
		return result
	}
	if readErr != nil {
		t.Fatalf("read test-owned init .env: %v", readErr)
	}
	info, err := os.Stat(".env")
	if err != nil {
		t.Fatalf("stat test-owned init .env: %v", err)
	}
	result.env = string(contents)
	result.mode = info.Mode().Perm()
	result.envExists = true
	return result
}

func TestREADMEExposesAcceptedSearchNameExample(t *testing.T) {
	readme := phase13README(t)
	if strings.Contains(readme, "coldkeep search report") {
		t.Fatal("README still exposes the rejected positional search form")
	}
	if !strings.Contains(readme, "coldkeep search --name report") {
		t.Fatal("README does not expose the accepted --name search form")
	}

	parsed, err := parseCommandLine([]string{"search", "--name", "report"}, flagsWithValues)
	if err != nil {
		t.Fatalf("parse documented search command: %v", err)
	}
	if parsed.method != "search" || len(parsed.positionals) != 0 {
		t.Fatalf("documented search shape parsed unexpectedly: %+v", parsed)
	}
	if name, ok := parsed.lastFlagValue("name"); !ok || name != "report" {
		t.Fatalf("documented search --name value mismatch: name=%q present=%v", name, ok)
	}

	legacy, err := parseCommandLine([]string{"search", "report"}, flagsWithValues)
	if err != nil {
		t.Fatalf("parse legacy positional search shape: %v", err)
	}
	legacyErr := runSearchCommand(legacy, outputModeText)
	if legacyErr == nil || classifyExitCode(legacyErr) != exitUsage {
		t.Fatalf("legacy positional search must remain rejected as usage: %v", legacyErr)
	}
}

func TestExitCodeDocumentationMatchesRuntimeAndJSONLayers(t *testing.T) {
	if exitSuccess != 0 || exitGeneral != 1 || exitUsage != 2 || exitVerify != 3 || exitRecovery != 4 {
		t.Fatalf("exit constants changed: success=%d general=%d usage=%d verify=%d recovery=%d", exitSuccess, exitGeneral, exitUsage, exitVerify, exitRecovery)
	}

	classifications := []struct {
		name string
		err  error
		want int
	}{
		{name: "success", want: exitSuccess},
		{name: "general", err: errors.New("execution failed"), want: exitGeneral},
		{name: "usage", err: usageErrorf("Usage: coldkeep search [filters]"), want: exitUsage},
		{name: "verify", err: verifyError(errors.New("verification failed")), want: exitVerify},
		{name: "recovery", err: recoveryError(errors.New("recovery failed")), want: exitRecovery},
	}
	for _, tc := range classifications {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyExitCode(tc.err); got != tc.want {
				t.Fatalf("classification mismatch: got=%d want=%d err=%v", got, tc.want, tc.err)
			}
		})
	}

	report := batch.Report{Results: []batch.ItemResult{
		{Status: batch.ResultFailed, RawValue: "bad", Message: "validation failure"},
		{ID: 7, Status: batch.ResultFailed, Message: "execution failure"},
		{ID: 8, Status: batch.ResultFailed, Message: "invariant failure", InvariantCode: invariants.CodeRepairRefusedOrphanRows},
	}}
	if got := deriveBatchFailureExitCode(report); got != exitVerify {
		t.Fatalf("batch invariant failure must take precedence: got=%d want=%d", got, exitVerify)
	}

	invariantErr := verifyError(invariants.New(
		invariants.CodeRepairRefusedOrphanRows,
		"repair refused",
		nil,
	))
	jsonCases := []struct {
		name          string
		err           error
		wantExit      int
		wantClass     string
		wantErrorCode string
		wantInvariant bool
	}{
		{name: "general JSON", err: errors.New("execution failed"), wantExit: exitGeneral, wantClass: "GENERAL", wantErrorCode: "INTERNAL"},
		{name: "usage JSON", err: usageErrorf("Usage: coldkeep search [filters]"), wantExit: exitUsage, wantClass: "USAGE", wantErrorCode: "INVALID_ARGUMENT"},
		{name: "invariant JSON", err: invariantErr, wantExit: exitVerify, wantClass: "VERIFY", wantErrorCode: "INTERNAL", wantInvariant: true},
		{name: "recovery JSON", err: recoveryError(errors.New("recovery failed")), wantExit: exitRecovery, wantClass: "RECOVERY", wantErrorCode: "INTERNAL"},
	}
	for _, tc := range jsonCases {
		t.Run(tc.name, func(t *testing.T) {
			jsonOutput := captureStderr(t, func() {
				if got := printCLIError(tc.err, outputModeJSON); got != tc.wantExit {
					t.Fatalf("JSON process exit mismatch: got=%d want=%d", got, tc.wantExit)
				}
			})
			var payload map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(jsonOutput)), &payload); err != nil {
				t.Fatalf("parse CLI error JSON: %v output=%q", err, jsonOutput)
			}
			if got, ok := payload["exit_code"].(float64); !ok || int(got) != tc.wantExit {
				t.Fatalf("exit_code must be the numeric process status: payload=%v", payload)
			}
			if got, _ := payload["error_class"].(string); got != tc.wantClass {
				t.Fatalf("error_class mismatch: got=%q want=%q payload=%v", got, tc.wantClass, payload)
			}
			encodedError, ok := payload["error"].(map[string]any)
			if !ok {
				t.Fatalf("nested error object missing: payload=%v", payload)
			}
			if got, _ := encodedError["code"].(string); got != tc.wantErrorCode {
				t.Fatalf("error.code coarse family mismatch: got=%q want=%q payload=%v", got, tc.wantErrorCode, payload)
			}
			if tc.wantInvariant {
				if got, _ := payload["invariant_code"].(string); got != invariants.CodeRepairRefusedOrphanRows {
					t.Fatalf("stable invariant_code mismatch: payload=%v", payload)
				}
				if got, _ := payload["recommended_action"].(string); strings.TrimSpace(got) == "" {
					t.Fatalf("recommended_action missing: payload=%v", payload)
				}
			}
		})
	}

	help := phase13PublicContractText(captureStdout(t, printHelp))
	readme := phase13PublicContractText(phase13README(t))
	required := []string{
		"exit 0: success",
		"exit 1: general/execution error",
		"exit 2: usage/pre-execution validation error",
		"exit 3: verification/invariant-integrity error",
		"exit 4: recovery error",
		"Batch failure precedence: invariant > execution > validation",
		"exit_code: numeric process exit status",
		"error_class: process-level label",
		"error.code: coarse error family",
		"invariant_code: stable invariant identifier",
		"recommended_action: operator remediation guidance",
	}
	phase13AssertContainsAll(t, "top-level help", help, required...)
	phase13AssertContainsAll(t, "README", readme, required...)
}

func TestInitCompressionHelpAndREADMEMatchParser(t *testing.T) {
	commands := []struct {
		args      []string
		wantCodec string
		wantLevel string
	}{
		{args: []string{"init"}},
		{args: []string{"init", "--compression", "none"}, wantCodec: "none"},
		{args: []string{"init", "--compression", "zstd"}, wantCodec: "zstd"},
		{args: []string{"init", "--compression", "zstd", "--compression-level", "1"}, wantCodec: "zstd", wantLevel: "1"},
		{args: []string{"init", "--compression", "zstd", "--compression-level", "9"}, wantCodec: "zstd", wantLevel: "9"},
	}
	for _, tc := range commands {
		parsed, err := parseCommandLine(tc.args, flagsWithValues)
		if err != nil {
			t.Fatalf("parse accepted init command %v: %v", tc.args, err)
		}
		if parsed.method != "init" || len(parsed.positionals) != 0 {
			t.Fatalf("accepted init command parsed unexpectedly: args=%v parsed=%+v", tc.args, parsed)
		}
		codec, codecPresent := parsed.lastFlagValue("compression")
		if codec != tc.wantCodec || codecPresent != (tc.wantCodec != "") {
			t.Fatalf("compression parse mismatch for %v: codec=%q present=%v", tc.args, codec, codecPresent)
		}
		if tc.wantLevel != "" {
			if level, ok := parsed.lastFlagValue("compression-level"); !ok || level != tc.wantLevel {
				t.Fatalf("compression-level parse mismatch for %v: level=%q present=%v", tc.args, level, ok)
			}
		}

		result := phase13RunInitInTemp(t, parsed)
		if result.err != nil {
			t.Fatalf("accepted init command %v failed: %v", tc.args, result.err)
		}
		if !result.envExists || result.mode != 0o600 {
			t.Fatalf("accepted init command %v must create test-owned 0600 .env: exists=%v mode=%#o", tc.args, result.envExists, result.mode)
		}
		if !strings.Contains(result.env, "COLDKEEP_KEY=") || !strings.Contains(result.env, "COLDKEEP_CODEC=aes-gcm") {
			t.Fatalf("accepted init command %v changed key/codec output: %q", tc.args, result.env)
		}
		if tc.wantCodec != "" && !strings.Contains(result.env, "COLDKEEP_COMPRESSION="+tc.wantCodec) {
			t.Fatalf("accepted init command %v missing compression output: %q", tc.args, result.env)
		}
		if tc.wantLevel != "" && !strings.Contains(result.env, "COLDKEEP_COMPRESSION_LEVEL="+tc.wantLevel) {
			t.Fatalf("accepted init command %v missing compression-level output: %q", tc.args, result.env)
		}
	}

	invalid := []parsedCommandLine{
		{method: "init", flags: map[string][]string{"compression-level": {"1"}}},
		{method: "init", flags: map[string][]string{"compression": {"none"}, "compression-level": {"1"}}},
		{method: "init", flags: map[string][]string{"compression": {"zstd"}, "compression-level": {"0"}}},
		{method: "init", flags: map[string][]string{"compression": {"zstd"}, "compression-level": {"10"}}},
		{method: "init", flags: map[string][]string{"compression": {"invalid"}}},
	}
	for _, parsed := range invalid {
		result := phase13RunInitInTemp(t, parsed)
		if result.err == nil {
			t.Fatalf("expected invalid init compression combination to fail: %+v", parsed)
		}
		if result.envExists {
			t.Fatalf("invalid init compression combination wrote test-owned .env: %+v", parsed)
		}
	}

	help := phase13PublicContractText(captureStdout(t, printHelp))
	readme := phase13PublicContractText(phase13README(t))
	required := []string{
		"coldkeep init --compression none",
		"coldkeep init --compression zstd --compression-level 1",
		"Compression is block-level and happens before encryption.",
		"none stores new blocks without compression",
		"zstd",
		"new blocks",
		"Compression settings affect new writes only; existing blocks are not modified.",
		"--compression-level is valid only with zstd and must be in the range 1-9.",
	}
	phase13AssertContainsAll(t, "top-level help", help, required...)
	phase13AssertContainsAll(t, "README", readme, required...)
}
