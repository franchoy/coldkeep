package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestCLIValidationErrorTranslationRemainsStable(t *testing.T) {
	output := captureStderr(t, func() {
		code := runCLI([]string{"doctor", "--output", "json", "--limit"})
		if code != exitUsage {
			t.Fatalf("expected usage exit code %d, got %d", exitUsage, code)
		}
	})

	payload := assertSingleJSONObjectLine(t, output)
	if got, _ := payload["status"].(string); got != "error" {
		t.Fatalf("expected status=error, got payload=%v", payload)
	}
	if got, _ := payload["error_class"].(string); got != "USAGE" {
		t.Fatalf("expected error_class=USAGE, got payload=%v", payload)
	}
	if got, _ := payload["exit_code"].(float64); int(got) != exitUsage {
		t.Fatalf("expected exit_code=%d, got payload=%v", exitUsage, payload)
	}
	message, _ := payload["message"].(string)
	if !strings.Contains(message, "missing value for --limit") {
		t.Fatalf("expected missing --limit usage message, got payload=%v", payload)
	}
	errorNode, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected structured error object, got payload=%v", payload)
	}
	if got, _ := errorNode["code"].(string); got != "INVALID_ARGUMENT" {
		t.Fatalf("expected public error code INVALID_ARGUMENT, got payload=%v", payload)
	}

	assertNoTaxonomyLeak(t, payload, message)
}

func TestCLIDomainErrorTranslationRemainsStable(t *testing.T) {
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
		t.Fatal("deleteSnapshotPhase must not run when preview fails")
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
			"output":  {"json"},
		},
	}, outputModeJSON)
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if got := classifyExitCode(err); got != exitGeneral {
		t.Fatalf("expected general exit code %d, got %d", exitGeneral, got)
	}

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeJSON)
		if code != exitGeneral {
			t.Fatalf("expected general exit code %d, got %d", exitGeneral, code)
		}
	})

	payload := assertSingleJSONObjectLine(t, output)
	if got, _ := payload["status"].(string); got != "error" {
		t.Fatalf("expected status=error, got payload=%v", payload)
	}
	if got, _ := payload["error_class"].(string); got != "GENERAL" {
		t.Fatalf("expected error_class=GENERAL, got payload=%v", payload)
	}
	if got, _ := payload["exit_code"].(float64); int(got) != exitGeneral {
		t.Fatalf("expected exit_code=%d, got payload=%v", exitGeneral, payload)
	}
	message, _ := payload["message"].(string)
	if got := message; got != `snapshot "missing-snap" not found` {
		t.Fatalf("expected exact snapshot not found message, got %q", got)
	}
	errorNode, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected structured error object, got payload=%v", payload)
	}
	if got, _ := errorNode["code"].(string); got != "INTERNAL" {
		t.Fatalf("expected public error code INTERNAL for generic domain error path, got payload=%v", payload)
	}
	if got, _ := errorNode["message"].(string); got != message {
		t.Fatalf("expected nested error.message to match top-level message, got payload=%v", payload)
	}

	assertNoTaxonomyLeak(t, payload, message)
}

func TestCLIInvariantVerifyErrorTranslationRemainsStable(t *testing.T) {
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

	payload := assertSingleJSONObjectLine(t, output)
	if got, _ := payload["error_class"].(string); got != "VERIFY" {
		t.Fatalf("expected error_class=VERIFY, got payload=%v", payload)
	}
	if got, _ := payload["exit_code"].(float64); int(got) != exitVerify {
		t.Fatalf("expected exit_code=%d, got payload=%v", exitVerify, payload)
	}
	if got, _ := payload["invariant_code"].(string); got != invariants.CodePhysicalGraphRefCountMismatch {
		t.Fatalf("expected invariant_code=%s, got payload=%v", invariants.CodePhysicalGraphRefCountMismatch, payload)
	}
	action, _ := payload["recommended_action"].(string)
	if !strings.Contains(action, "repair ref-counts") {
		t.Fatalf("expected recommended_action to mention repair ref-counts, got payload=%v", payload)
	}

	message, _ := payload["message"].(string)
	assertNoTaxonomyLeak(t, payload, message)
}

func TestCLIJSONErrorEnvelopeDoesNotExposeInternalTaxonomy(t *testing.T) {
	err := observabilityErrorf(exitGeneral, "NOT_FOUND", "logical file 45 not found")

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeJSON)
		if code != exitGeneral {
			t.Fatalf("expected general exit code %d, got %d", exitGeneral, code)
		}
	})

	var payload map[string]any
	if parseErr := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); parseErr != nil {
		t.Fatalf("parse JSON payload: %v output=%q", parseErr, output)
	}
	errorNode, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested error object, got payload=%v", payload)
	}
	if got, _ := errorNode["code"].(string); got != "NOT_FOUND" {
		t.Fatalf("expected error.code=NOT_FOUND, got payload=%v", payload)
	}

	message, _ := payload["message"].(string)
	assertNoTaxonomyLeak(t, payload, message)
}

func assertNoTaxonomyLeak(t *testing.T, payload map[string]any, message string) {
	t.Helper()

	for _, forbiddenKey := range []string{
		"taxonomy",
		"category",
		"classification",
		"is_unsupported",
		"is_deferred",
	} {
		if _, exists := payload[forbiddenKey]; exists {
			t.Fatalf("unexpected taxonomy leak key %q in payload=%v", forbiddenKey, payload)
		}
	}

	for _, forbiddenSnippet := range []string{
		"engine.IsUnsupported",
		"catalog.IsDeferred",
		"IsUnsupported",
		"IsDeferred",
	} {
		if strings.Contains(message, forbiddenSnippet) {
			t.Fatalf("unexpected helper name leak %q in message %q", forbiddenSnippet, message)
		}
	}

	for _, forbiddenWord := range []string{"Unsupported", "Deferred"} {
		if strings.Contains(message, forbiddenWord) {
			t.Fatalf("unexpected taxonomy wording leak %q in message %q", forbiddenWord, message)
		}
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload for taxonomy leak assertion: %v", err)
	}
	for _, forbiddenSnippet := range []string{
		"engine.IsUnsupported",
		"catalog.IsDeferred",
		"IsUnsupported",
		"IsDeferred",
		"\"taxonomy\"",
		"\"classification\"",
		"\"is_unsupported\"",
		"\"is_deferred\"",
	} {
		if strings.Contains(string(encoded), forbiddenSnippet) {
			t.Fatalf("unexpected taxonomy leak %q in encoded payload %s", forbiddenSnippet, string(encoded))
		}
	}
}
