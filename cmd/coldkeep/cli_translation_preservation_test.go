package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
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
	installSnapshotDeletePreviewFailure(t)

	err := runSnapshotDeleteDryRunJSON(t, "missing-snap")
	assertCLIErrorExitCode(t, err, exitGeneral)

	output := captureStderr(t, func() {
		code := printCLIError(err, outputModeJSON)
		if code != exitGeneral {
			t.Fatalf("expected general exit code %d, got %d", exitGeneral, code)
		}
	})

	payload := assertSingleJSONObjectLine(t, output)
	message := assertDomainErrorEnvelope(t, payload, `snapshot "missing-snap" not found`)
	assertNoTaxonomyLeak(t, payload, message)
}

func installSnapshotDeletePreviewFailure(t *testing.T) {
	t.Helper()

	originalLoad := loadDefaultStorageContextPhase
	originalDelete := deleteSnapshotPhase
	originalPreview := snapshotDeleteLineagePreviewPhase
	originalEngine := newCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		deleteSnapshotPhase = originalDelete
		snapshotDeleteLineagePreviewPhase = originalPreview
		newCommandEngine = originalEngine
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
		t.Fatalf("snapshotDeleteLineagePreviewPhase must not run when delete is engine-routed for %q", snapshotID)
		return nil, nil
	}
	newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
		return stubCommandEngine{
			snapshotDeleteFunc: func(_ context.Context, req engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error) {
				if req.Mode != engine.SnapshotDeleteModePreview {
					t.Fatalf("expected preview mode, got %+v", req)
				}
				return engine.SnapshotDeleteResult{}, fmt.Errorf("snapshot %q not found", req.SnapshotID)
			},
		}, nil
	}
}

func runSnapshotDeleteDryRunJSON(t *testing.T, snapshotID string) error {
	t.Helper()

	return runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"delete", snapshotID},
		flags: map[string][]string{
			"dry-run": {""},
			"output":  {"json"},
		},
	}, outputModeJSON)
}

func assertCLIErrorExitCode(t *testing.T, err error, want int) {
	t.Helper()

	if err == nil {
		t.Fatal("expected CLI error")
	}
	if got := classifyExitCode(err); got != want {
		t.Fatalf("expected exit code %d, got %d", want, got)
	}
}

func assertDomainErrorEnvelope(t *testing.T, payload map[string]any, wantMessage string) string {
	t.Helper()

	assertPayloadString(t, payload, "status", "error")
	assertPayloadString(t, payload, "error_class", "GENERAL")
	assertPayloadExitCode(t, payload, exitGeneral)

	message := assertPayloadString(t, payload, "message", wantMessage)
	errorNode := assertPayloadErrorNode(t, payload)
	assertNestedErrorString(t, payload, errorNode, "code", "INTERNAL")
	assertNestedErrorString(t, payload, errorNode, "message", message)

	return message
}

func assertPayloadString(t *testing.T, payload map[string]any, key string, want string) string {
	t.Helper()

	got, _ := payload[key].(string)
	if got != want {
		t.Fatalf("expected %s=%q, got payload=%v", key, want, payload)
	}
	return got
}

func assertPayloadExitCode(t *testing.T, payload map[string]any, want int) {
	t.Helper()

	got, _ := payload["exit_code"].(float64)
	if int(got) != want {
		t.Fatalf("expected exit_code=%d, got payload=%v", want, payload)
	}
}

func assertPayloadErrorNode(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()

	errorNode, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected structured error object, got payload=%v", payload)
	}
	return errorNode
}

func assertNestedErrorString(t *testing.T, payload map[string]any, errorNode map[string]any, key string, want string) {
	t.Helper()

	got, _ := errorNode[key].(string)
	if got != want {
		t.Fatalf("expected nested error.%s=%q, got payload=%v", key, want, payload)
	}
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

	assertNoTaxonomyLeakKeys(t, payload)
	assertNoTaxonomyLeakMessage(t, message)
	assertNoTaxonomyLeakEncodedPayload(t, payload)
}

func assertNoTaxonomyLeakKeys(t *testing.T, payload map[string]any) {
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
}

func assertNoTaxonomyLeakMessage(t *testing.T, message string) {
	t.Helper()

	assertNoStringContainsAny(t, message, []string{
		"engine.IsUnsupported",
		"catalog.IsDeferred",
		"IsUnsupported",
		"IsDeferred",
	}, "message")
	assertNoStringContainsAny(t, message, []string{"Unsupported", "Deferred"}, "message")
}

func assertNoTaxonomyLeakEncodedPayload(t *testing.T, payload map[string]any) {
	t.Helper()

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload for taxonomy leak assertion: %v", err)
	}
	assertNoStringContainsAny(t, string(encoded), []string{
		"engine.IsUnsupported",
		"catalog.IsDeferred",
		"IsUnsupported",
		"IsDeferred",
		"\"taxonomy\"",
		"\"classification\"",
		"\"is_unsupported\"",
		"\"is_deferred\"",
	}, "encoded payload")
}

func assertNoStringContainsAny(t *testing.T, value string, forbiddenSnippets []string, label string) {
	t.Helper()

	for _, forbiddenSnippet := range forbiddenSnippets {
		if strings.Contains(value, forbiddenSnippet) {
			t.Fatalf("unexpected taxonomy leak %q in %s %q", forbiddenSnippet, label, value)
		}
	}
}
