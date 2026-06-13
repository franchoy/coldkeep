package main

import (
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
)

func TestReadSideJSONOwnershipFamiliesRemainDistinct(t *testing.T) {
	t.Run("stats uses renderer envelope family", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"stats", "--json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertJSONEnvelopeShape(t, payload, "stats")
		if _, exists := payload["status"]; exists {
			t.Fatalf("did not expect CLI success envelope status field for stats, got payload=%v", payload)
		}
		if _, exists := payload["command"]; exists {
			t.Fatalf("did not expect CLI success envelope command field for stats, got payload=%v", payload)
		}
	})

	t.Run("inspect uses renderer envelope family", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"inspect", "chunk", "7", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertJSONEnvelopeShape(t, payload, "inspect")
		if _, exists := payload["status"]; exists {
			t.Fatalf("did not expect CLI success envelope status field for inspect, got payload=%v", payload)
		}
		if _, exists := payload["command"]; exists {
			t.Fatalf("did not expect CLI success envelope command field for inspect, got payload=%v", payload)
		}
	})

	t.Run("snapshot show uses CLI envelope family", func(t *testing.T) {
		installSnapshotShowCLISuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "show", "snap-preserve", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertPayloadString(t, payload, "status", "ok")
		assertPayloadString(t, payload, "command", "snapshot")
		if _, exists := payload["generated_at_utc"]; exists {
			t.Fatalf("did not expect renderer envelope generated_at_utc field for snapshot show, got payload=%v", payload)
		}
		if _, exists := payload["meta"]; exists {
			t.Fatalf("did not expect renderer envelope meta field for snapshot show, got payload=%v", payload)
		}
	})

	t.Run("snapshot diff uses CLI envelope family", func(t *testing.T) {
		installSnapshotDiffCLIDetailedSuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "diff", "base-preserve", "target-preserve", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertPayloadString(t, payload, "status", "ok")
		assertPayloadString(t, payload, "command", "snapshot diff")
		if _, exists := payload["generated_at_utc"]; exists {
			t.Fatalf("did not expect renderer envelope generated_at_utc field for snapshot diff, got payload=%v", payload)
		}
		if _, exists := payload["meta"]; exists {
			t.Fatalf("did not expect renderer envelope meta field for snapshot diff, got payload=%v", payload)
		}
	})
}

func TestReadSideJSONErrorsRemainCLIEnvelopeOwned(t *testing.T) {
	t.Run("stats validation error", func(t *testing.T) {
		installStep9CLIStubs(t)
		assertReadSideCLIErrorEnvelope(t, []string{"stats", "--json", "--output", "human"}, exitUsage)
	})

	t.Run("inspect unsupported entity", func(t *testing.T) {
		installStep9CLIStubs(t)
		assertReadSideCLIErrorEnvelope(t, []string{"inspect", "blob", "--json"}, exitUsage)
	})

	t.Run("snapshot show usage error", func(t *testing.T) {
		installNoopStartupRecovery(t)
		assertReadSideCLIErrorEnvelope(t, []string{"snapshot", "show", "--output", "json"}, exitUsage)
	})

	t.Run("snapshot diff usage error", func(t *testing.T) {
		installNoopStartupRecovery(t)
		assertReadSideCLIErrorEnvelope(t, []string{"snapshot", "diff", "snap-1", "--output", "json"}, exitUsage)
	})
}

func TestInspectPublicTargetsRemainDocumentedSet(t *testing.T) {
	installNoopStartupRecovery(t)

	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	tests := []struct {
		name       string
		args       []string
		wantEntity observability.EntityType
		wantID     string
	}{
		{
			name:       "repository",
			args:       []string{"inspect", "repository", "--output", "json"},
			wantEntity: observability.EntityRepository,
			wantID:     "",
		},
		{
			name:       "file",
			args:       []string{"inspect", "file", "42", "--output", "json"},
			wantEntity: observability.EntityFile,
			wantID:     "42",
		},
		{
			name:       "logical-file alias",
			args:       []string{"inspect", "logical-file", "42", "--output", "json"},
			wantEntity: observability.EntityFile,
			wantID:     "42",
		},
		{
			name:       "snapshot",
			args:       []string{"inspect", "snapshot", "snap-42", "--output", "json"},
			wantEntity: observability.EntitySnapshot,
			wantID:     "snap-42",
		},
		{
			name:       "chunk",
			args:       []string{"inspect", "chunk", "7", "--output", "json"},
			wantEntity: observability.EntityChunk,
			wantID:     "7",
		},
		{
			name:       "container",
			args:       []string{"inspect", "container", "9", "--output", "json"},
			wantEntity: observability.EntityContainer,
			wantID:     "9",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
				called = true
				if entity != tc.wantEntity {
					t.Fatalf("expected entity %q, got %q", tc.wantEntity, entity)
				}
				if id != tc.wantID {
					t.Fatalf("expected id %q, got %q", tc.wantID, id)
				}
				return &observability.InspectResult{
					EntityType: entity,
					EntityID:   id,
				}, nil
			}

			stdout, stderr, code := runCLIWithCapturedIO(t, tc.args)
			if code != exitSuccess {
				t.Fatalf("expected exitSuccess, got %d stdout=%q stderr=%q", code, stdout, stderr)
			}
			if !called {
				t.Fatalf("expected inspect route to call observability phase for args=%v", tc.args)
			}
		})
	}

	t.Run("physical-file remains internal only", func(t *testing.T) {
		runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
			t.Fatalf("unexpected observability inspect call for unsupported public target: entity=%q id=%q", entity, id)
			return nil, nil
		}

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"inspect", "physical-file", "1", "--json"})
		if code != exitUsage {
			t.Fatalf("expected exitUsage, got %d stdout=%q stderr=%q", code, stdout, stderr)
		}
		payload := lastJSONStderrPayload(t, stderr)
		assertPayloadString(t, payload, "status", "error")
		assertPayloadString(t, payload, "error_class", "USAGE")
		assertPayloadExitCode(t, payload, exitUsage)
		message, _ := payload["message"].(string)
		if !strings.Contains(message, `unsupported inspect entity "physical-file"`) {
			t.Fatalf("expected unsupported physical-file message, got payload=%v", payload)
		}
		errorNode := assertPayloadErrorNode(t, payload)
		assertNestedErrorString(t, payload, errorNode, "code", "INVALID_ARGUMENT")
		assertNoTaxonomyLeak(t, payload, message)
	})
}

func TestReadSideOutputDoesNotLeakTaxonomyHelperNames(t *testing.T) {
	t.Run("renderer-envelope success", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"stats", "--json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("CLI-envelope success", func(t *testing.T) {
		installSnapshotDiffCLIDetailedSuccessStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"snapshot", "diff", "base-preserve", "target-preserve", "--output", "json"})
		if code != exitSuccess {
			t.Fatalf("expected exitSuccess, got %d stderr=%q", code, stderr)
		}

		payload := assertSingleJSONObjectLine(t, stdout)
		assertReadSideSuccessPayloadNoTaxonomyLeak(t, payload)
	})

	t.Run("CLI error envelope", func(t *testing.T) {
		installStep9CLIStubs(t)

		stdout, stderr, code := runCLIWithCapturedIO(t, []string{"inspect", "blob", "--json"})
		if code != exitUsage {
			t.Fatalf("expected exitUsage, got %d stdout=%q stderr=%q", code, stdout, stderr)
		}

		payload := lastJSONStderrPayload(t, stderr)
		message, _ := payload["message"].(string)
		assertNoTaxonomyLeak(t, payload, message)
	})
}

func assertReadSideCLIErrorEnvelope(t *testing.T, args []string, wantExitCode int) {
	t.Helper()

	stdout, stderr, code := runCLIWithCapturedIO(t, args)
	if code != wantExitCode {
		t.Fatalf("expected exit code %d, got %d stdout=%q stderr=%q", wantExitCode, code, stdout, stderr)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout for JSON error path, got %q", stdout)
	}

	payload := lastJSONStderrPayload(t, stderr)
	assertPayloadString(t, payload, "status", "error")
	if _, ok := payload["error_class"].(string); !ok {
		t.Fatalf("expected error_class string, got payload=%v", payload)
	}
	assertPayloadExitCode(t, payload, wantExitCode)
	message, _ := payload["message"].(string)
	if strings.TrimSpace(message) == "" {
		t.Fatalf("expected non-empty message, got payload=%v", payload)
	}
	errorNode := assertPayloadErrorNode(t, payload)
	if _, ok := errorNode["code"].(string); !ok {
		t.Fatalf("expected nested error.code string, got payload=%v", payload)
	}
	if _, ok := errorNode["message"].(string); !ok {
		t.Fatalf("expected nested error.message string, got payload=%v", payload)
	}
	assertNoTaxonomyLeak(t, payload, message)
}
