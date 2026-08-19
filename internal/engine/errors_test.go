package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
)

func TestIsUnsupportedRecognizesTypedError(t *testing.T) {
	err := engine.NewError(engine.ErrorUnsupported, "operation", "unsupported mode", "", nil)
	if !engine.IsUnsupported(err) {
		t.Fatalf("expected typed error to classify as unsupported, got %v", err)
	}
}

func TestIsUnsupportedRejectsUnrelatedErrors(t *testing.T) {
	for _, err := range []error{
		nil,
		errors.New("plain unrelated error"),
		fmt.Errorf("wrapped unrelated: %w", errors.New("other")),
		fmt.Errorf("engine: store source path is required"),
	} {
		if engine.IsUnsupported(err) {
			t.Fatalf("expected unrelated error to not classify as unsupported: %v", err)
		}
	}
}

func TestEngineErrorCodesAreStable(t *testing.T) {
	want := map[engine.ErrorCode]string{
		engine.ErrorInvalidArgument:    "invalid_argument",
		engine.ErrorNotFound:           "not_found",
		engine.ErrorUnsupported:        "unsupported",
		engine.ErrorInvariantViolation: "invariant_violation",
		engine.ErrorVerificationFailed: "verification_failed",
		engine.ErrorRecoveryFailed:     "recovery_failed",
		engine.ErrorConflict:           "conflict",
		engine.ErrorCancelled:          "cancelled",
		engine.ErrorOperationFailed:    "operation_failed",
	}
	for code, text := range want {
		if string(code) != text {
			t.Errorf("error code changed: got=%q want=%q", code, text)
		}
	}
}

func TestNewErrorPreservesMessageFieldsAndCause(t *testing.T) {
	cause := errors.New("storage unavailable")
	err := engine.NewError(engine.ErrorConflict, "store", "repository busy", "LOCK_BUSY", cause)
	if err.Error() != "repository busy" {
		t.Fatalf("message changed: %q", err.Error())
	}
	if err.Code != engine.ErrorConflict || err.Operation != "store" || err.InvariantCode != "LOCK_BUSY" {
		t.Fatalf("unexpected typed fields: %+v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("typed error must preserve errors.Is cause chain")
	}
	if !engine.IsCode(err, engine.ErrorConflict) || engine.CodeOf(err) != engine.ErrorConflict {
		t.Fatalf("typed classification failed: %v", err)
	}
}

func TestNewErrorRejectsUnknownCodeFailClosed(t *testing.T) {
	err := engine.NewError(engine.ErrorCode("invented"), "inspect", "boom", "", nil)
	if err.Code != engine.ErrorOperationFailed {
		t.Fatalf("unknown code should fail closed: %+v", err)
	}
}

func TestTranslateErrorClassifiesUniversalFailures(t *testing.T) {
	invariant := invariants.New(invariants.CodeGCRefusedIntegrity, "GC refused", nil)
	tests := []struct {
		name          string
		err           error
		want          engine.ErrorCode
		invariantCode string
	}{
		{name: "cancelled", err: context.Canceled, want: engine.ErrorCancelled},
		{name: "deadline", err: context.DeadlineExceeded, want: engine.ErrorCancelled},
		{name: "unsupported", err: catalog.NewError(catalog.ErrorUnsupported, "operation", "", "unsupported mode", nil), want: engine.ErrorUnsupported},
		{name: "invariant", err: invariant, want: engine.ErrorInvariantViolation, invariantCode: invariants.CodeGCRefusedIntegrity},
		{name: "ordinary", err: errors.New("disk unavailable"), want: engine.ErrorOperationFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated := engine.TranslateError("operation", tc.err)
			if !engine.IsCode(translated, tc.want) {
				t.Fatalf("code mismatch: got=%q want=%q err=%v", engine.CodeOf(translated), tc.want, translated)
			}
			if translated.Error() != tc.err.Error() || !errors.Is(translated, tc.err) {
				t.Fatalf("translation changed message or chain: translated=%v original=%v", translated, tc.err)
			}
			var typed *engine.Error
			if !errors.As(translated, &typed) || typed.InvariantCode != tc.invariantCode {
				t.Fatalf("typed detail mismatch: %+v", typed)
			}
		})
	}
}

func TestTranslateErrorAsPreservesUniversalAndExplicitClassifications(t *testing.T) {
	notFound := errors.New("file not found")
	translated := engine.TranslateErrorAs("restore", engine.ErrorNotFound, notFound)
	if !engine.IsCode(translated, engine.ErrorNotFound) || translated.Error() != notFound.Error() {
		t.Fatalf("explicit classification failed: %v", translated)
	}

	cancelled := engine.TranslateErrorAs("restore", engine.ErrorNotFound, context.Canceled)
	if !engine.IsCode(cancelled, engine.ErrorCancelled) {
		t.Fatalf("cancellation must retain universal classification: %v", cancelled)
	}

	existing := engine.NewError(engine.ErrorConflict, "gc", "busy", "", nil)
	if got := engine.TranslateErrorAs("restore", engine.ErrorNotFound, existing); got != existing {
		t.Fatal("existing typed error must not be reclassified")
	}
}

func TestTranslateErrorNilIsNil(t *testing.T) {
	if got := engine.TranslateError("store", nil); got != nil {
		t.Fatalf("nil translation = %v", got)
	}
	if got := engine.TranslateErrorAs("store", engine.ErrorInvalidArgument, nil); got != nil {
		t.Fatalf("nil explicit translation = %v", got)
	}
}
