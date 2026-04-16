package invariants

import (
	"errors"
	"fmt"
	"testing"
)

func TestCodeExtractsTypedInvariantCode(t *testing.T) {
	err := New(CodeGCRefusedIntegrity, "GC refused", nil)

	code, ok := Code(err)
	if !ok {
		t.Fatal("expected invariant code to be present")
	}
	if code != CodeGCRefusedIntegrity {
		t.Fatalf("code mismatch: want=%s got=%s", CodeGCRefusedIntegrity, code)
	}
}

func TestCodeExtractsThroughWrappedErrors(t *testing.T) {
	inner := New(CodeRepairRefusedOrphanRows, "repair refused", nil)
	err := fmt.Errorf("outer layer: %w", inner)

	code, ok := Code(err)
	if !ok {
		t.Fatal("expected invariant code to be present")
	}
	if code != CodeRepairRefusedOrphanRows {
		t.Fatalf("code mismatch: want=%s got=%s", CodeRepairRefusedOrphanRows, code)
	}
}

func TestRecommendedActionForCode(t *testing.T) {
	action := RecommendedActionForCode(CodePhysicalGraphRefCountMismatch)
	if action == "" {
		t.Fatal("expected non-empty recommended action")
	}

	snapshotAction := RecommendedActionForCode(CodeSnapshotRetainedDeleteBlocked)
	if snapshotAction == "" {
		t.Fatal("expected non-empty recommended action for snapshot retention blocker")
	}

	if got := RecommendedActionForCode("UNKNOWN_CODE"); got != "" {
		t.Fatalf("expected empty action for unknown code, got=%q", got)
	}
}

func TestCodeReturnsFalseForNilAndNonInvariantErrors(t *testing.T) {
	if code, ok := Code(nil); ok || code != "" {
		t.Fatalf("expected nil error to return no code, got code=%q ok=%v", code, ok)
	}

	nonInvariant := errors.New("plain error")
	if code, ok := Code(nonInvariant); ok || code != "" {
		t.Fatalf("expected non-invariant error to return no code, got code=%q ok=%v", code, ok)
	}
}
