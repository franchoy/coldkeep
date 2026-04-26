package observability

import (
	"context"
	"testing"
	"time"
)

func TestSimulateFoundationPlaceholder(t *testing.T) {
	fixedNow := time.Date(2026, time.April, 26, 12, 30, 0, 0, time.UTC)
	svc := NewService(WithNowFunc(func() time.Time { return fixedNow }))

	result, err := svc.Simulate(context.Background(), SimulationTarget{Kind: "gc"})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}

	if result.GeneratedAtUTC != fixedNow {
		t.Fatalf("generated_at_utc mismatch: got %s want %s", result.GeneratedAtUTC, fixedNow)
	}
	if result.Exact {
		t.Fatal("expected exact=false for phase 1 placeholder")
	}
	if result.Mutated {
		t.Fatal("expected mutated=false")
	}
	if result.Kind != "gc" {
		t.Fatalf("unexpected kind: %q", result.Kind)
	}
	if len(result.Warnings) == 0 {
		t.Fatal("expected at least one warning")
	}
}

func TestSimulateRejectsEmptyKind(t *testing.T) {
	svc := NewService()

	_, err := svc.Simulate(context.Background(), SimulationTarget{})
	if err == nil {
		t.Fatal("expected error")
	}
}
