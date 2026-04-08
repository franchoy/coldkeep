package batch

import (
	"slices"
	"testing"
)

func TestResolveTargets(t *testing.T) {
	raw := []RawTarget{
		{Value: "12", Source: "args"},
		{Value: "oops", Source: "args"},
		{Value: "0", Source: "input"},
		{Value: "18", Source: "input"},
	}

	resolved, results := ResolveTargets(raw)
	ids := make([]int64, 0, len(resolved))
	for _, item := range resolved {
		ids = append(ids, item.ID)
	}

	if !slices.Equal(ids, []int64{12, 18}) {
		t.Fatalf("resolved IDs mismatch: got=%v", ids)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 invalid-target results, got=%d", len(results))
	}
	for _, result := range results {
		if result.Status != ResultFailed {
			t.Fatalf("expected failed result, got=%v", result)
		}
	}
}

func TestDeduplicateTargets(t *testing.T) {
	targets := []ResolvedTarget{{ID: 12}, {ID: 18}, {ID: 12}, {ID: 18}, {ID: 24}}
	deduped, results := DeduplicateTargets(targets)

	got := make([]int64, 0, len(deduped))
	for _, target := range deduped {
		got = append(got, target.ID)
	}
	if !slices.Equal(got, []int64{12, 18, 24}) {
		t.Fatalf("deduped IDs mismatch: got=%v", got)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 duplicate results, got=%d", len(results))
	}
	for _, result := range results {
		if result.Status != ResultSkipped {
			t.Fatalf("expected skipped duplicate result, got=%v", result)
		}
	}
}

func TestPrepareTargetsPreservesInputOrder(t *testing.T) {
	raw := []RawTarget{
		{Value: "12", Source: "args"},
		{Value: "abc", Source: "args"},
		{Value: "18", Source: "input"},
		{Value: "12", Source: "input"},
	}

	prepared := PrepareTargets(raw)
	if len(prepared) != 4 {
		t.Fatalf("prepared target length mismatch: got=%d", len(prepared))
	}

	if !prepared[0].Executable || prepared[0].ID != 12 {
		t.Fatalf("unexpected first prepared target: %+v", prepared[0])
	}
	if prepared[1].Executable || prepared[1].Result.Status != ResultFailed || prepared[1].Result.RawValue != "abc" {
		t.Fatalf("unexpected second prepared target: %+v", prepared[1])
	}
	if !prepared[2].Executable || prepared[2].ID != 18 {
		t.Fatalf("unexpected third prepared target: %+v", prepared[2])
	}
	if prepared[3].Executable || prepared[3].Result.Status != ResultSkipped || prepared[3].Result.ID != 12 {
		t.Fatalf("unexpected fourth prepared target: %+v", prepared[3])
	}
}

func TestHasExecutableTargets(t *testing.T) {
	if HasExecutableTargets(nil) {
		t.Fatal("expected no executable targets for nil slice")
	}

	onlyInvalid := PrepareTargets([]RawTarget{{Value: "abc", Source: "args"}})
	if HasExecutableTargets(onlyInvalid) {
		t.Fatalf("expected no executable targets, got=%+v", onlyInvalid)
	}

	withValid := PrepareTargets([]RawTarget{{Value: "12", Source: "args"}, {Value: "12", Source: "args"}})
	if !HasExecutableTargets(withValid) {
		t.Fatalf("expected executable target, got=%+v", withValid)
	}
}
