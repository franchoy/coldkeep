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
