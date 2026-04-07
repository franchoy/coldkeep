package batch

import (
	"fmt"
	"strconv"
	"strings"
)

// PreparedTarget retains input-order target preparation state.
type PreparedTarget struct {
	ID         int64
	Executable bool
	Result     ItemResult
}

// PrepareTargets parses and deduplicates raw targets while preserving input order.
func PrepareTargets(raw []RawTarget) []PreparedTarget {
	prepared := make([]PreparedTarget, 0, len(raw))
	seen := make(map[int64]struct{}, len(raw))

	for _, item := range raw {
		value := strings.TrimSpace(item.Value)
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil || id <= 0 {
			prepared = append(prepared, PreparedTarget{
				Executable: false,
				Result: ItemResult{
					RawValue: item.Value,
					Status:   ResultFailed,
					Message:  fmt.Sprintf("invalid file ID %q", item.Value),
				},
			})
			continue
		}

		if _, exists := seen[id]; exists {
			prepared = append(prepared, PreparedTarget{
				Executable: false,
				Result: ItemResult{
					ID:      id,
					Status:  ResultSkipped,
					Message: "duplicate target",
				},
			})
			continue
		}

		seen[id] = struct{}{}
		prepared = append(prepared, PreparedTarget{ID: id, Executable: true})
	}

	return prepared
}

// HasExecutableTargets reports whether at least one target can be executed.
func HasExecutableTargets(targets []PreparedTarget) bool {
	for _, target := range targets {
		if target.Executable {
			return true
		}
	}
	return false
}

// ResolveTargets validates and parses raw targets into integer IDs.
//
// Deprecated: transitional v1.1 helper from the legacy plan-based path.
// Prefer PrepareTargets for new code.
func ResolveTargets(raw []RawTarget) ([]ResolvedTarget, []ItemResult) {
	resolved := make([]ResolvedTarget, 0, len(raw))
	results := make([]ItemResult, 0, len(raw))

	for _, item := range raw {
		value := strings.TrimSpace(item.Value)
		if value == "" {
			results = append(results, ItemResult{
				Status:  ResultFailed,
				Message: fmt.Sprintf("invalid file ID %q", item.Value),
			})
			continue
		}

		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil || id <= 0 {
			results = append(results, ItemResult{
				Status:  ResultFailed,
				Message: fmt.Sprintf("invalid file ID %q", value),
			})
			continue
		}

		resolved = append(resolved, ResolvedTarget{ID: id})
	}

	return resolved, results
}

// DeduplicateTargets keeps first occurrence and reports duplicates as skipped.
//
// Deprecated: transitional v1.1 helper from the legacy plan-based path.
// Prefer PrepareTargets for new code.
func DeduplicateTargets(targets []ResolvedTarget) ([]ResolvedTarget, []ItemResult) {
	seen := make(map[int64]struct{}, len(targets))
	deduped := make([]ResolvedTarget, 0, len(targets))
	results := make([]ItemResult, 0)

	for _, target := range targets {
		if _, ok := seen[target.ID]; ok {
			results = append(results, ItemResult{
				ID:      target.ID,
				Status:  ResultSkipped,
				Message: "duplicate target",
			})
			continue
		}

		seen[target.ID] = struct{}{}
		deduped = append(deduped, target)
	}

	return deduped, results
}
