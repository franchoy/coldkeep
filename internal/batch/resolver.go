package batch

import (
	"fmt"
	"strconv"
	"strings"
)

// ResolveTargets validates and parses raw targets into integer IDs.
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
