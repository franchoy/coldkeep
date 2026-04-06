package batch

import "strings"

// Resolver turns raw targets into normalized internal targets.
type Resolver interface {
	Resolve(raw []RawTarget) ([]ResolvedTarget, []ItemResult, error)
}

// BasicResolver performs minimal normalization and deduplication.
type BasicResolver struct{}

// NewBasicResolver returns the Step 1 default resolver.
func NewBasicResolver() BasicResolver {
	return BasicResolver{}
}

// Resolve trims targets, removes duplicates, and reports skipped items.
func (r BasicResolver) Resolve(raw []RawTarget) ([]ResolvedTarget, []ItemResult, error) {
	seen := make(map[string]struct{}, len(raw))
	resolved := make([]ResolvedTarget, 0, len(raw))
	results := make([]ItemResult, 0)

	for _, item := range raw {
		name := strings.TrimSpace(item.Value)
		if name == "" {
			results = append(results, ItemResult{
				Target:  item.Value,
				Status:  ResultSkipped,
				Message: "empty target",
			})
			continue
		}

		if _, ok := seen[name]; ok {
			results = append(results, ItemResult{
				Target:  name,
				Status:  ResultSkipped,
				Message: "duplicate target",
			})
			continue
		}

		seen[name] = struct{}{}
		resolved = append(resolved, ResolvedTarget{Name: name, Source: item.Source})
	}

	return resolved, results, nil
}
