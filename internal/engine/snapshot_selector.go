package engine

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

// snapshotSelectorInput is the private neutral selector representation shared
// by read-side snapshot queries and snapshot restore selections. Limit is
// intentionally absent: SnapshotDiff limit semantics remain owned by Phase 9.
type snapshotSelectorInput struct {
	exactPaths     []string
	prefixes       []string
	pattern        string
	regex          string
	minSize        *int64
	maxSize        *int64
	modifiedAfter  *time.Time
	modifiedBefore *time.Time
}

func engineQueryToSnapshotQuery(query SnapshotQuery) (*snapshot.SnapshotQuery, error) {
	return normalizeSnapshotSelector(snapshotSelectorInput{
		exactPaths:     query.Paths,
		prefixes:       query.Prefixes,
		pattern:        query.Pattern,
		regex:          query.Regex,
		minSize:        query.MinSize,
		maxSize:        query.MaxSize,
		modifiedAfter:  query.ModifiedAfter,
		modifiedBefore: query.ModifiedBefore,
	})
}

func snapshotRestoreSelectionToSnapshotQuery(selection SnapshotRestoreSelection) (*snapshot.SnapshotQuery, error) {
	return normalizeSnapshotSelector(snapshotSelectorInput{
		exactPaths:     selection.ExactPaths,
		prefixes:       selection.Prefixes,
		pattern:        selection.Pattern,
		regex:          selection.Regex,
		minSize:        selection.MinSize,
		maxSize:        selection.MaxSize,
		modifiedAfter:  selection.ModifiedAfter,
		modifiedBefore: selection.ModifiedBefore,
	})
}

func normalizeSnapshotSelector(input snapshotSelectorInput) (*snapshot.SnapshotQuery, error) {
	query := &snapshot.SnapshotQuery{
		Pattern:        input.pattern,
		MinSize:        cloneSnapshotSelectorInt64(input.minSize),
		MaxSize:        cloneSnapshotSelectorInt64(input.maxSize),
		ModifiedAfter:  cloneSnapshotSelectorTime(input.modifiedAfter),
		ModifiedBefore: cloneSnapshotSelectorTime(input.modifiedBefore),
	}

	if len(input.exactPaths) > 0 {
		query.ExactPaths = make(map[string]struct{}, len(input.exactPaths))
		for _, rawPath := range input.exactPaths {
			normalized, err := snapshot.NormalizeSnapshotPath(rawPath)
			if err != nil {
				return nil, fmt.Errorf("invalid snapshot query path %q: %w", rawPath, err)
			}
			query.ExactPaths[normalized] = struct{}{}
		}
	}

	if len(input.prefixes) > 0 {
		query.Prefixes = make([]string, 0, len(input.prefixes))
		for _, rawPrefix := range input.prefixes {
			normalized, err := snapshot.NormalizeSnapshotPath(rawPrefix)
			if err != nil {
				return nil, fmt.Errorf("invalid snapshot query prefix %q: %w", rawPrefix, err)
			}
			if !strings.HasSuffix(normalized, "/") {
				return nil, fmt.Errorf("invalid snapshot query prefix %q: must end with '/'", rawPrefix)
			}
			query.Prefixes = append(query.Prefixes, normalized)
		}
	}

	if input.pattern != "" {
		if _, err := path.Match(input.pattern, ""); err != nil {
			return nil, fmt.Errorf("invalid snapshot query pattern %q: %w", input.pattern, err)
		}
	}
	if input.regex != "" {
		compiled, err := regexp.Compile(input.regex)
		if err != nil {
			return nil, fmt.Errorf("invalid snapshot query regex %q: %w", input.regex, err)
		}
		query.Regex = compiled
	}
	if (query.MinSize != nil && *query.MinSize < 0) || (query.MaxSize != nil && *query.MaxSize < 0) {
		return nil, fmt.Errorf("invalid snapshot query size range")
	}
	if query.MinSize != nil && query.MaxSize != nil && *query.MinSize > *query.MaxSize {
		return nil, fmt.Errorf("invalid snapshot query size range: minimum exceeds maximum")
	}
	if query.ModifiedAfter != nil && query.ModifiedBefore != nil && query.ModifiedAfter.After(*query.ModifiedBefore) {
		return nil, fmt.Errorf("invalid snapshot query time range: after exceeds before")
	}

	if snapshotSelectorIsEmpty(input) {
		return nil, nil
	}
	return query, nil
}

func snapshotSelectorIsEmpty(input snapshotSelectorInput) bool {
	return len(input.exactPaths) == 0 && len(input.prefixes) == 0 && input.pattern == "" && input.regex == "" &&
		input.minSize == nil && input.maxSize == nil && input.modifiedAfter == nil && input.modifiedBefore == nil
}

func cloneSnapshotSelectorInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneSnapshotSelectorTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}
