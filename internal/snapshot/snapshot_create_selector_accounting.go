package snapshot

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/franchoy/coldkeep/internal/catalog"
)

type snapshotCreateSelectorKind uint8

const (
	snapshotCreateSelectorExact snapshotCreateSelectorKind = iota
	snapshotCreateSelectorPrefix
)

type snapshotCreateSelector struct {
	kind       snapshotCreateSelectorKind
	canonical  string
	display    string
	matchCount int
}

type snapshotCreateSelectorAccounting struct {
	selectors []snapshotCreateSelector
}

func newPhysicalSnapshotCreateSelectorAccounting(
	base string,
	input snapshotCreateFilters,
	resolved snapshotCreateFilters,
) (*snapshotCreateSelectorAccounting, error) {
	prefixDisplay := make(map[string]string, len(input.dirPrefixes))
	for _, selector := range input.dirPrefixes {
		physicalPath, err := resolveSnapshotCreateSelector(base, strings.TrimSuffix(selector, "/"))
		if err != nil {
			return nil, err
		}
		if current, exists := prefixDisplay[physicalPath]; !exists || selector < current {
			prefixDisplay[physicalPath] = selector
		}
	}

	accounting := &snapshotCreateSelectorAccounting{}
	for _, exactPath := range resolved.exactFilters {
		display := resolved.exactDisplay[exactPath]
		if display == "" {
			display = exactPath
		}
		accounting.selectors = append(accounting.selectors, snapshotCreateSelector{
			kind:      snapshotCreateSelectorExact,
			canonical: exactPath,
			display:   display,
		})
	}
	for _, prefix := range resolved.dirPrefixes {
		display := prefixDisplay[prefix]
		if display == "" {
			relative, err := filepath.Rel(base, prefix)
			if err != nil {
				return nil, snapshotCreateOperationFailed("derive snapshot selector display path", err)
			}
			display = filepath.ToSlash(relative) + "/"
		}
		accounting.selectors = append(accounting.selectors, snapshotCreateSelector{
			kind:      snapshotCreateSelectorPrefix,
			canonical: prefix,
			display:   display,
		})
	}
	return accounting, nil
}

func (accounting *snapshotCreateSelectorAccounting) matchPhysical(source string) bool {
	if len(accounting.selectors) == 0 {
		return true
	}
	matched := false
	for index := range accounting.selectors {
		selector := &accounting.selectors[index]
		selectorMatched := selector.canonical == source
		if selector.kind == snapshotCreateSelectorPrefix {
			contained, err := snapshotPhysicalPathContained(selector.canonical, source)
			selectorMatched = err == nil && contained && filepath.Clean(selector.canonical) != filepath.Clean(source)
		}
		if selectorMatched {
			selector.matchCount++
			matched = true
		}
	}
	return matched
}

func (accounting *snapshotCreateSelectorAccounting) validate() error {
	missing := make([]string, 0)
	for _, selector := range accounting.selectors {
		if selector.matchCount == 0 {
			missing = append(missing, selector.display)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return catalog.NewError(
		catalog.ErrorNotFound,
		"create snapshot",
		"",
		fmt.Sprintf("path not found in current state: %s", strings.Join(missing, ", ")),
		nil,
	)
}
