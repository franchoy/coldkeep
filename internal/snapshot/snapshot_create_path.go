package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/franchoy/coldkeep/internal/catalog"
)

const snapshotMemberPathUnrepresentable = "SNAPSHOT_MEMBER_PATH_UNREPRESENTABLE"

func canonicalizeSnapshotSelectionBase(base string) (string, error) {
	if strings.TrimSpace(base) == "" {
		return "", snapshotCreateInvalidArgument("snapshot selection base cannot be empty", nil)
	}
	if strings.IndexByte(base, 0) >= 0 {
		return "", snapshotCreateInvalidArgument("snapshot selection base contains NUL byte", nil)
	}
	if !filepath.IsAbs(base) {
		return "", snapshotCreateInvalidArgument(
			fmt.Sprintf("snapshot selection base must be absolute: %q", base),
			nil,
		)
	}

	cleanBase := filepath.Clean(base)
	info, err := os.Stat(cleanBase)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrInvalid) {
			return "", snapshotCreateInvalidArgument(
				fmt.Sprintf("snapshot selection base must be an existing directory: %q", base),
				err,
			)
		}
		return "", snapshotCreateOperationFailed(
			fmt.Sprintf("inspect snapshot selection base %q", base),
			err,
		)
	}
	if !info.IsDir() {
		return "", snapshotCreateInvalidArgument(
			fmt.Sprintf("snapshot selection base must be a directory: %q", base),
			nil,
		)
	}

	resolvedBase, err := filepath.EvalSymlinks(cleanBase)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrInvalid) {
			return "", snapshotCreateInvalidArgument(
				fmt.Sprintf("snapshot selection base cannot be resolved: %q", base),
				err,
			)
		}
		return "", snapshotCreateOperationFailed(
			fmt.Sprintf("resolve snapshot selection base %q", base),
			err,
		)
	}
	resolvedBase = filepath.Clean(resolvedBase)
	if !filepath.IsAbs(resolvedBase) {
		return "", snapshotCreateInvalidArgument(
			fmt.Sprintf("resolved snapshot selection base is not absolute: %q", resolvedBase),
			nil,
		)
	}
	return resolvedBase, nil
}

func resolveSnapshotCreateFilters(base string, filters snapshotCreateFilters) (snapshotCreateFilters, error) {
	resolved := snapshotCreateFilters{
		exactSet:     make(map[string]struct{}),
		exactDisplay: make(map[string]string),
	}

	for _, selector := range filters.exactFilters {
		physicalPath, err := resolveSnapshotCreateSelector(base, selector)
		if err != nil {
			return snapshotCreateFilters{}, err
		}
		if _, exists := resolved.exactSet[physicalPath]; exists {
			continue
		}
		resolved.exactFilters = append(resolved.exactFilters, physicalPath)
		resolved.exactSet[physicalPath] = struct{}{}
		resolved.exactDisplay[physicalPath] = selector
	}

	seenPrefixes := make(map[string]struct{})
	for _, selector := range filters.dirPrefixes {
		physicalPath, err := resolveSnapshotCreateSelector(base, strings.TrimSuffix(selector, "/"))
		if err != nil {
			return snapshotCreateFilters{}, err
		}
		if _, exists := seenPrefixes[physicalPath]; exists {
			continue
		}
		seenPrefixes[physicalPath] = struct{}{}
		resolved.dirPrefixes = append(resolved.dirPrefixes, physicalPath)
	}

	sort.Strings(resolved.exactFilters)
	sort.Strings(resolved.dirPrefixes)
	return resolved, nil
}

func resolveSnapshotCreateSelector(base, selector string) (string, error) {
	candidate := filepath.Clean(filepath.Join(base, filepath.FromSlash(selector)))
	contained, err := snapshotPhysicalPathContained(base, candidate)
	if err != nil || !contained {
		return "", snapshotCreateInvalidArgument(
			fmt.Sprintf("snapshot selector %q resolves outside selection base %q", selector, base),
			err,
		)
	}

	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		}
		return "", snapshotCreateOperationFailed(
			fmt.Sprintf("resolve snapshot selector %q", selector),
			err,
		)
	}
	resolved = filepath.Clean(resolved)
	contained, err = snapshotPhysicalPathContained(base, resolved)
	if err != nil || !contained {
		return "", snapshotCreateInvalidArgument(
			fmt.Sprintf("snapshot selector %q resolves outside selection base %q", selector, base),
			err,
		)
	}
	return resolved, nil
}

func canonicalSnapshotPhysicalSource(source string) (string, error) {
	if !utf8.ValidString(source) || strings.IndexByte(source, 0) >= 0 || !filepath.IsAbs(source) {
		return "", snapshotCreateUnrepresentable(
			fmt.Sprintf("physical source identity is malformed: %q", source),
			nil,
		)
	}
	cleanSource := filepath.Clean(source)
	if cleanSource != source {
		return "", snapshotCreateUnrepresentable(
			fmt.Sprintf("physical source identity is not canonical: %q", source),
			nil,
		)
	}

	resolved, err := filepath.EvalSymlinks(cleanSource)
	if err == nil {
		resolved = filepath.Clean(resolved)
		if resolved != cleanSource {
			return "", snapshotCreateUnrepresentable(
				fmt.Sprintf("physical source identity does not match its canonical target: %q", source),
				nil,
			)
		}
		return cleanSource, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return cleanSource, nil
	}
	return "", snapshotCreateUnrepresentable(
		fmt.Sprintf("physical source identity cannot be canonicalized: %q", source),
		err,
	)
}

func snapshotCreatePhysicalPathMatches(
	source string,
	filters snapshotCreateFilters,
	foundExact map[string]struct{},
) bool {
	if len(filters.exactFilters) == 0 && len(filters.dirPrefixes) == 0 {
		return true
	}
	if _, exact := filters.exactSet[source]; exact {
		foundExact[source] = struct{}{}
		return true
	}
	for _, prefix := range filters.dirPrefixes {
		contained, err := snapshotPhysicalPathContained(prefix, source)
		if err == nil && contained && filepath.Clean(prefix) != filepath.Clean(source) {
			return true
		}
	}
	return false
}

func snapshotMemberPath(base, source string) (string, bool, error) {
	contained, err := snapshotPhysicalPathContained(base, source)
	if err != nil || !contained {
		return "", false, err
	}

	relative, err := filepath.Rel(base, source)
	if err != nil {
		return "", false, snapshotCreateUnrepresentable(
			fmt.Sprintf("derive snapshot member for physical source %q", source),
			err,
		)
	}
	if relative == "." || filepath.IsAbs(relative) {
		return "", true, snapshotCreateUnrepresentable(
			fmt.Sprintf("physical source %q cannot become a snapshot member", source),
			nil,
		)
	}

	components := strings.Split(relative, string(filepath.Separator))
	for _, component := range components {
		if component == "" || component == "." || component == ".." || !utf8.ValidString(component) {
			return "", true, snapshotCreateUnrepresentable(
				fmt.Sprintf("physical source %q has an unrepresentable native component", source),
				nil,
			)
		}
	}
	candidate := strings.Join(components, "/")
	normalized, err := NormalizeSnapshotPath(candidate)
	if err != nil || normalized != candidate || !equalSnapshotPathComponents(strings.Split(normalized, "/"), components) {
		return "", true, snapshotCreateUnrepresentable(
			fmt.Sprintf("physical source %q is not losslessly representable as a snapshot member", source),
			err,
		)
	}

	reconstructed := filepath.Clean(filepath.Join(append([]string{base}, components...)...))
	if reconstructed != source {
		return "", true, snapshotCreateUnrepresentable(
			fmt.Sprintf("snapshot member %q does not round-trip to physical source %q", candidate, source),
			nil,
		)
	}
	return candidate, true, nil
}

func snapshotPhysicalPathContained(base, candidate string) (bool, error) {
	relative, err := filepath.Rel(base, candidate)
	if err != nil {
		return false, err
	}
	if relative == "." {
		return true, nil
	}
	if filepath.IsAbs(relative) || relative == ".." {
		return false, nil
	}
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == ".." {
			return false, nil
		}
	}
	return true, nil
}

func equalSnapshotPathComponents(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func collectPendingSnapshotCreateFilesLegacy(
	ctx context.Context,
	tx *sql.Tx,
	dbconn *sql.DB,
	filters snapshotCreateFilters,
) ([]pendingSnapshotFile, error) {
	rows, err := tx.QueryContext(ctx, snapshotSourceQuery(dbconn))
	if err != nil {
		return nil, fmt.Errorf("query snapshot source rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	foundExact := make(map[string]struct{})
	seenPaths := make(map[string]struct{})
	pending := make([]pendingSnapshotFile, 0, 128)
	for rows.Next() {
		var entry pendingSnapshotFile
		var source string
		if err := rows.Scan(&source, &entry.logicalFileID, &entry.totalSize, &entry.mode, &entry.mtime); err != nil {
			return nil, fmt.Errorf("scan snapshot source row: %w", err)
		}
		entry.normalizedPath, err = normalizeSourcePathForSnapshot(source)
		if err != nil {
			return nil, fmt.Errorf("normalize source physical_file path %q: %w", source, err)
		}
		if !snapshotCreatePathMatches(entry.normalizedPath, filters, foundExact) {
			continue
		}
		if _, duplicate := seenPaths[entry.normalizedPath]; duplicate {
			continue
		}
		seenPaths[entry.normalizedPath] = struct{}{}
		pending = append(pending, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot source rows: %w", err)
	}
	return pending, validateSnapshotCreateExactMatches(filters, foundExact)
}

func snapshotCreateInvalidArgument(message string, cause error) error {
	return catalog.NewError(catalog.ErrorInvalidArgument, "create snapshot", "", message, cause)
}

func snapshotCreateOperationFailed(message string, cause error) error {
	return catalog.NewError(catalog.ErrorOperationFailed, "create snapshot", "", message, cause)
}

func snapshotCreateUnrepresentable(message string, cause error) error {
	return catalog.NewError(
		catalog.ErrorInvariantViolation,
		"create snapshot",
		snapshotMemberPathUnrepresentable,
		message,
		cause,
	)
}
