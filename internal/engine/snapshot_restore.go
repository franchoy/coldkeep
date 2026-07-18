package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

type preparedSnapshotRestoreRequest struct {
	snapshotID          string
	paths               []string
	requestedPathsCount int
	destinationMode     SnapshotRestoreDestinationMode
	outputTarget        string
	restoreSnapshotOpts snapshot.RestoreSnapshotOptions
}

func (e *DefaultEngine) SnapshotRestore(ctx context.Context, req SnapshotRestoreRequest) (SnapshotRestoreResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SnapshotRestoreResult{}, err
	}
	if e.config.StoreContext == nil {
		return SnapshotRestoreResult{}, fmt.Errorf("engine: snapshot restore requires injected StoreContext")
	}

	prepared, err := prepareSnapshotRestoreRequest(req, e.config.StoreContext)
	if err != nil {
		return SnapshotRestoreResult{}, err
	}

	result, err := snapshot.RestoreSnapshot(ctx, e.config.DB, prepared.snapshotID, prepared.paths, prepared.restoreSnapshotOpts)
	if err != nil {
		return SnapshotRestoreResult{}, err
	}

	return SnapshotRestoreResult{
		SnapshotID:          prepared.snapshotID,
		DestinationMode:     prepared.destinationMode,
		RequestedPathsCount: prepared.requestedPathsCount,
		RestoredFiles:       result.RestoredFiles,
		OutputTarget:        prepared.outputTarget,
		OutputPaths:         copyStrings(result.OutputPaths),
		Warnings:            mapSnapshotRestoreWarnings(result.Warnings),
	}, nil
}

func prepareSnapshotRestoreRequest(
	req SnapshotRestoreRequest,
	storeCtx *storage.StorageContext,
) (preparedSnapshotRestoreRequest, error) {
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if snapshotID == "" {
		return preparedSnapshotRestoreRequest{}, fmt.Errorf("snapshot id cannot be empty")
	}

	paths := copyStrings(req.Paths)
	requestedPathsCount := len(paths)

	destinationMode, outputTarget, err := validateSnapshotRestoreDestination(req.Destination)
	if err != nil {
		return preparedSnapshotRestoreRequest{}, err
	}

	restoreQuery, err := buildSnapshotRestoreQuery(req.Selection)
	if err != nil {
		return preparedSnapshotRestoreRequest{}, err
	}

	restoreOpts, err := buildSnapshotRestoreOptions(destinationMode, outputTarget, req.Metadata, restoreQuery, storeCtx)
	if err != nil {
		return preparedSnapshotRestoreRequest{}, err
	}

	return preparedSnapshotRestoreRequest{
		snapshotID:          snapshotID,
		paths:               paths,
		requestedPathsCount: requestedPathsCount,
		destinationMode:     destinationMode,
		outputTarget:        outputTarget,
		restoreSnapshotOpts: restoreOpts,
	}, nil
}

func validateSnapshotRestoreDestination(dest SnapshotRestoreDestination) (SnapshotRestoreDestinationMode, string, error) {
	outputTarget := strings.TrimSpace(dest.Path)
	if err := validateSnapshotRestoreDestinationMode(dest.Mode); err != nil {
		return "", "", err
	}
	if outputTarget == "" {
		return "", "", snapshotRestoreDestinationPathRequiredError(dest.Mode)
	}
	if dest.Mode == SnapshotRestoreDestinationOverride && hasTrailingPathSeparator(outputTarget) {
		return "", "", fmt.Errorf("engine: snapshot restore override mode requires exact destination file path")
	}
	return dest.Mode, outputTarget, nil
}

func validateSnapshotRestoreDestinationMode(mode SnapshotRestoreDestinationMode) error {
	switch mode {
	case SnapshotRestoreDestinationOriginal, SnapshotRestoreDestinationPrefix, SnapshotRestoreDestinationOverride:
		return nil
	case "":
		return fmt.Errorf("engine: snapshot restore destination mode is required")
	default:
		return fmt.Errorf("engine: unknown snapshot restore destination mode %q", mode)
	}
}

func snapshotRestoreDestinationPathRequiredError(mode SnapshotRestoreDestinationMode) error {
	switch mode {
	case SnapshotRestoreDestinationOriginal:
		return fmt.Errorf("engine: snapshot restore original mode requires destination path")
	case SnapshotRestoreDestinationPrefix:
		return fmt.Errorf("engine: snapshot restore prefix mode requires destination path")
	default:
		return fmt.Errorf("engine: snapshot restore override mode requires destination path")
	}
}

func buildSnapshotRestoreQuery(selection SnapshotRestoreSelection) (*snapshot.SnapshotQuery, error) {
	minSize, maxSize, err := snapshotRestoreSizeRange(selection)
	if err != nil {
		return nil, err
	}
	modifiedAfter, modifiedBefore, err := snapshotRestoreModifiedRange(selection)
	if err != nil {
		return nil, err
	}
	compiledRegex, err := compileSnapshotRestoreRegex(selection.Regex)
	if err != nil {
		return nil, err
	}

	query := &snapshot.SnapshotQuery{
		ExactPaths:     copyStringSet(selection.ExactPaths),
		Prefixes:       copyStrings(selection.Prefixes),
		Pattern:        selection.Pattern,
		Regex:          compiledRegex,
		MinSize:        minSize,
		MaxSize:        maxSize,
		ModifiedAfter:  modifiedAfter,
		ModifiedBefore: modifiedBefore,
	}
	if isEmptySnapshotRestoreQuery(query) {
		return nil, nil
	}
	return query, nil
}

func snapshotRestoreSizeRange(selection SnapshotRestoreSelection) (*int64, *int64, error) {
	minSize := cloneInt64(selection.MinSize)
	maxSize := cloneInt64(selection.MaxSize)
	if minSize != nil && maxSize != nil && *minSize > *maxSize {
		return nil, nil, fmt.Errorf("engine: snapshot restore min size cannot exceed max size")
	}
	return minSize, maxSize, nil
}

func snapshotRestoreModifiedRange(selection SnapshotRestoreSelection) (*time.Time, *time.Time, error) {
	modifiedAfter := cloneTime(selection.ModifiedAfter)
	modifiedBefore := cloneTime(selection.ModifiedBefore)
	if modifiedAfter != nil && modifiedBefore != nil && modifiedAfter.After(*modifiedBefore) {
		return nil, nil, fmt.Errorf("engine: snapshot restore modified-after cannot be after modified-before")
	}
	return modifiedAfter, modifiedBefore, nil
}

func compileSnapshotRestoreRegex(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, nil
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("engine: invalid snapshot restore regex %q: %w", pattern, err)
	}
	return compiled, nil
}

func buildSnapshotRestoreOptions(
	mode SnapshotRestoreDestinationMode,
	outputTarget string,
	metadataMode SnapshotRestoreMetadataMode,
	restoreQuery *snapshot.SnapshotQuery,
	storeCtx *storage.StorageContext,
) (snapshot.RestoreSnapshotOptions, error) {
	opts := snapshot.RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationMode(mode),
		StorageContext:  storeCtx,
		Query:           restoreQuery,
	}

	switch mode {
	case SnapshotRestoreDestinationOriginal:
		opts.OriginalRoot = outputTarget
	case SnapshotRestoreDestinationPrefix, SnapshotRestoreDestinationOverride:
		opts.Destination = outputTarget
	}

	switch metadataMode {
	case SnapshotRestoreMetadataBestEffort:
		return opts, nil
	case SnapshotRestoreMetadataStrict:
		opts.StrictMetadata = true
		return opts, nil
	case SnapshotRestoreMetadataNone:
		opts.NoMetadata = true
		return opts, nil
	default:
		return snapshot.RestoreSnapshotOptions{}, fmt.Errorf("engine: unknown snapshot restore metadata mode %q", metadataMode)
	}
}

func isEmptySnapshotRestoreQuery(q *snapshot.SnapshotQuery) bool {
	if q == nil {
		return true
	}
	return !hasSnapshotRestorePathSelector(q) && !hasSnapshotRestoreRangeSelector(q)
}

func hasSnapshotRestorePathSelector(q *snapshot.SnapshotQuery) bool {
	return len(q.ExactPaths) > 0 || len(q.Prefixes) > 0 || q.Pattern != "" || q.Regex != nil
}

func hasSnapshotRestoreRangeSelector(q *snapshot.SnapshotQuery) bool {
	return q.MinSize != nil || q.MaxSize != nil || q.ModifiedAfter != nil || q.ModifiedBefore != nil
}

func copyStringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}

func cloneInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneTime(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func hasTrailingPathSeparator(path string) bool {
	return strings.HasSuffix(path, "/") || strings.HasSuffix(path, string(filepath.Separator))
}

func mapSnapshotRestoreWarnings(warnings []snapshot.RestoreSnapshotWarning) []SnapshotRestoreWarning {
	if len(warnings) == 0 {
		return nil
	}
	result := make([]SnapshotRestoreWarning, len(warnings))
	for i, warning := range warnings {
		result[i] = SnapshotRestoreWarning{
			Code:      SnapshotRestoreWarningCode(warning.Code),
			Path:      warning.Path,
			Operation: warning.Operation,
			Detail:    warning.Detail,
		}
	}
	return result
}
