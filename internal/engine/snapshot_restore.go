package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

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

func (e *DefaultEngine) SnapshotRestore(ctx context.Context, req SnapshotRestoreRequest) (_ SnapshotRestoreResult, outErr error) {
	defer func() { outErr = TranslateError("snapshot_restore", outErr) }()
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
		return SnapshotRestoreResult{}, TranslateErrorAs("snapshot_restore", ErrorInvalidArgument, err)
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

	restoreQuery, err := snapshotRestoreSelectionToSnapshotQuery(req.Selection)
	if err != nil {
		return preparedSnapshotRestoreRequest{}, err
	}

	restoreOpts, err := buildSnapshotRestoreOptions(destinationMode, outputTarget, req.Metadata, req.Overwrite, restoreQuery, storeCtx)
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

func buildSnapshotRestoreOptions(
	mode SnapshotRestoreDestinationMode,
	outputTarget string,
	metadataMode SnapshotRestoreMetadataMode,
	overwrite bool,
	restoreQuery *snapshot.SnapshotQuery,
	storeCtx *storage.StorageContext,
) (snapshot.RestoreSnapshotOptions, error) {
	opts := snapshot.RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationMode(mode),
		StorageContext:  storeCtx,
		Query:           restoreQuery,
		Overwrite:       overwrite,
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

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
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
