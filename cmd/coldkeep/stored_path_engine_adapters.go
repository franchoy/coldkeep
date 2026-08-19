package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/storage"
)

var connectListSearchDBPhase = db.ConnectDB

var newCommandEngine = func(dbconn *sql.DB, containerDir string) (engine.Engine, error) {
	return engine.New(engine.Config{DB: dbconn, ContainerDir: containerDir})
}

var newSnapshotRestoreCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{
		DB:           sgctx.DB,
		ContainerDir: sgctx.EffectiveContainerDir(),
		StoreContext: &sgctx,
	})
}

var newStoreFolderCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{
		DB:           sgctx.DB,
		ContainerDir: sgctx.EffectiveContainerDir(),
		StoreContext: &sgctx,
	})
}

var newConfigurationCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}

var newSnapshotReadCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}

var newVerifyCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}

var newObservabilityCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}

var newRepairCommandEngine = func(dbconn *sql.DB) (engine.Engine, error) {
	return engine.New(engine.Config{DB: dbconn})
}

var connectRepairDBPhase = db.ConnectDB

func restoreStoredPathWithEngine(
	ctx context.Context,
	eng engine.Engine,
	storedPath string,
	mode storage.RestoreDestinationMode,
	destination string,
	overwrite bool,
	strictMetadata bool,
	noMetadata bool,
) (storage.RestoreFileResult, storage.RestoreDestinationMode, error) {
	engineMode, err := storageRestoreModeToEngine(mode)
	if err != nil {
		return storage.RestoreFileResult{}, "", err
	}

	req := engine.RestoreStoredPathRequest{
		StoredPath:      storedPath,
		DestinationMode: engineMode,
		Overwrite:       overwrite,
		StrictMetadata:  strictMetadata,
		NoMetadata:      noMetadata,
	}

	switch mode {
	case storage.RestoreDestinationOriginal:
	case storage.RestoreDestinationPrefix:
		req.DestinationRoot = destination
	case storage.RestoreDestinationOverride:
		req.DestinationPath = destination
	default:
		return storage.RestoreFileResult{}, "", fmt.Errorf("unsupported restore destination mode %q", mode)
	}

	result, err := eng.RestoreStoredPath(ctx, req)
	if err != nil {
		return storage.RestoreFileResult{}, "", err
	}

	normalizedMode, err := engineRestoreModeToStorage(result.DestinationMode)
	if err != nil {
		return storage.RestoreFileResult{}, "", err
	}

	return storage.RestoreFileResult{
		FileID:       result.FileID,
		OutputPath:   result.DestinationPath,
		RestoredHash: result.RestoredHash,
	}, normalizedMode, nil
}

func removeStoredPathWithEngine(
	ctx context.Context,
	eng engine.Engine,
	storedPath string,
) (storage.RemovePhysicalFileResult, error) {
	result, err := eng.RemoveStoredPaths(ctx, engine.RemoveStoredPathsRequest{
		StoredPaths: []string{storedPath},
		FailFast:    true,
	})
	if err != nil {
		return storage.RemovePhysicalFileResult{}, err
	}
	if len(result.Items) != 1 {
		return storage.RemovePhysicalFileResult{}, fmt.Errorf("remove stored path: expected one item result, got %d", len(result.Items))
	}

	item := result.Items[0]
	switch item.Status {
	case engine.BatchItemOK:
		return storage.RemovePhysicalFileResult{
			StoredPath:        item.StoredPath,
			LogicalFileID:     item.LogicalFileID,
			RemainingRefCount: item.RemainingRefCount,
			Removed:           item.MappingRemoved,
		}, nil
	case engine.BatchItemFailed:
		return storage.RemovePhysicalFileResult{}, removeStoredPathItemError(item)
	default:
		return storage.RemovePhysicalFileResult{}, fmt.Errorf("remove stored path: unexpected item status %q", item.Status)
	}
}

func removeStoredPathsResultToBatchReport(result engine.RemoveStoredPathsResult, failFast bool) batch.Report {
	results := make([]batch.ItemResult, 0, len(result.Items))
	for _, item := range result.Items {
		results = append(results, removeStoredPathItemToBatchResult(item))
	}

	report := batch.Report{
		Operation: batch.OperationRemove,
		DryRun:    result.DryRun,
		Results:   results,
		Summary: batch.Summary{
			Total:   result.Summary.OK + result.Summary.Failed + result.Summary.Skipped,
			Failed:  result.Summary.Failed,
			Skipped: result.Summary.Skipped,
		},
	}
	if result.DryRun {
		report.Summary.Planned = result.Summary.OK
		report.ExecutionMode = batch.ExecutionModeContinueOnError
	} else {
		report.Summary.Success = result.Summary.OK
		report.ExecutionMode = batch.ExecutionModeContinueOnError
	}
	if failFast {
		report.ExecutionMode = batch.ExecutionModeFailFast
	}

	return report
}

func removeStoredPathItemToBatchResult(item engine.RemoveStoredPathItemResult) batch.ItemResult {
	status := engineBatchStatusToCLI(item.Status)
	return batch.ItemResult{
		ID:                item.LogicalFileID,
		RawValue:          storedPathBatchRawValue(item),
		Status:            status,
		Message:           removeStoredPathBatchMessage(item),
		InvariantCode:     item.InvariantCode,
		RecommendedAction: item.RecommendedAction,
	}
}

func removeStoredPathBatchMessage(item engine.RemoveStoredPathItemResult) string {
	switch item.Status {
	case engine.BatchItemOK:
		return fmt.Sprintf("removed stored_path remaining_ref_count=%d", item.RemainingRefCount)
	case engine.BatchItemPlanned:
		return "would remove stored-path mapping"
	case engine.BatchItemSkipped:
		return defaultStoredPathBatchMessage(strings.TrimSpace(item.Error), "duplicate target")
	case engine.BatchItemFailed:
		return failedStoredPathBatchMessage(item)
	default:
		return strings.TrimSpace(item.Error)
	}
}

func defaultStoredPathBatchMessage(message, fallback string) string {
	if message == "" {
		return fallback
	}
	return message
}

func failedStoredPathBatchMessage(item engine.RemoveStoredPathItemResult) string {
	message := strings.TrimSpace(item.Error)
	if item.StoredPath == "" && item.RawTarget != "" && message == "stored path is required" {
		return fmt.Sprintf("invalid stored path %q", item.RawTarget)
	}
	return message
}

func storedPathBatchRawValue(item engine.RemoveStoredPathItemResult) string {
	if item.Status == engine.BatchItemSkipped && strings.TrimSpace(item.StoredPath) != "" {
		return item.StoredPath
	}
	if strings.TrimSpace(item.StoredPath) != "" {
		return item.StoredPath
	}
	return item.RawTarget
}

func removeStoredPathItemError(item engine.RemoveStoredPathItemResult) error {
	message := strings.TrimSpace(item.Error)
	if message == "" {
		message = "remove stored path failed"
	}
	if strings.TrimSpace(item.InvariantCode) != "" {
		return invariants.New(item.InvariantCode, message, nil)
	}
	return errors.New(message)
}

func storageRestoreModeToEngine(mode storage.RestoreDestinationMode) (engine.RestoreDestinationMode, error) {
	switch mode {
	case storage.RestoreDestinationOriginal:
		return engine.RestoreDestinationOriginal, nil
	case storage.RestoreDestinationPrefix:
		return engine.RestoreDestinationPrefix, nil
	case storage.RestoreDestinationOverride:
		return engine.RestoreDestinationOverride, nil
	default:
		return "", fmt.Errorf("unsupported restore destination mode %q", mode)
	}
}

func engineRestoreModeToStorage(mode engine.RestoreDestinationMode) (storage.RestoreDestinationMode, error) {
	switch mode {
	case engine.RestoreDestinationOriginal:
		return storage.RestoreDestinationOriginal, nil
	case engine.RestoreDestinationPrefix:
		return storage.RestoreDestinationPrefix, nil
	case engine.RestoreDestinationOverride:
		return storage.RestoreDestinationOverride, nil
	default:
		return "", fmt.Errorf("unsupported engine restore destination mode %q", mode)
	}
}

func engineBatchStatusToCLI(status engine.BatchItemStatus) batch.ItemResultStatus {
	switch status {
	case engine.BatchItemOK:
		return batch.ResultSuccess
	case engine.BatchItemPlanned:
		return batch.ResultPlanned
	case engine.BatchItemSkipped:
		return batch.ResultSkipped
	default:
		return batch.ResultFailed
	}
}
