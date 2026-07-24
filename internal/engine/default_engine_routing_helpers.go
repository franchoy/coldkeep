package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/snapshot"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
)

func isSnapshotDiffSummaryFastPath(req SnapshotDiffRequest) bool {
	return req.Summary && req.Filter == "" && req.Query == (SnapshotQuery{})
}

func (e *DefaultEngine) snapshotDiffSummaryFastPath(ctx context.Context, req SnapshotDiffRequest) (SnapshotDiffResult, error) {
	summary, err := snapshot.DiffSnapshotsSummarySQL(ctx, e.config.DB, req.BaseID, req.TargetID)
	if err != nil {
		return SnapshotDiffResult{}, err
	}
	total := int(summary.Added + summary.Removed + summary.Modified)
	return SnapshotDiffResult{
		BaseID:      req.BaseID,
		TargetID:    req.TargetID,
		SummaryMode: true,
		Summary: SnapshotDiffSummary{
			Added:    int(summary.Added),
			Removed:  int(summary.Removed),
			Modified: int(summary.Modified),
		},
		MatchedEntryCount: total,
		TotalEntryCount:   total,
	}, nil
}

func (e *DefaultEngine) snapshotDiffDetailed(ctx context.Context, req SnapshotDiffRequest) (SnapshotDiffResult, error) {
	query, err := snapshotQueryOrNil(req.Query)
	if err != nil {
		return SnapshotDiffResult{}, err
	}
	raw, err := snapshot.DiffSnapshots(ctx, e.config.DB, req.BaseID, req.TargetID, query)
	if err != nil {
		return SnapshotDiffResult{}, err
	}
	entries := make([]SnapshotDiffEntry, 0, len(raw.Entries))
	summary := SnapshotDiffSummary{}
	for _, entry := range raw.Entries {
		diffType := string(entry.Type)
		if !snapshotDiffEntryMatchesFilter(diffType, req.Filter) {
			continue
		}
		entries = append(entries, SnapshotDiffEntry{StoredPath: entry.Path, Change: SnapshotDiffChange(diffType)})
		addSnapshotDiffSummaryEntry(&summary, diffType)
	}
	return buildSnapshotDiffResult(req, entries, summary, len(raw.Entries)), nil
}

func snapshotQueryOrNil(q SnapshotQuery) (*snapshot.SnapshotQuery, error) {
	if q == (SnapshotQuery{}) {
		return nil, nil
	}
	return engineQueryToSnapshotQuery(q)
}

func snapshotDiffEntryMatchesFilter(diffType string, filter SnapshotDiffFilter) bool {
	return filter == "" || SnapshotDiffFilter(diffType) == filter
}

func addSnapshotDiffSummaryEntry(summary *SnapshotDiffSummary, diffType string) {
	switch diffType {
	case string(snapshot.DiffAdded):
		summary.Added++
	case string(snapshot.DiffRemoved):
		summary.Removed++
	case string(snapshot.DiffModified):
		summary.Modified++
	}
}

func buildSnapshotDiffResult(req SnapshotDiffRequest, entries []SnapshotDiffEntry, summary SnapshotDiffSummary, total int) SnapshotDiffResult {
	res := SnapshotDiffResult{
		BaseID:            req.BaseID,
		TargetID:          req.TargetID,
		SummaryMode:       req.Summary,
		Summary:           summary,
		MatchedEntryCount: len(entries),
		TotalEntryCount:   total,
	}
	if !req.Summary {
		res.Entries = entries
	}
	return res
}

func (e *DefaultEngine) GarbageCollect(ctx context.Context, req GarbageCollectRequest) (GarbageCollectResult, error) {
	containerDir := e.config.ContainerDir
	if containerDir == "" {
		containerDir = container.ContainersDir
	}
	gcRes, err := maintenance.RunGCWithDB(ctx, e.config.DB, req.DryRun, containerDir)
	if err != nil {
		return GarbageCollectResult{}, err
	}
	return GarbageCollectResult{
		DryRun:                           gcRes.DryRun,
		AffectedContainers:               gcRes.AffectedContainers,
		ContainerFilenames:               gcRes.ContainerFilenames,
		SnapshotRetainedContainers:       gcRes.SnapshotRetainedContainers,
		SnapshotRetainedLogicalFiles:     gcRes.SnapshotRetainedLogicalFiles,
		CurrentOnlyRetainedLogicalFiles:  gcRes.RetainedCurrentOnlyLogical,
		SnapshotOnlyRetainedLogicalFiles: gcRes.RetainedSnapshotOnlyLogical,
		SharedRetainedLogicalFiles:       gcRes.RetainedSharedLogical,
		BytesReclaimed:                   0, // not computed by current maintenance layer
	}, nil
}

func (e *DefaultEngine) Store(ctx context.Context, req StoreRequest) (StoreResult, error) {
	if err := ctx.Err(); err != nil {
		return StoreResult{}, err
	}
	if req.Recursive {
		return StoreResult{}, ErrNotImplemented
	}
	if strings.TrimSpace(req.SourcePath) == "" {
		return StoreResult{}, fmt.Errorf("engine: store source path is required")
	}
	if e.config.StoreContext == nil {
		return StoreResult{}, fmt.Errorf("engine: store requires injected StoreContext")
	}

	stored, err := storeWithOptionalCodec(*e.config.StoreContext, req)
	if err != nil {
		return StoreResult{}, err
	}
	return StoreResult{
		SourcePath:     req.SourcePath,
		StoredPath:     stored.Path,
		LogicalFileID:  stored.FileID,
		FileHash:       stored.FileHash,
		AlreadyStored:  stored.AlreadyStored,
		PhysicalFileID: 0,
	}, nil
}

func storeWithOptionalCodec(ctx storage.StorageContext, req StoreRequest) (storage.StoreFileResult, error) {
	if strings.TrimSpace(req.Codec) == "" {
		return storage.StoreFileWithStorageContextResult(ctx, req.SourcePath)
	}
	codec, err := blocks.ParseCodec(req.Codec)
	if err != nil {
		return storage.StoreFileResult{}, err
	}
	return storage.StoreFileWithStorageContextAndCodecResult(ctx, req.SourcePath, codec)
}

func validateRemoveRequest(req RemoveRequest) error {
	if len(req.FileIDs) == 0 {
		return fmt.Errorf("engine: remove requires at least one file ID")
	}
	return nil
}

func (e *DefaultEngine) removeFileIDs(req RemoveRequest) RemoveResult {
	result := RemoveResult{
		DryRun:        req.DryRun,
		ExecutionMode: ExecutionModeSequential,
		Items:         make([]RemoveItemResult, 0, len(req.FileIDs)),
	}
	for _, fileID := range req.FileIDs {
		item := e.removeFileID(req, fileID)
		appendRemoveItem(&result, item)
		if req.FailFast && item.Status == BatchItemFailed {
			break
		}
	}
	finalizeBatchSummary(&result.Summary, len(req.FileIDs))
	return result
}

func (e *DefaultEngine) removeFileID(req RemoveRequest, fileID int64) RemoveItemResult {
	if fileID <= 0 {
		return failedRemoveItem(fileID, fmt.Sprintf("invalid file ID %d", fileID))
	}
	if req.DryRun {
		return e.dryRunRemoveFileID(fileID)
	}
	return e.liveRemoveFileID(fileID)
}

func (e *DefaultEngine) dryRunRemoveFileID(fileID int64) RemoveItemResult {
	if err := dryRunRemoveByID(e.config.DB, fileID); err != nil {
		return failedRemoveItem(fileID, err.Error())
	}
	return RemoveItemResult{FileID: fileID, Status: BatchItemOK}
}

func (e *DefaultEngine) liveRemoveFileID(fileID int64) RemoveItemResult {
	removed, err := storage.RemoveFileWithDBResult(e.config.DB, fileID)
	if err != nil {
		return failedRemoveItemWithInvariant(fileID, err)
	}
	return RemoveItemResult{
		FileID:                   fileID,
		Status:                   BatchItemOK,
		RemovedChunkAssociations: removed.RemovedMappings,
		LogicalFileRemoved:       true,
	}
}

func failedRemoveItem(fileID int64, message string) RemoveItemResult {
	return RemoveItemResult{FileID: fileID, Status: BatchItemFailed, Error: message}
}

func failedRemoveItemWithInvariant(fileID int64, err error) RemoveItemResult {
	item := failedRemoveItem(fileID, err.Error())
	if code, ok := invariants.Code(err); ok {
		item.InvariantCode = code
		item.RecommendedAction = invariants.RecommendedActionForCode(code)
	}
	return item
}

func appendRemoveItem(result *RemoveResult, item RemoveItemResult) {
	result.Items = append(result.Items, item)
	if item.Status == BatchItemFailed {
		result.Summary.Failed++
		return
	}
	result.Summary.OK++
}

func validateRestoreRequest(req RestoreRequest) error {
	if len(req.FileIDs) == 0 {
		return fmt.Errorf("engine: restore requires at least one file ID")
	}
	if strings.TrimSpace(req.DestinationRoot) == "" {
		return fmt.Errorf("engine: restore output directory is required")
	}
	return nil
}

func (e *DefaultEngine) restoreFileIDs(req RestoreRequest) RestoreResult {
	result := RestoreResult{
		DryRun:        req.DryRun,
		ExecutionMode: ExecutionModeSequential,
		Items:         make([]RestoreItemResult, 0, len(req.FileIDs)),
	}
	for _, fileID := range req.FileIDs {
		item := e.restoreFileID(req, fileID)
		appendRestoreItem(&result, item)
		if req.FailFast && item.Status == BatchItemFailed {
			break
		}
	}
	finalizeBatchSummary(&result.Summary, len(req.FileIDs))
	return result
}

func (e *DefaultEngine) restoreFileID(req RestoreRequest, fileID int64) RestoreItemResult {
	if fileID <= 0 {
		return failedRestoreItem(fileID, fmt.Sprintf("invalid file ID %d", fileID))
	}
	if req.DryRun {
		return e.dryRunRestoreFileID(req, fileID)
	}
	return e.liveRestoreFileID(req, fileID)
}

func (e *DefaultEngine) dryRunRestoreFileID(req RestoreRequest, fileID int64) RestoreItemResult {
	item, err := dryRunRestoreByID(e.config.DB, fileID, req.DestinationRoot, req.Overwrite)
	if err != nil {
		item.Status = BatchItemFailed
		item.Error = err.Error()
	}
	return item
}

func (e *DefaultEngine) liveRestoreFileID(req RestoreRequest, fileID int64) RestoreItemResult {
	sgctx := storage.StorageContext{DB: e.config.DB, ContainerDir: e.config.ContainerDir}
	out, err := restoreByIDOutputPath(e.config.DB, fileID, req.DestinationRoot)
	if err != nil {
		return failedRestoreItem(fileID, err.Error())
	}
	r, err := storage.RestoreFileWithStorageContextResultOptions(sgctx, fileID, out, storage.RestoreOptions{
		Overwrite:   req.Overwrite,
		TrustedRoot: req.DestinationRoot,
	})
	if err != nil {
		return failedRestoreItem(fileID, err.Error())
	}
	return RestoreItemResult{
		FileID:          fileID,
		Status:          BatchItemOK,
		DestinationPath: r.OutputPath,
		RestoredHash:    r.RestoredHash,
	}
}

func failedRestoreItem(fileID int64, message string) RestoreItemResult {
	return RestoreItemResult{FileID: fileID, Status: BatchItemFailed, Error: message}
}

func appendRestoreItem(result *RestoreResult, item RestoreItemResult) {
	result.Items = append(result.Items, item)
	if item.Status == BatchItemFailed {
		result.Summary.Failed++
		return
	}
	result.Summary.OK++
}

func finalizeBatchSummary(summary *BatchSummary, requested int) {
	summary.Skipped = requested - summary.OK - summary.Failed
	if summary.Skipped < 0 {
		summary.Skipped = 0
	}
}

func dryRunRestoreByID(dbconn *sql.DB, fileID int64, outputDir string, overwrite bool) (RestoreItemResult, error) {
	item := RestoreItemResult{FileID: fileID, Status: BatchItemOK}
	out, err := restoreByIDOutputPath(dbconn, fileID, outputDir)
	if err != nil {
		return item, err
	}
	item.DestinationPath = out
	if !overwrite {
		if _, statErr := os.Stat(out); statErr == nil {
			return item, fmt.Errorf("output file already exists: %s (use --overwrite)", out)
		} else if !os.IsNotExist(statErr) {
			return item, fmt.Errorf("check output path %s: %w", out, statErr)
		}
	}
	return item, nil
}

func restoreByIDOutputPath(dbconn *sql.DB, fileID int64, outputDir string) (string, error) {
	info, err := storage.GetLogicalFileInfoWithDB(dbconn, fileID)
	if err != nil {
		return "", err
	}
	if info.Status != filestate.LogicalFileCompleted {
		return "", fmt.Errorf("file ID %d is not COMPLETED", fileID)
	}
	return filepath.Join(outputDir, info.OriginalName), nil
}

func dryRunRemoveByID(dbconn *sql.DB, fileID int64) error {
	info, err := storage.GetLogicalFileInfoWithDB(dbconn, fileID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("file ID %d not found", fileID)
		}
		return err
	}
	if info.Status == filestate.LogicalFileProcessing {
		return fmt.Errorf("file ID %d is still PROCESSING and cannot be removed", fileID)
	}
	return nil
}
