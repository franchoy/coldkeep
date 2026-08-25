package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/execution"
	internalgc "github.com/franchoy/coldkeep/internal/gc"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/snapshot"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
)

func isSnapshotDiffSummaryFastPath(req SnapshotDiffRequest) bool {
	return req.Summary && req.Filter == "" && isEmptySnapshotQuery(req.Query)
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
		entries = append(entries, SnapshotDiffEntry{
			StoredPath: entry.Path, Change: SnapshotDiffChange(diffType),
			BaseLogicalID: nullableInt64(entry.BaseLogicalID), TargetLogicalID: nullableInt64(entry.TargetLogicalID),
		})
		addSnapshotDiffSummaryEntry(&summary, diffType)
	}
	return buildSnapshotDiffResult(req, entries, summary, len(raw.Entries)), nil
}

func snapshotQueryOrNil(q SnapshotQuery) (*snapshot.SnapshotQuery, error) {
	if isEmptySnapshotQuery(q) {
		return nil, nil
	}
	return engineQueryToSnapshotQuery(q)
}

func isEmptySnapshotQuery(q SnapshotQuery) bool {
	return len(q.Paths) == 0 && len(q.Prefixes) == 0 && q.Pattern == "" && q.Regex == "" &&
		q.MinSize == nil && q.MaxSize == nil && q.ModifiedAfter == nil && q.ModifiedBefore == nil && q.Limit == 0
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

func (e *DefaultEngine) GarbageCollect(ctx context.Context, req GarbageCollectRequest) (_ GarbageCollectResult, outErr error) {
	defer func() { outErr = TranslateError("garbage_collect", outErr) }()
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

func (e *DefaultEngine) PlanGarbageCollection(ctx context.Context, req GarbageCollectionPlanRequest) (_ GarbageCollectionPlanResult, outErr error) {
	defer func() { outErr = TranslateError("plan_garbage_collection", outErr) }()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return GarbageCollectionPlanResult{}, err
	}
	result := GarbageCollectionPlanResult{SnapshotIDsToOmit: append([]string(nil), req.SnapshotIDsToOmit...)}
	if req.IncludeTrace {
		result.Trace = append(result.Trace, TraceEvent{
			Step: "simulate.gc.start", Message: "starting gc simulation",
			Metadata: map[string]Value{"assumed_deleted_snapshots": integerValue(len(req.SnapshotIDsToOmit))},
		})
		rootMetadata := map[string]Value{"excluded_snapshots": integerValue(len(req.SnapshotIDsToOmit))}
		if roots, rootsErr := catalog.NewServiceFromSQL(e.config.DB).LoadGCPlanMetadata(ctx, catalog.GCPlanInput{ExcludeSnapshotIDs: req.SnapshotIDsToOmit}); rootsErr == nil {
			rootMetadata["root_count"] = integerValue(len(roots.Roots))
		}
		result.Trace = append(result.Trace, TraceEvent{Step: "simulate.gc.roots.load", Message: "loading gc roots", Metadata: rootMetadata})
		for _, snapshotID := range req.SnapshotIDsToOmit {
			result.Trace = append(result.Trace, TraceEvent{
				Step: "simulate.gc.assumption.exclude_snapshot", Entity: "snapshot", EntityID: snapshotID,
				Message: "excluding snapshot from simulation roots",
			})
		}
		result.Trace = append(result.Trace, TraceEvent{Step: "simulate.gc.mark.start", Message: "starting reachable chunk mark traversal"})
	}
	plan, err := internalgc.BuildPlan(ctx, e.config.DB, internalgc.PlanOptions{
		AssumeDeletedSnapshots: append([]string(nil), req.SnapshotIDsToOmit...),
	})
	if err != nil {
		return result, err
	}
	result.Summary = GarbageCollectionPlanSummary{
		TotalChunks: plan.TotalChunks, ReachableChunks: plan.ReachableChunks,
		UnreachableChunks:          plan.Summary.UnreachableChunks,
		LogicallyReclaimableBytes:  plan.Summary.LogicallyReclaimableBytes,
		PhysicallyReclaimableBytes: plan.Summary.PhysicallyReclaimableBytes,
		FullyReclaimableContainers: plan.Summary.FullyReclaimableContainers,
		PartiallyDeadContainers:    plan.Summary.PartiallyDeadContainers,
		PackedBlocksLive:           plan.Summary.PackedBlocksLive, PackedBlocksDead: plan.Summary.PackedBlocksDead,
		PackedBytesLive: plan.Summary.PackedBytesLive, PackedBytesReclaimable: plan.Summary.PackedBytesReclaimable,
		RetainedDeadBytesDueToPackedBlocks: plan.Summary.RetainedDeadBytesDueToPackedBlocks,
	}
	if req.IncludeTrace {
		result.Trace = append(result.Trace,
			TraceEvent{Step: "simulate.gc.mark.complete", Message: "reachable chunk set computed", Metadata: map[string]Value{"reachable_chunks": integerValue(plan.ReachableChunks)}},
			TraceEvent{Step: "simulate.gc.unreachable.compute", Message: "computed unreachable chunk set and logical reclaimability", Metadata: map[string]Value{
				"unreachable_chunks": integerValue(plan.Summary.UnreachableChunks), "logically_reclaimable_bytes": integerValue(plan.Summary.LogicallyReclaimableBytes),
			}},
		)
	}
	for _, item := range plan.AffectedContainers {
		result.Containers = append(result.Containers, GarbageCollectionContainerImpact{
			ContainerID: item.ContainerID, Filename: item.Filename, TotalBytes: item.TotalBytes,
			LiveBytesAfterGC: item.LiveBytesAfterGC, ReclaimableBytes: item.ReclaimableBytes,
			ReclaimableChunks: item.ReclaimableChunks, TotalChunks: item.TotalChunks,
			FullyReclaimable: item.FullyReclaimable, RequiresCompaction: item.RequiresCompaction,
		})
	}
	for _, warning := range plan.Warnings {
		result.Warnings = append(result.Warnings, OperationWarning{Code: warning.Code, Message: warning.Message})
	}
	if req.IncludeTrace {
		result.Trace = append(result.Trace,
			TraceEvent{Step: "simulate.gc.container_impact.compute", Message: "computed per-container reclaim impact", Metadata: map[string]Value{
				"affected_containers":          integerValue(len(plan.AffectedContainers)),
				"fully_reclaimable_containers": integerValue(plan.Summary.FullyReclaimableContainers),
				"partially_dead_containers":    integerValue(plan.Summary.PartiallyDeadContainers),
			}},
			TraceEvent{Step: "simulate.gc.complete", Message: "completed gc simulation", Metadata: map[string]Value{
				"reachable_chunks": integerValue(result.Summary.ReachableChunks), "unreachable_chunks": integerValue(result.Summary.UnreachableChunks),
				"affected_containers": integerValue(len(result.Containers)), "warnings": integerValue(len(result.Warnings)),
				"physically_reclaimable_bytes": integerValue(result.Summary.PhysicallyReclaimableBytes),
			}},
		)
	}
	return result, nil
}

func integerValue(value any) Value {
	converted, err := valueFromAny(value)
	if err != nil {
		panic(err)
	}
	return converted
}

func (e *DefaultEngine) Store(ctx context.Context, req StoreRequest) (_ StoreResult, outErr error) {
	defer func() { outErr = TranslateError("store", outErr) }()
	if err := ctx.Err(); err != nil {
		return StoreResult{}, err
	}
	if strings.TrimSpace(req.SourcePath) == "" {
		return StoreResult{}, TranslateErrorAs("store", ErrorInvalidArgument, fmt.Errorf("engine: store source path is required"))
	}
	if e.config.StoreContext == nil {
		return StoreResult{}, fmt.Errorf("engine: store requires injected StoreContext")
	}
	if strings.TrimSpace(req.Codec) != "" {
		if _, err := blocks.ParseCodec(req.Codec); err != nil {
			return StoreResult{}, TranslateErrorAs("store", ErrorInvalidArgument, err)
		}
	}

	stored, err := storeWithOptionalCodec(ctx, *e.config.StoreContext, req)
	if err != nil {
		return StoreResult{}, err
	}
	return StoreResult{
		SourcePath:    req.SourcePath,
		StoredPath:    stored.Path,
		LogicalFileID: stored.FileID,
		FileHash:      stored.FileHash,
		AlreadyStored: stored.AlreadyStored,
	}, nil
}

func (e *DefaultEngine) StoreFolder(ctx context.Context, req StoreFolderRequest) (_ StoreFolderResult, outErr error) {
	defer func() { outErr = TranslateError("store_folder", outErr) }()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return StoreFolderResult{}, err
	}
	if strings.TrimSpace(req.SourcePath) == "" {
		return StoreFolderResult{}, TranslateErrorAs("store_folder", ErrorInvalidArgument, fmt.Errorf("engine: store folder source path is required"))
	}
	if req.Workers < 0 {
		return StoreFolderResult{}, TranslateErrorAs("store_folder", ErrorInvalidArgument, fmt.Errorf("engine: store folder workers must be zero or greater"))
	}
	if e.config.StoreContext == nil {
		return StoreFolderResult{}, fmt.Errorf("engine: store folder requires injected StoreContext")
	}
	workers := req.Workers
	if workers == 0 {
		workers = execution.DefaultOptions().StoreFolderWorkers
	}
	opts := execution.Options{StoreFolderWorkers: workers, PipelineDepth: 1, Deterministic: true}
	codec, err := blocks.LoadDefaultCodec()
	if strings.TrimSpace(req.Codec) != "" {
		codec, err = blocks.ParseCodec(req.Codec)
	}
	if err != nil {
		return StoreFolderResult{}, TranslateErrorAs("store_folder", ErrorInvalidArgument, err)
	}
	stats, err := storage.StoreFolderWithStorageContextAndCodecAndOptionsWithStatsContext(ctx, *e.config.StoreContext, req.SourcePath, codec, opts)
	result := StoreFolderResult{SourcePath: req.SourcePath, FilesStored: stats.TotalFilesProcessed, BytesLogical: stats.TotalBytesProcessed, WorkersUsed: stats.WorkersUsed}
	return result, err
}

func storeWithOptionalCodec(ctx context.Context, sgctx storage.StorageContext, req StoreRequest) (storage.StoreFileResult, error) {
	if strings.TrimSpace(req.Codec) == "" {
		return storage.StoreFileWithStorageContextResultContext(ctx, sgctx, req.SourcePath)
	}
	codec, err := blocks.ParseCodec(req.Codec)
	if err != nil {
		return storage.StoreFileResult{}, err
	}
	return storage.StoreFileWithStorageContextAndCodecResultContext(ctx, sgctx, req.SourcePath, codec)
}

func validateRemoveRequest(req RemoveRequest) error {
	if len(req.FileIDs) == 0 {
		return fmt.Errorf("engine: remove requires at least one file ID")
	}
	return nil
}

func (e *DefaultEngine) removeFileIDs(ctx context.Context, req RemoveRequest) (RemoveResult, error) {
	result := RemoveResult{
		DryRun:        req.DryRun,
		ExecutionMode: ExecutionModeSequential,
		Items:         make([]RemoveItemResult, 0, len(req.FileIDs)),
	}
	for _, fileID := range req.FileIDs {
		if err := ctx.Err(); err != nil {
			finalizeBatchSummary(&result.Summary, len(req.FileIDs))
			return result, err
		}
		item := e.removeFileID(ctx, req, fileID)
		appendRemoveItem(&result, item)
		if err := ctx.Err(); err != nil {
			finalizeBatchSummary(&result.Summary, len(req.FileIDs))
			return result, err
		}
		if req.FailFast && item.Status == BatchItemFailed {
			break
		}
	}
	finalizeBatchSummary(&result.Summary, len(req.FileIDs))
	return result, nil
}

func (e *DefaultEngine) removeFileID(ctx context.Context, req RemoveRequest, fileID int64) RemoveItemResult {
	if fileID <= 0 {
		return failedRemoveItem(fileID, fmt.Sprintf("invalid file ID %d", fileID))
	}
	if req.DryRun {
		return e.dryRunRemoveFileID(ctx, fileID)
	}
	return e.liveRemoveFileID(ctx, fileID)
}

func (e *DefaultEngine) dryRunRemoveFileID(ctx context.Context, fileID int64) RemoveItemResult {
	if err := dryRunRemoveByID(ctx, e.config.DB, fileID); err != nil {
		return failedRemoveItem(fileID, err.Error())
	}
	return RemoveItemResult{FileID: fileID, Status: BatchItemOK}
}

func (e *DefaultEngine) liveRemoveFileID(ctx context.Context, fileID int64) RemoveItemResult {
	removed, err := storage.RemoveFileWithDBResultContext(ctx, e.config.DB, fileID)
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

func (e *DefaultEngine) restoreFileIDs(ctx context.Context, req RestoreRequest) (RestoreResult, error) {
	result := RestoreResult{
		DryRun:        req.DryRun,
		ExecutionMode: ExecutionModeSequential,
		Items:         make([]RestoreItemResult, 0, len(req.FileIDs)),
	}
	for _, fileID := range req.FileIDs {
		if err := ctx.Err(); err != nil {
			finalizeBatchSummary(&result.Summary, len(req.FileIDs))
			return result, err
		}
		item := e.restoreFileID(ctx, req, fileID)
		appendRestoreItem(&result, item)
		if err := ctx.Err(); err != nil {
			finalizeBatchSummary(&result.Summary, len(req.FileIDs))
			return result, err
		}
		if req.FailFast && item.Status == BatchItemFailed {
			break
		}
	}
	finalizeBatchSummary(&result.Summary, len(req.FileIDs))
	return result, nil
}

func (e *DefaultEngine) restoreFileID(ctx context.Context, req RestoreRequest, fileID int64) RestoreItemResult {
	if fileID <= 0 {
		return failedRestoreItem(fileID, fmt.Sprintf("invalid file ID %d", fileID))
	}
	if req.DryRun {
		return e.dryRunRestoreFileID(ctx, req, fileID)
	}
	return e.liveRestoreFileID(ctx, req, fileID)
}

func (e *DefaultEngine) dryRunRestoreFileID(ctx context.Context, req RestoreRequest, fileID int64) RestoreItemResult {
	item, err := dryRunRestoreByID(ctx, e.config.DB, fileID, req.DestinationRoot, req.Overwrite)
	if err != nil {
		item.Status = BatchItemFailed
		item.Error = err.Error()
	}
	return item
}

func (e *DefaultEngine) liveRestoreFileID(ctx context.Context, req RestoreRequest, fileID int64) RestoreItemResult {
	sgctx := storage.StorageContext{DB: e.config.DB, ContainerDir: e.config.ContainerDir}
	out, originalName, err := restoreByIDOutputPath(ctx, e.config.DB, fileID, req.DestinationRoot)
	if err != nil {
		return failedRestoreItem(fileID, err.Error())
	}
	r, err := storage.RestoreFileWithStorageContextResultOptionsContext(ctx, sgctx, fileID, out, storage.RestoreOptions{
		Overwrite:   req.Overwrite,
		TrustedRoot: req.DestinationRoot,
	})
	if err != nil {
		return failedRestoreItem(fileID, err.Error())
	}
	return RestoreItemResult{
		FileID:          fileID,
		OriginalName:    originalName,
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

func dryRunRestoreByID(ctx context.Context, dbconn *sql.DB, fileID int64, outputDir string, overwrite bool) (RestoreItemResult, error) {
	item := RestoreItemResult{FileID: fileID, Status: BatchItemOK}
	out, originalName, err := restoreByIDOutputPath(ctx, dbconn, fileID, outputDir)
	if err != nil {
		return item, err
	}
	item.DestinationPath = out
	item.OriginalName = originalName
	if !overwrite {
		if _, statErr := os.Stat(out); statErr == nil {
			return item, fmt.Errorf("output file already exists: %s (use --overwrite)", out)
		} else if !os.IsNotExist(statErr) {
			return item, fmt.Errorf("check output path %s: %w", out, statErr)
		}
	}
	return item, nil
}

func restoreByIDOutputPath(ctx context.Context, dbconn *sql.DB, fileID int64, outputDir string) (string, string, error) {
	info, err := storage.GetLogicalFileInfoWithDBContext(ctx, dbconn, fileID)
	if err != nil {
		return "", "", err
	}
	if info.Status != filestate.LogicalFileCompleted {
		return "", "", fmt.Errorf("file ID %d is not COMPLETED", fileID)
	}
	return filepath.Join(outputDir, info.OriginalName), info.OriginalName, nil
}

func dryRunRemoveByID(ctx context.Context, dbconn *sql.DB, fileID int64) error {
	info, err := storage.GetLogicalFileInfoWithDBContext(ctx, dbconn, fileID)
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
