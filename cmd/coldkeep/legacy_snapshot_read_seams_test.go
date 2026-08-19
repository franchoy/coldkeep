package main

import (
	"context"
	"database/sql"
	"regexp"
	"time"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

// These seams exist only so pre-v1.13.12 CLI compatibility tests can keep
// supplying deterministic snapshot-domain fixtures. Production code has no
// direct snapshot read seam: it constructs and calls an Engine.
var listSnapshotsPhase = snapshot.ListSnapshots
var getSnapshotPhase = snapshot.GetSnapshot
var listSnapshotFilesPhase = snapshot.ListSnapshotFiles
var snapshotStatsPhase = snapshot.GetSnapshotStats
var diffSnapshotsPhase = snapshot.DiffSnapshots
var diffSnapshotSummaryPhase = snapshot.DiffSnapshotsSummarySQL

var productionSnapshotReadCommandEngine = newSnapshotReadCommandEngine

func init() {
	newSnapshotReadCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
		return legacySnapshotReadTestEngine{db: sgctx.DB}, nil
	}
}

type legacySnapshotReadTestEngine struct {
	engine.Engine
	db *sql.DB
}

func (e legacySnapshotReadTestEngine) SnapshotList(ctx context.Context, req engine.SnapshotListRequest) (engine.SnapshotListResult, error) {
	filter := snapshot.SnapshotListFilter{Since: req.Since, Until: req.Until, Limit: req.Limit, Tree: req.Tree}
	if req.Type != "" {
		value := string(req.Type)
		filter.Type = &value
	}
	if req.Label != "" {
		value := req.Label
		filter.Label = &value
	}
	items, err := listSnapshotsPhase(ctx, e.db, filter)
	if err != nil {
		return engine.SnapshotListResult{}, err
	}
	result := engine.SnapshotListResult{Snapshots: make([]engine.SnapshotMeta, len(items)), Count: len(items), TreeMode: req.Tree}
	for i, item := range items {
		result.Snapshots[i] = snapshotMetaFromLegacy(item)
	}
	if req.Tree {
		result.Graph = legacySnapshotGraph(items)
	}
	return result, nil
}

func (e legacySnapshotReadTestEngine) SnapshotShow(ctx context.Context, req engine.SnapshotShowRequest) (engine.SnapshotShowResult, error) {
	item, err := getSnapshotPhase(ctx, e.db, req.SnapshotID)
	if err != nil {
		return engine.SnapshotShowResult{}, err
	}
	query, err := legacySnapshotQuery(req.Query)
	if err != nil {
		return engine.SnapshotShowResult{}, err
	}
	files, err := listSnapshotFilesPhase(ctx, e.db, req.SnapshotID, req.Query.Limit, query)
	if err != nil {
		return engine.SnapshotShowResult{}, err
	}
	stats, err := snapshotStatsPhase(ctx, e.db, req.SnapshotID)
	if err != nil {
		return engine.SnapshotShowResult{}, err
	}
	result := engine.SnapshotShowResult{
		Snapshot:         snapshotMetaFromLegacy(*item),
		Files:            make([]engine.SnapshotFile, len(files)),
		MatchedFileCount: len(files),
		TotalFileCount:   int(stats.SnapshotFileCount),
	}
	for i, file := range files {
		result.Files[i] = engine.SnapshotFile{
			StoredPath: file.Path, LogicalFileID: file.LogicalFileID,
			Size: nullableInt64FromLegacy(file.Size), Mode: nullableInt64FromLegacy(file.Mode),
			ModTime: nullableTimeFromLegacy(file.MTime),
		}
	}
	return result, nil
}

func (e legacySnapshotReadTestEngine) SnapshotStats(ctx context.Context, req engine.SnapshotStatsRequest) (engine.SnapshotStatsResult, error) {
	stats, err := snapshotStatsPhase(ctx, e.db, req.SnapshotID)
	if err != nil {
		return engine.SnapshotStatsResult{}, err
	}
	return engine.SnapshotStatsResult{
		SnapshotCount: int(stats.SnapshotCount), SnapshotFileCount: int(stats.SnapshotFileCount),
		TotalSizeBytes: stats.TotalSizeBytes, HasReuse: stats.ParentSnapshotID.Valid,
		Reused: int(stats.ReusedFileCount.Int64), New: int(stats.NewFileCount.Int64),
		ReuseRatio: stats.ReuseRatioPct.Float64, LineageStatus: string(stats.LineageStatus),
		ParentSnapshotID: stats.ParentSnapshotID.String,
	}, nil
}

func (e legacySnapshotReadTestEngine) SnapshotDiff(ctx context.Context, req engine.SnapshotDiffRequest) (engine.SnapshotDiffResult, error) {
	if req.Summary && req.Filter == "" && isLegacySnapshotQueryEmpty(req.Query) {
		summary, err := diffSnapshotSummaryPhase(ctx, e.db, req.BaseID, req.TargetID)
		if err != nil {
			return engine.SnapshotDiffResult{}, err
		}
		total := int(summary.Added + summary.Removed + summary.Modified)
		return engine.SnapshotDiffResult{
			BaseID: req.BaseID, TargetID: req.TargetID, SummaryMode: true,
			Summary:           engine.SnapshotDiffSummary{Added: int(summary.Added), Removed: int(summary.Removed), Modified: int(summary.Modified)},
			MatchedEntryCount: total, TotalEntryCount: total,
		}, nil
	}
	query, err := legacySnapshotQuery(req.Query)
	if err != nil {
		return engine.SnapshotDiffResult{}, err
	}
	raw, err := diffSnapshotsPhase(ctx, e.db, req.BaseID, req.TargetID, query)
	if err != nil {
		return engine.SnapshotDiffResult{}, err
	}
	result := engine.SnapshotDiffResult{BaseID: req.BaseID, TargetID: req.TargetID, SummaryMode: req.Summary, TotalEntryCount: len(raw.Entries)}
	for _, item := range raw.Entries {
		if req.Filter != "" && string(req.Filter) != string(item.Type) {
			continue
		}
		result.MatchedEntryCount++
		switch item.Type {
		case snapshot.DiffAdded:
			result.Summary.Added++
		case snapshot.DiffRemoved:
			result.Summary.Removed++
		case snapshot.DiffModified:
			result.Summary.Modified++
		}
		if !req.Summary {
			result.Entries = append(result.Entries, engine.SnapshotDiffEntry{
				StoredPath: item.Path, Change: engine.SnapshotDiffChange(item.Type),
				BaseLogicalID:   nullableInt64FromLegacy(item.BaseLogicalID),
				TargetLogicalID: nullableInt64FromLegacy(item.TargetLogicalID),
			})
		}
	}
	return result, nil
}

func snapshotMetaFromLegacy(item snapshot.Snapshot) engine.SnapshotMeta {
	return engine.SnapshotMeta{
		ID: item.ID, Type: engine.SnapshotType(item.Type), Label: item.Label.String,
		ParentID: item.ParentID.String, CreatedAt: item.CreatedAt,
	}
}

func legacySnapshotGraph(items []snapshot.Snapshot) *engine.SnapshotGraph {
	ordered := snapshotSortAscending(items)
	result := &engine.SnapshotGraph{Nodes: make([]engine.SnapshotGraphNode, len(ordered)), RootIDs: make([]string, 0)}
	index := make(map[string]int, len(ordered))
	for i, item := range ordered {
		index[item.ID] = i
		result.Nodes[i] = engine.SnapshotGraphNode{Snapshot: snapshotMetaFromLegacy(item), ChildIDs: make([]string, 0)}
	}
	for i, item := range ordered {
		if !item.ParentID.Valid {
			result.Nodes[i].ParentState = engine.SnapshotParentNone
			result.RootIDs = append(result.RootIDs, item.ID)
			continue
		}
		parentIndex, ok := index[item.ParentID.String]
		if !ok {
			result.Nodes[i].ParentState = engine.SnapshotParentMissing
			result.RootIDs = append(result.RootIDs, item.ID)
			continue
		}
		result.Nodes[i].ParentState = engine.SnapshotParentPresent
		result.Nodes[parentIndex].ChildIDs = append(result.Nodes[parentIndex].ChildIDs, item.ID)
	}
	return result
}

func legacySnapshotQuery(query engine.SnapshotQuery) (*snapshot.SnapshotQuery, error) {
	if isLegacySnapshotQueryEmpty(query) {
		return nil, nil
	}
	result := &snapshot.SnapshotQuery{
		ExactPaths: make(map[string]struct{}, len(query.Paths)), Prefixes: append([]string(nil), query.Prefixes...),
		Pattern: query.Pattern, MinSize: query.MinSize, MaxSize: query.MaxSize,
		ModifiedAfter: query.ModifiedAfter, ModifiedBefore: query.ModifiedBefore,
	}
	for _, item := range query.Paths {
		result.ExactPaths[item] = struct{}{}
	}
	if query.Regex != "" {
		compiled, err := regexp.Compile(query.Regex)
		if err != nil {
			return nil, err
		}
		result.Regex = compiled
	}
	return result, nil
}

func isLegacySnapshotQueryEmpty(query engine.SnapshotQuery) bool {
	return len(query.Paths) == 0 && len(query.Prefixes) == 0 && query.Pattern == "" && query.Regex == "" &&
		query.MinSize == nil && query.MaxSize == nil && query.ModifiedAfter == nil && query.ModifiedBefore == nil && query.Limit == 0
}

func nullableInt64FromLegacy(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func nullableTimeFromLegacy(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
