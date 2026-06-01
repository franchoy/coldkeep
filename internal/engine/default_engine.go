package engine

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/verify"
)

// Config holds configuration for a DefaultEngine.
//
// Database backend selection (SQLite vs PostgreSQL) is not decided here;
// the caller is responsible for opening the correct backend and providing
// the connection. Config fields will expand as wrapper-only implementations
// require additional dependencies.
type Config struct {
	// DB is the active database connection.
	// The caller is responsible for the connection lifetime.
	DB *sql.DB
	// ContainerDir is the path to the coldkeep containers directory.
	// Defaults to container.ContainersDir if empty.
	ContainerDir string
}

// DefaultEngine is the canonical Engine implementation.
//
// Phase 2: wrapper-only. All methods delegate to existing domain packages.
// No business logic is moved; the engine is a thin delegation layer.
type DefaultEngine struct {
	config Config
	obs    *observability.Service
}

// New returns a new DefaultEngine with the given configuration.
// Returns an error if DB is nil or if the observability service cannot be
// initialized.
func New(cfg Config) (*DefaultEngine, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("engine: Config.DB is required")
	}
	obs, err := observability.NewService(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("engine: observability service: %w", err)
	}
	return &DefaultEngine{config: cfg, obs: obs}, nil
}

func (e *DefaultEngine) Stats(ctx context.Context, req StatsRequest) (StatsResult, error) {
	r, err := e.obs.Stats(ctx, observability.StatsOptions{
		IncludeContainers: req.IncludeContainers,
		Trace:             req.Trace,
	})
	if err != nil {
		return StatsResult{}, err
	}
	return StatsResult{Raw: r}, nil
}

func (e *DefaultEngine) Inspect(ctx context.Context, req InspectRequest) (InspectResult, error) {
	if err := validateInspectRequest(req); err != nil {
		return InspectResult{}, err
	}
	r, err := e.obs.Inspect(ctx, req.Entity, req.EntityID, req.Options)
	if err != nil {
		return InspectResult{}, err
	}
	return InspectResult{Raw: r}, nil
}

func (e *DefaultEngine) Verify(ctx context.Context, req VerifyRequest) (VerifyResult, error) {
	level, err := verifyLevelFromString(req.Level)
	if err != nil {
		return VerifyResult{}, err
	}
	target := req.Target
	if target == "" {
		target = "system"
	}
	if err := validateVerifyRequest(target, req.FileID); err != nil {
		return VerifyResult{}, err
	}
	containerDir := e.config.ContainerDir
	if containerDir == "" {
		containerDir = container.ContainersDir
	}
	if err := maintenance.VerifyCommandWithDBAndContainersDir(e.config.DB, containerDir, target, req.FileID, level); err != nil {
		return VerifyResult{}, err
	}
	return VerifyResult{}, nil
}

// validateInspectRequest returns an error if req contains an unrecognized entity
// type or an invalid/missing entity ID for that type. This duplicates the CLI
// validation so correctness does not depend solely on the CLI parsing path.
func validateInspectRequest(req InspectRequest) error {
	switch req.Entity {
	case observability.EntityRepository:
		// EntityRepository is the only entity that requires no ID.
		return nil
	case observability.EntitySnapshot:
		if strings.TrimSpace(req.EntityID) == "" {
			return fmt.Errorf("engine: entity ID is required for %s", req.Entity)
		}
		return nil
	case observability.EntityFile, observability.EntityLogicalFile, observability.EntityPhysicalFile,
		observability.EntityChunk, observability.EntityContainer:
		id := strings.TrimSpace(req.EntityID)
		if id == "" {
			return fmt.Errorf("engine: entity ID is required for %s", req.Entity)
		}
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("engine: %s ID must be a positive integer, got %q", req.Entity, req.EntityID)
		}
		return nil
	default:
		return fmt.Errorf("engine: unknown inspect entity %q", req.Entity)
	}
}

// validateVerifyRequest returns an error if target is not a recognized verify
// target, or if the file ID is non-positive when target is "file".
func validateVerifyRequest(target string, fileID int) error {
	switch target {
	case "system":
		return nil
	case "file":
		if fileID <= 0 {
			return fmt.Errorf("engine: file ID must be positive for verify file, got %d", fileID)
		}
		return nil
	default:
		return fmt.Errorf("engine: unknown verify target %q: must be system or file", target)
	}
}

// verifyLevelFromString maps the Level string from VerifyRequest to the
// internal verify.VerifyLevel type.
func verifyLevelFromString(s string) (verify.VerifyLevel, error) {
	switch s {
	case "fast":
		return verify.VerifyFast, nil
	case "", "standard":
		return verify.VerifyStandard, nil
	case "full":
		return verify.VerifyFull, nil
	case "deep":
		return verify.VerifyDeep, nil
	default:
		return 0, fmt.Errorf("unknown verify level %q: must be fast, standard, full, or deep", s)
	}
}

func (e *DefaultEngine) SnapshotList(ctx context.Context, req SnapshotListRequest) (SnapshotListResult, error) {
	svc := catalog.NewServiceFromSQL(e.config.DB)
	filter := catalog.SnapshotFilter{
		Type:           string(req.Type),
		LabelSubstring: req.Label,
		Since:          req.Since,
		Until:          req.Until,
		Limit:          req.Limit,
	}
	refs, err := svc.ListSnapshots(ctx, filter)
	if err != nil {
		return SnapshotListResult{}, err
	}
	metas := make([]SnapshotMeta, len(refs))
	for i, ref := range refs {
		metas[i] = SnapshotMeta{
			ID:        ref.ID,
			Type:      SnapshotType(ref.Type),
			Label:     ref.Label,
			ParentID:  ref.ParentID,
			CreatedAt: ref.CreatedAt,
		}
	}
	return SnapshotListResult{
		Snapshots: metas,
		Count:     len(metas),
		TreeMode:  req.Tree,
	}, nil
}

func (e *DefaultEngine) SnapshotShow(ctx context.Context, req SnapshotShowRequest) (SnapshotShowResult, error) {
	svc := catalog.NewServiceFromSQL(e.config.DB)
	ref, err := svc.FindSnapshot(ctx, req.SnapshotID)
	if err != nil {
		return SnapshotShowResult{}, err
	}
	if ref == nil {
		return SnapshotShowResult{}, fmt.Errorf("snapshot %q not found", req.SnapshotID)
	}
	meta := SnapshotMeta{
		ID:        ref.ID,
		Type:      SnapshotType(ref.Type),
		Label:     ref.Label,
		ParentID:  ref.ParentID,
		CreatedAt: ref.CreatedAt,
	}
	var snapshotQ *snapshot.SnapshotQuery
	if req.Query != (SnapshotQuery{}) {
		snapshotQ = engineQueryToSnapshotQuery(req.Query)
	}
	entries, err := snapshot.ListSnapshotFiles(ctx, e.config.DB, req.SnapshotID, req.Query.Limit, snapshotQ)
	if err != nil {
		return SnapshotShowResult{}, err
	}
	stats, err := snapshot.GetSnapshotStats(ctx, e.config.DB, req.SnapshotID)
	if err != nil {
		return SnapshotShowResult{}, err
	}
	files := make([]SnapshotFile, len(entries))
	for i, entry := range entries {
		files[i] = SnapshotFile{
			StoredPath:    entry.Path,
			LogicalFileID: entry.LogicalFileID,
			Size:          entry.Size.Int64,
			Mode:          uint32(entry.Mode.Int64),
			ModTime:       entry.MTime.Time,
		}
	}
	return SnapshotShowResult{
		Snapshot:         meta,
		Files:            files,
		MatchedFileCount: len(files),
		TotalFileCount:   int(stats.SnapshotFileCount),
	}, nil
}

func (e *DefaultEngine) SnapshotStats(ctx context.Context, req SnapshotStatsRequest) (SnapshotStatsResult, error) {
	stats, err := snapshot.GetSnapshotStats(ctx, e.config.DB, req.SnapshotID)
	if err != nil {
		return SnapshotStatsResult{}, err
	}
	result := SnapshotStatsResult{
		SnapshotCount:     int(stats.SnapshotCount),
		SnapshotFileCount: int(stats.SnapshotFileCount),
		TotalSizeBytes:    stats.TotalSizeBytes,
	}
	if stats.ParentSnapshotID.Valid && stats.ReusedFileCount.Valid {
		result.HasReuse = true
		result.Reused = int(stats.ReusedFileCount.Int64)
		result.New = int(stats.NewFileCount.Int64)
		result.ReuseRatio = stats.ReuseRatioPct.Float64
	} else {
		result.LineageStatus = string(stats.LineageStatus)
	}
	return result, nil
}

func (e *DefaultEngine) SnapshotDiff(ctx context.Context, req SnapshotDiffRequest) (SnapshotDiffResult, error) {
	// Summary fast path: no filter, no query.
	if req.Summary && req.Filter == "" {
		zeroQ := SnapshotQuery{}
		if req.Query == zeroQ {
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
	}
	var snapshotQ *snapshot.SnapshotQuery
	if req.Query != (SnapshotQuery{}) {
		snapshotQ = engineQueryToSnapshotQuery(req.Query)
	}
	raw, err := snapshot.DiffSnapshots(ctx, e.config.DB, req.BaseID, req.TargetID, snapshotQ)
	if err != nil {
		return SnapshotDiffResult{}, err
	}
	entries := make([]SnapshotDiffEntry, 0, len(raw.Entries))
	summary := SnapshotDiffSummary{}
	for _, entry := range raw.Entries {
		change := SnapshotDiffChange(entry.Type)
		if req.Filter != "" && SnapshotDiffFilter(entry.Type) != req.Filter {
			continue
		}
		entries = append(entries, SnapshotDiffEntry{StoredPath: entry.Path, Change: change})
		switch entry.Type {
		case snapshot.DiffAdded:
			summary.Added++
		case snapshot.DiffRemoved:
			summary.Removed++
		case snapshot.DiffModified:
			summary.Modified++
		}
	}
	res := SnapshotDiffResult{
		BaseID:            req.BaseID,
		TargetID:          req.TargetID,
		SummaryMode:       req.Summary,
		Summary:           summary,
		MatchedEntryCount: len(entries),
		TotalEntryCount:   len(raw.Entries),
	}
	if !req.Summary {
		res.Entries = entries
	}
	return res, nil
}

// engineQueryToSnapshotQuery maps an engine-level SnapshotQuery to the
// snapshot package's equivalent type.
func engineQueryToSnapshotQuery(q SnapshotQuery) *snapshot.SnapshotQuery {
	sq := &snapshot.SnapshotQuery{
		Pattern:        q.Pattern,
		MinSize:        q.MinSize,
		MaxSize:        q.MaxSize,
		ModifiedAfter:  q.ModifiedAfter,
		ModifiedBefore: q.ModifiedBefore,
	}
	if q.Path != "" {
		sq.ExactPaths = map[string]struct{}{q.Path: {}}
	}
	if q.Prefix != "" {
		sq.Prefixes = []string{q.Prefix}
	}
	if q.Regex != "" {
		if compiled, err := regexp.Compile(q.Regex); err == nil {
			sq.Regex = compiled
		}
	}
	return sq
}
