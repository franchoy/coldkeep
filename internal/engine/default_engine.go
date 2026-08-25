package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

// Config holds configuration for a DefaultEngine.
//
// Database backend selection (SQLite vs PostgreSQL) is not decided here;
// the caller is responsible for opening the correct backend and providing
// the connection. Additional dependencies are supplied only when an active
// engine method needs them.
type Config struct {
	// DB is the active database connection.
	// The caller is responsible for the connection lifetime.
	DB *sql.DB
	// ContainerDir is the path to the coldkeep containers directory.
	// Defaults to container.ContainersDir if empty.
	ContainerDir string
	// StoreContext provides writer+chunker-aware dependencies for active store
	// orchestration.
	StoreContext *storage.StorageContext
	// ChunkerDeprecationPolicy optionally rejects registered chunkers for new
	// repository defaults. Nil means no registered chunker is deprecated.
	ChunkerDeprecationPolicy func(chunk.Version) (bool, string)
}

// DefaultEngine is the canonical Engine implementation.
//
// It preserves existing command behavior while routing supported operations
// through typed engine methods and lower domain packages.
type DefaultEngine struct {
	config              Config
	obs                 *observability.Service
	snapshotIDGenerator snapshotIDGenerator
	doctorRecover       func(context.Context) (RecoverResult, error)
	doctorSchema        func(context.Context, *sql.DB) (int64, error)
	doctorVerify        func(context.Context, string) error
	doctorAudit         func(context.Context, *sql.DB) (DoctorPhysicalAudit, DoctorSnapshotAudit, error)
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
	return &DefaultEngine{
		config:              cfg,
		obs:                 obs,
		snapshotIDGenerator: secureSnapshotIDGenerator,
	}, nil
}

type snapshotIDGenerator func() (string, error)

func secureSnapshotIDGenerator() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate snapshot id entropy: %w", err)
	}
	return "snap-" + hex.EncodeToString(b), nil
}

func (e *DefaultEngine) Stats(ctx context.Context, req StatsRequest) (_ StatsResult, outErr error) {
	defer func() { outErr = TranslateError("stats", outErr) }()
	trace, collector := traceOptions(req.IncludeTrace)
	r, err := e.obs.Stats(ctx, observability.StatsOptions{
		IncludeContainers: req.IncludeContainers,
		Trace:             trace,
	})
	result := statsFromObservability(r, collector.events)
	if collector.err != nil {
		return result, TranslateError("stats", collector.err)
	}
	if err != nil {
		return result, TranslateError("stats", err)
	}
	return result, nil
}

func (e *DefaultEngine) Inspect(ctx context.Context, req InspectRequest) (_ InspectResult, outErr error) {
	defer func() { outErr = TranslateError("inspect", outErr) }()
	if err := validateInspectRequest(req); err != nil {
		return InspectResult{}, TranslateErrorAs("inspect", ErrorInvalidArgument, err)
	}
	trace, collector := traceOptions(req.Options.IncludeTrace)
	r, err := e.obs.Inspect(ctx, observability.EntityType(req.Entity), req.EntityID, observability.InspectOptions{
		Deep: req.Options.Deep, Relations: req.Options.Relations,
		Reverse: req.Options.Reverse, Limit: req.Options.Limit, Trace: trace,
	})
	result, conversionErr := inspectFromObservability(r, collector.events)
	if collector.err != nil {
		return result, TranslateError("inspect", collector.err)
	}
	if conversionErr != nil {
		return result, TranslateError("inspect", conversionErr)
	}
	if err != nil {
		if errors.Is(err, observability.ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
			return result, TranslateErrorAs("inspect", ErrorNotFound, err)
		}
		return result, TranslateError("inspect", err)
	}
	return result, nil
}

func (e *DefaultEngine) Verify(ctx context.Context, req VerifyRequest) (_ VerifyResult, outErr error) {
	defer func() { outErr = TranslateError("verify", outErr) }()
	if err := ctx.Err(); err != nil {
		return VerifyResult{}, err
	}
	level, err := verifyLevelFromString(req.Level)
	if err != nil {
		return VerifyResult{}, TranslateErrorAs("verify", ErrorInvalidArgument, err)
	}
	target := req.Target
	if target == "" {
		target = "system"
	}
	if err := validateVerifyRequest(target, req.FileID); err != nil {
		return VerifyResult{}, TranslateErrorAs("verify", ErrorInvalidArgument, err)
	}
	containerDir := e.config.ContainerDir
	if containerDir == "" {
		containerDir = container.ContainersDir
	}
	execution, err := maintenance.VerifyCommandWithDBAndContainersDirResultContext(ctx, e.config.DB, containerDir, target, req.FileID, level)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return VerifyResult{}, err
		}
		return VerifyResult{}, TranslateErrorAs("verify", ErrorVerificationFailed, err)
	}
	return VerifyResult{
		BlocksChecked:           execution.BlocksChecked,
		PhysicalHashChecked:     execution.PhysicalHashChecked,
		CompressedHashChecked:   execution.CompressedHashChecked,
		LogicalHashChecked:      execution.LogicalHashChecked,
		CompressedBlocksChecked: execution.CompressedBlocksChecked,
	}, nil
}

// validateInspectRequest returns an error if req contains an unrecognized entity
// type or an invalid/missing entity ID for that type. This duplicates the CLI
// validation so correctness does not depend solely on the CLI parsing path.
func validateInspectRequest(req InspectRequest) error {
	switch req.Entity {
	case InspectRepository:
		// EntityRepository is the only entity that requires no ID.
		return nil
	case InspectSnapshot:
		if strings.TrimSpace(req.EntityID) == "" {
			return fmt.Errorf("engine: entity ID is required for %s", req.Entity)
		}
		return nil
	case InspectFile, InspectLogicalFile, InspectPhysicalFile, InspectChunk, InspectContainer:
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

func (e *DefaultEngine) SnapshotList(ctx context.Context, req SnapshotListRequest) (_ SnapshotListResult, outErr error) {
	defer func() { outErr = TranslateError("snapshot_list", outErr) }()
	var tx *sql.Tx
	catalogDB := catalog.DB(e.config.DB)
	if req.Tree {
		var err error
		tx, err = e.config.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return SnapshotListResult{}, TranslateError("snapshot_list", fmt.Errorf("begin tree snapshot: %w", err))
		}
		defer func() { _ = tx.Rollback() }()
		catalogDB = tx
	}
	svc := catalog.NewService(catalogDB)
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
	var resultGraph *SnapshotGraph
	if req.Tree {
		graph, err := svc.LoadSnapshotGraph(ctx)
		if err != nil {
			return SnapshotListResult{}, TranslateError("snapshot_list", err)
		}
		resultGraph, metas = projectSelectedSnapshotGraph(graph, refs)
		if err := tx.Commit(); err != nil {
			return SnapshotListResult{}, TranslateError("snapshot_list", fmt.Errorf("commit tree snapshot: %w", err))
		}
	}
	return SnapshotListResult{
		Snapshots: metas,
		Count:     len(metas),
		TreeMode:  req.Tree,
		Graph:     resultGraph,
	}, nil
}

func projectSelectedSnapshotGraph(graph *catalog.SnapshotGraph, selected []catalog.SnapshotRef) (*SnapshotGraph, []SnapshotMeta) {
	selectedIDs := make(map[string]struct{}, len(selected))
	for _, ref := range selected {
		selectedIDs[ref.ID] = struct{}{}
	}
	result := &SnapshotGraph{Nodes: make([]SnapshotGraphNode, 0, len(selected)), RootIDs: make([]string, 0)}
	for _, node := range graph.Nodes {
		if _, ok := selectedIDs[node.Snapshot.ID]; !ok {
			continue
		}
		children := make([]string, 0, len(node.ChildIDs))
		for _, childID := range node.ChildIDs {
			if _, ok := selectedIDs[childID]; ok {
				children = append(children, childID)
			}
		}
		result.Nodes = append(result.Nodes, SnapshotGraphNode{
			Snapshot: SnapshotMeta{
				ID:        node.Snapshot.ID,
				Type:      SnapshotType(node.Snapshot.Type),
				Label:     node.Snapshot.Label,
				ParentID:  node.Snapshot.ParentID,
				CreatedAt: node.Snapshot.CreatedAt,
			},
			ParentState: SnapshotParentState(node.ParentState),
			ChildIDs:    children,
		})
	}
	// RootIDs are roots of the selected projection. A selected child whose
	// existing parent was filtered out remains parent-aware metadata, but it is
	// a top-level node for this projection. Historical missing parents are also
	// top-level without inventing a parent edge.
	for _, node := range result.Nodes {
		_, parentSelected := selectedIDs[node.Snapshot.ParentID]
		if node.ParentState != SnapshotParentPresent || !parentSelected {
			result.RootIDs = append(result.RootIDs, node.Snapshot.ID)
		}
	}

	// SnapshotList historically returns newest first. Deriving this projection
	// from the graph consumes catalog ordering while preserving that contract.
	metas := make([]SnapshotMeta, len(result.Nodes))
	for i := range result.Nodes {
		metas[len(result.Nodes)-1-i] = result.Nodes[i].Snapshot
	}
	return result, metas
}

func (e *DefaultEngine) SnapshotShow(ctx context.Context, req SnapshotShowRequest) (_ SnapshotShowResult, outErr error) {
	defer func() { outErr = TranslateError("snapshot_show", outErr) }()
	if strings.TrimSpace(req.SnapshotID) == "" {
		return SnapshotShowResult{}, TranslateErrorAs("snapshot_show", ErrorInvalidArgument, fmt.Errorf("snapshot id cannot be empty"))
	}
	svc := catalog.NewServiceFromSQL(e.config.DB)
	ref, err := svc.FindSnapshot(ctx, req.SnapshotID)
	if err != nil {
		return SnapshotShowResult{}, err
	}
	if ref == nil {
		return SnapshotShowResult{}, TranslateErrorAs("snapshot_show", ErrorNotFound, fmt.Errorf("snapshot %q not found", req.SnapshotID))
	}
	meta := SnapshotMeta{
		ID:        ref.ID,
		Type:      SnapshotType(ref.Type),
		Label:     ref.Label,
		ParentID:  ref.ParentID,
		CreatedAt: ref.CreatedAt,
	}
	snapshotQ, err := snapshotQueryOrNil(req.Query)
	if err != nil {
		return SnapshotShowResult{}, TranslateErrorAs("snapshot_show", ErrorInvalidArgument, err)
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
			Size:          nullableInt64(entry.Size),
			Mode:          nullableInt64(entry.Mode),
			ModTime:       nullableTime(entry.MTime),
		}
	}
	return SnapshotShowResult{
		Snapshot:         meta,
		Files:            files,
		MatchedFileCount: len(files),
		TotalFileCount:   int(stats.SnapshotFileCount),
	}, nil
}

func (e *DefaultEngine) SnapshotStats(ctx context.Context, req SnapshotStatsRequest) (_ SnapshotStatsResult, outErr error) {
	defer func() { outErr = TranslateError("snapshot_stats", outErr) }()
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
		result.ParentSnapshotID = stats.ParentSnapshotID.String
		result.Reused = int(stats.ReusedFileCount.Int64)
		result.New = int(stats.NewFileCount.Int64)
		result.ReuseRatio = stats.ReuseRatioPct.Float64
	} else {
		result.LineageStatus = string(stats.LineageStatus)
	}
	return result, nil
}

func (e *DefaultEngine) SnapshotDiff(ctx context.Context, req SnapshotDiffRequest) (_ SnapshotDiffResult, outErr error) {
	defer func() { outErr = TranslateError("snapshot_diff", outErr) }()
	if strings.TrimSpace(req.BaseID) == "" {
		return SnapshotDiffResult{}, TranslateErrorAs("snapshot_diff", ErrorInvalidArgument, fmt.Errorf("base snapshot id cannot be empty"))
	}
	if strings.TrimSpace(req.TargetID) == "" {
		return SnapshotDiffResult{}, TranslateErrorAs("snapshot_diff", ErrorInvalidArgument, fmt.Errorf("target snapshot id cannot be empty"))
	}
	if req.Filter != SnapshotDiffAll && req.Filter != SnapshotDiffAdded && req.Filter != SnapshotDiffRemoved && req.Filter != SnapshotDiffModified {
		return SnapshotDiffResult{}, TranslateErrorAs("snapshot_diff", ErrorInvalidArgument, fmt.Errorf("unknown snapshot diff filter %q", req.Filter))
	}
	query, err := snapshotQueryOrNil(req.Query)
	if err != nil {
		return SnapshotDiffResult{}, TranslateErrorAs("snapshot_diff", ErrorInvalidArgument, err)
	}
	if isSnapshotDiffSummaryFastPath(req) {
		return e.snapshotDiffSummaryFastPath(ctx, req)
	}
	return e.snapshotDiffDetailed(ctx, req, query)
}

func (e *DefaultEngine) Remove(ctx context.Context, req RemoveRequest) (_ RemoveResult, outErr error) {
	defer func() { outErr = TranslateError("remove", outErr) }()
	if err := ctx.Err(); err != nil {
		return RemoveResult{}, err
	}
	if err := validateRemoveRequest(req); err != nil {
		return RemoveResult{}, TranslateErrorAs("remove", ErrorInvalidArgument, err)
	}
	return e.removeFileIDs(ctx, req)
}

func (e *DefaultEngine) RemoveStoredPaths(ctx context.Context, req RemoveStoredPathsRequest) (_ RemoveStoredPathsResult, outErr error) {
	defer func() { outErr = TranslateError("remove_stored_paths", outErr) }()
	if err := ctx.Err(); err != nil {
		return RemoveStoredPathsResult{}, err
	}
	preflight, err := preflightRemoveStoredPaths(req)
	if err != nil {
		return RemoveStoredPathsResult{}, TranslateErrorAs("remove_stored_paths", ErrorInvalidArgument, err)
	}
	if !preflight.requiresRepository {
		return preflight.terminalResult, nil
	}
	if err := e.validateRemoveStoredPathsDependencies(); err != nil {
		return RemoveStoredPathsResult{}, err
	}
	return e.removeStoredPaths(ctx, req, preflight.prepared)
}

func (e *DefaultEngine) Restore(ctx context.Context, req RestoreRequest) (_ RestoreResult, outErr error) {
	defer func() { outErr = TranslateError("restore", outErr) }()
	if err := ctx.Err(); err != nil {
		return RestoreResult{}, err
	}
	if err := validateRestoreRequest(req); err != nil {
		return RestoreResult{}, TranslateErrorAs("restore", ErrorInvalidArgument, err)
	}
	return e.restoreFileIDs(ctx, req)
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
