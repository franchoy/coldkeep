package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/franchoy/coldkeep/internal/batch"
	corebenchmark "github.com/franchoy/coldkeep/internal/benchmark"
	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	chunkbenchmark "github.com/franchoy/coldkeep/internal/chunk/benchmark"
	"github.com/franchoy/coldkeep/internal/chunk/fastcdc"
	"github.com/franchoy/coldkeep/internal/chunk/simplecdc"
	clirender "github.com/franchoy/coldkeep/internal/cli/render"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/coordination"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/execution"
	internalgc "github.com/franchoy/coldkeep/internal/gc"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/iodebug"
	"github.com/franchoy/coldkeep/internal/listing"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	"github.com/franchoy/coldkeep/internal/version"
)

const (
	exitSuccess  = 0
	exitGeneral  = 1
	exitUsage    = 2
	exitVerify   = 3
	exitRecovery = 4
)

var stdoutRedirectMu sync.Mutex

var flagsWithValues = map[string]bool{
	"codec":             true,
	"compression":       true,
	"compression-level": true,
	"destination":       true,
	"filter":            true,
	"input":             true,
	"limit":             true,
	"mode":              true,
	"offset":            true,
	"name":              true,
	"id":                true,
	"from":              true,
	"label":             true,
	"since":             true,
	"type":              true,
	"until":             true,
	"min-size":          true,
	"max-size":          true,
	"output":            true,
	"stored-path":       true,
	"path":              true,
	"prefix":            true,
	"pattern":           true,
	"regex":             true,
	"delete-snapshot":   true,
	"modified-after":    true,
	"modified-before":   true,
	"dataset":           true,
	"repeat":            true,
	"compare":           true,
	"threshold":         true,
	"workers":           true,
	"extension":         true,
}

var flagsWithoutValues = map[string]bool{
	"batch":       true,
	"containers":  true,
	"deep":        true,
	"dry-run":     true,
	"dryRun":      true,
	"fail-fast":   true,
	"failFast":    true,
	"fast":        true,
	"force":       true,
	"full":        true,
	"h":           true,
	"help":        true,
	"json":        true,
	"no-metadata": true,
	"overwrite":   true,
	"relations":   true,
	"reverse":     true,
	"standard":    true,
	"strict":      true,
	"summary":     true,
	"trace":       true,
	"trace-json":  true,
	"tree":        true,
}

var repeatableFlags = map[string]bool{
	"delete-snapshot": true,
	"path":            true,
	"prefix":          true,
}

type cliOutputMode string

const (
	outputModeText cliOutputMode = "text"
	outputModeJSON cliOutputMode = "json"
)

type parsedCommandLine struct {
	method      string
	positionals []string
	flags       map[string][]string
}

type cliRuntime struct {
	newCoordinator  func() coordination.Coordinator
	newOutputSpool  func() (*coordinatedOutputSpool, error)
	resolveIdentity func(string) (coordination.Identity, error)
	newOwner        func(coordination.Operation, coordination.Identity, string, time.Time) (coordination.Owner, error)
	recover         func(cliOutputMode) (recovery.Report, error)
	dispatch        func(parsedCommandLine, cliOutputMode) error
	renderSuccess   func(parsedCommandLine, cliOutputMode)
	now             func() time.Time
}

type verifyOutputSummary struct {
	BlocksChecked           int64
	PhysicalHashChecked     int64
	CompressedHashChecked   int64
	LogicalHashChecked      int64
	CompressedBlocksChecked int64
}

type verifyFailureDetails struct {
	Stage     string
	Block     *int64
	Container *int64
	Offset    *int64
	Reason    string
}

// doctorReport is the stable v1.0 JSON data payload for `coldkeep doctor --output json`.
// All fields are frozen API: do not remove or rename fields without a major version bump.
//
// The Recovery field intentionally includes the full recovery.Report counter set
// (aborted_logical_files, aborted_chunks, quarantined_missing, quarantined_corrupt_tail,
// quarantined_orphan, skipped_dir_entries, checked_container_record, checked_disk_files,
// sealing_completed, sealing_quarantined). These counters are actionable for operators
// and monitoring scripts: any non-zero quarantined_* or aborted_* value signals that
// corrective action was taken and should trigger alerting or human review.
// Including the full report here is a deliberate decision, not an oversight.
type doctorReport struct {
	Recovery       recovery.Report `json:"recovery"`
	VerifyLevel    string          `json:"verify_level"`
	SchemaVersion  int64           `json:"schema_version"`
	RecoveryStatus string          `json:"recovery_status"`
	VerifyStatus   string          `json:"verify_status"`
	SchemaStatus   string          `json:"schema_status"`
	physicalAudit  verify.PhysicalFileIntegritySummary
	snapshotAudit  verify.SnapshotReachabilityIntegritySummary
}

// Frozen v1.0 product contract: doctor is the fast corrective recovery + health gate.
// Default remains `standard`; operators can opt into `--full` / `--deep`.
const doctorDefaultVerifyLevel = verify.VerifyStandard

const doctorOperationalHint = "After significant operations, run coldkeep doctor to validate system health."

// Current intentional CLI/domain ownership: doctor owns corrective recovery
// orchestration directly through lower-layer recovery and verify hooks. Phase
// 14 made this boundary decision, and the Phase 16 honesty proof confirmed the
// seam remains outside active engine ownership. Any future activation belongs
// to an explicit early-v2.0 design; these direct hooks are intentional.
var doctorRecoveryPhase = recovery.SystemRecoveryReportWithContainersDir
var doctorSchemaVersionPhase = db.QueryCurrentSchemaVersion
var doctorVerifyPhase = maintenance.VerifyCommandWithContainersDir
var doctorSystemAuditPhase = maintenance.CollectSystemAuditSummary

// Current intentional CLI/domain ownership: repair remains direct maintenance
// execution rather than active engine ownership. Phase 14 made this boundary
// decision, and the Phase 16 honesty proof confirmed it. Any future activation
// belongs to an explicit early-v2.0 design; this direct hook is intentional.
var repairLogicalRefCountsPhase = maintenance.RepairLogicalRefCountsResultRun

// Current intentional CLI/domain ownership: chunk live-ref-count repair remains
// a direct maintenance hook, not an engine-routed workflow. Phase 14 made this
// boundary decision, and the Phase 16 honesty proof confirmed it. Any future
// early-v2.0 activation design must be explicit and behavior-preserving.
var repairChunkLiveRefCountsPhase = maintenance.RepairChunkLiveRefCountsResultRun
var storeByFilePhase = func(sgctx *storage.StorageContext, path, codecName string) (storage.StoreFileResult, error) {
	if sgctx == nil || sgctx.DB == nil {
		return storage.StoreFileResult{}, fmt.Errorf("store: storage context DB is required")
	}
	eng, err := engine.New(engine.Config{
		DB:           sgctx.DB,
		ContainerDir: sgctx.EffectiveContainerDir(),
		StoreContext: sgctx,
	})
	if err != nil {
		return storage.StoreFileResult{}, err
	}

	res, err := eng.Store(context.Background(), engine.StoreRequest{
		SourcePath: path,
		Codec:      strings.TrimSpace(codecName),
	})
	if err != nil {
		return storage.StoreFileResult{}, err
	}

	return storage.StoreFileResult{
		FileID:        res.LogicalFileID,
		FileHash:      res.FileHash,
		Path:          res.StoredPath,
		AlreadyStored: res.AlreadyStored,
	}, nil
}
var removeByIDPhase = func(sgctx *storage.StorageContext, fileID int64, dryRun bool) batch.ItemResult {
	// By-ID remove remains the active engine-owned path.
	if sgctx == nil || sgctx.DB == nil {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: "remove: storage context DB is required"}
	}

	eng, err := engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
	if err != nil {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
	}

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		FileIDs:  []int64{fileID},
		DryRun:   dryRun,
		FailFast: true,
	})
	if err != nil {
		item := batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
		annotateBatchFailureFromError(err, &item)
		return item
	}
	if len(res.Items) != 1 {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("remove: expected one item result, got %d", len(res.Items))}
	}

	item := res.Items[0]
	if item.Status == engine.BatchItemFailed {
		return batch.ItemResult{
			ID:                fileID,
			Status:            batch.ResultFailed,
			Message:           item.Error,
			InvariantCode:     item.InvariantCode,
			RecommendedAction: item.RecommendedAction,
		}
	}

	if dryRun {
		return batch.ItemResult{ID: fileID, Status: batch.ResultPlanned, Message: "would remove"}
	}
	return batch.ItemResult{ID: fileID, Status: batch.ResultSuccess, Message: fmt.Sprintf("removed mappings=%d", item.RemovedChunkAssociations)}
}
var restoreByIDPhase = func(sgctx *storage.StorageContext, fileID int64, outputDir string, overwrite bool, dryRun bool) (storage.RestoreFileResult, error) {
	// By-ID restore remains the active engine-owned path.
	if sgctx == nil || sgctx.DB == nil {
		return storage.RestoreFileResult{}, fmt.Errorf("restore: storage context DB is required")
	}
	eng, err := engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
	if err != nil {
		return storage.RestoreFileResult{}, err
	}

	info, err := storage.GetLogicalFileInfoWithDB(sgctx.DB, fileID)
	if err != nil {
		return storage.RestoreFileResult{}, err
	}

	res, err := eng.Restore(context.Background(), engine.RestoreRequest{
		FileIDs:         []int64{fileID},
		DestinationRoot: outputDir,
		Overwrite:       overwrite,
		DryRun:          dryRun,
		FailFast:        true,
	})
	if err != nil {
		return storage.RestoreFileResult{}, err
	}
	if len(res.Items) != 1 {
		return storage.RestoreFileResult{}, fmt.Errorf("restore: expected one item result, got %d", len(res.Items))
	}
	item := res.Items[0]
	if item.Status == engine.BatchItemFailed {
		return storage.RestoreFileResult{}, errors.New(item.Error)
	}

	return storage.RestoreFileResult{
		FileID:       fileID,
		OriginalName: info.OriginalName,
		OutputPath:   item.DestinationPath,
		RestoredHash: item.RestoredHash,
	}, nil
}
var runGCPhase = func(dryRun bool, containersDir string) (maintenance.GCResult, error) {
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return maintenance.GCResult{}, err
	}
	defer func() { _ = sgctx.DB.Close() }()

	eng, err := engine.New(engine.Config{
		DB:           sgctx.DB,
		ContainerDir: containersDir,
	})
	if err != nil {
		return maintenance.GCResult{}, err
	}

	result, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: dryRun})
	if err != nil {
		return maintenance.GCResult{}, err
	}
	return maintenance.GCResult{
		DryRun:                       result.DryRun,
		AffectedContainers:           result.AffectedContainers,
		ContainerFilenames:           result.ContainerFilenames,
		SnapshotRetainedContainers:   result.SnapshotRetainedContainers,
		SnapshotRetainedLogicalFiles: result.SnapshotRetainedLogicalFiles,
		RetainedCurrentOnlyLogical:   result.CurrentOnlyRetainedLogicalFiles,
		RetainedSnapshotOnlyLogical:  result.SnapshotOnlyRetainedLogicalFiles,
		RetainedSharedLogical:        result.SharedRetainedLogicalFiles,
	}, nil
}
var startupRecoveryPhase = recovery.SystemRecoveryReportWithContainersDir
var loadDefaultStorageContextPhase = storage.LoadDefaultStorageContext

// Compatibility-only direct snapshot-domain create seam retained for lower-
// level tests and non-CLI callers. Production CLI snapshot create routes
// through Engine.SnapshotCreate.
var createSnapshotPhase = snapshot.CreateSnapshotWithOptions

// Compatibility-only direct snapshot-domain restore seam retained for lower-
// level tests and non-CLI callers. Production CLI snapshot restore routes
// through Engine.SnapshotRestore.
var restoreSnapshotPhase = snapshot.RestoreSnapshot
var currentWorkingDirectoryPhase = os.Getwd
var listSnapshotsPhase = func(ctx context.Context, db *sql.DB, filter snapshot.SnapshotListFilter) ([]snapshot.Snapshot, error) {
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		return nil, err
	}
	req := engine.SnapshotListRequest{
		Since: filter.Since,
		Until: filter.Until,
		Limit: filter.Limit,
	}
	if filter.Type != nil {
		req.Type = engine.SnapshotType(*filter.Type)
	}
	if filter.Label != nil {
		req.Label = *filter.Label
	}
	result, err := eng.SnapshotList(ctx, req)
	if err != nil {
		return nil, err
	}
	items := make([]snapshot.Snapshot, len(result.Snapshots))
	for i, m := range result.Snapshots {
		items[i] = snapshotMetaToSnapshot(m)
	}
	return items, nil
}
var getSnapshotPhase = func(ctx context.Context, db *sql.DB, id string) (*snapshot.Snapshot, error) {
	// Engine.SnapshotShow is active, but the CLI workflow may still combine
	// engine metadata with direct snapshot-domain listing, counting, or
	// rendering. Active method presence does not prove complete read-side
	// workflow ownership; early v2.0 owns the remaining ownership decision.
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		return nil, err
	}
	result, err := eng.SnapshotShow(ctx, engine.SnapshotShowRequest{SnapshotID: id})
	if err != nil {
		return nil, err
	}
	s := snapshotMetaToSnapshot(result.Snapshot)
	return &s, nil
}

// Engine.SnapshotShow is active, while snapshot show file listing remains
// direct snapshot-domain work in this mixed CLI workflow. Active method
// presence does not prove complete read-side workflow ownership; early v2.0
// owns the remaining ownership decision.
var listSnapshotFilesPhase = snapshot.ListSnapshotFiles
var snapshotStatsPhase = func(ctx context.Context, db *sql.DB, id string) (*snapshot.SnapshotStats, error) {
	// Engine.SnapshotStats is active, but snapshot show and stats workflows may
	// still use direct snapshot-domain helpers alongside engine-backed seams.
	// Active method presence does not prove complete read-side workflow
	// ownership; early v2.0 owns the remaining ownership decision.
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		return nil, err
	}
	result, err := eng.SnapshotStats(ctx, engine.SnapshotStatsRequest{SnapshotID: id})
	if err != nil {
		return nil, err
	}
	stats := &snapshot.SnapshotStats{
		SnapshotCount:     int64(result.SnapshotCount),
		SnapshotFileCount: int64(result.SnapshotFileCount),
		TotalSizeBytes:    result.TotalSizeBytes,
		LineageStatus:     snapshot.SnapshotLineageStatus(result.LineageStatus),
	}
	if result.HasReuse {
		stats.ParentSnapshotID = sql.NullString{Valid: true, String: result.ParentSnapshotID}
		stats.ReusedFileCount = sql.NullInt64{Valid: true, Int64: int64(result.Reused)}
		stats.NewFileCount = sql.NullInt64{Valid: true, Int64: int64(result.New)}
		stats.ReuseRatioPct = sql.NullFloat64{Valid: true, Float64: result.ReuseRatio}
	}
	return stats, nil
}

// Compatibility-only direct snapshot-domain delete seams remain for lower-
// level tests and non-CLI callers. Production CLI snapshot delete routes
// through Engine.SnapshotDelete.
var deleteSnapshotPhase = snapshot.DeleteSnapshot
var snapshotDeleteLineagePreviewPhase = loadSnapshotDeleteLineagePreview
var diffSnapshotsPhase = func(ctx context.Context, db *sql.DB, baseID, targetID string, query *snapshot.SnapshotQuery) (*snapshot.SnapshotDiffResult, error) {
	// The CLI accepts repeated path and prefix selectors. Keep that full
	// snapshot-domain query shape instead of narrowing it through the current
	// single-path engine seam.
	return snapshot.DiffSnapshots(ctx, db, baseID, targetID, query)
}
var diffSnapshotSummaryPhase = func(ctx context.Context, db *sql.DB, baseID, targetID string) (*snapshot.SnapshotDiffSummary, error) {
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		return nil, err
	}
	result, err := eng.SnapshotDiff(ctx, engine.SnapshotDiffRequest{
		BaseID: baseID, TargetID: targetID, Summary: true,
	})
	if err != nil {
		return nil, err
	}
	return &snapshot.SnapshotDiffSummary{
		Added:    int64(result.Summary.Added),
		Removed:  int64(result.Summary.Removed),
		Modified: int64(result.Summary.Modified),
	}, nil
}

// snapshotMetaToSnapshot maps an engine.SnapshotMeta to a snapshot.Snapshot for
// CLI renderers that expect the snapshot package's type with sql.NullString fields.
func snapshotMetaToSnapshot(m engine.SnapshotMeta) snapshot.Snapshot {
	s := snapshot.Snapshot{ID: m.ID, CreatedAt: m.CreatedAt, Type: string(m.Type)}
	if m.Label != "" {
		s.Label = sql.NullString{Valid: true, String: m.Label}
	}
	if m.ParentID != "" {
		s.ParentID = sql.NullString{Valid: true, String: m.ParentID}
	}
	return s
}

var newObservabilityServicePhase = observability.NewService
var runObservabilityStatsPhase = func(opts observability.StatsOptions) (*observability.StatsResult, error) {
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return nil, err
	}
	defer func() { _ = sgctx.DB.Close() }()

	eng, err := engine.New(engine.Config{
		DB:           sgctx.DB,
		ContainerDir: sgctx.EffectiveContainerDir(),
	})
	if err != nil {
		return nil, err
	}

	result, err := eng.Stats(context.Background(), engine.StatsRequest{
		IncludeContainers: opts.IncludeContainers,
		Trace:             opts.Trace,
	})
	if err != nil {
		return nil, err
	}

	return result.Raw, nil
}
var runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
	// Engine.Inspect is active, while this production CLI path intentionally
	// still calls the observability service directly. Active method presence does
	// not prove complete read-side workflow ownership; early v2.0 owns any
	// remaining ownership decision.
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return nil, err
	}
	defer func() { _ = sgctx.DB.Close() }()

	svc, err := newObservabilityServicePhase(sgctx.DB)
	if err != nil {
		return nil, err
	}

	r, err := svc.Inspect(context.Background(), entity, id, opts)
	if err != nil {
		return nil, err
	}

	return r, nil
}
var verifyCommandPhase = func(dbconn *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
	eng, err := engine.New(engine.Config{DB: dbconn, ContainerDir: container.ContainersDir})
	if err != nil {
		return err
	}
	_, err = eng.Verify(context.Background(), engine.VerifyRequest{
		Level:  verifyLevelToString(level),
		Target: target,
		FileID: fileID,
	})
	return err
}
var verifySummaryPhase = func(dbconn *sql.DB, target string, fileID int64) (verifyOutputSummary, error) {
	return collectVerifyOutputSummary(dbconn, target, fileID)
}
var runChunkerBenchmarkPhase = runChunkerBenchmark
var runCoreBenchmarkPhase = runCoreBenchmark
var runBenchmarkDeterminismPhase = validateBenchmarkDeterminism
var isDeprecatedChunkerVersionPhase = func(v chunk.Version) (bool, string) {
	// Future-proof policy hook: no deprecated chunkers currently.
	return false, ""
}

type cliError struct {
	code       int
	msg        string
	err        error
	publicCode string
}

func (e *cliError) Error() string {
	if e.msg != "" {
		return e.msg
	}
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

func (e *cliError) Unwrap() error {
	return e.err
}

func usageErrorf(format string, args ...any) error {
	return &cliError{code: exitUsage, msg: fmt.Sprintf(format, args...)}
}

func observabilityErrorf(exitCode int, publicCode, format string, args ...any) error {
	return &cliError{code: exitCode, msg: fmt.Sprintf(format, args...), publicCode: publicCode}
}

func observabilityWrappedError(exitCode int, publicCode, publicMessage string, cause error) error {
	return &cliError{code: exitCode, msg: publicMessage, err: cause, publicCode: publicCode}
}

func publicErrorCode(err error, exitCode int) string {
	var ce *cliError
	if errors.As(err, &ce) && strings.TrimSpace(ce.publicCode) != "" {
		return strings.TrimSpace(ce.publicCode)
	}

	if exitCode == exitUsage {
		return "INVALID_ARGUMENT"
	}

	return "INTERNAL"
}

func inspectEntityLabel(entityName string) string {
	switch strings.TrimSpace(strings.ToLower(entityName)) {
	case "repository":
		return "repository"
	case "file":
		return "logical file"
	case "chunk":
		return "chunk"
	case "container":
		return "container"
	case "snapshot":
		return "snapshot"
	default:
		return strings.TrimSpace(strings.ToLower(entityName))
	}
}

var missingSnapshotPattern = regexp.MustCompile(`snapshot\s+"([^"]+)"\s+does\s+not\s+exist|snapshot\s+(\S+)\s+does\s+not\s+exist`)

func missingSnapshotFromError(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	matches := missingSnapshotPattern.FindStringSubmatch(err.Error())
	if len(matches) < 3 {
		return "", false
	}
	if strings.TrimSpace(matches[1]) != "" {
		return strings.TrimSpace(matches[1]), true
	}
	if strings.TrimSpace(matches[2]) != "" {
		return strings.TrimSpace(matches[2]), true
	}
	return "", false
}

func verifyError(err error) error {
	if err == nil {
		return nil
	}
	return &cliError{code: exitVerify, err: err}
}

func recoveryError(err error) error {
	if err == nil {
		return nil
	}
	return &cliError{code: exitRecovery, err: err}
}

func main() {
	code := runCLI(os.Args[1:])
	if code != exitSuccess {
		os.Exit(code)
	}
}

func runCLI(args []string) int {
	return runCLIWithRuntime(args, cliRuntime{
		newCoordinator:  coordination.NewCoordinator,
		newOutputSpool:  newDefaultCoordinatedOutputSpool,
		resolveIdentity: coordination.ResolveIdentity,
		newOwner:        coordination.NewOwner,
		recover:         runStartupRecoveryWithOptionalLogBuffering,
		dispatch:        dispatchCLICommand,
		renderSuccess:   printCLISuccess,
		now:             time.Now,
	})
}

func runCLIWithRuntime(args []string, runtime cliRuntime) int {
	startupMode := inferOutputModeFromArgs(args)
	if startupMode == outputModeJSON {
		prevOutput := log.Writer()
		prevFlags := log.Flags()
		log.SetOutput(io.Discard)
		log.SetFlags(0)
		defer func() {
			log.SetOutput(prevOutput)
			log.SetFlags(prevFlags)
		}()
	}

	if len(args) < 1 {
		printHelp()
		return exitSuccess
	}

	parsed, err := parseCommandLine(args, flagsWithValues)
	if err != nil {
		return printCLIError(err, startupMode)
	}

	ioSubcommand := ""
	if len(parsed.positionals) > 0 {
		ioSubcommand = strings.TrimSpace(parsed.positionals[0])
	}
	iodebug.StartOperation()
	defer func() {
		_ = iodebug.FlushProcessCounters(parsed.method, ioSubcommand)
	}()

	outputMode, err := resolveOutputMode(parsed)
	if err != nil {
		return printCLIError(err, startupMode)
	}

	policy := repositoryCoordinationPolicyFor(parsed)
	err = executeCLICommand(args, parsed, outputMode, policy, runtime)
	if err != nil {
		return printCLIError(err, outputMode)
	}

	runtime.renderSuccess(parsed, outputMode)

	return exitSuccess
}

func executeCLICommand(
	args []string,
	parsed parsedCommandLine,
	outputMode cliOutputMode,
	policy repositoryCoordinationPolicy,
	runtime cliRuntime,
) (err error) {
	if !policy.Required {
		return runtime.dispatch(parsed, outputMode)
	}
	newOutputSpool := runtime.newOutputSpool
	if newOutputSpool == nil {
		newOutputSpool = newDefaultCoordinatedOutputSpool
	}
	outputSpool, err := newOutputSpool()
	if err != nil {
		return err
	}
	if outputSpool == nil {
		return fmt.Errorf("create coordinated command output spool: factory returned nil spool")
	}
	defer func() {
		err = errors.Join(err, outputSpool.cleanup())
	}()

	identity, err := runtime.resolveIdentity(container.ContainersDir)
	if err != nil {
		return err
	}
	owner, err := runtime.newOwner(policy.Operation, identity, version.String(), runtime.now())
	if err != nil {
		return err
	}
	request := coordination.Request{
		Operation: policy.Operation,
		Mode:      policy.Mode,
		Owner:     owner,
	}

	var operationErr error
	var outputDestination *os.File
	lifecycleErr := coordination.WithLease(
		context.Background(),
		cliRepositoryCoordinator{delegate: runtime.newCoordinator()},
		identity,
		request,
		func() error {
			outputDestination, operationErr = outputSpool.capture(func() error {
				if shouldRunStartupRecovery(args) {
					recoveryReport, recoveryErr := runtime.recover(outputMode)
					if recoveryErr != nil {
						log.Printf("System recovery failed: %v\n", recoveryErr)
					}
					emitStartupRecoveryReport(outputMode, recoveryReport, recoveryErr)
					if outputMode != outputModeJSON {
						checkEnvFilePermissions()
					}
				}
				return runtime.dispatch(parsed, outputMode)
			})
			return operationErr
		},
	)
	shouldReplayOutput := lifecycleErr == nil || operationErr != nil
	if shouldReplayOutput && outputDestination != nil {
		if replayErr := outputSpool.replayTo(outputDestination); replayErr != nil {
			return errors.Join(lifecycleErr, replayErr)
		}
	}
	return lifecycleErr
}

func dispatchCLICommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	var err error
	switch parsed.method {
	case "init":
		err = initCommand(parsed, outputMode)
	case "config":
		err = runConfigCommand(parsed, outputMode)
	case "doctor":
		err = runDoctorCommand(parsed, outputMode)
	case "store":
		err = runStoreCommand(parsed, outputMode)
	case "store-folder":
		err = runStoreFolderCommand(parsed, outputMode)
	case "restore":
		err = runRestoreCommand(parsed, outputMode)
	case "remove":
		err = runRemoveCommand(parsed, outputMode)
	case "repair":
		err = runRepairCommand(parsed, outputMode)
	case "gc":
		err = runGCCommand(parsed, outputMode)
	case "simulate":
		err = runSimulateCommand(parsed, outputMode)
	case "benchmark":
		err = runBenchmarkCommand(parsed, outputMode)
	case "stats":
		err = runStatsCommand(parsed, outputMode)
	case "inspect":
		err = runInspectCommand(parsed, outputMode)
	case "help", "-h", "--help":
		if len(parsed.positionals) != 0 {
			err = usageErrorf("Usage: coldkeep %s", parsed.method)
			break
		}
		printHelp()
	case "version", "-v", "--version":
		if len(parsed.positionals) != 0 {
			err = usageErrorf("Usage: coldkeep %s", parsed.method)
			break
		}
		err = runVersionCommand(outputMode)
	case "list":
		err = runListCommand(parsed, outputMode)
	case "search":
		err = runSearchCommand(parsed, outputMode)
	case "verify":
		err = runVerifyCommand(parsed, outputMode)
	case "snapshot":
		err = runSnapshotCommand(parsed, outputMode)
	default:
		err = usageErrorf("unknown command: %s", parsed.method)
	}
	return err
}

func runStartupRecoveryWithOptionalLogBuffering(mode cliOutputMode) (recovery.Report, error) {
	if mode != outputModeText || !isQuietHealthyStartupRecoveryEnabled() {
		return startupRecoveryPhase(container.ContainersDir)
	}

	prevOutput := log.Writer()
	prevFlags := log.Flags()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(prevOutput)
		log.SetFlags(prevFlags)
	}()

	recoveryReport, recoveryErr := startupRecoveryPhase(container.ContainersDir)
	if shouldReplayBufferedRecoveryLogs(recoveryReport, recoveryErr) {
		if _, err := io.Copy(prevOutput, &buf); err != nil {
			log.Printf("failed to replay buffered startup recovery logs: %v", err)
		}
	}

	return recoveryReport, recoveryErr
}

func isQuietHealthyStartupRecoveryEnabled() bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv("COLDKEEP_QUIET_HEALTHY_STARTUP_RECOVERY")))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func shouldReplayBufferedRecoveryLogs(report recovery.Report, err error) bool {
	if err != nil {
		return true
	}
	if report.AbortedLogicalFiles > 0 || report.AbortedChunks > 0 {
		return true
	}
	if report.QuarantinedMissing > 0 || report.QuarantinedCorruptTail > 0 || report.QuarantinedOrphan > 0 {
		return true
	}
	if report.SealingCompleted > 0 || report.SealingQuarantined > 0 {
		return true
	}
	return false
}

func emitStartupRecoveryReport(mode cliOutputMode, report recovery.Report, err error) {
	if mode == outputModeText && isQuietHealthyStartupRecoveryEnabled() && !shouldReplayBufferedRecoveryLogs(report, err) {
		return
	}

	if mode == outputModeJSON {
		// Startup recovery JSON is an event-style diagnostic stream on stderr.
		// It is intentionally separate from command result contracts on stdout.
		payload := map[string]any{
			"event":                               "startup_recovery",
			"status":                              "ok",
			"aborted_logical_files":               report.AbortedLogicalFiles,
			"aborted_chunks":                      report.AbortedChunks,
			"quarantined_missing_containers":      report.QuarantinedMissing,
			"quarantined_corrupt_tail_containers": report.QuarantinedCorruptTail,
			"quarantined_orphan_containers":       report.QuarantinedOrphan,
			"checked_container_records":           report.CheckedContainerRecord,
			"checked_disk_files":                  report.CheckedDiskFiles,
			"skipped_dir_entries":                 report.SkippedDirEntries,
		}
		if err != nil {
			payload["status"] = "error"
			payload["message"] = strings.TrimSpace(err.Error())
		}
		encoded, _ := json.Marshal(payload)
		fmt.Fprintln(os.Stderr, string(encoded))
		return
	}

	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"RECOVERY status=error aborted_logical_files=%d aborted_chunks=%d quarantined_missing_containers=%d quarantined_corrupt_tail_containers=%d quarantined_orphan_containers=%d checked_container_records=%d checked_disk_files=%d skipped_dir_entries=%d message=%q\n",
			report.AbortedLogicalFiles,
			report.AbortedChunks,
			report.QuarantinedMissing,
			report.QuarantinedCorruptTail,
			report.QuarantinedOrphan,
			report.CheckedContainerRecord,
			report.CheckedDiskFiles,
			report.SkippedDirEntries,
			strings.TrimSpace(err.Error()),
		)
		return
	}

	fmt.Fprintf(
		os.Stderr,
		"RECOVERY status=ok aborted_logical_files=%d aborted_chunks=%d quarantined_missing_containers=%d quarantined_corrupt_tail_containers=%d quarantined_orphan_containers=%d checked_container_records=%d checked_disk_files=%d skipped_dir_entries=%d\n",
		report.AbortedLogicalFiles,
		report.AbortedChunks,
		report.QuarantinedMissing,
		report.QuarantinedCorruptTail,
		report.QuarantinedOrphan,
		report.CheckedContainerRecord,
		report.CheckedDiskFiles,
		report.SkippedDirEntries,
	)
}

func printCLIError(err error, mode cliOutputMode) int {
	err = stableCLIError(err)
	code := classifyExitCode(err)
	message := strings.TrimSpace(err.Error())
	publicCode := publicErrorCode(err, code)
	invariantCode, hasInvariantCode := invariants.Code(err)
	recommendedAction := invariants.RecommendedActionForError(err)
	dbHint := localDBSetupHint(err)
	verifyDetails, hasVerifyDetails := extractVerifyFailureDetails(err)
	if mode == outputModeJSON {
		payload := map[string]any{
			"status":      "error",
			"error_class": exitErrorClassLabel(code),
			"exit_code":   code,
			"message":     message,
			"error": map[string]any{
				"code":    publicCode,
				"message": message,
			},
		}
		if hasInvariantCode {
			payload["invariant_code"] = invariantCode
		}
		if strings.TrimSpace(recommendedAction) != "" {
			payload["recommended_action"] = recommendedAction
		}
		if code == exitVerify && hasVerifyDetails {
			payload["stage"] = verifyDetails.Stage
			if verifyDetails.Block != nil {
				payload["block"] = *verifyDetails.Block
			}
			if verifyDetails.Container != nil {
				payload["container"] = *verifyDetails.Container
			}
			if verifyDetails.Offset != nil {
				payload["offset"] = *verifyDetails.Offset
			}
			payload["reason"] = verifyDetails.Reason
		}
		encoded, _ := json.Marshal(payload)
		fmt.Fprintln(os.Stderr, string(encoded))
		return code
	}

	fmt.Fprintf(os.Stderr, "ERROR[%s]: %s\n", exitErrorClassLabel(code), message)
	if code == exitVerify && hasVerifyDetails {
		fmt.Fprintln(os.Stderr, "verify failed")
		fmt.Fprintf(os.Stderr, "stage: %s\n", verifyDetails.Stage)
		if verifyDetails.Block != nil {
			fmt.Fprintf(os.Stderr, "block: %d\n", *verifyDetails.Block)
		}
		if verifyDetails.Container != nil {
			fmt.Fprintf(os.Stderr, "container: %d\n", *verifyDetails.Container)
		}
		if verifyDetails.Offset != nil {
			fmt.Fprintf(os.Stderr, "offset: %d\n", *verifyDetails.Offset)
		}
		fmt.Fprintf(os.Stderr, "reason: %s\n", verifyDetails.Reason)
	}
	if hasInvariantCode {
		fmt.Fprintf(os.Stderr, "INVARIANT_CODE: %s\n", invariantCode)
	}
	if strings.TrimSpace(recommendedAction) != "" {
		fmt.Fprintf(os.Stderr, "Recommended action: %s\n", recommendedAction)
	}
	if strings.TrimSpace(dbHint) != "" {
		fmt.Fprintln(os.Stderr, dbHint)
	}
	return code
}

func localDBSetupHint(err error) string {
	if err == nil {
		return ""
	}

	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if !strings.Contains(msg, "failed to connect to local db") {
		return ""
	}

	missing := make([]string, 0, 6)
	for _, key := range []string{"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSLMODE"} {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			missing = append(missing, key)
		}
	}

	b := &strings.Builder{}
	b.WriteString("DB setup hint: local mode requires PostgreSQL connection env vars.")
	if len(missing) > 0 {
		b.WriteString("\nMissing/empty: ")
		b.WriteString(strings.Join(missing, ", "))
		b.WriteString("\nExample:")
		b.WriteString("\n  export DB_HOST=127.0.0.1")
		b.WriteString("\n  export DB_PORT=5432")
		b.WriteString("\n  export DB_USER=coldkeep")
		b.WriteString("\n  export DB_PASSWORD=coldkeep")
		b.WriteString("\n  export DB_NAME=coldkeep")
		b.WriteString("\n  export DB_SSLMODE=disable")
		b.WriteString("\n  export COLDKEEP_DB_AUTO_BOOTSTRAP=true")
		return b.String()
	}

	if strings.Contains(msg, "ssl is not enabled on the server") {
		b.WriteString("\nYour database rejected SSL negotiation; for local Docker PostgreSQL use DB_SSLMODE=disable.")
		return b.String()
	}

	b.WriteString("\nCheck DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME/DB_SSLMODE values and PostgreSQL availability.")
	return b.String()
}

func printCLISuccess(parsed parsedCommandLine, mode cliOutputMode) {
	if mode != outputModeJSON {
		return
	}
	// These commands emit their own structured JSON payload.
	// Keep this list in sync with TestPrintCLISuccessJSONCommandPolicy.
	switch parsed.method {
	case "store", "store-folder", "restore", "remove", "repair", "gc", "list", "search", "stats", "inspect", "simulate", "benchmark", "doctor", "snapshot", "config", "version", "-v", "--version", "verify":
		return
	}

	payload := map[string]any{
		"status":  "ok",
		"command": parsed.method,
	}

	if len(parsed.positionals) > 0 {
		payload["target"] = parsed.positionals[0]
	}
	if parsed.method == "verify" {
		if verifyLevel, err := parseVerifyLevel(parsed); err == nil {
			payload["level"] = verifyLevelToString(verifyLevel)
		}
	}

	encoded, _ := json.Marshal(payload)
	fmt.Println(string(encoded))
}

func verifyFailureReason(vf *verify.VerifyFailure) string {
	if vf == nil {
		return "verification failed"
	}
	switch strings.TrimSpace(vf.Category) {
	case "physical_hash_mismatch":
		return "physical hash mismatch"
	case "compressed_hash_mismatch":
		return "compressed hash mismatch"
	case "block_hash_mismatch":
		return "logical hash mismatch"
	case "chunk_hash_mismatch":
		return "chunk hash mismatch"
	}
	if strings.TrimSpace(vf.Detail) != "" {
		return strings.TrimSpace(vf.Detail)
	}
	if strings.TrimSpace(vf.Category) != "" {
		return strings.TrimSpace(vf.Category)
	}
	return "verification failed"
}

func extractVerifyFailureDetails(err error) (verifyFailureDetails, bool) {
	var vf *verify.VerifyFailure
	if !errors.As(err, &vf) || vf == nil {
		return verifyFailureDetails{}, false
	}

	details := verifyFailureDetails{
		Stage:  strings.TrimSpace(string(vf.Stage)),
		Reason: verifyFailureReason(vf),
	}
	if vf.BlockID != nil {
		v := *vf.BlockID
		details.Block = &v
	}
	if vf.ContainerID != nil {
		v := *vf.ContainerID
		details.Container = &v
	}
	if vf.Offset != nil {
		v := *vf.Offset
		details.Offset = &v
	}
	return details, true
}

func countVerifySummaryForSystem(dbconn *sql.DB) (verifyOutputSummary, error) {
	var s verifyOutputSummary

	if err := dbconn.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN physical_hash IS NOT NULL AND length(physical_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN compressed_hash IS NOT NULL AND length(compressed_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN block_hash IS NOT NULL AND length(block_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) != 'none' THEN 1 ELSE 0 END), 0)
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE c.quarantine = FALSE
	`).Scan(&s.BlocksChecked, &s.PhysicalHashChecked, &s.CompressedHashChecked, &s.LogicalHashChecked, &s.CompressedBlocksChecked); err != nil {
		return verifyOutputSummary{}, err
	}

	var legacyBlocks int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		WHERE c.status = 'COMPLETED'
		  AND NOT EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = c.id)
	`).Scan(&legacyBlocks); err != nil {
		return verifyOutputSummary{}, err
	}

	s.BlocksChecked += legacyBlocks
	return s, nil
}

func countVerifySummaryForFile(dbconn *sql.DB, fileID int64) (verifyOutputSummary, error) {
	var s verifyOutputSummary

	if err := dbconn.QueryRow(`
		WITH target_blocks AS (
			SELECT DISTINCT sb.id, sb.physical_hash, sb.compressed_hash, sb.block_hash, sb.compression_codec
			FROM file_chunk fc
			JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
			JOIN storage_blocks sb ON sb.id = r.block_id
			JOIN container c ON c.id = sb.container_id
			WHERE fc.logical_file_id = $1
			  AND c.quarantine = FALSE
		)
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN physical_hash IS NOT NULL AND length(physical_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN compressed_hash IS NOT NULL AND length(compressed_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN block_hash IS NOT NULL AND length(block_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) != 'none' THEN 1 ELSE 0 END), 0)
		FROM target_blocks
	`, fileID).Scan(&s.BlocksChecked, &s.PhysicalHashChecked, &s.CompressedHashChecked, &s.LogicalHashChecked, &s.CompressedBlocksChecked); err != nil {
		return verifyOutputSummary{}, err
	}

	var legacyBlocks int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM file_chunk fc
		JOIN blocks b ON b.chunk_id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		  AND NOT EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = b.chunk_id)
	`, fileID).Scan(&legacyBlocks); err != nil {
		return verifyOutputSummary{}, err
	}

	s.BlocksChecked += legacyBlocks
	return s, nil
}

func collectVerifyOutputSummary(dbconn *sql.DB, target string, fileID int64) (verifyOutputSummary, error) {
	if dbconn == nil {
		return verifyOutputSummary{}, fmt.Errorf("verify summary DB connection is nil")
	}

	switch target {
	case "system":
		return countVerifySummaryForSystem(dbconn)
	case "file":
		return countVerifySummaryForFile(dbconn, fileID)
	default:
		return verifyOutputSummary{}, fmt.Errorf("unknown verify target: %s", target)
	}
}

func runVersionCommand(mode cliOutputMode) error {
	if mode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "version",
			"data": map[string]any{
				"version": version.String(),
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Println("coldkeep version", version.String())
	return nil
}

func validateConfigDefaultChunkerVersion(raw string) (chunk.Version, error) {
	v := chunk.Version(strings.TrimSpace(raw))
	if !chunk.IsWellFormedVersion(v) {
		return "", usageErrorf("invalid default-chunker value %q: malformed version", raw)
	}
	if _, ok := chunk.DefaultRegistry().Get(v); !ok {
		return "", usageErrorf("invalid default-chunker value %q: unknown chunker version", raw)
	}
	if deprecated, reason := isDeprecatedChunkerVersionPhase(v); deprecated {
		reason = strings.TrimSpace(reason)
		if reason == "" {
			return "", usageErrorf("invalid default-chunker value %q: deprecated chunker version", raw)
		}
		return "", usageErrorf("invalid default-chunker value %q: deprecated chunker version (%s)", raw, reason)
	}
	return v, nil
}

func runConfigCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) < 2 {
		return usageErrorf("Usage: coldkeep config <get|set> <default-chunker|compression|compression-level> [value]")
	}

	subcommand := strings.TrimSpace(strings.ToLower(parsed.positionals[0]))
	key := strings.TrimSpace(strings.ToLower(parsed.positionals[1]))
	if key != "default-chunker" && key != "compression" && key != "compression-level" {
		return usageErrorf("unknown config key: %s", parsed.positionals[1])
	}

	switch subcommand {
	case "get":
		if len(parsed.positionals) != 2 {
			return usageErrorf("Usage: coldkeep config get <default-chunker|compression|compression-level>")
		}
	case "set":
		if len(parsed.positionals) != 3 {
			return usageErrorf("Usage: coldkeep config set <default-chunker|compression|compression-level> <value>")
		}
	default:
		return usageErrorf("unknown config subcommand: %s", parsed.positionals[0])
	}

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	repo := storage.NewRepository(sgctx.DB)

	switch subcommand {
	case "get":
		switch key {
		case "compression":
			codec, err := repo.GetDefaultCompression(sgctx.DB)
			if err != nil {
				return err
			}
			if outputMode == outputModeJSON {
				payload := map[string]any{
					"status":  "ok",
					"command": "config get",
					"data": map[string]any{
						"key":   "compression",
						"value": codec,
					},
				}
				encoded, _ := json.Marshal(payload)
				fmt.Println(string(encoded))
				return nil
			}
			_, _ = fmt.Fprintln(os.Stdout, codec)
			return nil

		case "compression-level":
			level, err := repo.GetDefaultCompressionLevel(sgctx.DB)
			if err != nil {
				return err
			}
			if outputMode == outputModeJSON {
				payload := map[string]any{
					"status":  "ok",
					"command": "config get",
					"data": map[string]any{
						"key":   "compression-level",
						"value": level,
					},
				}
				encoded, _ := json.Marshal(payload)
				fmt.Println(string(encoded))
				return nil
			}
			_, _ = fmt.Fprintf(os.Stdout, "%d\n", level)
			return nil
		}

		v, err := repo.GetDefaultChunkerVersion()
		if err != nil {
			return err
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "config get",
				"data": map[string]any{
					"key":   "default-chunker",
					"value": string(v),
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintln(os.Stdout, string(v))
		return nil

	case "set":
		switch key {
		case "compression":
			codec := strings.TrimSpace(parsed.positionals[2])
			if !storage.IsRegisteredCompressionCodec(codec) {
				return usageErrorf("invalid compression codec %q, must be 'none' or 'zstd'", codec)
			}
			previous, err := repo.GetDefaultCompression(sgctx.DB)
			if err != nil {
				return err
			}
			if err := repo.SetDefaultCompression(sgctx.DB, codec); err != nil {
				return err
			}
			if outputMode == outputModeJSON {
				payload := map[string]any{
					"status":  "ok",
					"command": "config set",
					"data": map[string]any{
						"key":   "compression",
						"value": codec,
					},
				}
				encoded, _ := json.Marshal(payload)
				fmt.Println(string(encoded))
				return nil
			}
			_, _ = fmt.Fprintf(os.Stdout, "compression set to %s\n", codec)
			if previous != codec {
				_, _ = fmt.Fprintln(os.Stdout, "ℹ️  This affects only NEW blocks. Existing blocks are not recompressed.")
				_, _ = fmt.Fprintln(os.Stdout, "    Blocks remain readable according to their stored metadata.")
			}
			return nil

		case "compression-level":
			levelStr := strings.TrimSpace(parsed.positionals[2])
			level, err := strconv.Atoi(levelStr)
			if err != nil {
				return usageErrorf("invalid compression-level %q, must be an integer 1-9", levelStr)
			}
			if level < 1 || level > 9 {
				return usageErrorf("compression-level %d out of range, must be 1-9", level)
			}
			previous, err := repo.GetDefaultCompressionLevel(sgctx.DB)
			if err != nil {
				return err
			}
			if err := repo.SetDefaultCompressionLevel(sgctx.DB, level); err != nil {
				return err
			}
			if outputMode == outputModeJSON {
				payload := map[string]any{
					"status":  "ok",
					"command": "config set",
					"data": map[string]any{
						"key":   "compression-level",
						"value": level,
					},
				}
				encoded, _ := json.Marshal(payload)
				fmt.Println(string(encoded))
				return nil
			}
			_, _ = fmt.Fprintf(os.Stdout, "compression-level set to %d\n", level)
			if previous != level {
				_, _ = fmt.Fprintln(os.Stdout, "ℹ️  This affects only NEW blocks. Existing blocks are not recompressed.")
			}
			return nil
		}

		v, err := validateConfigDefaultChunkerVersion(parsed.positionals[2])
		if err != nil {
			return err
		}

		previous, err := repo.GetDefaultChunkerVersion()
		if err != nil {
			return err
		}

		if err := repo.SetDefaultChunkerVersion(v); err != nil {
			return err
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "config set",
				"data": map[string]any{
					"key":   "default-chunker",
					"value": string(v),
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintf(os.Stdout, "default-chunker set to %s\n", v)
		if previous != v {
			_, _ = fmt.Fprintln(os.Stdout, "Warning: This affects only new stored data.")
			_, _ = fmt.Fprintln(os.Stdout, "Existing data remains unchanged.")
		}
		return nil

	default:
		return usageErrorf("unknown config subcommand: %s", parsed.positionals[0])
	}
}

func verifyLevelToString(level verify.VerifyLevel) string {
	switch level {
	case verify.VerifyFast:
		return "fast"
	case verify.VerifyStandard:
		return "standard"
	case verify.VerifyFull:
		return "full"
	case verify.VerifyDeep:
		return "deep"
	default:
		return "unknown"
	}
}

func resolveOutputMode(parsed parsedCommandLine) (cliOutputMode, error) {
	value, hasValue := parsed.lastFlagValue("output")
	hasJSONFlag := parsed.hasFlag("json")
	normalized := strings.ToLower(strings.TrimSpace(value))

	if hasJSONFlag && hasValue {
		return outputModeText, usageErrorf("cannot combine --json with --output")
	}
	if hasJSONFlag {
		return outputModeJSON, nil
	}

	if !hasValue {
		return outputModeText, nil
	}

	switch normalized {
	case "", "text", "human":
		return outputModeText, nil
	case "table":
		return outputModeText, usageErrorf("invalid --output value %q (allowed: human, text, json)", value)
	case "json":
		return outputModeJSON, nil
	default:
		return outputModeText, usageErrorf("invalid --output value %q (allowed: human, text, json)", value)
	}
}

func resolveTraceOptions(parsed parsedCommandLine) (observability.TraceOptions, error) {
	hasTraceText := parsed.hasFlag("trace")
	hasTraceJSON := parsed.hasFlag("trace-json")

	if hasTraceText && hasTraceJSON {
		return observability.TraceOptions{}, usageErrorf("cannot combine --trace with --trace-json")
	}

	if hasTraceJSON {
		return observability.TraceOptions{Enabled: true, Sink: observability.NewJSONTraceSink(os.Stderr)}, nil
	}
	if hasTraceText {
		return observability.TraceOptions{Enabled: true, Sink: observability.HumanTraceSink{W: os.Stderr}}, nil
	}

	return observability.TraceOptions{}, nil
}

var outputSupportedCommands = map[string]bool{
	"config":       true,
	"doctor":       true,
	"inspect":      true,
	"verify":       true,
	"list":         true,
	"search":       true,
	"stats":        true,
	"store":        true,
	"store-folder": true,
	"restore":      true,
	"remove":       true,
	"repair":       true,
	"gc":           true,
	"simulate":     true,
	"snapshot":     true,
}

func inferOutputModeFromArgs(args []string) cliOutputMode {
	if len(args) < 1 || !outputSupportedCommands[args[0]] {
		return outputModeText
	}
	for i := 1; i < len(args); i++ {
		if args[i] == "--json" {
			return outputModeJSON
		}
	}

	for i := 1; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--output=") {
			if strings.EqualFold(strings.TrimPrefix(arg, "--output="), "json") {
				return outputModeJSON
			}
		}
		if arg == "--output" && i+1 < len(args) {
			if strings.EqualFold(args[i+1], "json") {
				return outputModeJSON
			}
		}
	}

	return outputModeText
}

func shouldRunStartupRecovery(args []string) bool {
	if len(args) == 0 {
		return false
	}
	for _, arg := range args[1:] {
		if arg == "--help" || arg == "-h" {
			return false
		}
	}
	switch args[0] {
	// doctor runs its own corrective recovery phase inside runDoctorCommand so it can
	// report corrective recovery/verify/schema in a single command-specific payload.
	case "store", "store-folder", "restore", "remove", "repair", "gc", "stats", "inspect", "list", "search", "verify", "snapshot":
		return true
	default:
		return false
	}
}

func exitErrorClassLabel(code int) string {
	switch code {
	case exitUsage:
		return "USAGE"
	case exitVerify:
		return "VERIFY"
	case exitRecovery:
		return "RECOVERY"
	default:
		return "GENERAL"
	}
}

// Keep fallback matching intentionally narrow; typed cliError classifications are authoritative.
// Usage-like verify parser errors are handled in the usage branch inside classifyExitCode.
func isLikelyVerifyFailureMessage(msg string) bool {
	if strings.Contains(msg, "verification failed") {
		return true
	}

	return strings.Contains(msg, "verify phase failed") ||
		strings.Contains(msg, "verify command failed")
}

func classifyExitCode(err error) int {
	if err == nil {
		return exitSuccess
	}

	var ce *cliError
	if errors.As(err, &ce) {
		switch ce.code {
		case exitUsage:
			return exitUsage
		case exitVerify:
			return exitVerify
		case exitRecovery:
			return exitRecovery
		default:
			return exitGeneral
		}
	}

	msg := strings.ToLower(strings.TrimSpace(err.Error()))

	if strings.Contains(msg, "usage:") ||
		strings.Contains(msg, "missing command") ||
		strings.Contains(msg, "missing value for --") ||
		strings.Contains(msg, "unknown flag(s)") ||
		strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "unknown option for gc") ||
		strings.Contains(msg, "no valid file ids after parsing input") ||
		strings.Contains(msg, "invalid fileid") ||
		strings.Contains(msg, "invalid file id") ||
		strings.Contains(msg, "unknown target for verify") ||
		strings.Contains(msg, "unknown verify level") ||
		strings.Contains(msg, "multiple verify levels provided") ||
		strings.Contains(msg, "verify level provided both as flag and positional argument") ||
		strings.Contains(msg, "invalid --limit") ||
		strings.Contains(msg, "invalid --min-size") ||
		strings.Contains(msg, "invalid --offset") ||
		strings.Contains(msg, "invalid --max-size") ||
		strings.Contains(msg, "unknown simulate subcommand") {
		return exitUsage
	}

	if isLikelyVerifyFailureMessage(msg) {
		return exitVerify
	}

	if strings.Contains(msg, "recovery phase failed") || strings.Contains(msg, "system recovery failed") {
		return exitRecovery
	}

	return exitGeneral
}

func runStoreCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "codec", "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep store [--codec <plain|aes-gcm>] <filePath>")
	}

	path := parsed.positionals[0]
	codecName, _ := parsed.lastFlagValue("codec")

	perf := newPerfTimer()
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()
	perf.Mark("setup")

	var result storage.StoreFileResult
	if codecName == "" {
		result, err = storeByFilePhase(&sgctx, path, "")
	} else {
		if codecName == "plain" {
			_, _ = fmt.Fprintln(os.Stderr, "WARNING: data would be stored without encryption")
		}

		codec, parseErr := blocks.ParseCodec(codecName)
		if parseErr != nil {
			return parseErr
		}
		result, err = storeByFilePhase(&sgctx, path, string(codec))
	}
	perf.Mark("operation")
	if sgctx.Writer != nil {
		_ = sgctx.Writer.FinalizeContainer()
	}
	perf.Mark("finalize")
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "store",
			"data": map[string]any{
				"path":           result.Path,
				"stored_path":    result.Path,
				"file_id":        result.FileID,
				"file_hash":      result.FileHash,
				"already_stored": result.AlreadyStored,
				"perf_spans":     perf.Spans(),
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if result.AlreadyStored {
		_, _ = fmt.Fprintln(os.Stdout, "File already stored: "+result.Path)
	} else {
		_, _ = fmt.Fprintln(os.Stdout, "File stored successfully: "+result.Path)
	}
	_, _ = fmt.Fprintln(os.Stdout, "  FileID: "+strconv.FormatInt(result.FileID, 10))
	_, _ = fmt.Fprintln(os.Stdout, "  SHA256: "+result.FileHash)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runStoreFolderCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "codec", "output", "json", "workers"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep store-folder [--codec <plain|aes-gcm>] [--workers <N>] <folderPath>")
	}

	path := parsed.positionals[0]
	codecName, _ := parsed.lastFlagValue("codec")
	opts, err := execution.FromEnv(execution.DefaultOptions())
	if err != nil {
		return fmt.Errorf("store-folder execution options: %w", err)
	}
	if rawWorkers, hasWorkers := parsed.lastFlagValue("workers"); hasWorkers {
		workers, convErr := strconv.Atoi(strings.TrimSpace(rawWorkers))
		if convErr != nil || workers <= 0 {
			return usageErrorf("invalid --workers value %q (must be integer > 0)", rawWorkers)
		}
		opts.StoreFolderWorkers = workers
	}

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if codecName == "" {
		err = storage.StoreFolderWithStorageContextAndOptions(sgctx, path, opts)
	} else {
		if codecName == "plain" {
			_, _ = fmt.Fprintln(os.Stderr, "WARNING: data would be stored without encryption")
		}

		codec, parseErr := blocks.ParseCodec(codecName)
		if parseErr != nil {
			return parseErr
		}

		err = storage.StoreFolderWithStorageContextAndCodecAndOptions(sgctx, path, codec, opts)
	}
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "store-folder",
			"target":  path,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	_, _ = fmt.Fprintln(os.Stdout, "Folder stored successfully: "+path)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runRestoreCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json", "input", "dry-run", "dryRun", "fail-fast", "failFast", "overwrite", "stored-path", "mode", "destination", "strict", "no-metadata"); err != nil {
		return err
	}
	if err := rejectBlankFlagValues(parsed, "stored-path"); err != nil {
		return err
	}

	storedPath, _ := parsed.lastFlagValue("stored-path")
	hasStoredPath := strings.TrimSpace(storedPath) != ""
	overwrite := parsed.hasFlag("overwrite")

	if hasStoredPath {
		if len(parsed.positionals) != 0 {
			return usageErrorf("Usage: coldkeep restore --stored-path <path> [--mode <original|prefix|override>] [--destination <path>] [--overwrite] [--strict] [--no-metadata]")
		}
		if parsed.hasFlag("input") {
			return usageErrorf("--input is not supported with --stored-path")
		}
		if parsed.hasFlag("dry-run", "dryRun", "fail-fast", "failFast") {
			return usageErrorf("--dry-run and --fail-fast are not supported with --stored-path")
		}

		strictMetadata := parsed.hasFlag("strict")
		noMetadata := parsed.hasFlag("no-metadata")
		if strictMetadata && noMetadata {
			return usageErrorf("--strict and --no-metadata cannot be used together")
		}

		destinationMode, err := parseRestoreDestinationMode(parsed)
		if err != nil {
			return err
		}
		destination, _ := parsed.lastFlagValue("destination")
		destination = strings.TrimSpace(destination)
		if destinationMode == storage.RestoreDestinationOriginal && destination != "" {
			return usageErrorf("--destination is only supported with --mode prefix or --mode override")
		}
		if (destinationMode == storage.RestoreDestinationPrefix || destinationMode == storage.RestoreDestinationOverride) && destination == "" {
			return usageErrorf("--destination is required with --mode %s", destinationMode)
		}

		perf := newPerfTimer()
		sgctx, err := loadDefaultStorageContextPhase()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()
		perf.Mark("setup")

		eng, err := newCommandEngine(sgctx.DB, sgctx.EffectiveContainerDir())
		if err != nil {
			return err
		}
		result, normalizedMode, err := restoreStoredPathWithEngine(
			context.Background(),
			eng,
			storedPath,
			destinationMode,
			destination,
			overwrite,
			strictMetadata,
			noMetadata,
		)
		if err != nil {
			return err
		}
		perf.Mark("operation")

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "restore",
				"data": map[string]any{
					"stored_path":   strings.TrimSpace(storedPath),
					"output_path":   result.OutputPath,
					"file_id":       result.FileID,
					"restored_hash": result.RestoredHash,
					"mode":          normalizedMode,
					"perf_spans":    perf.Spans(),
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintln(os.Stdout, "File restored successfully: "+result.OutputPath)
		_, _ = fmt.Fprintln(os.Stdout, "  FileID: "+strconv.FormatInt(result.FileID, 10))
		_, _ = fmt.Fprintln(os.Stdout, "  SHA256: "+result.RestoredHash)
		_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
		return nil
	}

	if parsed.hasFlag("strict", "no-metadata") {
		return usageErrorf("--strict and --no-metadata are only supported with --stored-path")
	}

	inputFile, _ := parsed.lastFlagValue("input")
	hasInput := strings.TrimSpace(inputFile) != ""
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep restore <fileID> [fileID ...] <outputDir>")
	}
	if !hasInput && len(parsed.positionals) < 2 {
		return usageErrorf("Usage: coldkeep restore <fileID> [fileID ...] <outputDir>")
	}
	dryRun := parsed.hasFlag("dry-run", "dryRun")
	failFast := parsed.hasFlag("fail-fast", "failFast")
	targetArgs := parsed.positionals[:len(parsed.positionals)-1]
	outputRoot := parsed.positionals[len(parsed.positionals)-1]
	if !hasInput && len(targetArgs) == 1 {
		target := strings.TrimSpace(targetArgs[0])
		id, parseErr := strconv.ParseInt(target, 10, 64)
		if parseErr != nil || id <= 0 {
			return usageErrorf("Invalid fileID: %s (restore expects numeric logical file IDs; for path-based restore use --stored-path)\nDid you mean: coldkeep restore --stored-path <path> --destination <outputPath> --mode override", targetArgs[0])
		}
	}

	rawTargets, err := batch.LoadRawTargets(targetArgs, inputFile)
	if err != nil {
		return usageErrorf("failed to open/read input file: %v", err)
	}
	preparedTargets := batch.PrepareTargets(rawTargets)
	// Defensive fallback: empty prepared targets can still happen when no
	// materialized IDs are provided (for example, empty args/input combinations).
	if len(preparedTargets) == 0 {
		return usageErrorf("no valid file IDs after parsing input")
	}
	if !batch.HasExecutableTargets(preparedTargets) {
		report := batch.ExecutePrepared(batch.OperationRestore, dryRun, failFast, preparedTargets, nil)
		return emitBatchCommandReport("restore", report, outputMode)
	}

	outputPath, err := ensureRestoreOutputDir(outputRoot, !dryRun)
	if err != nil {
		return err
	}

	restorePerf := newPerfTimer()
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()
	restorePerf.Mark("setup")

	execFunc := func(fileID int64) batch.ItemResult {
		if dryRun {
			return executeRestoreDryRunItem(&sgctx, fileID, outputPath, overwrite)
		}
		return executeRestoreItem(&sgctx, fileID, outputPath, overwrite)
	}

	report := batch.ExecutePrepared(batch.OperationRestore, dryRun, failFast, preparedTargets, execFunc)
	restorePerf.Mark("operation")
	return emitBatchCommandReport("restore", report, outputMode, restorePerf.Spans())
}

func parseRestoreDestinationMode(parsed parsedCommandLine) (storage.RestoreDestinationMode, error) {
	value, hasValue := parsed.lastFlagValue("mode")
	if !hasValue {
		return storage.RestoreDestinationOriginal, nil
	}

	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(storage.RestoreDestinationOriginal):
		return storage.RestoreDestinationOriginal, nil
	case string(storage.RestoreDestinationPrefix):
		return storage.RestoreDestinationPrefix, nil
	case string(storage.RestoreDestinationOverride):
		return storage.RestoreDestinationOverride, nil
	default:
		return "", usageErrorf("invalid --mode value %q (allowed: original, prefix, override)", value)
	}
}

func runRemoveCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json", "input", "dry-run", "dryRun", "fail-fast", "failFast", "stored-path", "stored-paths"); err != nil {
		return err
	}
	if err := rejectBlankFlagValues(parsed, "stored-path"); err != nil {
		return err
	}

	removePerf := newPerfTimer()

	storedPath, _ := parsed.lastFlagValue("stored-path")
	hasStoredPath := strings.TrimSpace(storedPath) != ""
	storedPathsMode := parsed.hasFlag("stored-paths")
	if storedPathsMode && hasStoredPath {
		return usageErrorf("--stored-path and --stored-paths cannot be used together")
	}
	if hasStoredPath {
		if len(parsed.positionals) != 0 {
			return usageErrorf("Usage: coldkeep remove --stored-path <path>")
		}
		if parsed.hasFlag("input") {
			return usageErrorf("--input is not supported with --stored-path")
		}
		if parsed.hasFlag("dry-run", "dryRun", "fail-fast", "failFast") {
			return usageErrorf("--dry-run and --fail-fast are not supported with --stored-path")
		}

		sgctx, err := loadDefaultStorageContextPhase()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()

		eng, err := newCommandEngine(sgctx.DB, sgctx.EffectiveContainerDir())
		if err != nil {
			return err
		}
		result, err := removeStoredPathWithEngine(context.Background(), eng, storedPath)
		if err != nil {
			return err
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "remove",
				"data": map[string]any{
					"stored_path":         result.StoredPath,
					"logical_file_id":     result.LogicalFileID,
					"remaining_ref_count": result.RemainingRefCount,
					"removed":             result.Removed,
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintln(os.Stdout, "Stored path mapping removed: "+result.StoredPath)
		_, _ = fmt.Fprintln(os.Stdout, "  LogicalFileID: "+strconv.FormatInt(result.LogicalFileID, 10))
		_, _ = fmt.Fprintln(os.Stdout, "  Remaining refs: "+strconv.FormatInt(result.RemainingRefCount, 10))
		_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
		return nil
	}

	if storedPathsMode {
		inputFile, _ := parsed.lastFlagValue("input")
		hasInput := strings.TrimSpace(inputFile) != ""
		if !hasInput && len(parsed.positionals) < 1 {
			return usageErrorf("Usage: coldkeep remove --stored-paths <path> [path ...]")
		}
		dryRun := parsed.hasFlag("dry-run", "dryRun")
		failFast := parsed.hasFlag("fail-fast", "failFast")

		rawTargets, err := batch.LoadRawTargets(parsed.positionals, inputFile)
		if err != nil {
			return usageErrorf("failed to open/read input file: %v", err)
		}
		if len(rawTargets) == 0 {
			return usageErrorf("no valid stored paths after parsing input")
		}

		orderedTargets := make([]string, 0, len(rawTargets))
		for _, target := range rawTargets {
			orderedTargets = append(orderedTargets, target.Value)
		}
		req := engine.RemoveStoredPathsRequest{
			StoredPaths: orderedTargets,
			DryRun:      dryRun,
			FailFast:    failFast,
		}
		terminalResult, requiresRepository, err := engine.PreflightRemoveStoredPaths(req)
		if err != nil {
			return err
		}
		if !requiresRepository {
			report := removeStoredPathsResultToBatchReport(terminalResult, failFast)
			removePerf.Mark("operation")
			return emitBatchCommandReport("remove", report, outputMode, removePerf.Spans())
		}

		sgctx, err := loadDefaultStorageContextPhase()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()
		removePerf.Mark("setup")

		eng, err := newCommandEngine(sgctx.DB, sgctx.EffectiveContainerDir())
		if err != nil {
			return err
		}
		result, err := eng.RemoveStoredPaths(context.Background(), req)
		if err != nil {
			return err
		}
		report := removeStoredPathsResultToBatchReport(result, failFast)
		removePerf.Mark("operation")
		return emitBatchCommandReport("remove", report, outputMode, removePerf.Spans())
	}

	inputFile, _ := parsed.lastFlagValue("input")
	hasInput := strings.TrimSpace(inputFile) != ""
	if !hasInput && len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep remove <fileID> [fileID ...]")
	}
	dryRun := parsed.hasFlag("dry-run", "dryRun")
	failFast := parsed.hasFlag("fail-fast", "failFast")

	rawTargets, err := batch.LoadRawTargets(parsed.positionals, inputFile)
	if err != nil {
		return usageErrorf("failed to open/read input file: %v", err)
	}
	preparedTargets := batch.PrepareTargets(rawTargets)
	// Defensive fallback: empty prepared targets can still happen when no
	// materialized IDs are provided (for example, empty args/input combinations).
	if len(preparedTargets) == 0 {
		return usageErrorf("no valid file IDs after parsing input")
	}
	if !batch.HasExecutableTargets(preparedTargets) {
		removePerf.Mark("setup")
		report := batch.ExecutePrepared(batch.OperationRemove, dryRun, failFast, preparedTargets, nil)
		removePerf.Mark("operation")
		return emitBatchCommandReport("remove", report, outputMode, removePerf.Spans())
	}

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()
	removePerf.Mark("setup")

	execFunc := func(fileID int64) batch.ItemResult {
		return removeByIDPhase(&sgctx, fileID, dryRun)
	}

	report := batch.ExecutePrepared(batch.OperationRemove, dryRun, failFast, preparedTargets, execFunc)
	removePerf.Mark("operation")
	return emitBatchCommandReport("remove", report, outputMode, removePerf.Spans())
}

func ensureRestoreOutputDir(path string, createIfMissing bool) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", usageErrorf("Usage: coldkeep restore <fileID> [fileID ...] <outputDir>")
	}

	if createIfMissing {
		if err := os.MkdirAll(path, 0755); err != nil {
			return "", fmt.Errorf("create output directory %q: %w", path, err)
		}
	}

	st, err := os.Stat(path)
	if err != nil {
		if !createIfMissing && os.IsNotExist(err) {
			return path, nil
		}
		return "", fmt.Errorf("stat output directory %q: %w", path, err)
	}
	if !st.IsDir() {
		return "", usageErrorf("restore output destination must be a directory: %s", path)
	}

	return path, nil
}

func batchOverallStatus(report batch.Report) string {
	if report.Summary.Failed == 0 {
		return "ok"
	}
	if report.Summary.Success > 0 || report.Summary.Planned > 0 {
		return "partial_failure"
	}
	return "error"
}

func printBatchHumanReport(label string, report batch.Report) {
	if report.DryRun {
		fmt.Printf("[%s DRY-RUN]\n", label)
	} else {
		fmt.Printf("[%s]\n", label)
	}
	for _, item := range report.Results {
		switch item.Status {
		case batch.ResultSuccess:
			if item.OutputPath != "" {
				fmt.Printf("✔ id=%-6d -> %s\n", item.ID, item.OutputPath)
			} else if item.ID > 0 && item.Message != "" {
				fmt.Printf("✔ id=%-6d %s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" && item.Message != "" {
				fmt.Printf("✔ input=%q %s\n", item.RawValue, item.Message)
			} else if item.Message != "" {
				fmt.Printf("✔ %s\n", item.Message)
			} else {
				fmt.Printf("✔ success\n")
			}
		case batch.ResultFailed:
			if item.ID > 0 {
				fmt.Printf("✖ id=%-6d error=%s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" {
				fmt.Printf("✖ input=%q error=%s\n", item.RawValue, item.Message)
			} else {
				fmt.Printf("✖ error=%s\n", item.Message)
			}
			if strings.TrimSpace(item.InvariantCode) != "" {
				fmt.Printf("  invariant_code=%s\n", item.InvariantCode)
			}
			if strings.TrimSpace(item.RecommendedAction) != "" {
				fmt.Printf("  recommended_action=%s\n", item.RecommendedAction)
			}
		case batch.ResultSkipped:
			if item.ID > 0 {
				fmt.Printf("↷ id=%-6d skipped %s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" {
				fmt.Printf("↷ input=%q skipped %s\n", item.RawValue, item.Message)
			} else {
				fmt.Printf("↷ skipped %s\n", item.Message)
			}
		case batch.ResultPlanned:
			if item.ID > 0 {
				fmt.Printf("  id=%-6d %s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" {
				fmt.Printf("  input=%q %s\n", item.RawValue, item.Message)
			} else {
				fmt.Printf("  %s\n", item.Message)
			}
		}
	}
	fmt.Println("Summary:")
	fmt.Printf("  total:   %d\n", report.Summary.Total)
	if report.DryRun {
		fmt.Printf("  planned: %d\n", report.Summary.Planned)
		fmt.Printf("  failed:  %d\n", report.Summary.Failed)
		fmt.Printf("  skipped: %d\n", report.Summary.Skipped)
	} else {
		fmt.Printf("  success: %d\n", report.Summary.Success)
		fmt.Printf("  failed:  %d\n", report.Summary.Failed)
		fmt.Printf("  skipped: %d\n", report.Summary.Skipped)
	}
}

func executeRestoreDryRunItem(sgctx *storage.StorageContext, fileID int64, outputDir string, overwrite bool) batch.ItemResult {
	result, err := restoreByIDPhase(sgctx, fileID, outputDir, overwrite, true)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("file ID %d not found", fileID)}
		}
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
	}

	return batch.ItemResult{
		ID:           fileID,
		Status:       batch.ResultPlanned,
		Message:      fmt.Sprintf("would restore -> %s", result.OutputPath),
		OriginalName: result.OriginalName,
		OutputPath:   result.OutputPath,
	}
}

func executeRestoreItem(sgctx *storage.StorageContext, fileID int64, outputDir string, overwrite bool) batch.ItemResult {
	result, err := restoreByIDPhase(sgctx, fileID, outputDir, overwrite, false)
	if err != nil {
		item := batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
		annotateBatchFailureFromError(err, &item)
		return item
	}

	return batch.ItemResult{
		ID:           fileID,
		Status:       batch.ResultSuccess,
		Message:      "restored",
		OriginalName: result.OriginalName,
		OutputPath:   result.OutputPath,
	}
}

func annotateBatchFailureFromError(err error, item *batch.ItemResult) {
	if err == nil || item == nil {
		return
	}

	code, ok := invariants.Code(err)
	if !ok {
		return
	}

	item.InvariantCode = code
	item.RecommendedAction = invariants.RecommendedActionForCode(code)
}

func emitBatchCommandReport(command string, report batch.Report, outputMode cliOutputMode, spans ...[]perfSpan) error {
	executionMode := report.ExecutionMode
	if executionMode == "" {
		executionMode = batch.ExecutionModeContinueOnError
	}

	if outputMode == outputModeJSON {
		jsonResults := make([]map[string]any, 0, len(report.Results))
		for _, item := range report.Results {
			encoded := map[string]any{
				"status": item.Status,
			}
			if item.ID > 0 {
				encoded["id"] = item.ID
			}
			if strings.TrimSpace(item.RawValue) != "" {
				encoded["raw_value"] = item.RawValue
			}
			if item.OutputPath != "" {
				encoded["output_path"] = item.OutputPath
			}
			if item.OriginalName != "" {
				encoded["original_name"] = item.OriginalName
			}
			if strings.TrimSpace(item.InvariantCode) != "" {
				encoded["invariant_code"] = item.InvariantCode
			}
			if strings.TrimSpace(item.RecommendedAction) != "" {
				encoded["recommended_action"] = item.RecommendedAction
			}
			if item.Status == batch.ResultFailed && item.Message != "" {
				encoded["error"] = item.Message
			} else if item.Status == batch.ResultSkipped {
				message := strings.TrimSpace(item.Message)
				if message == "" {
					message = "skipped"
				}
				encoded["message"] = message
			} else if item.Message != "" {
				encoded["message"] = item.Message
			}
			jsonResults = append(jsonResults, encoded)
		}

		payload := map[string]any{
			"status":         batchOverallStatus(report),
			"command":        command,
			"dry_run":        report.DryRun,
			"execution_mode": executionMode,
			"summary":        report.Summary,
			"results":        jsonResults,
		}
		if len(spans) > 0 && len(spans[0]) > 0 {
			payload["perf_spans"] = spans[0]
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
	} else {
		printBatchHumanReport(strings.ToUpper(command), report)
		if !report.DryRun {
			fmt.Printf("  Hint: %s\n", doctorOperationalHint)
		}
	}

	if batch.ExitCodeFromReport(report) != 0 {
		return &cliError{code: deriveBatchFailureExitCode(report), msg: fmt.Sprintf("one or more %s operations failed", command)}
	}
	return nil
}

func deriveBatchFailureExitCode(report batch.Report) int {
	hasValidationFailures := false
	hasExecutionFailures := false
	hasVerifyFailures := false

	for _, item := range report.Results {
		if item.Status != batch.ResultFailed {
			continue
		}

		if strings.TrimSpace(item.InvariantCode) != "" {
			hasVerifyFailures = true
			continue
		}

		if strings.TrimSpace(item.RawValue) != "" || item.ID <= 0 {
			hasValidationFailures = true
			continue
		}

		hasExecutionFailures = true
	}

	if hasVerifyFailures {
		return exitVerify
	}
	if hasExecutionFailures {
		return exitGeneral
	}
	if hasValidationFailures {
		return exitUsage
	}
	return exitGeneral
}

func runGCCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "dry-run", "dryRun", "output", "json"); err != nil {
		return err
	}

	dryRun := parsed.hasFlag("dry-run", "dryRun")
	switch len(parsed.positionals) {
	case 0:
	case 1:
		switch parsed.positionals[0] {
		case "dry-run", "dryRun":
			dryRun = true
		default:
			return usageErrorf("Unknown option for gc: %s", parsed.positionals[0])
		}
	default:
		return usageErrorf("Usage: coldkeep gc [--dry-run]")
	}

	perf := newPerfTimer()
	result, err := runGCPhase(dryRun, container.ContainersDir)
	if err != nil {
		return err
	}
	perf.Mark("operation")

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "gc",
			"data":    result,
		}
		payload["perf_spans"] = perf.Spans()
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if result.AffectedContainers == 0 {
		fmt.Println("GC completed. No containers eligible for deletion.")
		if dryRun && result.SnapshotRetainedContainers > 0 {
			fmt.Printf("GC skipped containers still retained by snapshots: %d\n", result.SnapshotRetainedContainers)
		}
		if result.SnapshotRetainedLogicalFiles > 0 {
			fmt.Printf("GC retained snapshot-protected logical files: %d\n", result.SnapshotRetainedLogicalFiles)
		}
		if result.RetainedCurrentOnlyLogical > 0 || result.RetainedSnapshotOnlyLogical > 0 || result.RetainedSharedLogical > 0 {
			fmt.Printf("GC retention roots (logical files): current_only=%d snapshot_only=%d shared=%d\n", result.RetainedCurrentOnlyLogical, result.RetainedSnapshotOnlyLogical, result.RetainedSharedLogical)
		}
		fmt.Printf("Hint: %s\n", doctorOperationalHint)
		return nil
	}

	if dryRun {
		for _, filename := range result.ContainerFilenames {
			fmt.Printf("[DRY-RUN] Would delete container: %s\n", filename)
		}
		fmt.Printf("GC dry-run completed. Containers eligible for deletion: %d\n", result.AffectedContainers)
		if result.SnapshotRetainedContainers > 0 {
			fmt.Printf("GC skipped containers still retained by snapshots: %d\n", result.SnapshotRetainedContainers)
		}
		if result.SnapshotRetainedLogicalFiles > 0 {
			fmt.Printf("GC retained snapshot-protected logical files: %d\n", result.SnapshotRetainedLogicalFiles)
		}
		if result.RetainedCurrentOnlyLogical > 0 || result.RetainedSnapshotOnlyLogical > 0 || result.RetainedSharedLogical > 0 {
			fmt.Printf("GC retention roots (logical files): current_only=%d snapshot_only=%d shared=%d\n", result.RetainedCurrentOnlyLogical, result.RetainedSnapshotOnlyLogical, result.RetainedSharedLogical)
		}
		fmt.Printf("Hint: %s\n", doctorOperationalHint)
		return nil
	}

	for _, filename := range result.ContainerFilenames {
		fmt.Printf("Deleted container: %s\n", filename)
	}
	fmt.Printf("GC completed. Containers deleted: %d\n", result.AffectedContainers)
	if result.SnapshotRetainedLogicalFiles > 0 {
		fmt.Printf("GC retained snapshot-protected logical files: %d\n", result.SnapshotRetainedLogicalFiles)
	}
	if result.RetainedCurrentOnlyLogical > 0 || result.RetainedSnapshotOnlyLogical > 0 || result.RetainedSharedLogical > 0 {
		fmt.Printf("GC retention roots (logical files): current_only=%d snapshot_only=%d shared=%d\n", result.RetainedCurrentOnlyLogical, result.RetainedSnapshotOnlyLogical, result.RetainedSharedLogical)
	}
	fmt.Printf("Hint: %s\n", doctorOperationalHint)
	return nil
}

func runStatsCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json", "containers", "trace", "trace-json", "help", "h"); err != nil {
		return err
	}
	if parsed.hasFlag("help", "h") {
		printStatsHelp()
		return nil
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep stats [--output <human|json>] [--json] [--containers] [--trace|--trace-json]")
	}
	traceOptions, err := resolveTraceOptions(parsed)
	if err != nil {
		return err
	}

	includeContainers := parsed.hasFlag("containers")
	r, err := runObservabilityStatsPhase(observability.StatsOptions{IncludeContainers: includeContainers, Trace: traceOptions})
	if err != nil {
		return observabilityWrappedError(exitGeneral, "INTERNAL", "stats collection failed", err)
	}

	renderer := resolveRenderer(outputMode)
	return renderer.RenderStats(os.Stdout, r)
}

func runInspectCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json", "relations", "reverse", "deep", "limit", "trace", "trace-json", "help", "h"); err != nil {
		return err
	}
	if parsed.hasFlag("help", "h") {
		printInspectHelp()
		return nil
	}
	traceOptions, err := resolveTraceOptions(parsed)
	if err != nil {
		return err
	}

	validEntities := map[string]observability.EntityType{
		"repository":   observability.EntityRepository,
		"file":         observability.EntityFile,
		"logical-file": observability.EntityFile,
		"snapshot":     observability.EntitySnapshot,
		"chunk":        observability.EntityChunk,
		"container":    observability.EntityContainer,
	}

	if len(parsed.positionals) == 0 || len(parsed.positionals) > 2 {
		return usageErrorf("Usage: coldkeep inspect repository\n       coldkeep inspect (file|logical-file|snapshot|chunk|container) <id>")
	}
	entityName := parsed.positionals[0]
	entityType, ok := validEntities[entityName]
	if !ok {
		return observabilityErrorf(exitUsage, "INVALID_ARGUMENT", "unsupported inspect entity %q", entityName)
	}
	entityLabel := inspectEntityLabel(entityName)

	entityID := ""
	if entityType == observability.EntityRepository {
		if len(parsed.positionals) == 2 {
			return usageErrorf("Usage: coldkeep inspect repository")
		}
	} else {
		if len(parsed.positionals) != 2 {
			return usageErrorf("Usage: coldkeep inspect (file|logical-file|snapshot|chunk|container) <id>")
		}
		entityID = strings.TrimSpace(parsed.positionals[1])
		if entityID == "" {
			return observabilityErrorf(exitUsage, "INVALID_ARGUMENT", "invalid %s id %q", entityLabel, parsed.positionals[1])
		}
	}

	// For file/chunk/container a numeric id is required; validate early for a clear error.
	if entityType == observability.EntityFile || entityType == observability.EntityChunk || entityType == observability.EntityContainer {
		if n, err := strconv.ParseInt(entityID, 10, 64); err != nil || n <= 0 {
			return observabilityErrorf(exitUsage, "INVALID_ARGUMENT", "invalid %s id %q", entityLabel, entityID)
		}
	}

	opts := observability.InspectOptions{
		Relations: parsed.hasFlag("relations"),
		Reverse:   parsed.hasFlag("reverse"),
		Deep:      parsed.hasFlag("deep"),
		Trace:     traceOptions,
	}
	if limitStr, hasLimit := parsed.lastFlagValue("limit"); hasLimit {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n <= 0 {
			return usageErrorf("Invalid --limit value: %s", limitStr)
		}
		opts.Limit = n
	}

	r, err := runObservabilityInspectPhase(entityType, entityID, opts)
	if err != nil {
		if errors.Is(err, observability.ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
			return observabilityErrorf(exitGeneral, "NOT_FOUND", "%s %s not found", entityLabel, entityID)
		}
		return observabilityWrappedError(exitGeneral, "INTERNAL", "inspect failed", err)
	}

	renderer := resolveRenderer(outputMode)
	return renderer.RenderInspect(os.Stdout, r)
}

func runRepairCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json", "batch", "input", "fail-fast", "failFast"); err != nil {
		return err
	}

	batchMode := parsed.hasFlag("batch")
	if batchMode {
		inputFile, _ := parsed.lastFlagValue("input")
		rawTargets, err := batch.LoadRawTargets(parsed.positionals, inputFile)
		if err != nil {
			return usageErrorf("failed to open/read input file: %v", err)
		}
		if len(rawTargets) == 0 {
			return usageErrorf("Usage: coldkeep repair ref-counts --batch [--input <file>] [--fail-fast] [--output <text|json>]")
		}

		prepared := prepareRepairTargets(rawTargets)
		if len(prepared) == 0 {
			return usageErrorf("no valid repair targets after parsing input")
		}

		report := executeRepairPrepared(parsed.hasFlag("fail-fast", "failFast"), prepared)
		return emitBatchCommandReport("repair", report, outputMode)
	}

	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep repair <ref-counts|chunk-live-ref-counts> [--output <text|json>]")
	}

	target := strings.TrimSpace(parsed.positionals[0])
	switch target {
	case "ref-counts":
		result, err := repairLogicalRefCountsPhase()
		if err != nil {
			return verifyError(fmt.Errorf("repair ref-counts failed: %w", err))
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "repair",
				"data": map[string]any{
					"target":                    "ref-counts",
					"scanned_logical_files":     result.ScannedLogicalFiles,
					"updated_logical_files":     result.UpdatedLogicalFiles,
					"orphan_physical_file_rows": result.OrphanPhysicalFileRows,
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		fmt.Printf("Recomputed logical_file.ref_count from physical_file rows. scanned_logical_files=%d updated_logical_files=%d orphan_physical_file_rows=%d\n",
			result.ScannedLogicalFiles,
			result.UpdatedLogicalFiles,
			result.OrphanPhysicalFileRows,
		)
		fmt.Printf("Hint: %s\n", doctorOperationalHint)
		return nil

	case "chunk-live-ref-counts":
		result, err := repairChunkLiveRefCountsPhase()
		if err != nil {
			return verifyError(fmt.Errorf("repair chunk-live-ref-counts failed: %w", err))
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "repair",
				"data": map[string]any{
					"target":         "chunk-live-ref-counts",
					"scanned_chunks": result.ScannedChunks,
					"updated_chunks": result.UpdatedChunks,
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		fmt.Printf("Recomputed chunk.live_ref_count from file_chunk rows. scanned_chunks=%d updated_chunks=%d\n",
			result.ScannedChunks,
			result.UpdatedChunks,
		)
		fmt.Printf("Hint: %s\n", doctorOperationalHint)
		return nil
	default:
		return usageErrorf("Usage: coldkeep repair <ref-counts|chunk-live-ref-counts> [--output <text|json>]")
	}
}

type preparedRepairTarget struct {
	Target     string
	Executable bool
	Result     batch.ItemResult
}

func prepareRepairTargets(raw []batch.RawTarget) []preparedRepairTarget {
	prepared := make([]preparedRepairTarget, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))

	for _, item := range raw {
		target := strings.TrimSpace(item.Value)
		if target == "" {
			prepared = append(prepared, preparedRepairTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: item.Value,
					Status:   batch.ResultFailed,
					Message:  fmt.Sprintf("invalid repair target %q", item.Value),
				},
			})
			continue
		}

		if target != "ref-counts" && target != "chunk-live-ref-counts" {
			prepared = append(prepared, preparedRepairTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: item.Value,
					Status:   batch.ResultFailed,
					Message:  fmt.Sprintf("unknown repair target %q", item.Value),
				},
			})
			continue
		}

		if _, exists := seen[target]; exists {
			prepared = append(prepared, preparedRepairTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: target,
					Status:   batch.ResultSkipped,
					Message:  "duplicate target",
				},
			})
			continue
		}

		seen[target] = struct{}{}
		prepared = append(prepared, preparedRepairTarget{Target: target, Executable: true})
	}

	return prepared
}

func executeRepairPrepared(failFast bool, targets []preparedRepairTarget) batch.Report {
	results := make([]batch.ItemResult, 0, len(targets))

	for _, target := range targets {
		if !target.Executable {
			results = append(results, target.Result)
			continue
		}

		switch target.Target {
		case "ref-counts":
			result, err := repairLogicalRefCountsPhase()
			if err != nil {
				item := batch.ItemResult{
					RawValue: target.Target,
					Status:   batch.ResultFailed,
					Message:  fmt.Sprintf("repair ref-counts failed: %v", err),
				}
				if code, ok := invariants.Code(err); ok {
					item.InvariantCode = code
					item.RecommendedAction = invariants.RecommendedActionForCode(code)
				}
				results = append(results, item)
				if failFast {
					break
				}
				continue
			}

			results = append(results, batch.ItemResult{
				RawValue: target.Target,
				Status:   batch.ResultSuccess,
				Message: fmt.Sprintf(
					"repaired scanned_logical_files=%d updated_logical_files=%d orphan_physical_file_rows=%d",
					result.ScannedLogicalFiles,
					result.UpdatedLogicalFiles,
					result.OrphanPhysicalFileRows,
				),
			})

		case "chunk-live-ref-counts":
			result, err := repairChunkLiveRefCountsPhase()
			if err != nil {
				item := batch.ItemResult{
					RawValue: target.Target,
					Status:   batch.ResultFailed,
					Message:  fmt.Sprintf("repair chunk-live-ref-counts failed: %v", err),
				}
				if code, ok := invariants.Code(err); ok {
					item.InvariantCode = code
					item.RecommendedAction = invariants.RecommendedActionForCode(code)
				}
				results = append(results, item)
				if failFast {
					break
				}
				continue
			}

			results = append(results, batch.ItemResult{
				RawValue: target.Target,
				Status:   batch.ResultSuccess,
				Message:  fmt.Sprintf("repaired scanned_chunks=%d updated_chunks=%d", result.ScannedChunks, result.UpdatedChunks),
			})
		}
	}

	report := batch.NewReport(batch.OperationRepair, false, results)
	report.ExecutionMode = batch.ExecutionModeContinueOnError
	if failFast {
		report.ExecutionMode = batch.ExecutionModeFailFast
	}
	return report
}

func runListCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "limit", "offset", "output", "json"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "offset"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep list [--limit <count>] [--offset <count>]")
	}
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if outputMode == outputModeJSON {
		files, err := listing.ListFilesResultWithDB(dbconn, listArgs(parsed))
		if err != nil {
			return err
		}
		if files == nil {
			files = []listing.FileRecord{}
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "list",
			"files":   files,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	files, err := listing.ListFilesResultWithDB(dbconn, listArgs(parsed))
	if err != nil {
		return err
	}
	printFileRecordsTable(files)
	return nil
}

func runSearchCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := rejectBlankFlagValues(parsed, "name", "path", "extension"); err != nil {
		return err
	}
	if err := ensureAllowedFlags(parsed, "name", "min-size", "max-size", "limit", "offset", "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep search [--name <pattern>] [--min-size <bytes>] [--max-size <bytes>] [--limit <count>] [--offset <count>] [--output <text|json>]")
	}

	// Validate numeric filter values at CLI level before forwarding to SQL.
	if err := validateNonNegativeIntegerFlag(parsed, "min-size"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "max-size"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "offset"); err != nil {
		return err
	}
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if outputMode == outputModeJSON {
		files, err := listing.SearchFilesResultWithDB(dbconn, searchArgs(parsed))
		if err != nil {
			return err
		}
		if files == nil {
			files = []listing.FileRecord{}
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "search",
			"files":   files,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	files, err := listing.SearchFilesResultWithDB(dbconn, searchArgs(parsed))
	if err != nil {
		return err
	}
	printFileRecordsTable(files)
	return nil
}

func printFileRecordsTable(records []listing.FileRecord) {
	fmt.Printf("%-6s %-25s %-15s %-20s\n", "ID", "PATH", "SIZE(bytes)", "CREATED_AT")
	fmt.Println("---------------------------------------------------------------------")
	for _, r := range records {
		fmt.Printf("%-6d %-25s %-15d %-20s\n", r.ID, r.Name, r.SizeBytes, r.CreatedAt)
	}
}

// runVerifyCommand executes recovered-state verification. The verification phase
// itself is read-only; any corrective mutation happens earlier via automatic
// startup recovery before this function is called. It is not intended to be an
// online checker during active writes, where transient metadata/data divergence
// can produce false positives.
func runVerifyCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "fast", "standard", "full", "deep", "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) == 0 {
		return usageErrorf("Usage: coldkeep verify <system|file <fileID>> [--fast|--standard|--full|--deep]\nDid you mean: coldkeep verify system --fast")
	}

	if parsed.positionals[0] == "system" {
		if len(parsed.positionals) == 2 && !isVerifyLevelName(parsed.positionals[1]) {
			return usageErrorf("Usage: coldkeep verify system [--fast|--standard|--full|--deep]")
		}
		if len(parsed.positionals) > 2 {
			return usageErrorf("Usage: coldkeep verify system [--fast|--standard|--full|--deep]")
		}
	}

	verifyLevel, err := parseVerifyLevel(parsed)
	if err != nil {
		return err
	}

	target := parsed.positionals[0]
	switch target {
	case "system":
		sgctx, err := loadDefaultStorageContextPhase()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()

		verifyErr := verifyCommandPhase(sgctx.DB, target, 0, verifyLevel)
		if verifyErr != nil {
			return verifyError(verifyErr)
		}
		summary, err := verifySummaryPhase(sgctx.DB, target, 0)
		if err != nil {
			return fmt.Errorf("collect verify summary: %w", err)
		}
		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":                    "ok",
				"command":                   "verify",
				"target":                    target,
				"level":                     verifyLevelToString(verifyLevel),
				"verify":                    "ok",
				"blocks_checked":            summary.BlocksChecked,
				"physical_hash_checked":     summary.PhysicalHashChecked,
				"compressed_hash_checked":   summary.CompressedHashChecked,
				"logical_hash_checked":      summary.LogicalHashChecked,
				"compressed_blocks_checked": summary.CompressedBlocksChecked,
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}
		fmt.Println("verify ok")
		fmt.Printf("blocks_checked: %d\n", summary.BlocksChecked)
		fmt.Printf("physical_hash_checked: %d\n", summary.PhysicalHashChecked)
		fmt.Printf("compressed_hash_checked: %d\n", summary.CompressedHashChecked)
		fmt.Printf("logical_hash_checked: %d\n", summary.LogicalHashChecked)
		fmt.Printf("compressed_blocks_checked: %d\n", summary.CompressedBlocksChecked)
		if outputMode == outputModeText {
			fmt.Printf("Hint: %s\n", doctorOperationalHint)
		}
		return nil
	case "file":
		if len(parsed.positionals) < 2 || len(parsed.positionals) > 3 {
			return usageErrorf("Usage: coldkeep verify file <fileID> [--fast|--standard|--full|--deep]")
		}

		fileIDText := parsed.positionals[1]
		fileID, err := strconv.Atoi(fileIDText)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return usageErrorf("Invalid fileID: value %s exceeds platform int range", fileIDText)
			}
			return usageErrorf("Invalid fileID: %v", err)
		}
		if fileID <= 0 {
			return usageErrorf("Invalid fileID: must be a positive integer")
		}

		sgctx, err := loadDefaultStorageContextPhase()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()

		verifyErr := verifyCommandPhase(sgctx.DB, target, fileID, verifyLevel)
		if verifyErr != nil {
			return verifyError(verifyErr)
		}
		summary, err := verifySummaryPhase(sgctx.DB, target, int64(fileID))
		if err != nil {
			return fmt.Errorf("collect verify summary: %w", err)
		}
		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":                    "ok",
				"command":                   "verify",
				"target":                    target,
				"file_id":                   fileID,
				"level":                     verifyLevelToString(verifyLevel),
				"verify":                    "ok",
				"blocks_checked":            summary.BlocksChecked,
				"physical_hash_checked":     summary.PhysicalHashChecked,
				"compressed_hash_checked":   summary.CompressedHashChecked,
				"logical_hash_checked":      summary.LogicalHashChecked,
				"compressed_blocks_checked": summary.CompressedBlocksChecked,
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}
		fmt.Println("verify ok")
		fmt.Printf("blocks_checked: %d\n", summary.BlocksChecked)
		fmt.Printf("physical_hash_checked: %d\n", summary.PhysicalHashChecked)
		fmt.Printf("compressed_hash_checked: %d\n", summary.CompressedHashChecked)
		fmt.Printf("logical_hash_checked: %d\n", summary.LogicalHashChecked)
		fmt.Printf("compressed_blocks_checked: %d\n", summary.CompressedBlocksChecked)
		if outputMode == outputModeText {
			fmt.Printf("Hint: %s\n", doctorOperationalHint)
		}
		return nil
	default:
		return usageErrorf("Unknown target for verify: %s (expected 'system' or 'file <fileID>')", target)
	}
}

// runDoctorCommand implements the doctor corrective recovery command.
// Doctor is NOT read-only: it runs corrective recovery before verification, and may update
// database metadata (aborting dangling PROCESSING writes, clearing stale sealing
// markers) before any integrity check executes. Running doctor on a fresh
// deployment or after an unclean shutdown is safe and intended.
func runDoctorCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "standard", "full", "deep", "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep doctor [--standard|--full|--deep]")
	}

	verifyLevel, err := parseDoctorVerifyLevel(parsed)
	if err != nil {
		return err
	}

	report := doctorReport{
		VerifyLevel: verifyLevelToString(verifyLevel),
	}

	recoveryReport, recoveryErr := doctorRecoveryPhase(container.ContainersDir)
	report.Recovery = recoveryReport
	if recoveryErr != nil {
		report.RecoveryStatus = "error"
		return recoveryError(fmt.Errorf("doctor recovery phase failed: %w", recoveryErr))
	}
	report.RecoveryStatus = "ok"

	schemaVersion, schemaErr := doctorSchemaVersionPhase()
	if schemaErr != nil {
		report.SchemaStatus = "error"
		return fmt.Errorf("doctor schema/version check failed: %w", schemaErr)
	}
	report.SchemaVersion = schemaVersion
	report.SchemaStatus = "ok"

	verifyErr := doctorVerifyPhase(container.ContainersDir, "system", 0, verifyLevel)
	if verifyErr != nil {
		report.VerifyStatus = "error"
		return verifyError(fmt.Errorf("doctor verify phase failed: %w", verifyErr))
	}
	report.VerifyStatus = "ok"

	auditSummary, auditErr := doctorSystemAuditPhase()
	if auditErr != nil {
		return verifyError(fmt.Errorf("doctor audit summary phase failed: %w", auditErr))
	}
	report.physicalAudit = auditSummary.Physical
	report.snapshotAudit = auditSummary.Snapshot

	// Intentional JSON contract (frozen v1.0):
	// - Startup/preflight recovery diagnostics are emitted as stderr events
	//   (`event=startup_recovery`) outside this doctor command payload.
	// - Success: doctor-specific payload emitted to stdout; includes phase statuses,
	//   verify_level, schema_version, and the full recovery counter set under "recovery".
	// - Execution short-circuits by phase on error: recovery -> schema -> verify.
	//   This avoids running expensive later checks once an earlier gate already failed.
	// - Failure: generic CLI error payload on stderr via printCLIError.
	// Doctor does not emit partial doctor data on failure.
	// See doctorReport for the full field list and rationale for including recovery counters.

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "doctor",
			"data":    report,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if outputMode == outputModeText {
		fmt.Print(formatDoctorTextReport(report))
	}

	return nil
}

func formatDoctorTextReport(report doctorReport) string {
	overallStatus := "ok"
	if report.RecoveryStatus != "ok" || report.VerifyStatus != "ok" || report.SchemaStatus != "ok" {
		overallStatus = "error"
	}
	recommendedNextStep := doctorRecommendedNextStep(report, overallStatus)

	var b strings.Builder
	b.WriteString("Doctor health report\n")
	_, _ = fmt.Fprintf(&b, "  Overall status:      %s\n", overallStatus)
	_, _ = fmt.Fprintf(&b, "  Verify level:        %s\n", report.VerifyLevel)
	_, _ = fmt.Fprintf(&b, "  Phase 1 - Recovery:  %s\n", report.RecoveryStatus)
	_, _ = fmt.Fprintf(&b, "  Phase 2 - Verify:    %s\n", report.VerifyStatus)
	if report.SchemaStatus == "ok" {
		_, _ = fmt.Fprintf(&b, "  Phase 3 - Schema:    %s (version=%d)\n", report.SchemaStatus, report.SchemaVersion)
	} else {
		_, _ = fmt.Fprintf(&b, "  Phase 3 - Schema:    %s\n", report.SchemaStatus)
	}
	b.WriteString("  Note: Recovery phase may have modified metadata\n")
	_, _ = fmt.Fprintf(&b, "  Recovery summary: aborted_logical_files=%d aborted_chunks=%d quarantined_missing_containers=%d quarantined_corrupt_tail_containers=%d quarantined_orphan_containers=%d\n",
		report.Recovery.AbortedLogicalFiles,
		report.Recovery.AbortedChunks,
		report.Recovery.QuarantinedMissing,
		report.Recovery.QuarantinedCorruptTail,
		report.Recovery.QuarantinedOrphan,
	)
	_, _ = fmt.Fprintf(&b, "  Physical mapping integrity: orphan_physical_file_rows=%d logical_ref_count_mismatches=%d negative_logical_ref_count_rows=%d\n",
		report.physicalAudit.OrphanPhysicalFileRows,
		report.physicalAudit.LogicalRefCountMismatches,
		report.physicalAudit.NegativeLogicalRefCounts,
	)
	_, _ = fmt.Fprintf(&b, "  Snapshot retention integrity: snapshot_file_rows=%d snapshot_referenced_logical_files=%d snapshot_only_logical_files=%d shared_logical_files=%d orphan_snapshot_logical_refs=%d invalid_snapshot_lifecycle_states=%d retained_missing_chunk_graph=%d\n",
		report.snapshotAudit.SnapshotFileRows,
		report.snapshotAudit.SnapshotReferencedLogicalFiles,
		report.snapshotAudit.SnapshotOnlyLogicalFiles,
		report.snapshotAudit.SharedLogicalFiles,
		report.snapshotAudit.OrphanSnapshotLogicalRefs,
		report.snapshotAudit.InvalidSnapshotLifecycleStates,
		report.snapshotAudit.RetainedMissingChunkGraph,
	)
	_, _ = fmt.Fprintf(&b, "  Recommended next step: %s\n", recommendedNextStep)

	return b.String()
}

func doctorRecommendedNextStep(report doctorReport, overallStatus string) string {
	if overallStatus != "ok" {
		return "inspect stderr / doctor output"
	}

	if report.VerifyLevel == verifyLevelToString(verify.VerifyStandard) {
		return "run doctor --full"
	}

	return "none"
}

func parseDoctorVerifyLevel(parsed parsedCommandLine) (verify.VerifyLevel, error) {
	if !parsed.hasFlag("standard", "full", "deep") {
		return doctorDefaultVerifyLevel, nil
	}

	return parseVerifyLevel(parsed)
}

// BenchmarkChunkersReport is the deterministic output payload for
// `coldkeep benchmark chunkers`.
type BenchmarkChunkersReport struct {
	GeneratedAtUTC string                          `json:"generated_at_utc"`
	Rows           []BenchmarkChunkersReportRecord `json:"rows"`
}

// BenchmarkChunkersReportRecord is one dataset-level comparison row.
type BenchmarkChunkersReportRecord struct {
	Dataset       string  `json:"dataset"`
	Metric        string  `json:"metric"`
	V1SimplePct   float64 `json:"v1_simple_pct"`
	V2FastCDCPct  float64 `json:"v2_fastcdc_pct"`
	DeltaPct      float64 `json:"delta_pct"`
	WinnerVersion string  `json:"winner_version"`
}

// ---- Phase 1 performance measurement ----
// perfSpan is a named, sequential timing span within a single command invocation.
// Spans are emitted as perf_spans in --output json responses.
// Phase 1 records three coarse phases per command:
//
//	setup      – storage context / DB bootstrap
//	operation  – the primary work (store / restore / gc / snapshot-create)
//	finalize   – container sealing (store only)
//
// No changes to internal packages (db, container, chunk, storage) are made;
// all timings are taken at the command-handler boundary.
type perfSpan struct {
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
}

// perfTimer records sequential named spans. Call Mark after each sub-phase.
type perfTimer struct {
	spans []perfSpan
	last  time.Time
}

func newPerfTimer() *perfTimer {
	return &perfTimer{last: time.Now()}
}

// Mark ends the current span and starts the next.
func (p *perfTimer) Mark(name string) {
	now := time.Now()
	p.spans = append(p.spans, perfSpan{
		Name:       name,
		DurationMs: now.Sub(p.last).Milliseconds(),
	})
	p.last = now
}

// Spans returns all recorded spans in order.
func (p *perfTimer) Spans() []perfSpan { return p.spans }

// BenchmarkRunReport is the output payload for `coldkeep benchmark run`.
type BenchmarkRunReport struct {
	SchemaVersion  int                             `json:"schema_version"`
	GeneratedAtUTC string                          `json:"generated_at_utc"`
	Dataset        string                          `json:"dataset"`
	Repeat         int                             `json:"repeat"`
	Fixture        corebenchmark.FixtureDescriptor `json:"fixture"`
	Execution      BenchmarkExecution              `json:"execution"`
	ExecutionStats BenchmarkExecutionStats         `json:"execution_stats"`
	Rows           []BenchmarkRunCaseRow           `json:"rows"`
}

// BenchmarkExecution captures execution policy knobs used for this run.
type BenchmarkExecution struct {
	StoreFolderWorkers int  `json:"store_folder_workers"`
	PipelineDepth      int  `json:"pipeline_depth"`
	Deterministic      bool `json:"deterministic"`
}

type BenchmarkExecutionStats struct {
	TotalFiles            int                `json:"total_files"`
	TotalBytes            int64              `json:"total_bytes"`
	WorkersUsed           int                `json:"workers_used"`
	ContainerAppendCount  int64              `json:"container_append_count,omitempty"`
	FsyncCount            int64              `json:"fsync_count,omitempty"`
	ContainerOpenCount    int64              `json:"container_open_count,omitempty"`
	ContainerCloseCount   int64              `json:"container_close_count,omitempty"`
	SnapshotMetadataWrite int64              `json:"snapshot_metadata_write_count,omitempty"`
	IO                    BenchmarkIOMetrics `json:"io"`
}

type BenchmarkIOMetrics struct {
	ContainerOpens   int64 `json:"container_opens"`
	ContainerAppends int64 `json:"container_appends"`
	Fsyncs           int64 `json:"fsyncs"`
	BytesWritten     int64 `json:"bytes_written"`
	BytesRead        int64 `json:"bytes_read"`
}

// BenchmarkRunCaseRow is one per-case benchmark summary row.
type BenchmarkRunCaseRow struct {
	Case                 string                  `json:"case"`
	DurationMs           int64                   `json:"duration_ms"`
	ThroughputMBps       float64                 `json:"throughput_mbps"`
	Execution            BenchmarkExecution      `json:"execution"`
	ExecutionStats       BenchmarkExecutionStats `json:"execution_stats"`
	DiagnosticFinalState json.RawMessage         `json:"diagnostic_final_state,omitempty"`
}

func runBenchmarkCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "dataset", "repeat", "compare", "threshold", "workers"); err != nil {
		return err
	}
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep benchmark <chunkers|run> [options]")
	}

	subcommand := parsed.positionals[0]
	switch subcommand {
	case "chunkers":
		if len(parsed.positionals) > 1 {
			return usageErrorf("unexpected benchmark arguments: %s", strings.Join(parsed.positionals[1:], " "))
		}
		return runBenchmarkChunkersCommand(outputMode)
	case "run":
		if len(parsed.positionals) > 1 {
			return usageErrorf("unexpected benchmark arguments: %s", strings.Join(parsed.positionals[1:], " "))
		}
		return runBenchmarkRunCommand(parsed, outputMode)
	default:
		return usageErrorf("unknown benchmark subcommand %q (expected: chunkers, run)", subcommand)
	}
}

func runBenchmarkChunkersCommand(outputMode cliOutputMode) error {

	report, err := runChunkerBenchmarkPhase()
	if err != nil {
		return fmt.Errorf("benchmark chunkers: %w", err)
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "benchmark",
			"data":    report,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Println("Chunker benchmark (deterministic synthetic datasets)")
	fmt.Println()
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "DATASET\tMETRIC\tV1 SIMPLE (%)\tV2 FASTCDC (%)\tDELTA (%)\tWINNER")
	for _, row := range report.Rows {
		_, _ = fmt.Fprintf(
			tw,
			"%s\t%s\t%.2f\t%.2f\t%.2f\t%s\n",
			row.Dataset,
			row.Metric,
			row.V1SimplePct,
			row.V2FastCDCPct,
			row.DeltaPct,
			row.WinnerVersion,
		)
	}
	_ = tw.Flush()

	fmt.Println()
	fmt.Println("Typical outcomes (informational):")
	fmt.Println("  Small modifications: v1 ~92-96% reuse, v2 ~94-98% reuse")
	fmt.Println("  Shifted data:        v1 ~5-20% reuse,  v2 ~25-50% reuse")
	fmt.Println("  The shifted-data gap is the key justification signal.")
	fmt.Println("  FastCDC is designed to improve dedup stability over time; actual results depend on workload.")

	return nil
}

func runBenchmarkRunCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	rawPreset, _ := parsed.lastFlagValue("dataset")
	preset, err := corebenchmark.ParseDatasetPreset(rawPreset)
	if err != nil {
		return usageErrorf("%s", err.Error())
	}

	opts := execution.DefaultOptions()
	if rawWorkers, hasWorkers := parsed.lastFlagValue("workers"); hasWorkers {
		workers, err := strconv.Atoi(strings.TrimSpace(rawWorkers))
		if err != nil || workers <= 0 {
			return usageErrorf("invalid --workers value %q (must be integer > 0)", rawWorkers)
		}
		opts.StoreFolderWorkers = workers
	}
	opts, err = execution.FromEnv(opts)
	if err != nil {
		return fmt.Errorf("benchmark execution options: %w", err)
	}

	repeat := 1
	if rawRepeat, hasRepeat := parsed.lastFlagValue("repeat"); hasRepeat {
		repeat, err = strconv.Atoi(strings.TrimSpace(rawRepeat))
		if err != nil || repeat <= 0 {
			return usageErrorf("invalid --repeat value %q (must be integer > 0)", rawRepeat)
		}
	}
	if corebenchmark.RequiresCaseDatabaseIsolation(preset) && repeat != 1 {
		return usageErrorf("%s requires --repeat 1; independent sampling is owned by the benchmark gate scripts", preset)
	}

	report, err := runCoreBenchmarkPhase(preset, repeat, opts)
	if err != nil {
		return fmt.Errorf("benchmark run: %w", err)
	}
	report.Execution = BenchmarkExecution{
		StoreFolderWorkers: opts.StoreFolderWorkers,
		PipelineDepth:      opts.PipelineDepth,
		Deterministic:      opts.Deterministic,
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "benchmark",
			"data":    report,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
	} else {
		fmt.Printf("Benchmark run (%s preset, repeat=%d)\n", report.Dataset, report.Repeat)
		fmt.Printf(
			"Execution: workers=%d pipeline_depth=%d deterministic=%t\n",
			report.Execution.StoreFolderWorkers,
			report.Execution.PipelineDepth,
			report.Execution.Deterministic,
		)
		fmt.Println()
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "CASE\tTIME\tMB/s\tW_CFG\tW_USED\tFILES")
		for _, row := range report.Rows {
			_, _ = fmt.Fprintf(
				tw,
				"%s\t%.1fs\t%.0f\t%d\t%d\t%d\n",
				row.Case,
				float64(row.DurationMs)/1000.0,
				row.ThroughputMBps,
				row.Execution.StoreFolderWorkers,
				row.ExecutionStats.WorkersUsed,
				row.ExecutionStats.TotalFiles,
			)
		}
		_ = tw.Flush()
	}

	if baselinePath, hasCompare := parsed.lastFlagValue("compare"); hasCompare {
		threshold := 20.0
		if rawThreshold, hasThreshold := parsed.lastFlagValue("threshold"); hasThreshold {
			v, err := strconv.ParseFloat(strings.TrimSpace(rawThreshold), 64)
			if err != nil || v <= 0 {
				return usageErrorf("invalid --threshold value %q (must be a positive number representing a percentage, e.g. 20 or 100)", rawThreshold)
			}
			threshold = v
		}
		if err := compareWithBaseline(report, baselinePath, threshold); err != nil {
			return err
		}
	}

	return nil
}

// compareWithBaseline reads a baseline JSON file produced by a previous
// `benchmark run --output json` invocation and reports any cases whose
// duration or throughput has regressed beyond thresholdPct percent.
// Use thresholdPct=20 for local dev work and thresholdPct=100 for CI
// (fail only if a scenario becomes more than 2× slower).
func compareWithBaseline(current BenchmarkRunReport, baselinePath string, thresholdPct float64) error {

	raw, err := os.ReadFile(baselinePath)
	if err != nil {
		return fmt.Errorf("read baseline %q: %w", baselinePath, err)
	}

	// The baseline file is the full JSON envelope written by --output json.
	var envelope struct {
		Status  string             `json:"status"`
		Command string             `json:"command"`
		Data    BenchmarkRunReport `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("parse baseline %q: %w", baselinePath, err)
	}
	if envelope.Status != "ok" || envelope.Command != "benchmark" {
		return fmt.Errorf("parse baseline %q: expected successful benchmark envelope", baselinePath)
	}
	baseline := envelope.Data
	if err := validateLegacyBenchmarkComparisonInputs(baseline, current); err != nil {
		return fmt.Errorf("validate baseline %q: %w", baselinePath, err)
	}

	baselineByCase := make(map[string]BenchmarkRunCaseRow, len(baseline.Rows))
	for _, row := range baseline.Rows {
		baselineByCase[row.Case] = row
	}

	type regressionEntry struct {
		caseName string
		field    string
		baseline float64
		current  float64
		pct      float64
	}
	var regressions []regressionEntry

	for _, row := range current.Rows {
		base := baselineByCase[row.Case]
		if base.DurationMs > 0 {
			delta := float64(row.DurationMs-base.DurationMs) / float64(base.DurationMs) * 100.0
			if delta > thresholdPct {
				regressions = append(regressions, regressionEntry{
					caseName: row.Case,
					field:    "duration_ms",
					baseline: float64(base.DurationMs),
					current:  float64(row.DurationMs),
					pct:      delta,
				})
			}
		}
		if base.ThroughputMBps > 0 {
			delta := (base.ThroughputMBps - row.ThroughputMBps) / base.ThroughputMBps * 100.0
			if delta > thresholdPct {
				regressions = append(regressions, regressionEntry{
					caseName: row.Case,
					field:    "throughput_mbps",
					baseline: base.ThroughputMBps,
					current:  row.ThroughputMBps,
					pct:      delta,
				})
			}
		}
	}

	if len(regressions) == 0 {
		return nil
	}

	fmt.Fprintf(os.Stderr, "Benchmark regressions detected (>%.0f%% threshold):\n\n", thresholdPct)
	tw := tabwriter.NewWriter(os.Stderr, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "CASE\tFIELD\tBASELINE\tCURRENT\tDEGRADATION")
	for _, r := range regressions {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%.2f\t%.2f\t+%.1f%%\n", r.caseName, r.field, r.baseline, r.current, r.pct)
	}
	_ = tw.Flush()

	return fmt.Errorf("benchmark regression: %d case(s) exceeded the %.0f%% degradation threshold", len(regressions), thresholdPct)
}

func validateLegacyBenchmarkComparisonInputs(baseline, current BenchmarkRunReport) error {
	if len(baseline.Rows) == 0 || len(current.Rows) == 0 {
		return fmt.Errorf("benchmark reports must contain at least one case")
	}
	if baseline.Dataset != "" && current.Dataset != "" && baseline.Dataset != current.Dataset {
		return fmt.Errorf("dataset mismatch: baseline=%q current=%q", baseline.Dataset, current.Dataset)
	}
	if baseline.Repeat > 0 && current.Repeat > 0 && baseline.Repeat != current.Repeat {
		return fmt.Errorf("repeat mismatch: baseline=%d current=%d", baseline.Repeat, current.Repeat)
	}
	if baseline.Execution.StoreFolderWorkers > 0 &&
		current.Execution.StoreFolderWorkers > 0 &&
		baseline.Execution != current.Execution {
		return fmt.Errorf("execution policy mismatch")
	}

	validateRows := func(label string, rows []BenchmarkRunCaseRow) (map[string]struct{}, error) {
		seen := make(map[string]struct{}, len(rows))
		for index, row := range rows {
			if strings.TrimSpace(row.Case) == "" {
				return nil, fmt.Errorf("%s case at index %d has empty name", label, index)
			}
			if _, exists := seen[row.Case]; exists {
				return nil, fmt.Errorf("%s contains duplicate case %q", label, row.Case)
			}
			if row.DurationMs <= 0 {
				return nil, fmt.Errorf("%s case %q has non-positive duration", label, row.Case)
			}
			if math.IsNaN(row.ThroughputMBps) || math.IsInf(row.ThroughputMBps, 0) || row.ThroughputMBps <= 0 {
				return nil, fmt.Errorf("%s case %q has invalid throughput", label, row.Case)
			}
			seen[row.Case] = struct{}{}
		}
		return seen, nil
	}

	baselineCases, err := validateRows("baseline", baseline.Rows)
	if err != nil {
		return err
	}
	currentCases, err := validateRows("current", current.Rows)
	if err != nil {
		return err
	}
	if len(baselineCases) != len(currentCases) {
		return fmt.Errorf("case set mismatch")
	}
	for index, row := range baseline.Rows {
		if _, ok := currentCases[row.Case]; !ok {
			return fmt.Errorf("current report is missing case %q", row.Case)
		}
		if current.Rows[index].Case != row.Case {
			return fmt.Errorf("case order mismatch at index %d", index)
		}
	}
	return nil
}

func runCoreBenchmark(preset corebenchmark.DatasetPreset, repeat int, opts execution.Options) (BenchmarkRunReport, error) {
	var report corebenchmark.RunReport
	var err error
	if corebenchmark.RequiresCaseDatabaseIsolation(preset) {
		report, err = runGatePresetWithIsolatedDatabases(preset, repeat, opts)
	} else {
		if err := runBenchmarkDeterminismPhase(preset, opts); err != nil {
			return BenchmarkRunReport{}, err
		}
		report, err = runPresetInTemporaryDatabase(preset, repeat, opts, "report")
	}
	if err != nil {
		return BenchmarkRunReport{}, err
	}

	out := BenchmarkRunReport{
		SchemaVersion:  2,
		GeneratedAtUTC: report.GeneratedAtUTC,
		Dataset:        string(report.Dataset),
		Repeat:         report.Repeat,
		Fixture:        report.Fixture,
		Execution: BenchmarkExecution{
			StoreFolderWorkers: opts.StoreFolderWorkers,
			PipelineDepth:      opts.PipelineDepth,
			Deterministic:      opts.Deterministic,
		},
		Rows: make([]BenchmarkRunCaseRow, 0),
	}

	type runAgg struct {
		durationMs           int64
		bytes                int64
		files                int
		execution            BenchmarkExecution
		stats                BenchmarkExecutionStats
		diagnosticFinalState json.RawMessage
	}
	caseAgg := make(map[string]runAgg)
	caseOrder := make([]string, 0)
	for _, iteration := range report.Iterations {
		for _, result := range iteration.Results {
			agg := caseAgg[result.Name]
			if _, seen := caseAgg[result.Name]; !seen {
				caseOrder = append(caseOrder, result.Name)
				agg.execution = BenchmarkExecution{
					StoreFolderWorkers: result.Execution.StoreFolderWorkers,
					PipelineDepth:      result.Execution.PipelineDepth,
					Deterministic:      result.Execution.Deterministic,
				}
				agg.stats.WorkersUsed = result.ExecStats.WorkersUsed
				agg.diagnosticFinalState = append(json.RawMessage(nil), result.DiagnosticFinalState...)
			} else if !bytes.Equal(agg.diagnosticFinalState, result.DiagnosticFinalState) {
				return BenchmarkRunReport{}, fmt.Errorf("diagnostic final state changed across repeats for case %q", result.Name)
			}
			agg.durationMs += result.Metrics.Duration.Milliseconds()
			agg.bytes += result.Metrics.BytesProcessed
			agg.files += result.Metrics.FilesProcessed
			agg.stats.TotalBytes += result.ExecStats.TotalBytesProcessed
			agg.stats.TotalFiles += result.ExecStats.TotalFilesProcessed
			agg.stats.ContainerAppendCount += result.ExecStats.ContainerAppendCount
			agg.stats.FsyncCount += result.ExecStats.FsyncCount
			agg.stats.ContainerOpenCount += result.ExecStats.ContainerOpenCount
			agg.stats.ContainerCloseCount += result.ExecStats.ContainerCloseCount
			agg.stats.IO.ContainerAppends += result.ExecStats.ContainerAppendCount
			agg.stats.IO.Fsyncs += result.ExecStats.FsyncCount
			agg.stats.IO.ContainerOpens += result.ExecStats.ContainerOpenCount
			agg.stats.IO.BytesWritten += result.ExecStats.BytesWritten
			agg.stats.IO.BytesRead += result.ExecStats.BytesRead
			agg.stats.SnapshotMetadataWrite += result.ExecStats.SnapshotMetadataWrites
			caseAgg[result.Name] = agg
		}
	}

	for _, caseName := range caseOrder {
		agg := caseAgg[caseName]
		throughput := 0.0
		if agg.durationMs > 0 && agg.bytes > 0 {
			seconds := float64(agg.durationMs) / 1000.0
			throughput = (float64(agg.bytes) / (1024.0 * 1024.0)) / seconds
		}
		out.Rows = append(out.Rows, BenchmarkRunCaseRow{
			Case:                 caseName,
			DurationMs:           agg.durationMs,
			ThroughputMBps:       throughput,
			Execution:            agg.execution,
			ExecutionStats:       agg.stats,
			DiagnosticFinalState: agg.diagnosticFinalState,
		})
		out.ExecutionStats.TotalFiles += agg.stats.TotalFiles
		out.ExecutionStats.TotalBytes += agg.stats.TotalBytes
		if agg.stats.WorkersUsed > out.ExecutionStats.WorkersUsed {
			out.ExecutionStats.WorkersUsed = agg.stats.WorkersUsed
		}
		out.ExecutionStats.ContainerAppendCount += agg.stats.ContainerAppendCount
		out.ExecutionStats.FsyncCount += agg.stats.FsyncCount
		out.ExecutionStats.ContainerOpenCount += agg.stats.ContainerOpenCount
		out.ExecutionStats.ContainerCloseCount += agg.stats.ContainerCloseCount
		out.ExecutionStats.SnapshotMetadataWrite += agg.stats.SnapshotMetadataWrite
		out.ExecutionStats.IO.ContainerOpens += agg.stats.IO.ContainerOpens
		out.ExecutionStats.IO.ContainerAppends += agg.stats.IO.ContainerAppends
		out.ExecutionStats.IO.Fsyncs += agg.stats.IO.Fsyncs
		out.ExecutionStats.IO.BytesWritten += agg.stats.IO.BytesWritten
		out.ExecutionStats.IO.BytesRead += agg.stats.IO.BytesRead
	}

	return out, nil
}

func runGatePresetWithIsolatedDatabases(
	preset corebenchmark.DatasetPreset,
	repeat int,
	opts execution.Options,
) (corebenchmark.RunReport, error) {
	if repeat <= 0 {
		return corebenchmark.RunReport{}, fmt.Errorf("repeat must be > 0")
	}
	cfg, err := corebenchmark.PresetScenarioConfig(preset)
	if err != nil {
		return corebenchmark.RunReport{}, err
	}
	cfg.ColdkeepExecutable = resolveSelfExecutable()
	cfg.Codec = strings.TrimSpace(os.Getenv("COLDKEEP_CODEC"))
	cfg.Compression = strings.TrimSpace(os.Getenv("COLDKEEP_COMPRESSION"))
	cfg.Execution = opts
	cfg.CaseEnvironmentFactory = func(caseName string) (map[string]string, func() error, error) {
		dbName, cleanup, err := createTemporaryBenchmarkDatabase("gate-" + caseName)
		if err != nil {
			return nil, nil, err
		}
		return map[string]string{
			"DB_NAME":                       dbName,
			"COLDKEEP_DB_AUTO_BOOTSTRAP":    "true",
			"COLDKEEP_STORE_FOLDER_WORKERS": strconv.Itoa(opts.StoreFolderWorkers),
		}, cleanup, nil
	}
	report := corebenchmark.RunReport{
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Dataset:        preset,
		Repeat:         repeat,
		Fixture:        corebenchmark.FixtureDescriptorFor(preset, cfg),
		Iterations:     make([]corebenchmark.IterationReport, 0, repeat),
	}
	for iteration := 1; iteration <= repeat; iteration++ {
		iterCfg := cfg
		iterCfg.RunTag = fmt.Sprintf("iter-%02d", iteration)
		results, err := corebenchmark.RunBenchmarkWithEnvironmentFactoryAndObserver(
			corebenchmark.CoreScenarios(iterCfg),
			iterCfg.CaseEnvironmentFactory,
			captureBenchmarkDiagnosticFinalState,
		)
		report.Iterations = append(report.Iterations, corebenchmark.IterationReport{
			Iteration: iteration,
			Results:   results,
		})
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

const benchmarkDiagnosticFinalStateSchemaVersion = 2

type benchmarkDiagnosticDigest struct {
	Count      int64  `json:"count"`
	TotalBytes int64  `json:"total_bytes"`
	SHA256     string `json:"sha256"`
}

type benchmarkDiagnosticStatusTotals struct {
	Completed  int64 `json:"completed"`
	Processing int64 `json:"processing"`
	Aborted    int64 `json:"aborted"`
}

type benchmarkDiagnosticGC struct {
	TotalChunks                int64 `json:"total_chunks"`
	ReachableChunks            int64 `json:"reachable_chunks"`
	UnreachableChunks          int64 `json:"unreachable_chunks"`
	LogicallyReclaimableBytes  int64 `json:"logically_reclaimable_bytes"`
	PhysicallyReclaimableBytes int64 `json:"physically_reclaimable_bytes"`
	PackedBlocksLive           int64 `json:"packed_blocks_live"`
	PackedBlocksDead           int64 `json:"packed_blocks_dead"`
	PackedBytesLive            int64 `json:"packed_bytes_live"`
	PackedBytesReclaimable     int64 `json:"packed_bytes_reclaimable"`
	RetainedDeadBytes          int64 `json:"retained_dead_bytes"`
}

type benchmarkDiagnosticVerification struct {
	BlocksChecked              int64 `json:"blocks_checked"`
	PhysicalHashesChecked      int64 `json:"physical_hashes_checked"`
	CompressedHashesChecked    int64 `json:"compressed_hashes_checked"`
	LogicalHashesChecked       int64 `json:"logical_hashes_checked"`
	CompressedBlocksChecked    int64 `json:"compressed_blocks_checked"`
	PhysicalFileIssues         int64 `json:"physical_file_issues"`
	SnapshotMembershipRows     int64 `json:"snapshot_membership_rows"`
	SnapshotReachabilityIssues int64 `json:"snapshot_reachability_issues"`
}

type benchmarkDiagnosticPhysical struct {
	ContainerCount      int64  `json:"container_count"`
	StorageBlockCount   int64  `json:"storage_block_count"`
	LegacyBlockCount    int64  `json:"legacy_block_count"`
	ChunkReferenceCount int64  `json:"chunk_reference_count"`
	PayloadBytes        int64  `json:"payload_bytes"`
	ContainerBytes      int64  `json:"container_bytes"`
	CanonicalSHA256     string `json:"canonical_sha256"`
}

type benchmarkDiagnosticFinalState struct {
	SchemaVersion          int                             `json:"schema_version"`
	ActiveLogicalNamespace benchmarkDiagnosticDigest       `json:"active_logical_namespace"`
	LogicalCatalog         benchmarkDiagnosticDigest       `json:"logical_catalog"`
	LogicalStatuses        benchmarkDiagnosticStatusTotals `json:"logical_statuses"`
	ChunkGraph             benchmarkDiagnosticDigest       `json:"chunk_graph"`
	RestoredTree           benchmarkDiagnosticDigest       `json:"restored_tree"`
	Snapshots              benchmarkDiagnosticDigest       `json:"snapshots"`
	SnapshotCount          int64                           `json:"snapshot_count"`
	GC                     benchmarkDiagnosticGC           `json:"gc"`
	Verification           benchmarkDiagnosticVerification `json:"verification"`
	Physical               benchmarkDiagnosticPhysical     `json:"physical"`
	PhysicalLayoutSHA256   string                          `json:"physical_layout_sha256"`
}

type benchmarkActiveLogicalRawRow struct {
	ID             int64
	Path           string
	FileHash       string
	TotalSize      int64
	Status         string
	ChunkerVersion string
}

type benchmarkActiveLogicalCanonicalRow struct {
	Path           string `json:"path"`
	FileHash       string `json:"file_hash"`
	TotalSize      int64  `json:"total_size"`
	Status         string `json:"status"`
	ChunkerVersion string `json:"chunker_version"`
}

type benchmarkLogicalCatalogRawRow struct {
	ID                     int64
	FileHash               string
	TotalSize              int64
	Status                 string
	RefCount               int64
	ChunkerVersion         string
	ActivePathCount        int64
	SnapshotReferenceCount int64
}

type benchmarkLogicalCatalogCanonicalRow struct {
	FileHash               string `json:"file_hash"`
	TotalSize              int64  `json:"total_size"`
	Status                 string `json:"status"`
	RefCount               int64  `json:"ref_count"`
	ChunkerVersion         string `json:"chunker_version"`
	ActivePathCount        int64  `json:"active_path_count"`
	SnapshotReferenceCount int64  `json:"snapshot_reference_count"`
	ReachabilityClass      string `json:"reachability_class"`
}

type benchmarkChunkGraphRawRow struct {
	LogicalID      int64
	ChunkID        int64
	FileHash       string
	FileSize       int64
	ChunkOrder     int64
	ChunkHash      string
	ChunkSize      int64
	Status         string
	LiveRefCount   int64
	PinCount       int64
	ChunkerVersion string
}

type benchmarkChunkGraphCanonicalRow struct {
	FileHash       string `json:"file_hash"`
	FileSize       int64  `json:"file_size"`
	ChunkOrder     int64  `json:"chunk_order"`
	ChunkHash      string `json:"chunk_hash"`
	ChunkSize      int64  `json:"chunk_size"`
	Status         string `json:"status"`
	LiveRefCount   int64  `json:"live_ref_count"`
	PinCount       int64  `json:"pin_count"`
	ChunkerVersion string `json:"chunker_version"`
}

type benchmarkSnapshotRawRow struct {
	SnapshotID string
	Type       string
	Label      string
	ParentID   string
	Path       string
	FileHash   string
	Size       int64
}

type benchmarkSnapshotCanonicalRow struct {
	Type      string `json:"type"`
	Label     string `json:"label"`
	HasParent bool   `json:"has_parent"`
	Path      string `json:"path"`
	FileHash  string `json:"file_hash"`
	Size      int64  `json:"size"`
}

type benchmarkPhysicalRawRow struct {
	BlockID          int64
	ContainerID      int64
	FormatVersion    int
	Codec            string
	PlaintextSize    int64
	CompressionCodec string
	CompressionLevel sql.NullInt64
	CompressedSize   sql.NullInt64
	StoredSize       int64
	BlockHash        string
	CompressedHash   string
	PhysicalHash     string
	ContainerOffset  int64
	ChunkHash        string
	OffsetInBlock    sql.NullInt64
	SizeInBlock      sql.NullInt64
}

type benchmarkPhysicalCanonicalRow struct {
	FormatVersion         int    `json:"format_version"`
	Codec                 string `json:"codec"`
	CompressionCodec      string `json:"compression_codec"`
	CompressionLevel      *int64 `json:"compression_level"`
	ChunkHash             string `json:"chunk_hash"`
	ChunkSize             *int64 `json:"chunk_size"`
	UnreferencedBlockHash string `json:"unreferenced_block_hash,omitempty"`
}

type benchmarkPhysicalLayoutRow struct {
	Canonical       benchmarkPhysicalCanonicalRow `json:"canonical"`
	PlaintextSize   int64                         `json:"plaintext_size"`
	CompressedSize  *int64                        `json:"compressed_size"`
	StoredSize      int64                         `json:"stored_size"`
	BlockHash       string                        `json:"block_hash"`
	CompressedHash  string                        `json:"compressed_hash"`
	PhysicalHash    string                        `json:"physical_hash"`
	ContainerOffset int64                         `json:"container_offset"`
	OffsetInBlock   *int64                        `json:"offset_in_block"`
}

type benchmarkContainerRawRow struct {
	ID            int64
	Sealed        bool
	Sealing       bool
	Quarantine    bool
	CurrentSize   int64
	MaxSize       int64
	ContainerHash string
}

type benchmarkContainerLayout struct {
	Sealed        bool                         `json:"sealed"`
	Sealing       bool                         `json:"sealing"`
	Quarantine    bool                         `json:"quarantine"`
	CurrentSize   int64                        `json:"current_size"`
	MaxSize       int64                        `json:"max_size"`
	ContainerHash string                       `json:"container_hash"`
	Rows          []benchmarkPhysicalLayoutRow `json:"rows"`
}

func captureBenchmarkDiagnosticFinalState(_ string, benchmarkContext corebenchmark.BenchmarkContext) (json.RawMessage, error) {
	dbName := strings.TrimSpace(benchmarkContext.ExtraEnv["DB_NAME"])
	if dbName == "" {
		return nil, fmt.Errorf("diagnostic observer requires an isolated database")
	}
	connStr, err := db.BuildPostgresConnStringFromEnv(dbName)
	if err != nil {
		return nil, fmt.Errorf("build diagnostic database connection: %w", err)
	}
	dbconn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("open diagnostic database: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := dbconn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping diagnostic database: %w", err)
	}

	state, err := buildBenchmarkDiagnosticFinalState(ctx, dbconn, benchmarkContext)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode diagnostic final state: %w", err)
	}
	return encoded, nil
}

func benchmarkCanonicalDigest(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalBenchmarkPath(root, rawPath string) (string, error) {
	if strings.TrimSpace(rawPath) == "" {
		return "", nil
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve benchmark data root: %w", err)
	}
	absPath, err := filepath.Abs(rawPath)
	if err != nil {
		return "", fmt.Errorf("resolve benchmark path: %w", err)
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return "", fmt.Errorf("relativize benchmark path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("benchmark path is outside the isolated data root")
	}
	return filepath.ToSlash(rel), nil
}

func canonicalBenchmarkSnapshotPath(dataRoot, rawPath string) (string, error) {
	if strings.TrimSpace(rawPath) == "" {
		return "", nil
	}
	if filepath.IsAbs(rawPath) {
		return canonicalBenchmarkPath(dataRoot, rawPath)
	}
	normalizedRaw := filepath.ToSlash(filepath.Clean(filepath.FromSlash(rawPath)))
	absDataRoot, err := filepath.Abs(dataRoot)
	if err != nil {
		return "", fmt.Errorf("resolve benchmark data root: %w", err)
	}
	normalizedRoot := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(absDataRoot)), "/")
	if normalizedRaw == normalizedRoot {
		return ".", nil
	}
	if strings.HasPrefix(normalizedRaw, normalizedRoot+"/") {
		return strings.TrimPrefix(normalizedRaw, normalizedRoot+"/"), nil
	}
	cleaned := filepath.Clean(filepath.FromSlash(normalizedRaw))
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) || filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("snapshot path escapes the isolated data root")
	}
	return filepath.ToSlash(cleaned), nil
}

func canonicalizeBenchmarkActiveLogicalRows(rows []benchmarkActiveLogicalRawRow, dataRoot string) ([]benchmarkActiveLogicalCanonicalRow, error) {
	out := make([]benchmarkActiveLogicalCanonicalRow, 0, len(rows))
	seenPaths := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		pathValue, err := canonicalBenchmarkPath(dataRoot, row.Path)
		if err != nil {
			return nil, err
		}
		if pathValue == "" {
			return nil, fmt.Errorf("active logical path is empty")
		}
		if _, exists := seenPaths[pathValue]; exists {
			return nil, fmt.Errorf("duplicate canonical active logical path")
		}
		seenPaths[pathValue] = struct{}{}
		out = append(out, benchmarkActiveLogicalCanonicalRow{
			Path: pathValue, FileHash: row.FileHash, TotalSize: row.TotalSize,
			Status: row.Status, ChunkerVersion: row.ChunkerVersion,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := json.Marshal(out[i])
		right, _ := json.Marshal(out[j])
		return bytes.Compare(left, right) < 0
	})
	return out, nil
}

func benchmarkLogicalReachabilityClass(activePathCount, snapshotReferenceCount int64) string {
	switch {
	case activePathCount > 0 && snapshotReferenceCount > 0:
		return "shared"
	case activePathCount > 0:
		return "current_only"
	case snapshotReferenceCount > 0:
		return "snapshot_only"
	default:
		return "unreachable_history"
	}
}

func canonicalizeBenchmarkLogicalCatalogRows(rows []benchmarkLogicalCatalogRawRow) []benchmarkLogicalCatalogCanonicalRow {
	out := make([]benchmarkLogicalCatalogCanonicalRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, benchmarkLogicalCatalogCanonicalRow{
			FileHash: row.FileHash, TotalSize: row.TotalSize, Status: row.Status,
			RefCount: row.RefCount, ChunkerVersion: row.ChunkerVersion,
			ActivePathCount: row.ActivePathCount, SnapshotReferenceCount: row.SnapshotReferenceCount,
			ReachabilityClass: benchmarkLogicalReachabilityClass(row.ActivePathCount, row.SnapshotReferenceCount),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := json.Marshal(out[i])
		right, _ := json.Marshal(out[j])
		return bytes.Compare(left, right) < 0
	})
	return out
}

func canonicalizeBenchmarkChunkGraphRows(rows []benchmarkChunkGraphRawRow) []benchmarkChunkGraphCanonicalRow {
	out := make([]benchmarkChunkGraphCanonicalRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, benchmarkChunkGraphCanonicalRow{
			FileHash: row.FileHash, FileSize: row.FileSize, ChunkOrder: row.ChunkOrder,
			ChunkHash: row.ChunkHash, ChunkSize: row.ChunkSize, Status: row.Status,
			LiveRefCount: row.LiveRefCount, PinCount: row.PinCount, ChunkerVersion: row.ChunkerVersion,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := json.Marshal(out[i])
		right, _ := json.Marshal(out[j])
		return bytes.Compare(left, right) < 0
	})
	return out
}

func nullableInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	v := value.Int64
	return &v
}

func canonicalizeBenchmarkPhysicalRows(rows []benchmarkPhysicalRawRow) []benchmarkPhysicalCanonicalRow {
	out := make([]benchmarkPhysicalCanonicalRow, 0, len(rows))
	for _, row := range rows {
		unreferencedBlockHash := ""
		if row.ChunkHash == "" {
			unreferencedBlockHash = row.BlockHash
		}
		out = append(out, benchmarkPhysicalCanonicalRow{
			FormatVersion: row.FormatVersion, Codec: row.Codec,
			CompressionCodec: row.CompressionCodec, CompressionLevel: nullableInt64Pointer(row.CompressionLevel),
			ChunkHash: row.ChunkHash, ChunkSize: nullableInt64Pointer(row.SizeInBlock),
			UnreferencedBlockHash: unreferencedBlockHash,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := json.Marshal(out[i])
		right, _ := json.Marshal(out[j])
		return bytes.Compare(left, right) < 0
	})
	return out
}

func buildBenchmarkDiagnosticFinalState(
	ctx context.Context,
	dbconn *sql.DB,
	benchmarkContext corebenchmark.BenchmarkContext,
) (benchmarkDiagnosticFinalState, error) {
	activeRaw, activeBytes, err := readBenchmarkActiveLogicalNamespace(ctx, dbconn)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, err
	}
	activeRows, err := canonicalizeBenchmarkActiveLogicalRows(activeRaw, benchmarkContext.DataPath)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("canonicalize active logical namespace: %w", err)
	}
	activeDigest, err := benchmarkCanonicalDigest(activeRows)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("digest active logical namespace: %w", err)
	}

	catalogRaw, catalogBytes, statuses, err := readBenchmarkLogicalCatalog(ctx, dbconn)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, err
	}
	catalogRows := canonicalizeBenchmarkLogicalCatalogRows(catalogRaw)
	catalogDigest, err := benchmarkCanonicalDigest(catalogRows)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("digest logical catalog: %w", err)
	}

	chunkRaw, chunkBytes, err := readBenchmarkChunkGraph(ctx, dbconn)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, err
	}
	chunkRows := canonicalizeBenchmarkChunkGraphRows(chunkRaw)
	chunkDigest, err := benchmarkCanonicalDigest(chunkRows)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("digest chunk graph: %w", err)
	}

	restoredRows, restoredBytes, err := readBenchmarkRestoredTree(filepath.Join(benchmarkContext.RepoPath, "restore-output"))
	if err != nil {
		return benchmarkDiagnosticFinalState{}, err
	}
	restoredDigest, err := benchmarkCanonicalDigest(restoredRows)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("digest restored tree: %w", err)
	}

	snapshotRows, snapshotCount, snapshotBytes, err := readBenchmarkSnapshotState(ctx, dbconn, benchmarkContext.DataPath)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, err
	}
	snapshotDigest, err := benchmarkCanonicalDigest(snapshotRows)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("digest snapshot membership: %w", err)
	}

	gcPlan, err := internalgc.BuildPlan(ctx, dbconn, internalgc.PlanOptions{})
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("capture GC reachability totals: %w", err)
	}
	verifyTotals, err := countVerifySummaryForSystem(dbconn)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("capture verification totals: %w", err)
	}
	physicalAudit, err := verify.CheckPhysicalFileGraphIntegrity(dbconn)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("capture physical-file verification totals: %w", err)
	}
	snapshotAudit, err := verify.CheckSnapshotReachabilityIntegrity(dbconn)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, fmt.Errorf("capture snapshot verification totals: %w", err)
	}

	physical, layoutDigest, err := readBenchmarkPhysicalState(ctx, dbconn)
	if err != nil {
		return benchmarkDiagnosticFinalState{}, err
	}

	return benchmarkDiagnosticFinalState{
		SchemaVersion:          benchmarkDiagnosticFinalStateSchemaVersion,
		ActiveLogicalNamespace: benchmarkDiagnosticDigest{Count: int64(len(activeRows)), TotalBytes: activeBytes, SHA256: activeDigest},
		LogicalCatalog:         benchmarkDiagnosticDigest{Count: int64(len(catalogRows)), TotalBytes: catalogBytes, SHA256: catalogDigest},
		LogicalStatuses:        statuses,
		ChunkGraph:             benchmarkDiagnosticDigest{Count: int64(len(chunkRows)), TotalBytes: chunkBytes, SHA256: chunkDigest},
		RestoredTree:           benchmarkDiagnosticDigest{Count: int64(len(restoredRows)), TotalBytes: restoredBytes, SHA256: restoredDigest},
		Snapshots:              benchmarkDiagnosticDigest{Count: int64(len(snapshotRows)), TotalBytes: snapshotBytes, SHA256: snapshotDigest},
		SnapshotCount:          snapshotCount,
		GC: benchmarkDiagnosticGC{
			TotalChunks: gcPlan.TotalChunks, ReachableChunks: gcPlan.ReachableChunks,
			UnreachableChunks: gcPlan.UnreachableChunks, LogicallyReclaimableBytes: gcPlan.ReclaimableBytes,
			PhysicallyReclaimableBytes: gcPlan.PhysicallyReclaimableBytes,
			PackedBlocksLive:           gcPlan.Summary.PackedBlocksLive, PackedBlocksDead: gcPlan.Summary.PackedBlocksDead,
			PackedBytesLive: gcPlan.Summary.PackedBytesLive, PackedBytesReclaimable: gcPlan.Summary.PackedBytesReclaimable,
			RetainedDeadBytes: gcPlan.Summary.RetainedDeadBytesDueToPackedBlocks,
		},
		Verification: benchmarkDiagnosticVerification{
			BlocksChecked: verifyTotals.BlocksChecked, PhysicalHashesChecked: verifyTotals.PhysicalHashChecked,
			CompressedHashesChecked: verifyTotals.CompressedHashChecked, LogicalHashesChecked: verifyTotals.LogicalHashChecked,
			CompressedBlocksChecked: verifyTotals.CompressedBlocksChecked,
			PhysicalFileIssues:      physicalAudit.OrphanPhysicalFileRows + physicalAudit.LogicalRefCountMismatches + physicalAudit.NegativeLogicalRefCounts,
			SnapshotMembershipRows:  snapshotAudit.SnapshotFileRows,
			SnapshotReachabilityIssues: snapshotAudit.OrphanSnapshotPathRefs + snapshotAudit.DuplicateSnapshotPathPairs +
				snapshotAudit.OrphanSnapshotLogicalRefs + snapshotAudit.InvalidSnapshotLifecycleStates + snapshotAudit.RetainedMissingChunkGraph,
		},
		Physical:             physical,
		PhysicalLayoutSHA256: layoutDigest,
	}, nil
}

func readBenchmarkActiveLogicalNamespace(
	ctx context.Context,
	dbconn *sql.DB,
) ([]benchmarkActiveLogicalRawRow, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT lf.id, pf.path, lf.file_hash, lf.total_size, lf.status, lf.chunker_version
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
	`)
	if err != nil {
		return nil, 0, fmt.Errorf("query diagnostic active logical namespace: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []benchmarkActiveLogicalRawRow
	var totalBytes int64
	for rows.Next() {
		var row benchmarkActiveLogicalRawRow
		if err := rows.Scan(&row.ID, &row.Path, &row.FileHash, &row.TotalSize, &row.Status, &row.ChunkerVersion); err != nil {
			return nil, 0, fmt.Errorf("scan diagnostic active logical namespace: %w", err)
		}
		out = append(out, row)
		totalBytes += row.TotalSize
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate diagnostic active logical namespace: %w", err)
	}
	return out, totalBytes, nil
}

func readBenchmarkLogicalCatalog(
	ctx context.Context,
	dbconn *sql.DB,
) ([]benchmarkLogicalCatalogRawRow, int64, benchmarkDiagnosticStatusTotals, error) {
	rows, err := dbconn.QueryContext(ctx, `
		WITH active_paths AS (
			SELECT logical_file_id, COUNT(*) AS active_path_count
			FROM physical_file
			GROUP BY logical_file_id
		), snapshot_references AS (
			SELECT logical_file_id, COUNT(*) AS snapshot_reference_count
			FROM snapshot_file
			GROUP BY logical_file_id
		)
		SELECT lf.id, lf.file_hash, lf.total_size, lf.status, lf.ref_count, lf.chunker_version,
		       COALESCE(ap.active_path_count, 0), COALESCE(sr.snapshot_reference_count, 0)
		FROM logical_file lf
		LEFT JOIN active_paths ap ON ap.logical_file_id = lf.id
		LEFT JOIN snapshot_references sr ON sr.logical_file_id = lf.id
	`)
	if err != nil {
		return nil, 0, benchmarkDiagnosticStatusTotals{}, fmt.Errorf("query diagnostic logical catalog: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []benchmarkLogicalCatalogRawRow
	var totalBytes int64
	var statuses benchmarkDiagnosticStatusTotals
	for rows.Next() {
		var row benchmarkLogicalCatalogRawRow
		if err := rows.Scan(
			&row.ID, &row.FileHash, &row.TotalSize, &row.Status, &row.RefCount, &row.ChunkerVersion,
			&row.ActivePathCount, &row.SnapshotReferenceCount,
		); err != nil {
			return nil, 0, benchmarkDiagnosticStatusTotals{}, fmt.Errorf("scan diagnostic logical catalog: %w", err)
		}
		out = append(out, row)
		totalBytes += row.TotalSize
		switch row.Status {
		case "COMPLETED":
			statuses.Completed++
		case "PROCESSING":
			statuses.Processing++
		case "ABORTED":
			statuses.Aborted++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, benchmarkDiagnosticStatusTotals{}, fmt.Errorf("iterate diagnostic logical catalog: %w", err)
	}
	return out, totalBytes, statuses, nil
}

func readBenchmarkChunkGraph(ctx context.Context, dbconn *sql.DB) ([]benchmarkChunkGraphRawRow, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT lf.id, c.id, lf.file_hash, lf.total_size, fc.chunk_order,
		       c.chunk_hash, c.size, c.status, c.live_ref_count, c.pin_count, c.chunker_version
		FROM file_chunk fc
		JOIN logical_file lf ON lf.id = fc.logical_file_id
		JOIN chunk c ON c.id = fc.chunk_id
	`)
	if err != nil {
		return nil, 0, fmt.Errorf("query diagnostic chunk graph: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []benchmarkChunkGraphRawRow
	var totalBytes int64
	for rows.Next() {
		var row benchmarkChunkGraphRawRow
		if err := rows.Scan(&row.LogicalID, &row.ChunkID, &row.FileHash, &row.FileSize, &row.ChunkOrder,
			&row.ChunkHash, &row.ChunkSize, &row.Status, &row.LiveRefCount, &row.PinCount, &row.ChunkerVersion); err != nil {
			return nil, 0, fmt.Errorf("scan diagnostic chunk graph: %w", err)
		}
		out = append(out, row)
		totalBytes += row.ChunkSize
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate diagnostic chunk graph: %w", err)
	}
	return out, totalBytes, nil
}

type benchmarkRestoredCanonicalRow struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

func readBenchmarkRestoredTree(root string) ([]benchmarkRestoredCanonicalRow, int64, error) {
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return make([]benchmarkRestoredCanonicalRow, 0), 0, nil
		}
		return nil, 0, fmt.Errorf("inspect restored tree: %w", err)
	}
	hashes, err := corebenchmark.HashRestoredTree(root)
	if err != nil {
		return nil, 0, fmt.Errorf("capture restored tree: %w", err)
	}
	paths := make([]string, 0, len(hashes))
	for relativePath := range hashes {
		paths = append(paths, relativePath)
	}
	sort.Strings(paths)
	out := make([]benchmarkRestoredCanonicalRow, 0, len(paths))
	var totalBytes int64
	for _, relativePath := range paths {
		info, err := os.Stat(filepath.Join(root, filepath.FromSlash(relativePath)))
		if err != nil {
			return nil, 0, fmt.Errorf("stat restored tree file: %w", err)
		}
		out = append(out, benchmarkRestoredCanonicalRow{Path: relativePath, SHA256: hashes[relativePath], Size: info.Size()})
		totalBytes += info.Size()
	}
	return out, totalBytes, nil
}

func readBenchmarkSnapshotState(
	ctx context.Context,
	dbconn *sql.DB,
	dataRoot string,
) ([]benchmarkSnapshotCanonicalRow, int64, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT s.id, s.type, COALESCE(s.label, ''), COALESCE(s.parent_id, ''),
		       COALESCE(sp.path, ''), COALESCE(lf.file_hash, ''), COALESCE(sf.size, 0)
		FROM snapshot s
		LEFT JOIN snapshot_file sf ON sf.snapshot_id = s.id
		LEFT JOIN snapshot_path sp ON sp.id = sf.path_id
		LEFT JOIN logical_file lf ON lf.id = sf.logical_file_id
	`)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("query diagnostic snapshot membership: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var rawRows []benchmarkSnapshotRawRow
	snapshotIDs := make(map[string]struct{})
	for rows.Next() {
		var row benchmarkSnapshotRawRow
		if err := rows.Scan(&row.SnapshotID, &row.Type, &row.Label, &row.ParentID, &row.Path, &row.FileHash, &row.Size); err != nil {
			return nil, 0, 0, fmt.Errorf("scan diagnostic snapshot membership: %w", err)
		}
		rawRows = append(rawRows, row)
		snapshotIDs[row.SnapshotID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, fmt.Errorf("iterate diagnostic snapshot membership: %w", err)
	}
	out := make([]benchmarkSnapshotCanonicalRow, 0, len(rawRows))
	var totalBytes int64
	for _, row := range rawRows {
		pathValue, err := canonicalBenchmarkSnapshotPath(dataRoot, row.Path)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("canonicalize snapshot path: %w", err)
		}
		out = append(out, benchmarkSnapshotCanonicalRow{
			Type: row.Type, Label: row.Label, HasParent: row.ParentID != "",
			Path: pathValue, FileHash: row.FileHash, Size: row.Size,
		})
		totalBytes += row.Size
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := json.Marshal(out[i])
		right, _ := json.Marshal(out[j])
		return bytes.Compare(left, right) < 0
	})
	return out, int64(len(snapshotIDs)), totalBytes, nil
}

func readBenchmarkPhysicalState(
	ctx context.Context,
	dbconn *sql.DB,
) (benchmarkDiagnosticPhysical, string, error) {
	storageRows, err := dbconn.QueryContext(ctx, `
		SELECT sb.id, sb.container_id, sb.format_version, sb.codec, sb.plaintext_size,
		       sb.compression_codec, sb.compression_level, sb.compressed_size, sb.stored_size,
		       encode(sb.block_hash, 'hex'), COALESCE(encode(sb.compressed_hash, 'hex'), ''),
		       COALESCE(encode(sb.physical_hash, 'hex'), ''), sb.container_offset,
		       COALESCE(ch.chunk_hash, ''), cbr.offset_in_block, cbr.size_in_block
		FROM storage_blocks sb
		LEFT JOIN chunk_block_refs cbr ON cbr.block_id = sb.id
		LEFT JOIN chunk ch ON ch.id = cbr.chunk_id
	`)
	if err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("query diagnostic storage blocks: %w", err)
	}
	var rawRows []benchmarkPhysicalRawRow
	for storageRows.Next() {
		var row benchmarkPhysicalRawRow
		if err := storageRows.Scan(
			&row.BlockID, &row.ContainerID, &row.FormatVersion, &row.Codec, &row.PlaintextSize,
			&row.CompressionCodec, &row.CompressionLevel, &row.CompressedSize, &row.StoredSize,
			&row.BlockHash, &row.CompressedHash, &row.PhysicalHash, &row.ContainerOffset,
			&row.ChunkHash, &row.OffsetInBlock, &row.SizeInBlock,
		); err != nil {
			_ = storageRows.Close()
			return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("scan diagnostic storage block: %w", err)
		}
		rawRows = append(rawRows, row)
	}
	if err := storageRows.Close(); err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("close diagnostic storage block rows: %w", err)
	}
	if err := storageRows.Err(); err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("iterate diagnostic storage blocks: %w", err)
	}

	legacyRows, err := dbconn.QueryContext(ctx, `
		SELECT b.id, b.container_id, b.format_version, b.codec, b.plaintext_size,
		       b.stored_size, b.block_offset, c.chunk_hash
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
	`)
	if err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("query diagnostic legacy blocks: %w", err)
	}
	for legacyRows.Next() {
		var row benchmarkPhysicalRawRow
		if err := legacyRows.Scan(
			&row.BlockID, &row.ContainerID, &row.FormatVersion, &row.Codec,
			&row.PlaintextSize, &row.StoredSize, &row.ContainerOffset, &row.ChunkHash,
		); err != nil {
			_ = legacyRows.Close()
			return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("scan diagnostic legacy block: %w", err)
		}
		row.CompressionCodec = "none"
		row.BlockHash = row.ChunkHash
		row.OffsetInBlock = sql.NullInt64{Int64: 0, Valid: true}
		row.SizeInBlock = sql.NullInt64{Int64: row.PlaintextSize, Valid: true}
		rawRows = append(rawRows, row)
	}
	if err := legacyRows.Close(); err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("close diagnostic legacy block rows: %w", err)
	}
	if err := legacyRows.Err(); err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("iterate diagnostic legacy blocks: %w", err)
	}

	var physical benchmarkDiagnosticPhysical
	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM container),
			(SELECT COUNT(*) FROM storage_blocks),
			(SELECT COUNT(*) FROM blocks),
			(SELECT COUNT(*) FROM chunk_block_refs),
			COALESCE((SELECT SUM(stored_size) FROM storage_blocks), 0) +
				COALESCE((SELECT SUM(stored_size) FROM blocks), 0),
			COALESCE((SELECT SUM(current_size) FROM container), 0)
	`).Scan(
		&physical.ContainerCount, &physical.StorageBlockCount, &physical.LegacyBlockCount,
		&physical.ChunkReferenceCount, &physical.PayloadBytes, &physical.ContainerBytes,
	); err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("query diagnostic physical totals: %w", err)
	}
	canonicalRows := canonicalizeBenchmarkPhysicalRows(rawRows)
	physical.CanonicalSHA256, err = benchmarkCanonicalDigest(canonicalRows)
	if err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("digest canonical physical content: %w", err)
	}

	containerRows, err := dbconn.QueryContext(ctx, `
		SELECT id, sealed, sealing, quarantine, current_size, max_size, COALESCE(container_hash, '')
		FROM container
	`)
	if err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("query diagnostic containers: %w", err)
	}
	containers := make(map[int64]*benchmarkContainerLayout)
	for containerRows.Next() {
		var row benchmarkContainerRawRow
		if err := containerRows.Scan(&row.ID, &row.Sealed, &row.Sealing, &row.Quarantine, &row.CurrentSize, &row.MaxSize, &row.ContainerHash); err != nil {
			_ = containerRows.Close()
			return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("scan diagnostic container: %w", err)
		}
		containers[row.ID] = &benchmarkContainerLayout{
			Sealed: row.Sealed, Sealing: row.Sealing, Quarantine: row.Quarantine,
			CurrentSize: row.CurrentSize, MaxSize: row.MaxSize, ContainerHash: row.ContainerHash,
			Rows: make([]benchmarkPhysicalLayoutRow, 0),
		}
	}
	if err := containerRows.Close(); err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("close diagnostic container rows: %w", err)
	}
	if err := containerRows.Err(); err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("iterate diagnostic containers: %w", err)
	}
	for _, raw := range rawRows {
		containerLayout, ok := containers[raw.ContainerID]
		if !ok {
			return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("diagnostic block references a missing container")
		}
		canonical := canonicalizeBenchmarkPhysicalRows([]benchmarkPhysicalRawRow{raw})[0]
		containerLayout.Rows = append(containerLayout.Rows, benchmarkPhysicalLayoutRow{
			Canonical: canonical, PlaintextSize: raw.PlaintextSize,
			CompressedSize: nullableInt64Pointer(raw.CompressedSize), StoredSize: raw.StoredSize,
			BlockHash: raw.BlockHash, CompressedHash: raw.CompressedHash, PhysicalHash: raw.PhysicalHash,
			ContainerOffset: raw.ContainerOffset, OffsetInBlock: nullableInt64Pointer(raw.OffsetInBlock),
		})
	}
	layouts := make([]benchmarkContainerLayout, 0, len(containers))
	for _, layout := range containers {
		sort.Slice(layout.Rows, func(i, j int) bool {
			left, _ := json.Marshal(layout.Rows[i])
			right, _ := json.Marshal(layout.Rows[j])
			return bytes.Compare(left, right) < 0
		})
		layouts = append(layouts, *layout)
	}
	sort.Slice(layouts, func(i, j int) bool {
		left, _ := json.Marshal(layouts[i])
		right, _ := json.Marshal(layouts[j])
		return bytes.Compare(left, right) < 0
	})
	layoutDigest, err := benchmarkCanonicalDigest(layouts)
	if err != nil {
		return benchmarkDiagnosticPhysical{}, "", fmt.Errorf("digest physical layout: %w", err)
	}
	return physical, layoutDigest, nil
}

type benchmarkStateSnapshot struct {
	ChunkCount        int64
	LogicalFileHashes []string
	SnapshotContent   []string
}

func validateBenchmarkDeterminism(preset corebenchmark.DatasetPreset, opts execution.Options) error {
	firstReport, firstState, err := runPresetAndCaptureStateInTemporaryDatabase(preset, 1, opts, "determinism-a")
	if err != nil {
		return fmt.Errorf("determinism run A failed: %w", err)
	}
	_ = firstReport

	secondReport, secondState, err := runPresetAndCaptureStateInTemporaryDatabase(preset, 1, opts, "determinism-b")
	if err != nil {
		return fmt.Errorf("determinism run B failed: %w", err)
	}
	_ = secondReport

	if firstState.ChunkCount != secondState.ChunkCount {
		return fmt.Errorf("determinism validation failed: chunk count mismatch (%d != %d)", firstState.ChunkCount, secondState.ChunkCount)
	}
	if !equalStringSlices(firstState.LogicalFileHashes, secondState.LogicalFileHashes) {
		return fmt.Errorf("determinism validation failed: logical file hash set mismatch")
	}
	if !equalStringSlices(firstState.SnapshotContent, secondState.SnapshotContent) {
		return fmt.Errorf("determinism validation failed: snapshot content mismatch")
	}

	// Verify that the user-visible restore output is also bit-for-bit identical
	// across independent runs. This is a stronger guarantee than DB hash
	// equality: it proves store→restore→hash(bytes) is stable.
	firstTree, err := runRestoreDeterminismCheck("restore-det-a")
	if err != nil {
		return fmt.Errorf("restore determinism run A failed: %w", err)
	}
	secondTree, err := runRestoreDeterminismCheck("restore-det-b")
	if err != nil {
		return fmt.Errorf("restore determinism run B failed: %w", err)
	}
	if ok, reason := corebenchmark.EqualRestoredTreeHashes(firstTree, secondTree); !ok {
		return fmt.Errorf("determinism validation failed: restored tree mismatch: %s", reason)
	}
	return nil
}

// runRestoreDeterminismCheck performs a minimal store→restore cycle in an
// isolated temporary database and returns a map of relative path → SHA-256
// digest for all files in the restored output directory. The same fixed seed
// and file size are used on every call so that two independent invocations
// should produce identical maps.
func runRestoreDeterminismCheck(runLabel string) (result map[string]string, err error) {
	dbName, cleanup, err := createTemporaryBenchmarkDatabase(runLabel)
	if err != nil {
		return nil, err
	}
	defer func() { err = finishBenchmarkDatabaseCleanup(err, cleanup) }()

	workDir, err := os.MkdirTemp("", "coldkeep-restore-det-*")
	if err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	storageDir := filepath.Join(workDir, "storage", "containers")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage dir: %w", err)
	}

	// Write a small deterministic file (fixed seed, fixed size).
	srcFile := filepath.Join(workDir, "source.bin")
	if err := corebenchmark.WriteDeterministicFile(srcFile, 256*1024, 0xDEADBEEF); err != nil {
		return nil, fmt.Errorf("write deterministic source file: %w", err)
	}

	exe := resolveSelfExecutable()
	baseEnv := buildDeterminismEnv(dbName, storageDir)

	if err := runSubprocess(exe, []string{"store", srcFile}, workDir, baseEnv); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}

	restoreDir := filepath.Join(workDir, "restore-output")
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		return nil, fmt.Errorf("create restore output dir: %w", err)
	}
	restoreDest := filepath.Join(restoreDir, "source.bin")
	if err := runSubprocess(exe, []string{
		"restore",
		"--stored-path", srcFile,
		"--mode", "override",
		"--destination", restoreDest,
		"--overwrite",
	}, workDir, baseEnv); err != nil {
		return nil, fmt.Errorf("restore: %w", err)
	}

	return corebenchmark.HashRestoredTree(restoreDir)
}

func buildDeterminismEnv(dbName, storageDir string) []string {
	host := strings.TrimSpace(os.Getenv("DB_HOST"))
	port := strings.TrimSpace(os.Getenv("DB_PORT"))
	user := strings.TrimSpace(os.Getenv("DB_USER"))
	password := os.Getenv("DB_PASSWORD")
	sslMode := strings.TrimSpace(os.Getenv("DB_SSLMODE"))
	codec := strings.TrimSpace(os.Getenv("COLDKEEP_CODEC"))
	key := os.Getenv("COLDKEEP_KEY")
	if sslMode == "" {
		sslMode = "disable"
	}
	if codec == "" {
		codec = "plain"
	}
	if strings.EqualFold(codec, "aes-gcm") && strings.TrimSpace(key) == "" {
		codec = "plain"
	}

	env := os.Environ()
	overrides := map[string]string{
		"DB_HOST":                    host,
		"DB_PORT":                    port,
		"DB_USER":                    user,
		"DB_PASSWORD":                password,
		"DB_SSLMODE":                 sslMode,
		"DB_NAME":                    dbName,
		"COLDKEEP_DB_AUTO_BOOTSTRAP": "true",
		"COLDKEEP_STORAGE_DIR":       storageDir,
		"COLDKEEP_CODEC":             codec,
	}
	if strings.TrimSpace(key) != "" {
		overrides["COLDKEEP_KEY"] = key
	}
	// Build a deduplicated env slice: start from os.Environ(), then apply overrides.
	seen := make(map[string]bool)
	result := make([]string, 0, len(env)+len(overrides))
	for k, v := range overrides {
		result = append(result, k+"="+v)
		seen[k] = true
	}
	for _, kv := range env {
		key := kv
		if idx := strings.IndexByte(kv, '='); idx >= 0 {
			key = kv[:idx]
		}
		if !seen[key] {
			result = append(result, kv)
		}
	}
	return result
}

func runSubprocess(exe string, args []string, workDir string, env []string) error {
	cmd := exec.Command(exe, args...) // #nosec G204 — exe is always resolveSelfExecutable()
	cmd.Dir = workDir
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		if len(out) > 0 {
			return fmt.Errorf("%w\n%s", err, out)
		}
		return err
	}
	return nil
}

func resolveSelfExecutable() string {
	if exe, err := os.Executable(); err == nil {
		return exe
	}
	// Fall back to argv[0] if os.Executable() fails (should not happen in practice).
	return os.Args[0]
}

func runPresetInTemporaryDatabase(preset corebenchmark.DatasetPreset, repeat int, opts execution.Options, runLabel string) (corebenchmark.RunReport, error) {
	report, _, err := runPresetAndCaptureStateInTemporaryDatabase(preset, repeat, opts, runLabel)
	return report, err
}

func runPresetAndCaptureStateInTemporaryDatabase(preset corebenchmark.DatasetPreset, repeat int, opts execution.Options, runLabel string) (report corebenchmark.RunReport, state benchmarkStateSnapshot, err error) {
	dbName, cleanup, err := createTemporaryBenchmarkDatabase(runLabel)
	if err != nil {
		return corebenchmark.RunReport{}, benchmarkStateSnapshot{}, err
	}
	defer func() { err = finishBenchmarkDatabaseCleanup(err, cleanup) }()

	report, err = corebenchmark.RunPreset(preset, repeat, corebenchmark.ScenarioConfig{
		ColdkeepExecutable: resolveSelfExecutable(),
		Codec:              strings.TrimSpace(os.Getenv("COLDKEEP_CODEC")),
		Compression:        strings.TrimSpace(os.Getenv("COLDKEEP_COMPRESSION")),
		Execution:          opts,
		ExtraEnv: map[string]string{
			"DB_NAME":                       dbName,
			"COLDKEEP_DB_AUTO_BOOTSTRAP":    "true",
			"COLDKEEP_STORE_FOLDER_WORKERS": strconv.Itoa(opts.StoreFolderWorkers),
		},
	})
	if err != nil {
		return corebenchmark.RunReport{}, benchmarkStateSnapshot{}, err
	}

	state, err = captureBenchmarkState(dbName)
	if err != nil {
		return corebenchmark.RunReport{}, benchmarkStateSnapshot{}, err
	}

	return report, state, nil
}

// finishBenchmarkDatabaseCleanup preserves an operation failure while making a
// cleanup failure observable to the caller. Both errors remain discoverable
// through errors.Is when the operation and cleanup fail together.
func finishBenchmarkDatabaseCleanup(operationErr error, cleanup func() error) error {
	cleanupErr := cleanup()
	if cleanupErr == nil {
		return operationErr
	}
	cleanupErr = fmt.Errorf("cleanup benchmark database: %w", cleanupErr)
	if operationErr == nil {
		return cleanupErr
	}
	return errors.Join(operationErr, cleanupErr)
}

var dropTemporaryBenchmarkDatabase = dropTemporaryBenchmarkDatabaseByName

func createTemporaryBenchmarkDatabase(label string) (string, func() error, error) {
	host := strings.TrimSpace(os.Getenv("DB_HOST"))
	port := strings.TrimSpace(os.Getenv("DB_PORT"))
	user := strings.TrimSpace(os.Getenv("DB_USER"))
	if host == "" || port == "" || user == "" {
		return "", nil, fmt.Errorf("determinism validation requires DB_HOST, DB_PORT, and DB_USER")
	}

	maintenanceDB := strings.TrimSpace(os.Getenv("COLDKEEP_TEST_DB_MAINTENANCE"))
	if maintenanceDB == "" {
		maintenanceDB = "postgres"
	}

	name := fmt.Sprintf("coldkeep_bench_%s_%d", sanitizeDBNamePart(label), time.Now().UnixNano())
	connStr, err := db.BuildPostgresConnStringFromEnv(maintenanceDB)
	if err != nil {
		return "", nil, fmt.Errorf("build maintenance DB connection string: %w", err)
	}
	adminDB, err := sql.Open("postgres", connStr)
	if err != nil {
		return "", nil, fmt.Errorf("open maintenance DB: %w", err)
	}
	closeAdminOnReturn := true
	defer func() {
		if closeAdminOnReturn {
			_ = adminDB.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := adminDB.PingContext(ctx); err != nil {
		return "", nil, fmt.Errorf("ping maintenance DB: %w", err)
	}
	if _, err := adminDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", name)); err != nil {
		return "", nil, fmt.Errorf("create benchmark DB %q: %w", name, err)
	}

	closeAdminOnReturn = false
	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func() error {
		cleanupOnce.Do(func() {
			cleanupErr = dropTemporaryBenchmarkDatabase(adminDB, name)
			if err := adminDB.Close(); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close maintenance DB for benchmark %q: %w", name, err))
			}
		})
		return cleanupErr
	}

	return name, cleanup, nil
}

func dropTemporaryBenchmarkDatabaseByName(adminDB *sql.DB, name string) error {
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()

	var errs []error
	if _, err := adminDB.ExecContext(cleanupCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, name); err != nil {
		errs = append(errs, fmt.Errorf("terminate sessions for benchmark DB %q: %w", name, err))
	}
	if _, err := adminDB.ExecContext(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteBenchmarkDatabaseIdentifier(name))); err != nil {
		errs = append(errs, fmt.Errorf("drop benchmark DB %q: %w", name, err))
	}
	return errors.Join(errs...)
}

func quoteBenchmarkDatabaseIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func sanitizeDBNamePart(label string) string {
	label = strings.ToLower(strings.TrimSpace(label))
	if label == "" {
		return "run"
	}
	var b strings.Builder
	for _, r := range label {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func captureBenchmarkState(dbName string) (benchmarkStateSnapshot, error) {
	connStr, err := db.BuildPostgresConnStringFromEnv(dbName)
	if err != nil {
		return benchmarkStateSnapshot{}, fmt.Errorf("build benchmark DB connection string: %w", err)
	}
	dbconn, err := sql.Open("postgres", connStr)
	if err != nil {
		return benchmarkStateSnapshot{}, fmt.Errorf("open benchmark DB %q: %w", dbName, err)
	}
	defer func() { _ = dbconn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	state := benchmarkStateSnapshot{}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk`).Scan(&state.ChunkCount); err != nil {
		return benchmarkStateSnapshot{}, fmt.Errorf("query chunk count: %w", err)
	}

	hashRows, err := dbconn.QueryContext(ctx, `SELECT file_hash FROM logical_file WHERE status = 'COMPLETED' ORDER BY file_hash ASC, id ASC`)
	if err != nil {
		return benchmarkStateSnapshot{}, fmt.Errorf("query logical file hashes: %w", err)
	}
	for hashRows.Next() {
		var hash string
		if err := hashRows.Scan(&hash); err != nil {
			_ = hashRows.Close()
			return benchmarkStateSnapshot{}, fmt.Errorf("scan logical file hash: %w", err)
		}
		state.LogicalFileHashes = append(state.LogicalFileHashes, hash)
	}
	if err := hashRows.Close(); err != nil {
		return benchmarkStateSnapshot{}, fmt.Errorf("close logical file hash rows: %w", err)
	}

	snapshotRows, err := dbconn.QueryContext(ctx, `
		SELECT lf.file_hash
		FROM snapshot_file sf
		JOIN logical_file lf ON lf.id = sf.logical_file_id
		ORDER BY lf.file_hash ASC, sf.snapshot_id ASC, sf.path_id ASC
	`)
	if err != nil {
		return benchmarkStateSnapshot{}, fmt.Errorf("query snapshot content: %w", err)
	}
	for snapshotRows.Next() {
		var hash string
		if err := snapshotRows.Scan(&hash); err != nil {
			_ = snapshotRows.Close()
			return benchmarkStateSnapshot{}, fmt.Errorf("scan snapshot content row: %w", err)
		}
		state.SnapshotContent = append(state.SnapshotContent, hash)
	}
	if err := snapshotRows.Close(); err != nil {
		return benchmarkStateSnapshot{}, fmt.Errorf("close snapshot content rows: %w", err)
	}

	return state, nil
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func runChunkerBenchmark() (BenchmarkChunkersReport, error) {
	type metricSpec struct {
		datasetName string
		metricName  string
		compute     func(base, candidate chunkbenchmark.Result) (float64, error)
	}

	metrics := []metricSpec{
		{
			datasetName: "slight-modifications",
			metricName:  "reuse-after-small-edit",
			compute: func(base, candidate chunkbenchmark.Result) (float64, error) {
				reuse, err := chunkbenchmark.CompareReuse(base, candidate)
				if err != nil {
					return 0, err
				}
				return reuse.ReuseRatioPct, nil
			},
		},
		{
			datasetName: "shifted-data",
			metricName:  "reuse-after-shift",
			compute: func(base, candidate chunkbenchmark.Result) (float64, error) {
				stability, err := chunkbenchmark.CompareBoundaryStability(base, candidate)
				if err != nil {
					return 0, err
				}
				return stability.ReuseAfterShiftPct, nil
			},
		},
	}

	index := make(map[string]chunkbenchmark.Dataset)
	for _, dataset := range chunkbenchmark.DefaultDatasets() {
		index[dataset.Name] = dataset
	}

	v1 := simplecdc.New()
	v2 := fastcdc.New()

	rows := make([]BenchmarkChunkersReportRecord, 0, len(metrics))
	for _, spec := range metrics {
		dataset, ok := index[spec.datasetName]
		if !ok {
			return BenchmarkChunkersReport{}, fmt.Errorf("missing benchmark dataset %q", spec.datasetName)
		}
		if len(dataset.Mutations) == 0 {
			return BenchmarkChunkersReport{}, fmt.Errorf("benchmark dataset %q has no mutation variants", spec.datasetName)
		}

		baseV1 := chunkbenchmark.RunChunker(v1, dataset.Base.Data)
		candidateV1 := chunkbenchmark.RunChunker(v1, dataset.Mutations[0].Data)
		if err := chunkbenchmark.ValidateCoverageInvariants(int64(len(dataset.Base.Data)), baseV1); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate base coverage for %q v1: %w", spec.datasetName, err)
		}
		if err := chunkbenchmark.ValidateCoverageInvariants(int64(len(dataset.Mutations[0].Data)), candidateV1); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate candidate coverage for %q v1: %w", spec.datasetName, err)
		}
		v1Pct, err := spec.compute(baseV1, candidateV1)
		if err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("compute %s for %q v1: %w", spec.metricName, spec.datasetName, err)
		}

		baseV2 := chunkbenchmark.RunChunker(v2, dataset.Base.Data)
		candidateV2 := chunkbenchmark.RunChunker(v2, dataset.Mutations[0].Data)
		if err := chunkbenchmark.ValidateCoverageInvariants(int64(len(dataset.Base.Data)), baseV2); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate base coverage for %q v2: %w", spec.datasetName, err)
		}
		if err := chunkbenchmark.ValidateCoverageInvariants(int64(len(dataset.Mutations[0].Data)), candidateV2); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate candidate coverage for %q v2: %w", spec.datasetName, err)
		}
		v2Pct, err := spec.compute(baseV2, candidateV2)
		if err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("compute %s for %q v2: %w", spec.metricName, spec.datasetName, err)
		}

		winner := string(chunk.VersionV2FastCDC)
		if v1Pct > v2Pct {
			winner = string(chunk.VersionV1SimpleRolling)
		}
		if math.Abs(v2Pct-v1Pct) < 0.0001 {
			winner = "tie"
		}

		rows = append(rows, BenchmarkChunkersReportRecord{
			Dataset:       spec.datasetName,
			Metric:        spec.metricName,
			V1SimplePct:   v1Pct,
			V2FastCDCPct:  v2Pct,
			DeltaPct:      v2Pct - v1Pct,
			WinnerVersion: winner,
		})
	}

	return BenchmarkChunkersReport{
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Rows:           rows,
	}, nil
}

func runSimulateCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if parsed.hasFlag("help", "h") {
		if len(parsed.positionals) > 0 && parsed.positionals[0] == "gc" {
			printSimulateGCHelp()
			return nil
		}
		printSimulateHelp()
		return nil
	}

	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep simulate <gc|store|store-folder> ...")
	}

	subcommand := parsed.positionals[0]

	if subcommand == "gc" {
		if len(parsed.positionals) > 1 {
			return usageErrorf("Usage: coldkeep simulate gc [--delete-snapshot <id>] [--containers] [--output <text|json>]")
		}
		return runSimulateGCCommand(parsed, outputMode)
	}

	if err := ensureAllowedFlags(parsed, "codec", "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) < 2 || len(parsed.positionals) > 2 {
		return usageErrorf("Usage: coldkeep simulate <store|store-folder> [--codec <codec>] <path>")
	}

	path := parsed.positionals[1]
	codecName, _ := parsed.lastFlagValue("codec")

	switch subcommand {
	case "store", "store-folder":
	default:
		return usageErrorf("unknown simulate subcommand %q (expected: gc, store, store-folder)", subcommand)
	}

	var codec blocks.Codec
	if codecName != "" {
		if codecName == "plain" {
			fmt.Fprintln(os.Stderr, "WARNING: data would be stored without encryption")
		}
		var err error
		codec, err = blocks.ParseCodec(codecName)
		if err != nil {
			return err
		}
	}

	sgctx, err := storage.ParseStorageContext("simulated")
	if err != nil {
		return fmt.Errorf("create simulated storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	// Run the simulation with stdout suppressed so that internal progress prints
	// don't appear before the structured simulation report.
	err = suppressStdoutDuring(func() error {
		switch subcommand {
		case "store":
			if codecName == "" {
				return storage.StoreFileWithStorageContext(sgctx, path)
			}
			return storage.StoreFileWithStorageContextAndCodec(sgctx, path, codec)
		case "store-folder":
			opts, optsErr := execution.FromEnv(execution.DefaultOptions())
			if optsErr != nil {
				return fmt.Errorf("store-folder execution options: %w", optsErr)
			}
			if codecName == "" {
				return storage.StoreFolderWithStorageContextAndOptions(sgctx, path, opts)
			}
			return storage.StoreFolderWithStorageContextAndCodecAndOptions(sgctx, path, codec, opts)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return emitSimulateReport(sgctx, subcommand, path, outputMode)
}

// runSimulateGCCommand implements `coldkeep simulate gc [--delete-snapshot <id>]*`.
// It is a pure read-only operation: it calls BuildPlan and reports what would
// be reclaimable, without deleting anything.
func runSimulateGCCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json", "delete-snapshot", "containers", "trace", "trace-json"); err != nil {
		return err
	}
	traceOptions, err := resolveTraceOptions(parsed)
	if err != nil {
		return err
	}

	deleteSnapshots := parsed.flagValues("delete-snapshot")
	includeContainers := parsed.hasFlag("containers")

	r, err := runObservabilitySimulateGCPhase(observability.SimulationOptions{
		Kind:                   observability.SimulationKindGC,
		Trace:                  traceOptions,
		AssumeDeletedSnapshots: deleteSnapshots,
	})
	if err != nil {
		if snapshotID, ok := missingSnapshotFromError(err); ok {
			return observabilityErrorf(exitGeneral, "NOT_FOUND", "snapshot %s not found", snapshotID)
		}
		return observabilityWrappedError(exitGeneral, "INTERNAL", "gc simulation failed", err)
	}

	renderResult := clirender.CloneSimulationResult(r)
	if renderResult.GC != nil {
		if !includeContainers {
			renderResult.GC.Containers = nil
		}
		if len(renderResult.GC.Assumptions.DeletedSnapshots) == 0 && len(deleteSnapshots) > 0 {
			renderResult.GC.Assumptions.DeletedSnapshots = append([]string(nil), deleteSnapshots...)
		}
	}

	renderer := resolveRenderer(outputMode)
	return renderer.RenderSimulation(os.Stdout, renderResult)
}

func resolveRenderer(outputMode cliOutputMode) clirender.Renderer {
	if outputMode == outputModeJSON {
		return clirender.JSONRenderer{}
	}
	return clirender.HumanRenderer{}
}

var runObservabilitySimulateGCPhase = func(opts observability.SimulationOptions) (*observability.SimulationResult, error) {
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return nil, err
	}
	defer func() { _ = sgctx.DB.Close() }()

	svc, err := newObservabilityServicePhase(sgctx.DB)
	if err != nil {
		return nil, err
	}

	return svc.Simulate(context.Background(), opts)
}

var runObservabilitySimulateStoreReportPhase = func(dbconn *sql.DB, subcommand, path string) (*observability.SimulateStoreReport, error) {
	svc, err := newObservabilityServicePhase(dbconn)
	if err != nil {
		return nil, err
	}

	return svc.SimulateStoreReport(context.Background(), subcommand, path)
}

// suppressStdoutDuring redirects os.Stdout to /dev/null for the duration of fn.
func suppressStdoutDuring(fn func() error) error {
	restore, err := suppressStdout()
	if err != nil {
		return fn()
	}
	defer restore()
	fnErr := fn()
	return fnErr
}

func suppressStdout() (func(), error) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return nil, err
	}

	stdoutRedirectMu.Lock()
	old := os.Stdout
	os.Stdout = devNull

	return func() {
		os.Stdout = old
		_ = devNull.Close()
		stdoutRedirectMu.Unlock()
	}, nil
}

func emitSimulateReport(sgctx storage.StorageContext, subcommand, path string, outputMode cliOutputMode) error {
	r, err := runObservabilitySimulateStoreReportPhase(sgctx.DB, subcommand, path)
	if err != nil {
		return fmt.Errorf("query simulate stats: %w", err)
	}

	if outputMode == outputModeJSON {
		return clirender.RenderSimulateStoreJSON(os.Stdout, r)
	}

	return clirender.RenderSimulateStoreHuman(os.Stdout, r)
}

func runSnapshotCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep snapshot <create|restore|list|show|stats|delete|diff> ...")
	}

	subcommand := strings.TrimSpace(strings.ToLower(parsed.positionals[0]))
	switch subcommand {
	case "create":
		return runSnapshotCreateCommand(parsed, outputMode)
	case "restore":
		return runSnapshotRestoreCommand(parsed, outputMode)
	case "list":
		return runSnapshotListCommand(parsed, outputMode)
	case "show":
		return runSnapshotShowCommand(parsed, outputMode)
	case "stats":
		return runSnapshotStatsCommand(parsed, outputMode)
	case "delete":
		return runSnapshotDeleteCommand(parsed, outputMode)
	case "diff":
		return runSnapshotDiffCommand(parsed, outputMode)
	default:
		return usageErrorf("unknown snapshot subcommand: %s", parsed.positionals[0])
	}
}

func parseSnapshotDateFlag(flagName, value string, endOfDay bool) (*time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, usageErrorf("--%s cannot be empty", flagName)
	}

	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		utc := parsed.UTC()
		return &utc, nil
	}

	parsedDate, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return nil, usageErrorf("invalid --%s value %q: use RFC3339 or YYYY-MM-DD", flagName, value)
	}
	if endOfDay {
		parsedDate = parsedDate.UTC().Add(24*time.Hour - time.Nanosecond)
	} else {
		parsedDate = parsedDate.UTC()
	}
	return &parsedDate, nil
}

func loadSnapshotDB() (storage.StorageContext, error) {
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return storage.StorageContext{}, fmt.Errorf("load storage context: %w", err)
	}
	if sgctx.DB == nil {
		_ = sgctx.Close()
		return storage.StorageContext{}, errors.New("storage context DB is nil")
	}
	return sgctx, nil
}

// parseSnapshotQuery builds a SnapshotQuery from the query-related flags in parsed.
// Returns nil if no query flags are set. Recognized flags:
// --path, --prefix, --pattern, --regex, --min-size, --max-size,
// --modified-after, --modified-before.
func parseSnapshotQuery(parsed parsedCommandLine) (*snapshot.SnapshotQuery, error) {
	q := &snapshot.SnapshotQuery{}
	hasAny := false

	if values := parsed.flagValues("path"); len(values) > 0 {
		q.ExactPaths = make(map[string]struct{}, len(values))
		for _, value := range values {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				return nil, usageErrorf("--path cannot be empty")
			}
			normalized, err := snapshot.NormalizeSnapshotPath(trimmed)
			if err != nil {
				return nil, usageErrorf("invalid --path value %q: %v", trimmed, err)
			}
			q.ExactPaths[normalized] = struct{}{}
		}
		hasAny = true
	}

	if values := parsed.flagValues("prefix"); len(values) > 0 {
		q.Prefixes = make([]string, 0, len(values))
		for _, value := range values {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				return nil, usageErrorf("--prefix cannot be empty")
			}
			normalized, err := snapshot.NormalizeSnapshotPath(trimmed)
			if err != nil {
				return nil, usageErrorf("invalid --prefix value %q: %v", trimmed, err)
			}
			if !strings.HasSuffix(normalized, "/") {
				return nil, usageErrorf("invalid --prefix value %q: must end with '/'", trimmed)
			}
			q.Prefixes = append(q.Prefixes, normalized)
		}
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("pattern"); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return nil, usageErrorf("--pattern cannot be empty")
		}
		// Validate the glob syntax early so users get a clear error rather than
		// a silent empty-result caused by path.ErrBadPattern at match time.
		if _, err := path.Match(trimmed, ""); err != nil {
			return nil, usageErrorf("invalid --pattern value %q: %v", trimmed, err)
		}
		q.Pattern = trimmed
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("regex"); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return nil, usageErrorf("--regex cannot be empty")
		}
		compiled, err := regexp.Compile(trimmed)
		if err != nil {
			return nil, usageErrorf("invalid --regex value %q: %v", trimmed, err)
		}
		q.Regex = compiled
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("min-size"); ok {
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 {
			return nil, usageErrorf("invalid --min-size value %q: must be a non-negative integer", value)
		}
		q.MinSize = &n
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("max-size"); ok {
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 {
			return nil, usageErrorf("invalid --max-size value %q: must be a non-negative integer", value)
		}
		q.MaxSize = &n
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("modified-after"); ok {
		parsedTime, err := parseSnapshotDateFlag("modified-after", value, false)
		if err != nil {
			return nil, err
		}
		q.ModifiedAfter = parsedTime
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("modified-before"); ok {
		parsedTime, err := parseSnapshotDateFlag("modified-before", value, true)
		if err != nil {
			return nil, err
		}
		q.ModifiedBefore = parsedTime
		hasAny = true
	}

	if q.MinSize != nil && q.MaxSize != nil && *q.MinSize > *q.MaxSize {
		return nil, usageErrorf("--min-size must be <= --max-size")
	}
	if q.ModifiedAfter != nil && q.ModifiedBefore != nil && q.ModifiedAfter.After(*q.ModifiedBefore) {
		return nil, usageErrorf("--modified-after must be <= --modified-before")
	}

	if !hasAny {
		return nil, nil
	}
	return q, nil
}

func snapshotLabelJSONValue(label sql.NullString) any {
	if !label.Valid {
		return nil
	}
	return label.String
}

func snapshotTimeJSONValue(value sql.NullTime) any {
	if !value.Valid {
		return nil
	}
	return value.Time.UTC().Format(time.RFC3339)
}

func snapshotIntJSONValue(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func snapshotSummaryJSON(item snapshot.Snapshot) map[string]any {
	return map[string]any{
		"id":         item.ID,
		"type":       item.Type,
		"created_at": item.CreatedAt.UTC().Format(time.RFC3339),
		"label":      snapshotLabelJSONValue(item.Label),
		"parent_id":  snapshotLabelJSONValue(item.ParentID),
	}
}

func snapshotSortAscending(items []snapshot.Snapshot) []snapshot.Snapshot {
	if len(items) < 2 {
		return append([]snapshot.Snapshot(nil), items...)
	}

	isAscending := true
	isDescending := true
	for i := 1; i < len(items); i++ {
		prev := items[i-1]
		curr := items[i]

		prevBeforeCurr := prev.CreatedAt.Before(curr.CreatedAt) || (prev.CreatedAt.Equal(curr.CreatedAt) && prev.ID < curr.ID)
		currBeforePrev := curr.CreatedAt.Before(prev.CreatedAt) || (prev.CreatedAt.Equal(curr.CreatedAt) && curr.ID < prev.ID)

		if !prevBeforeCurr {
			isAscending = false
		}
		if !currBeforePrev {
			isDescending = false
		}
		if !isAscending && !isDescending {
			break
		}
	}

	if isAscending {
		return append([]snapshot.Snapshot(nil), items...)
	}
	if isDescending {
		ordered := append([]snapshot.Snapshot(nil), items...)
		for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
			ordered[i], ordered[j] = ordered[j], ordered[i]
		}
		return ordered
	}

	ordered := append([]snapshot.Snapshot(nil), items...)
	// Defensive fallback for unsorted/custom inputs; normal list flow should not hit this.
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].CreatedAt.Equal(ordered[j].CreatedAt) {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].CreatedAt.Before(ordered[j].CreatedAt)
	})
	return ordered
}

func renderSnapshotTreeLines(items []snapshot.Snapshot) []string {
	if len(items) == 0 {
		return nil
	}

	// Tree visualization is derived only from snapshot metadata (id, parent_id,
	// created_at). Missing/NULL parent links are treated as roots so rendering
	// stays resilient after parent deletion or lineage cleanup.
	ordered := snapshotSortAscending(items)
	byID := make(map[string]snapshot.Snapshot, len(ordered))
	children := make(map[string][]snapshot.Snapshot)
	roots := make([]snapshot.Snapshot, 0)
	for _, item := range ordered {
		byID[item.ID] = item
	}
	for _, item := range ordered {
		if item.ParentID.Valid {
			if _, ok := byID[item.ParentID.String]; ok {
				children[item.ParentID.String] = append(children[item.ParentID.String], item)
				continue
			}
		}
		roots = append(roots, item)
	}

	lines := make([]string, 0, len(ordered))
	visited := make(map[string]struct{}, len(ordered))
	var walk func(node snapshot.Snapshot, prefix string, isLast bool, hasParent bool)
	walk = func(node snapshot.Snapshot, prefix string, isLast bool, hasParent bool) {
		if _, seen := visited[node.ID]; seen {
			return
		}
		visited[node.ID] = struct{}{}

		linePrefix := prefix
		if hasParent {
			if isLast {
				linePrefix += "└── "
			} else {
				linePrefix += "├── "
			}
		}
		lines = append(lines, linePrefix+node.ID)

		nextPrefix := prefix
		if hasParent {
			if isLast {
				nextPrefix += "    "
			} else {
				nextPrefix += "│   "
			}
		}

		childItems := children[node.ID]
		for idx, child := range childItems {
			if _, seen := visited[child.ID]; seen {
				log.Printf("WARNING: snapshot tree cycle detected; skipping edge parent=%s child=%s", node.ID, child.ID)
				continue
			}
			walk(child, nextPrefix, idx == len(childItems)-1, true)
		}
	}

	emitTopLevel := func(node snapshot.Snapshot) {
		if _, seen := visited[node.ID]; seen {
			return
		}
		if len(lines) > 0 {
			lines = append(lines, "")
		}
		walk(node, "", true, false)
	}

	for idx, root := range roots {
		_ = idx
		emitTopLevel(root)
	}
	for _, item := range ordered {
		if _, seen := visited[item.ID]; !seen {
			emitTopLevel(item)
		}
	}
	return lines
}

func snapshotFilesJSON(items []snapshot.SnapshotFileEntry) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, map[string]any{
			"path":            item.Path,
			"logical_file_id": item.LogicalFileID,
			"size":            snapshotIntJSONValue(item.Size),
			"mode":            snapshotIntJSONValue(item.Mode),
			"mtime":           snapshotTimeJSONValue(item.MTime),
		})
	}
	return result
}

func runSnapshotListCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := rejectBlankFlagValues(parsed, "path"); err != nil {
		return err
	}
	if err := ensureAllowedFlags(parsed, "type", "label", "since", "until", "limit", "tree", "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep snapshot list [--type <full|partial>] [--label <substring>] [--since <RFC3339|YYYY-MM-DD>] [--until <RFC3339|YYYY-MM-DD>] [--limit <count>] [--tree] [--output <text|json>]")
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}
	treeMode := parsed.hasFlag("tree")

	filter := snapshot.SnapshotListFilter{}
	if value, ok := parsed.lastFlagValue("type"); ok {
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed != "full" && trimmed != "partial" {
			return usageErrorf("invalid --type value %q (allowed: full, partial)", value)
		}
		filter.Type = &trimmed
	}
	if value, ok := parsed.lastFlagValue("label"); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return usageErrorf("--label cannot be empty")
		}
		filter.Label = &trimmed
	}
	if value, ok := parsed.lastFlagValue("since"); ok {
		parsedTime, err := parseSnapshotDateFlag("since", value, false)
		if err != nil {
			return err
		}
		filter.Since = parsedTime
	}
	if value, ok := parsed.lastFlagValue("until"); ok {
		parsedTime, err := parseSnapshotDateFlag("until", value, true)
		if err != nil {
			return err
		}
		filter.Until = parsedTime
	}
	if filter.Since != nil && filter.Until != nil && filter.Since.After(*filter.Until) {
		return usageErrorf("--since must be <= --until")
	}
	if value, ok := parsed.lastFlagValue("limit"); ok {
		parsedLimit, _ := strconv.Atoi(value)
		filter.Limit = parsedLimit
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	items, err := listSnapshotsPhase(ctx, sgctx.DB, filter)
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		jsonItems := make([]map[string]any, 0, len(items))
		for _, item := range items {
			jsonItems = append(jsonItems, snapshotSummaryJSON(item))
		}
		data := map[string]any{
			"action":      "list",
			"count":       len(items),
			"duration_ms": time.Since(startedAt).Milliseconds(),
			"snapshots":   jsonItems,
		}
		if treeMode {
			data["tree_mode"] = true
			data["tree_lines"] = renderSnapshotTreeLines(items)
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if treeMode {
		lines := renderSnapshotTreeLines(items)
		return clirender.RenderSnapshotListHuman(os.Stdout, items, true, lines, time.Since(startedAt).Milliseconds(), doctorOperationalHint)
	}

	return clirender.RenderSnapshotListHuman(os.Stdout, items, false, nil, time.Since(startedAt).Milliseconds(), doctorOperationalHint)
}

func runSnapshotShowCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "limit", "output", "json", "path", "prefix", "pattern", "regex", "min-size", "max-size", "modified-after", "modified-before"); err != nil {
		return err
	}
	if len(parsed.positionals) != 2 {
		return usageErrorf("Usage: coldkeep snapshot show <snapshotID> [--limit <count>] [--path <path>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <timestamp>] [--modified-before <timestamp>] [--output <text|json>]")
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}

	snapshotID := strings.TrimSpace(parsed.positionals[1])
	if snapshotID == "" {
		return usageErrorf("snapshotID cannot be empty")
	}
	limit := 0
	if value, ok := parsed.lastFlagValue("limit"); ok {
		limit, _ = strconv.Atoi(value)
	}

	query, err := parseSnapshotQuery(parsed)
	if err != nil {
		return err
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	item, err := getSnapshotPhase(ctx, sgctx.DB, snapshotID)
	if err != nil {
		return err
	}
	files, err := listSnapshotFilesPhase(ctx, sgctx.DB, snapshotID, limit, query)
	if err != nil {
		return err
	}
	stats, err := snapshotStatsPhase(ctx, sgctx.DB, snapshotID)
	if err != nil {
		return err
	}
	matchedFileCount := len(files)

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data": map[string]any{
				"action":                    "show",
				"snapshot":                  snapshotSummaryJSON(*item),
				"file_count":                matchedFileCount,
				"matched_file_count":        matchedFileCount,
				"total_snapshot_file_count": stats.SnapshotFileCount,
				"files":                     snapshotFilesJSON(files),
				"duration_ms":               time.Since(startedAt).Milliseconds(),
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	return clirender.RenderSnapshotShowHuman(
		os.Stdout,
		*item,
		files,
		matchedFileCount,
		stats.SnapshotFileCount,
		time.Since(startedAt).Milliseconds(),
		doctorOperationalHint,
	)
}

func runSnapshotStatsCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) > 2 {
		return usageErrorf("Usage: coldkeep snapshot stats [<snapshotID>] [--output <text|json>]")
	}

	snapshotID := ""
	if len(parsed.positionals) == 2 {
		snapshotID = strings.TrimSpace(parsed.positionals[1])
		if snapshotID == "" {
			return usageErrorf("snapshotID cannot be empty")
		}
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	stats, err := snapshotStatsPhase(ctx, sgctx.DB, snapshotID)
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		data := map[string]any{
			"action":              "stats",
			"snapshot_id":         snapshotID,
			"snapshot_count":      stats.SnapshotCount,
			"snapshot_file_count": stats.SnapshotFileCount,
			"total_size_bytes":    stats.TotalSizeBytes,
			"duration_ms":         time.Since(startedAt).Milliseconds(),
		}
		if stats.ParentSnapshotID.Valid {
			if stats.ReusedFileCount.Valid {
				data["reused"] = stats.ReusedFileCount.Int64
			}
			if stats.NewFileCount.Valid {
				data["new"] = stats.NewFileCount.Int64
			}
			if stats.ReuseRatioPct.Valid {
				data["reuse_ratio"] = stats.ReuseRatioPct.Float64
			}
		}

		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if snapshotID == "" {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshots: %d\n", stats.SnapshotCount)
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot files: %d\n", stats.SnapshotFileCount)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot: %s\n", snapshotID)
		_, _ = fmt.Fprintf(os.Stdout, "  Files: %d\n", stats.SnapshotFileCount)
		if stats.ParentSnapshotID.Valid {
			_, _ = fmt.Fprintf(os.Stdout, "  Reused: %d\n", stats.ReusedFileCount.Int64)
			_, _ = fmt.Fprintf(os.Stdout, "  New: %d\n", stats.NewFileCount.Int64)
			_, _ = fmt.Fprintf(os.Stdout, "  Reuse ratio: %.1f%%\n", stats.ReuseRatioPct.Float64)
		} else {
			_, _ = fmt.Fprintf(os.Stdout, "  (%s)\n", snapshotLineageUnavailableMessage(stats.LineageStatus))
		}
	}
	_, _ = fmt.Fprintf(os.Stdout, "Total logical size: %d bytes\n", stats.TotalSizeBytes)
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", time.Since(startedAt).Milliseconds())
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func snapshotLineageUnavailableMessage(status snapshot.SnapshotLineageStatus) string {
	switch status {
	case snapshot.SnapshotLineageStatusNoParent, "":
		return "Reused/New not available -- no parent snapshot metadata"
	case snapshot.SnapshotLineageStatusParentMissing:
		return "Reused/New not available -- parent snapshot metadata exists but parent snapshot is missing"
	case snapshot.SnapshotLineageStatusSkipped:
		return "Reused/New not available -- lineage analysis was intentionally skipped for this snapshot scope"
	default:
		return "Reused/New not available"
	}
}

func runSnapshotDeleteCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	req, dryRun, err := parseSnapshotDeleteCommandRequest(parsed)
	if err != nil {
		return err
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	result, err := runSnapshotDeleteEngine(ctx, sgctx, req)
	if err != nil {
		return err
	}

	renderedSnapshotID := result.SnapshotID
	if renderedSnapshotID == "" {
		renderedSnapshotID = req.SnapshotID
	}
	preview, err := snapshotDeletePreviewFromEngineResult(result)
	if err != nil {
		return err
	}

	return renderSnapshotDeleteResult(outputMode, renderedSnapshotID, dryRun, preview, time.Since(startedAt))
}

type snapshotDeleteLineagePreview = snapshot.DeleteLineagePreview

type snapshotDeleteCommandRequest struct {
	SnapshotID string
	Mode       engine.SnapshotDeleteMode
}

func parseSnapshotDeleteCommandRequest(parsed parsedCommandLine) (snapshotDeleteCommandRequest, bool, error) {
	if err := ensureAllowedFlags(parsed, "force", "dry-run", "dryRun", "output", "json"); err != nil {
		return snapshotDeleteCommandRequest{}, false, err
	}
	if len(parsed.positionals) != 2 {
		return snapshotDeleteCommandRequest{}, false, usageErrorf("Usage: coldkeep snapshot delete <snapshotID> (--force|--dry-run) [--output <text|json>]")
	}
	dryRun := parsed.hasFlag("dry-run", "dryRun")
	if !dryRun && !parsed.hasFlag("force") {
		return snapshotDeleteCommandRequest{}, false, usageErrorf("snapshot delete requires --force or --dry-run")
	}

	snapshotID := strings.TrimSpace(parsed.positionals[1])
	if snapshotID == "" {
		return snapshotDeleteCommandRequest{}, false, usageErrorf("snapshotID cannot be empty")
	}

	mode := engine.SnapshotDeleteModeExecute
	if dryRun {
		mode = engine.SnapshotDeleteModePreview
	}
	return snapshotDeleteCommandRequest{
		SnapshotID: snapshotID,
		Mode:       mode,
	}, dryRun, nil
}

func runSnapshotDeleteEngine(
	ctx context.Context,
	sgctx storage.StorageContext,
	req snapshotDeleteCommandRequest,
) (engine.SnapshotDeleteResult, error) {
	eng, err := newCommandEngine(sgctx.DB, sgctx.EffectiveContainerDir())
	if err != nil {
		return engine.SnapshotDeleteResult{}, err
	}
	return eng.SnapshotDelete(ctx, engine.SnapshotDeleteRequest{
		SnapshotID: req.SnapshotID,
		Mode:       req.Mode,
	})
}

func renderSnapshotDeleteResult(
	outputMode cliOutputMode,
	snapshotID string,
	dryRun bool,
	preview *snapshotDeleteLineagePreview,
	duration time.Duration,
) error {
	if outputMode == outputModeJSON {
		payload := snapshotDeleteJSONPayload(snapshotID, dryRun, preview, duration)
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}
	if dryRun {
		output := formatSnapshotDeleteDryRunOutput(snapshotID, preview)
		_, _ = fmt.Fprint(os.Stdout, output)
		return nil
	}
	_, _ = fmt.Fprintf(os.Stdout, "Snapshot deleted: id=%s\n", snapshotID)
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", duration.Milliseconds())
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func snapshotDeleteJSONPayload(
	snapshotID string,
	dryRun bool,
	preview *snapshotDeleteLineagePreview,
	duration time.Duration,
) map[string]any {
	action := "delete"
	if dryRun {
		action = "delete_dry_run"
	}
	return map[string]any{
		"status":  "ok",
		"command": "snapshot",
		"data": map[string]any{
			"action":         action,
			"snapshot_id":    snapshotID,
			"dry_run":        dryRun,
			"parent_id":      snapshotLabelJSONValue(previewParentID(preview)),
			"parent_missing": previewParentMissing(preview),
			"children":       previewChildren(preview),
			"total_files":    previewTotalFiles(preview),
			"unique_files":   previewUniqueFiles(preview),
			"shared_files":   previewSharedFiles(preview),
			"warnings":       previewWarnings(preview),
			"duration_ms":    duration.Milliseconds(),
		},
	}
}

func snapshotDeletePreviewFromEngineResult(result engine.SnapshotDeleteResult) (*snapshotDeleteLineagePreview, error) {
	if result.Mode != engine.SnapshotDeleteModePreview {
		return nil, nil
	}
	if result.Preview == nil {
		return nil, fmt.Errorf("snapshot delete preview result missing preview payload")
	}

	preview := &snapshotDeleteLineagePreview{
		SnapshotID:       result.SnapshotID,
		ChildSnapshotIDs: append([]string(nil), result.Preview.Children...),
		TotalFiles:       result.Preview.TotalFiles,
		UniqueFiles:      result.Preview.UniqueFiles,
		SharedFiles:      result.Preview.SharedFiles,
	}
	switch result.Preview.Parent.State {
	case engine.SnapshotDeleteParentNone:
	case engine.SnapshotDeleteParentPresent:
		preview.ParentID = sql.NullString{String: result.Preview.Parent.ID, Valid: true}
	case engine.SnapshotDeleteParentMissing:
		preview.ParentID = sql.NullString{String: result.Preview.Parent.ID, Valid: true}
		preview.ParentMissing = true
	default:
		return nil, fmt.Errorf("snapshot delete preview result has unknown parent state %q", result.Preview.Parent.State)
	}
	return preview, nil
}

func loadSnapshotDeleteLineagePreview(ctx context.Context, dbconn *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
	return snapshot.LoadDeleteLineagePreview(ctx, dbconn, snapshotID)
}

func previewParentID(preview *snapshotDeleteLineagePreview) sql.NullString {
	if preview == nil {
		return sql.NullString{}
	}
	return preview.ParentID
}

func previewParentMissing(preview *snapshotDeleteLineagePreview) bool {
	if preview == nil {
		return false
	}
	return preview.ParentMissing
}

func previewChildren(preview *snapshotDeleteLineagePreview) []string {
	if preview == nil {
		return nil
	}
	return append([]string(nil), preview.ChildSnapshotIDs...)
}

func previewTotalFiles(preview *snapshotDeleteLineagePreview) int64 {
	if preview == nil {
		return 0
	}
	return preview.TotalFiles
}

func previewUniqueFiles(preview *snapshotDeleteLineagePreview) int64 {
	if preview == nil {
		return 0
	}
	return preview.UniqueFiles
}

func previewSharedFiles(preview *snapshotDeleteLineagePreview) int64 {
	if preview == nil {
		return 0
	}
	return preview.SharedFiles
}

// formatNumberWithCommas formats a number int64 with comma separators for readability.
// Kept as a compatibility helper for existing CLI tests.
func formatNumberWithCommas(n int64) string {
	if n < 0 {
		return "-" + formatNumberWithCommas(-n)
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

func previewWarnings(preview *snapshotDeleteLineagePreview) []map[string]any {
	return clirender.SnapshotDeleteWarnings(preview)
}

// formatSnapshotDeleteDryRunOutput builds the formatted text output for dry-run delete
func formatSnapshotDeleteDryRunOutput(snapshotID string, preview *snapshotDeleteLineagePreview) string {
	return clirender.FormatSnapshotDeleteDryRunOutput(snapshotID, preview)
}

func runSnapshotDiffCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "filter", "summary", "output", "json", "path", "prefix", "pattern", "regex", "min-size", "max-size", "modified-after", "modified-before"); err != nil {
		return err
	}
	if len(parsed.positionals) != 3 {
		return usageErrorf("Usage: coldkeep snapshot diff <baseSnapshotID> <targetSnapshotID> [--summary] [--filter <added|removed|modified>] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <timestamp>] [--modified-before <timestamp>] [--output <text|json>]")
	}

	baseID := strings.TrimSpace(parsed.positionals[1])
	targetID := strings.TrimSpace(parsed.positionals[2])
	if baseID == "" {
		return usageErrorf("baseSnapshotID cannot be empty")
	}
	if targetID == "" {
		return usageErrorf("targetSnapshotID cannot be empty")
	}

	filterType := ""
	summaryMode := parsed.hasFlag("summary")
	if value, ok := parsed.lastFlagValue("filter"); ok {
		filterType = strings.ToLower(strings.TrimSpace(value))
		switch filterType {
		case "added", "removed", "modified":
		default:
			return usageErrorf("invalid --filter value %q (allowed: added, removed, modified)", value)
		}
	}

	query, err := parseSnapshotQuery(parsed)
	if err != nil {
		return err
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	useSummaryFastPath := summaryMode && filterType == "" && query == nil
	if useSummaryFastPath {
		summary, err := diffSnapshotSummaryPhase(ctx, sgctx.DB, baseID, targetID)
		if err != nil {
			return err
		}
		totalEntryCount := int(summary.Added + summary.Removed + summary.Modified)

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "snapshot diff",
				"data": map[string]any{
					"base":                   baseID,
					"target":                 targetID,
					"entry_count":            totalEntryCount,
					"matched_entry_count":    totalEntryCount,
					"total_diff_entry_count": totalEntryCount,
					"summary":                summary,
					"summary_mode":           true,
					"duration_ms":            time.Since(startedAt).Milliseconds(),
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		return clirender.RenderSnapshotDiffSummaryHuman(os.Stdout, baseID, targetID, *summary)
	}

	result, err := diffSnapshotsPhase(ctx, sgctx.DB, baseID, targetID, query)
	if err != nil {
		return err
	}

	entries := make([]snapshot.SnapshotDiffEntry, 0, len(result.Entries))
	summary := snapshot.SnapshotDiffSummary{}
	for _, entry := range result.Entries {
		if filterType != "" && entry.Type != snapshot.DiffType(filterType) {
			continue
		}
		entries = append(entries, entry)
		switch entry.Type {
		case snapshot.DiffAdded:
			summary.Added++
		case snapshot.DiffRemoved:
			summary.Removed++
		case snapshot.DiffModified:
			summary.Modified++
		}
	}
	totalEntryCount := len(result.Entries)
	matchedEntryCount := len(entries)

	if outputMode == outputModeJSON {
		jsonEntries := make([]map[string]any, 0, len(entries))
		if !summaryMode {
			for _, entry := range entries {
				jsonEntries = append(jsonEntries, map[string]any{
					"path":              entry.Path,
					"type":              entry.Type,
					"base_logical_id":   snapshotIntJSONValue(entry.BaseLogicalID),
					"target_logical_id": snapshotIntJSONValue(entry.TargetLogicalID),
				})
			}
		}

		data := map[string]any{
			"base":                   baseID,
			"target":                 targetID,
			"entry_count":            matchedEntryCount,
			"matched_entry_count":    matchedEntryCount,
			"total_diff_entry_count": totalEntryCount,
			"summary":                summary,
			"duration_ms":            time.Since(startedAt).Milliseconds(),
		}
		if summaryMode {
			data["summary_mode"] = true
		} else {
			data["entries"] = jsonEntries
		}

		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot diff",
			"data":    data,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if summaryMode {
		return clirender.RenderSnapshotDiffSummaryHuman(os.Stdout, baseID, targetID, summary)
	}

	return clirender.RenderSnapshotDiffDetailedHuman(
		os.Stdout,
		baseID,
		targetID,
		entries,
		summary,
		matchedEntryCount,
		totalEntryCount,
		time.Since(startedAt).Milliseconds(),
		doctorOperationalHint,
	)
}

func runSnapshotCreateCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	perf := newPerfTimer()

	if err := ensureAllowedFlags(parsed, "id", "label", "from", "output", "json"); err != nil {
		return err
	}
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep snapshot create [<path> ...] [--id <snapshotID>] [--label <label>] [--from <snapshotID>] [--output <text|json>]")
	}

	paths := parsed.positionals[1:]

	snapshotID, hasSnapshotID := parsed.lastFlagValue("id")
	snapshotID = strings.TrimSpace(snapshotID)
	if hasSnapshotID && snapshotID == "" {
		return usageErrorf("--id cannot be empty")
	}

	label := ""
	if rawLabel, hasLabel := parsed.lastFlagValue("label"); hasLabel {
		trimmed := strings.TrimSpace(rawLabel)
		if trimmed == "" {
			return usageErrorf("--label cannot be empty")
		}
		label = trimmed
	}

	parentID := ""
	if fromID, hasFrom := parsed.lastFlagValue("from"); hasFrom {
		trimmed := strings.TrimSpace(fromID)
		if trimmed == "" {
			return usageErrorf("--from cannot be empty")
		}
		parentID = trimmed
	}

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if sgctx.DB == nil {
		return errors.New("storage context DB is nil")
	}
	eng, err := newCommandEngine(sgctx.DB, sgctx.EffectiveContainerDir())
	if err != nil {
		return err
	}
	perf.Mark("setup")

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	result, err := eng.SnapshotCreate(ctx, engine.SnapshotCreateRequest{
		ID:       snapshotID,
		Label:    label,
		ParentID: parentID,
		Paths:    append([]string(nil), paths...),
	})
	if err != nil {
		return err
	}
	perf.Mark("operation")

	if outputMode == outputModeJSON {
		totalMs := int64(0)
		for _, s := range perf.Spans() {
			totalMs += s.DurationMs
		}
		data := map[string]any{
			"snapshot_id":    result.SnapshotID,
			"type":           string(result.Type),
			"paths_count":    result.PathsCount,
			"files_inserted": result.FilesInserted,
			"duration_ms":    totalMs,
			"perf_spans":     perf.Spans(),
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		if result.Label != "" {
			data["label"] = result.Label
		}
		if result.ParentID != "" {
			data["parent_id"] = result.ParentID
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if result.ParentID != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot %q created from parent %q\n", result.SnapshotID, result.ParentID)
	} else if result.Type == engine.SnapshotTypeFull {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot created: id=%s type=%s (all paths)\n", result.SnapshotID, result.Type)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot created: id=%s type=%s paths=%d\n", result.SnapshotID, result.Type, result.PathsCount)
	}
	_, _ = fmt.Fprintf(os.Stdout, "  Files: %d\n", result.FilesInserted)
	totalMs := int64(0)
	for _, s := range perf.Spans() {
		totalMs += s.DurationMs
	}
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", totalMs)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runSnapshotRestoreCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "mode", "destination", "overwrite", "strict", "no-metadata", "output", "json", "path", "prefix", "pattern", "regex", "min-size", "max-size", "modified-after", "modified-before"); err != nil {
		return err
	}
	if len(parsed.positionals) < 2 {
		return usageErrorf("Usage: coldkeep snapshot restore <snapshotID> [<path> ...] [--mode <original|prefix|override>] [--destination <path>] [--overwrite] [--strict] [--no-metadata] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <timestamp>] [--modified-before <timestamp>] [--output <text|json>]")
	}

	snapshotID := strings.TrimSpace(parsed.positionals[1])
	if snapshotID == "" {
		return usageErrorf("snapshotID cannot be empty")
	}
	paths := parsed.positionals[2:]

	strictMetadata := parsed.hasFlag("strict")
	noMetadata := parsed.hasFlag("no-metadata")
	if strictMetadata && noMetadata {
		return usageErrorf("--strict and --no-metadata cannot be used together")
	}

	destinationMode, err := parseRestoreDestinationMode(parsed)
	if err != nil {
		return err
	}
	destination, _ := parsed.lastFlagValue("destination")
	destination = strings.TrimSpace(destination)
	if destinationMode == storage.RestoreDestinationOriginal && destination != "" {
		return usageErrorf("--destination is only supported with --mode prefix or --mode override")
	}
	if (destinationMode == storage.RestoreDestinationPrefix || destinationMode == storage.RestoreDestinationOverride) && destination == "" {
		return usageErrorf("--destination is required with --mode %s", destinationMode)
	}

	selection, err := parseSnapshotRestoreSelection(parsed)
	if err != nil {
		return err
	}

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if sgctx.DB == nil {
		return errors.New("storage context DB is nil")
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	req, jsonOutputRoot, err := buildSnapshotRestoreEngineRequest(snapshotID, paths, selection, destinationMode, destination, parsed)
	if err != nil {
		return err
	}

	result, err := runSnapshotRestoreEngine(ctx, sgctx, req)
	if err != nil {
		return err
	}

	durationMS := time.Since(startedAt).Milliseconds()
	actionType := "full"
	if req.RequestedPathsCount() > 0 {
		actionType = "partial_restore"
	}

	if outputMode == outputModeJSON {
		data := map[string]any{
			"action":                "restore",
			"snapshot_id":           snapshotID,
			"type":                  actionType,
			"requested_paths_count": req.RequestedPathsCount(),
			"restored_files":        result.RestoredFiles,
			"duration_ms":           durationMS,
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		if jsonOutputRoot != "" {
			data["output_root"] = jsonOutputRoot
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	emptyNote := ""
	if result.RestoredFiles == 0 {
		emptyNote = " (empty snapshot selection)"
	}
	if req.RequestedPathsCount() == 0 {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot restored: id=%s files=%d%s\n", snapshotID, result.RestoredFiles, emptyNote)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot restored: id=%s requested_paths=%d restored_files=%d%s\n", snapshotID, req.RequestedPathsCount(), result.RestoredFiles, emptyNote)
	}
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", durationMS)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

type snapshotRestoreCommandRequest struct {
	EngineRequest engine.SnapshotRestoreRequest
}

func (r snapshotRestoreCommandRequest) RequestedPathsCount() int {
	return len(r.EngineRequest.Paths)
}

func parseSnapshotRestoreSelection(parsed parsedCommandLine) (engine.SnapshotRestoreSelection, error) {
	selection := engine.SnapshotRestoreSelection{}

	if err := parseSnapshotRestoreExactPathSelectors(parsed, &selection); err != nil {
		return engine.SnapshotRestoreSelection{}, err
	}
	if err := parseSnapshotRestorePrefixSelectors(parsed, &selection); err != nil {
		return engine.SnapshotRestoreSelection{}, err
	}
	if err := parseSnapshotRestorePatternSelector(parsed, &selection); err != nil {
		return engine.SnapshotRestoreSelection{}, err
	}
	if err := parseSnapshotRestoreRegexSelector(parsed, &selection); err != nil {
		return engine.SnapshotRestoreSelection{}, err
	}
	if err := parseSnapshotRestoreSizeSelectors(parsed, &selection); err != nil {
		return engine.SnapshotRestoreSelection{}, err
	}
	if err := parseSnapshotRestoreModifiedSelectors(parsed, &selection); err != nil {
		return engine.SnapshotRestoreSelection{}, err
	}

	return selection, nil
}

func parseSnapshotRestoreExactPathSelectors(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	values := parsed.flagValues("path")
	if len(values) == 0 {
		return nil
	}

	selection.ExactPaths = make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return usageErrorf("--path cannot be empty")
		}
		normalized, err := snapshot.NormalizeSnapshotPath(trimmed)
		if err != nil {
			return usageErrorf("invalid --path value %q: %v", trimmed, err)
		}
		selection.ExactPaths = append(selection.ExactPaths, normalized)
	}
	return nil
}

func parseSnapshotRestorePrefixSelectors(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	values := parsed.flagValues("prefix")
	if len(values) == 0 {
		return nil
	}

	selection.Prefixes = make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return usageErrorf("--prefix cannot be empty")
		}
		normalized, err := snapshot.NormalizeSnapshotPath(trimmed)
		if err != nil {
			return usageErrorf("invalid --prefix value %q: %v", trimmed, err)
		}
		if !strings.HasSuffix(normalized, "/") {
			return usageErrorf("invalid --prefix value %q: must end with '/'", trimmed)
		}
		selection.Prefixes = append(selection.Prefixes, normalized)
	}
	return nil
}

func parseSnapshotRestorePatternSelector(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	value, ok := parsed.lastFlagValue("pattern")
	if !ok {
		return nil
	}

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return usageErrorf("--pattern cannot be empty")
	}
	if _, err := path.Match(trimmed, ""); err != nil {
		return usageErrorf("invalid --pattern value %q: %v", trimmed, err)
	}
	selection.Pattern = trimmed
	return nil
}

func parseSnapshotRestoreRegexSelector(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	value, ok := parsed.lastFlagValue("regex")
	if !ok {
		return nil
	}

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return usageErrorf("--regex cannot be empty")
	}
	if _, err := regexp.Compile(trimmed); err != nil {
		return usageErrorf("invalid --regex value %q: %v", trimmed, err)
	}
	selection.Regex = trimmed
	return nil
}

func parseSnapshotRestoreSizeSelectors(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	if err := parseSnapshotRestoreMinSizeSelector(parsed, selection); err != nil {
		return err
	}
	if err := parseSnapshotRestoreMaxSizeSelector(parsed, selection); err != nil {
		return err
	}
	if selection.MinSize != nil && selection.MaxSize != nil && *selection.MinSize > *selection.MaxSize {
		return usageErrorf("--min-size must be <= --max-size")
	}
	return nil
}

func parseSnapshotRestoreMinSizeSelector(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	value, ok := parsed.lastFlagValue("min-size")
	if !ok {
		return nil
	}

	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || n < 0 {
		return usageErrorf("invalid --min-size value %q: must be a non-negative integer", value)
	}
	selection.MinSize = &n
	return nil
}

func parseSnapshotRestoreMaxSizeSelector(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	value, ok := parsed.lastFlagValue("max-size")
	if !ok {
		return nil
	}

	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || n < 0 {
		return usageErrorf("invalid --max-size value %q: must be a non-negative integer", value)
	}
	selection.MaxSize = &n
	return nil
}

func parseSnapshotRestoreModifiedSelectors(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	if err := parseSnapshotRestoreModifiedAfterSelector(parsed, selection); err != nil {
		return err
	}
	if err := parseSnapshotRestoreModifiedBeforeSelector(parsed, selection); err != nil {
		return err
	}
	if selection.ModifiedAfter != nil && selection.ModifiedBefore != nil && selection.ModifiedAfter.After(*selection.ModifiedBefore) {
		return usageErrorf("--modified-after must be <= --modified-before")
	}
	return nil
}

func parseSnapshotRestoreModifiedAfterSelector(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	value, ok := parsed.lastFlagValue("modified-after")
	if !ok {
		return nil
	}

	parsedTime, err := parseSnapshotDateFlag("modified-after", value, false)
	if err != nil {
		return err
	}
	selection.ModifiedAfter = parsedTime
	return nil
}

func parseSnapshotRestoreModifiedBeforeSelector(parsed parsedCommandLine, selection *engine.SnapshotRestoreSelection) error {
	value, ok := parsed.lastFlagValue("modified-before")
	if !ok {
		return nil
	}

	parsedTime, err := parseSnapshotDateFlag("modified-before", value, true)
	if err != nil {
		return err
	}
	selection.ModifiedBefore = parsedTime
	return nil
}

func buildSnapshotRestoreEngineRequest(
	snapshotID string,
	paths []string,
	selection engine.SnapshotRestoreSelection,
	destinationMode storage.RestoreDestinationMode,
	destination string,
	parsed parsedCommandLine,
) (snapshotRestoreCommandRequest, string, error) {
	engineMode, err := storageSnapshotRestoreModeToEngine(destinationMode)
	if err != nil {
		return snapshotRestoreCommandRequest{}, "", err
	}

	outputTarget := destination
	jsonOutputRoot := destination
	if destinationMode == storage.RestoreDestinationOriginal {
		cwd, err := currentWorkingDirectoryPhase()
		if err != nil {
			return snapshotRestoreCommandRequest{}, "", fmt.Errorf("resolve current working directory: %w", err)
		}
		outputTarget = cwd
		jsonOutputRoot = ""
	}

	return snapshotRestoreCommandRequest{
		EngineRequest: engine.SnapshotRestoreRequest{
			SnapshotID: snapshotID,
			Paths:      append([]string(nil), paths...),
			Selection:  selection,
			Destination: engine.SnapshotRestoreDestination{
				Mode: engineMode,
				Path: outputTarget,
			},
			Overwrite: parsed.hasFlag("overwrite"),
			Metadata:  snapshotRestoreMetadataModeFromCLI(parsed.hasFlag("strict"), parsed.hasFlag("no-metadata")),
		},
	}, jsonOutputRoot, nil
}

func runSnapshotRestoreEngine(
	ctx context.Context,
	sgctx storage.StorageContext,
	req snapshotRestoreCommandRequest,
) (engine.SnapshotRestoreResult, error) {
	eng, err := newSnapshotRestoreCommandEngine(sgctx)
	if err != nil {
		return engine.SnapshotRestoreResult{}, err
	}
	return eng.SnapshotRestore(ctx, req.EngineRequest)
}

func storageSnapshotRestoreModeToEngine(mode storage.RestoreDestinationMode) (engine.SnapshotRestoreDestinationMode, error) {
	switch mode {
	case storage.RestoreDestinationOriginal:
		return engine.SnapshotRestoreDestinationOriginal, nil
	case storage.RestoreDestinationPrefix:
		return engine.SnapshotRestoreDestinationPrefix, nil
	case storage.RestoreDestinationOverride:
		return engine.SnapshotRestoreDestinationOverride, nil
	default:
		return "", fmt.Errorf("unsupported restore destination mode %q", mode)
	}
}

func snapshotRestoreMetadataModeFromCLI(strictMetadata bool, noMetadata bool) engine.SnapshotRestoreMetadataMode {
	switch {
	case strictMetadata:
		return engine.SnapshotRestoreMetadataStrict
	case noMetadata:
		return engine.SnapshotRestoreMetadataNone
	default:
		return engine.SnapshotRestoreMetadataBestEffort
	}
}

func printHelp() {
	fmt.Printf("coldkeep (v%s)\n", version.String())
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  coldkeep <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	printHelpRows([][2]string{
		{"  init", "Initialize Coldkeep with a new aes-gcm encryption key"},
		{"  config get default-chunker [--output <text|json>]", "Get repository default chunker for new writes"},
		{"  config set default-chunker <value> [--output <text|json>]", "Set repository default chunker for new writes. Affects only new stored data. Existing data is not modified."},
		{"  doctor [--standard|--full|--deep] [--output <text|json>]", "Recommended operator health gate (corrective; may update metadata via recovery before verify; default: --standard)"},
		{"  store [--codec <codec>] <file>", "Store a single file (state-changing)"},
		{"  store-folder [--codec <codec>] <folder>", "Store all files in a folder recursively (state-changing)"},
		{"  restore <fileID> [<fileID> ...] <outputDir> [--input <file>] [--dry-run] [--overwrite] [--fail-fast] [--output <text|json>]", "Restore one or more logical file IDs byte-identically (chunker-version independent)"},
		{"  remove <fileID> [<fileID> ...] [--input <file>] [--dry-run] [--fail-fast] [--output <text|json>]", "Remove one or more logical file IDs (legacy mode)"},
		{"  remove --stored-path <path> [--output <text|json>]", "Remove one current-state physical path mapping"},
		{"  remove --stored-paths <path> [<path> ...] [--input <file>] [--dry-run] [--fail-fast] [--output <text|json>]", "Batch remove physical path mappings in deterministic input order"},
		{"  repair ref-counts [--batch] [--input <file>] [--fail-fast] [--output <text|json>]", "Recompute logical_file.ref_count from physical_file rows (explicit repair)"},
		{"  repair chunk-live-ref-counts [--batch] [--input <file>] [--fail-fast] [--output <text|json>]", "Recompute chunk.live_ref_count from file_chunk rows (explicit repair)"},
		{"  gc [options]", "Run garbage collection (state-changing unless --dry-run)"},
		{"    (no options)", "Remove unreferenced data"},
		{"    --dry-run", "Show what would be removed without deleting"},
		{"  stats [--output <human|json>] [--json] [--containers] [--trace|--trace-json]", "Show repository statistics (read-only); use --containers for opt-in container detail output"},
		{"  inspect <entity> <id> [--relations] [--reverse] [--deep] [--limit <n>] [--output <human|json>] [--json] [--trace|--trace-json]", "Inspect one entity (file|snapshot|chunk|container) through the read-only observability pipeline"},
		{"  verify [target] [fileID] [options]", "Observational layered integrity verification (assumes recovered state; verification phase is read-only; default: --standard)"},
		{"    [target] can be 'system' or 'file'", ""},
		{"    [options] can be '--standard', '--full', or '--deep'", ""},
		{"    no options defaults to '--standard'", ""},
		{"    verify system [options]", "Perform system-wide verification"},
		{"    verify file <fileID> [options]", "Perform verification for specific file"},
		{"  help", "Show this help message"},
		{"  version", "Show version information"},
		{"  list [--limit <count>] [--offset <count>]", "List stored logical files"},
		{"  search [filters] [--limit <count>] [--offset <count>]", "Search files by filters"},
		{"  snapshot create [<path> ...] [--id <snapshotID>] [--label <label>] [--from <snapshotID>] [--output <text|json>]", "Create a full snapshot (no paths) or partial snapshot (with paths)"},
		{"  snapshot restore <snapshotID> [<path> ...] [--mode ...] [--destination <path>] [--overwrite] [--strict] [--no-metadata] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <ts>] [--modified-before <ts>] [--output <text|json>]", "Restore full or partial content from snapshot_file history"},
		{"  snapshot list [--type <full|partial>] [--label <substring>] [--since <RFC3339|YYYY-MM-DD>] [--until <RFC3339|YYYY-MM-DD>] [--limit <count>] [--tree] [--output <text|json>]", "List snapshots with optional filters; use --tree for lineage view"},
		{"  snapshot show <snapshotID> [--limit <count>] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <ts>] [--modified-before <ts>] [--output <text|json>]", "Inspect one snapshot and list its files with optional query filters"},
		{"  snapshot stats [<snapshotID>] [--output <text|json>]", "Show global or per-snapshot statistics"},
		{"  snapshot delete <snapshotID> (--force|--dry-run) [--output <text|json>]", "Delete snapshot metadata; --dry-run shows a read-only impact preview"},
		{"  snapshot diff <baseSnapshotID> <targetSnapshotID> [--summary] [--filter <added|removed|modified>] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <ts>] [--modified-before <ts>] [--output <text|json>]", "Compare snapshots by path and logical_file_id; --summary returns counts only"},
		{"  simulate gc [--delete-snapshot <id>] [--containers] [--output <human|json>] [--json] [--trace|--trace-json]", "Preview actual GC reclaimability without mutations; use --containers for per-container detail"},
		{"  simulate <store|store-folder> <path>", "Dry-run store estimate without writing to storage (not proof of physical durability)"},
	})
	fmt.Println("    Filters:")
	fmt.Println("      --name <substring>")
	fmt.Println("      --min-size <bytes>")
	fmt.Println("      --max-size <bytes>")
	fmt.Println("      --limit <count>")
	fmt.Println("      --offset <count>")
	fmt.Println("    Snapshot path matching:")
	fmt.Println("      exact path: snapshot create docs/file.txt")
	fmt.Println("      directory prefix: snapshot create docs/")
	fmt.Println("    Snapshot identity:")
	fmt.Println("      snapshot_id is the system identifier (set via --id on create)")
	fmt.Println("      pass snapshot_id positionally to show/restore/stats/diff/delete")
	fmt.Println("      --label is optional metadata only and is never used for targeting")
	fmt.Println("      --from records lineage metadata only; it does not make a snapshot depend on its parent")
	fmt.Println("      --tree renders metadata lineage only")
	fmt.Println("      --summary returns count-only diff output (no entry list)")
	fmt.Println("      --dry-run performs a read-only preview and never writes data")
	fmt.Println("    Tracing:")
	fmt.Println("      --trace and --trace-json emit diagnostic events to stderr only")
	fmt.Println("      tracing never changes command results on stdout")
	fmt.Println("      missing lineage parent metadata is shown as Parent: (missing); snapshot data remains usable")
	fmt.Println("    Store codecs:")
	fmt.Println("      plain")
	fmt.Println("      aes-gcm")
	fmt.Println()
	fmt.Println("Environment Variables:")
	fmt.Println("  DB_HOST")
	fmt.Println("  DB_PORT")
	fmt.Println("  DB_USER")
	fmt.Println("  DB_PASSWORD")
	fmt.Println("  DB_NAME")
	fmt.Println("  DB_SSLMODE (default: disable)")
	fmt.Println("  COLDKEEP_DB_CONNECT_TIMEOUT_MS (default: 5000)")
	fmt.Println("  COLDKEEP_DB_OPERATION_TIMEOUT_MS (default: 300000)")
	fmt.Println("  COLDKEEP_DB_STATEMENT_TIMEOUT_MS (default: 30000)")
	fmt.Println("  COLDKEEP_DB_LOCK_TIMEOUT_MS (default: 5000)")
	fmt.Println("  COLDKEEP_DB_IDLE_IN_TX_TIMEOUT_MS (default: 60000)")
	fmt.Println("  COLDKEEP_DB_MAX_OPEN_CONNS (default: 25)")
	fmt.Println("  COLDKEEP_DB_MAX_IDLE_CONNS (default: 5)")
	fmt.Println("  COLDKEEP_DB_CONN_MAX_LIFETIME_MS (default: 1800000)")
	fmt.Println("  COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS (default: 300000)")
	fmt.Println("  COLDKEEP_STORAGE_DIR (default: ./storage/containers)")
	fmt.Println("  COLDKEEP_CONTAINER_MAX_SIZE_MB (default: 64)")
	fmt.Println("  COLDKEEP_BLOCK_TARGET_SIZE_MB (default: 1; advanced operator override for new writes)")
	fmt.Println("  COLDKEEP_LOGICAL_FILE_WAIT_MS (default: 100)")
	fmt.Println("  COLDKEEP_CHUNK_WAIT_MS (default: 100)")
	fmt.Println("  COLDKEEP_MAX_CLAIM_POLL_WAIT_MS (default: 2000)")
	fmt.Println("  COLDKEEP_MAX_CLAIM_WAIT_MS (default: 120000)")
	fmt.Println("  COLDKEEP_CODEC (default: aes-gcm)")
	fmt.Println("  COLDKEEP_KEY (required for aes-gcm)")
	fmt.Println("  COLDKEEP_STRICT_RECOVERY (default: true; recommended for production)")
	fmt.Println("    true: fail startup on suspicious orphan container conflicts (intentional trust-first behavior)")
	fmt.Println("    false: warn and continue (relaxed mode for messy/retrier/restart-race environments)")
	fmt.Println("  COLDKEEP_REUSE_SEMANTIC_VALIDATION (default: suspicious)")
	fmt.Println("    off: graph-only reuse checks (fastest, no payload/hash re-validation)")
	fmt.Println("    suspicious: deep semantic checks only for risk signals (recommended)")
	fmt.Println("    always: deep semantic checks for every reuse candidate (highest read/CPU cost)")
	fmt.Println("  COLDKEEP_QUIET_HEALTHY_STARTUP_RECOVERY (default: off)")
	fmt.Println("    true/1/yes/on: suppress healthy startup recovery logs in text mode")
	fmt.Println("    recovery logs still replay automatically when corrective actions/errors occur")
	fmt.Println("  Startup recovery is corrective/state-changing and runs automatically before: store, store-folder, restore, remove, repair, gc, stats, list, search, verify, snapshot")
	fmt.Println("  Verify is observational and assumes recovered state (its verification phase is read-only)")
	fmt.Println("  Doctor runs its own corrective recovery pass even if startup recovery already ran")
	fmt.Println("  Batch JSON contract (restore/remove --output json): status=ok|partial_failure|error")
	fmt.Println("    ok: no item failed")
	fmt.Println("    partial_failure: at least one item failed and at least one item succeeded or was planned")
	fmt.Println("    error: all executable items failed")
	fmt.Println("  Batch process exit contract (restore/remove):")
	fmt.Println("    exit 0: no item failed")
	fmt.Println("    exit 1: any item failed (partial_failure or error)")
	fmt.Println("    exit 2: usage/validation error before execution")
	fmt.Println("  Simulated mode is not proof of physical durability")
	fmt.Println()
	fmt.Println("Operator quick check:")
	fmt.Println("  coldkeep doctor --standard")
	fmt.Println("  Recommended operator health gate: run coldkeep doctor after significant operations.")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  coldkeep init")
	fmt.Println("  coldkeep config get default-chunker")
	fmt.Println("  coldkeep config set default-chunker v2-fastcdc")
	fmt.Println("  coldkeep doctor --full")
	fmt.Println("  coldkeep store myfile.bin")
	fmt.Println("  coldkeep store --codec aes-gcm myfile.bin")
	fmt.Println("  coldkeep store-folder --codec plain ./samples")
	fmt.Println("  coldkeep list --limit 50 --offset 100")
	fmt.Println("  coldkeep search --name report --min-size 1024 --limit 25")
	fmt.Println("  coldkeep restore 12 ./restored")
	fmt.Println("  coldkeep remove 12")
	fmt.Println("  coldkeep repair ref-counts")
	fmt.Println("  coldkeep verify system --full")
	fmt.Println("  coldkeep verify file 12 --deep")
	fmt.Println("  coldkeep snapshot create")
	fmt.Println("  coldkeep snapshot create docs/ report.txt --label release-2026-04")
	fmt.Println("  coldkeep snapshot create --id day2 --from day1")
	fmt.Println("  coldkeep snapshot restore snap-123 --mode prefix --destination ./restored")
	fmt.Println("  coldkeep snapshot restore snap-123 docs/ --mode prefix --destination ./restored")
	fmt.Println("  coldkeep snapshot restore snap-123 docs/a.txt --mode override --destination ./restored/a.txt")
	fmt.Println("  coldkeep snapshot list --type full --limit 10")
	fmt.Println("  coldkeep snapshot list --tree")
	fmt.Println("  coldkeep snapshot show snap-123 --limit 50")
	fmt.Println("  coldkeep snapshot stats")
	fmt.Println("  coldkeep snapshot stats snap-123")
	fmt.Println("  coldkeep snapshot delete snap-123 --force")
	fmt.Println("  coldkeep snapshot delete snap-123 --dry-run")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --summary")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --filter modified")
	fmt.Println("  coldkeep snapshot show snap-123 --prefix docs/")
	fmt.Println("  coldkeep snapshot show snap-123 --pattern \"*.txt\" --min-size 1024")
	fmt.Println("  coldkeep snapshot restore snap-123 --prefix docs/ --mode prefix --destination ./restored")
	fmt.Println("  coldkeep snapshot restore snap-123 --pattern \"*.txt\" --overwrite --mode original")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --prefix docs/ --filter added")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --regex \"\\.log$\"")
	fmt.Println("  coldkeep gc --dry-run")
	fmt.Println("  coldkeep stats")
	fmt.Println("  coldkeep stats --json")
	fmt.Println("  coldkeep stats --containers")
	fmt.Println("  coldkeep simulate store myfile.bin")
	fmt.Println("  coldkeep simulate store-folder --codec aes-gcm ./samples")
}

func printStatsHelp() {
	fmt.Println("Usage:")
	fmt.Println("  coldkeep stats [--output <human|json>] [--json] [--containers] [--trace|--trace-json]")
	fmt.Println()
	fmt.Println("Show repository statistics through the observability pipeline (read-only).")
	fmt.Println()
	fmt.Println("JSON support:")
	fmt.Println("  --json is shorthand for --output json")
	fmt.Println("  output keys are stable for machine parsing")
	fmt.Println("Trace support:")
	fmt.Println("  --trace or --trace-json emits diagnostics to stderr only")
	fmt.Println("  trace output never changes command stdout")
	fmt.Println("Deterministic output guarantee:")
	fmt.Println("  for identical repository state and flags, rendered output order is deterministic")
}

func printInspectHelp() {
	fmt.Println("Usage:")
	fmt.Println("  coldkeep inspect repository [--relations] [--reverse] [--deep] [--limit <n>] [--output <human|json>] [--json] [--trace|--trace-json]")
	fmt.Println("  coldkeep inspect <entity> <id> [--relations] [--reverse] [--deep] [--limit <n>] [--output <human|json>] [--json] [--trace|--trace-json]")
	fmt.Println()
	fmt.Println("Inspect one entity through the observability pipeline (read-only).")
	fmt.Println("Supported entities: repository, file (alias: logical-file), snapshot, chunk, container")
	fmt.Println()
	fmt.Println("Traversal options:")
	fmt.Println("  --relations includes forward linked records")
	fmt.Println("  --reverse includes reverse references where available")
	fmt.Println("  --deep enables deeper traversal/detail output")
	fmt.Println("  --limit bounds deep traversal output; use it with --deep to keep output manageable")
	fmt.Println("JSON support:")
	fmt.Println("  --json is shorthand for --output json")
	fmt.Println("  output schema is stable for automation")
	fmt.Println("Trace support:")
	fmt.Println("  --trace or --trace-json emits diagnostics to stderr only")
	fmt.Println("  trace output never changes command stdout")
	fmt.Println("Deterministic output guarantee:")
	fmt.Println("  for identical inputs and flags, sections and records are emitted deterministically")
}

func printSimulateHelp() {
	fmt.Println("Usage:")
	fmt.Println("  coldkeep simulate gc [--delete-snapshot <id>] [--containers] [--output <human|json>] [--json] [--trace|--trace-json]")
	fmt.Println("  coldkeep simulate <store|store-folder> [--codec <codec>] <path>")
	fmt.Println()
	fmt.Println("JSON support:")
	fmt.Println("  --json is shorthand for --output json")
	fmt.Println("  simulation payloads keep a stable schema for automation")
	fmt.Println("Trace support:")
	fmt.Println("  --trace or --trace-json emits diagnostics to stderr only")
	fmt.Println("  trace output never changes command stdout")
	fmt.Println("Deterministic output guarantee:")
	fmt.Println("  for identical repository state, assumptions, and flags, simulation output is deterministic")
	fmt.Println("Simulation safety guarantee:")
	fmt.Println("  simulate commands are observational and never modify repository state")
	fmt.Println()
	fmt.Println("Example")
	fmt.Println("simulate gc")
	fmt.Println()
	fmt.Println("Preview garbage collection effects without modifying data.")
	fmt.Println()
	fmt.Println("This command computes exactly what GC would consider reclaimable,")
	fmt.Println("including optional hypothetical snapshot deletion.")
	fmt.Println()
	fmt.Println("No state is modified.")
}

func printSimulateGCHelp() {
	fmt.Println("Usage:")
	fmt.Println("  coldkeep simulate gc [--delete-snapshot <id>] [--containers] [--output <human|json>] [--json] [--trace|--trace-json]")
	fmt.Println()
	fmt.Println("Preview actual GC reclaimability without modifying repository state (read-only).")
	fmt.Println("It uses the shared GC planning layer (gc.BuildPlan) to compute exact reclaimability.")
	fmt.Println("This reflects real GC behavior, including fully-dead active containers that real GC")
	fmt.Println("would also reclaim. It is not equivalent to 'gc --dry-run', which uses a lighter path.")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --delete-snapshot <id> simulates GC after excluding the given snapshot from simulated roots")
	fmt.Println("  --containers includes per-container detail in the rendered result")
	fmt.Println("JSON support:")
	fmt.Println("  --json is shorthand for --output json")
	fmt.Println("  simulation payloads keep a stable schema for automation")
	fmt.Println("Trace support:")
	fmt.Println("  --trace or --trace-json emits diagnostics to stderr only")
	fmt.Println("  trace output never changes command stdout")
	fmt.Println("Safety guarantee:")
	fmt.Println("  simulate gc is exact for GC reclaimability decisions and never mutates repository state")
}

func printHelpRows(rows [][2]string) {
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, row := range rows {
		if row[1] == "" {
			_, _ = fmt.Fprintln(writer, row[0])
			continue
		}
		_, _ = fmt.Fprintf(writer, "%s\t%s\n", row[0], row[1])
	}
	_ = writer.Flush()
}

func parseCommandLine(args []string, valueFlags map[string]bool) (parsedCommandLine, error) {
	if len(args) == 0 {
		return parsedCommandLine{}, usageErrorf("missing command")
	}

	parsed := parsedCommandLine{
		method:      args[0],
		positionals: make([]string, 0),
		flags:       make(map[string][]string),
	}

	seenSingletons := make(map[string]struct{})

	for i := 1; i < len(args); i++ {
		arg := args[i]

		if arg == "--" {
			parsed.positionals = append(parsed.positionals, args[i+1:]...)
			break
		}

		if !strings.HasPrefix(arg, "--") {
			parsed.positionals = append(parsed.positionals, arg)
			continue
		}

		flagToken := strings.TrimPrefix(arg, "--")
		if name, value, found := strings.Cut(flagToken, "="); found {
			if valueFlags[name] && isKnownFlagTokenValue(value, valueFlags) {
				return parsedCommandLine{}, usageErrorf("missing value for --%s", name)
			}
			if flagsWithoutValues[name] {
				if _, err := strconv.ParseBool(strings.TrimSpace(value)); err != nil {
					return parsedCommandLine{}, usageErrorf("invalid boolean value for --%s: %q", name, value)
				}
			}
			if err := rejectDuplicateSingletonFlag(name, seenSingletons, valueFlags); err != nil {
				return parsedCommandLine{}, err
			}
			parsed.flags[name] = append(parsed.flags[name], value)
			continue
		}

		if valueFlags[flagToken] {
			if i+1 >= len(args) {
				return parsedCommandLine{}, usageErrorf("missing value for --%s", flagToken)
			}
			if isKnownFlagTokenValue(args[i+1], valueFlags) {
				return parsedCommandLine{}, usageErrorf("missing value for --%s", flagToken)
			}
			if err := rejectDuplicateSingletonFlag(flagToken, seenSingletons, valueFlags); err != nil {
				return parsedCommandLine{}, err
			}
			i++
			parsed.flags[flagToken] = append(parsed.flags[flagToken], args[i])
			continue
		}

		if err := rejectDuplicateSingletonFlag(flagToken, seenSingletons, valueFlags); err != nil {
			return parsedCommandLine{}, err
		}

		parsed.flags[flagToken] = append(parsed.flags[flagToken], "")
	}

	return parsed, nil
}

func rejectDuplicateSingletonFlag(name string, seen map[string]struct{}, valueFlags map[string]bool) error {
	canonical := canonicalFlagName(name)
	if !isSingletonFlag(canonical, valueFlags) {
		return nil
	}
	if _, ok := seen[canonical]; ok {
		return usageErrorf("duplicate singleton flag: --%s", canonical)
	}
	seen[canonical] = struct{}{}
	return nil
}

func canonicalFlagName(name string) string {
	switch name {
	case "dryRun":
		return "dry-run"
	case "failFast":
		return "fail-fast"
	default:
		return name
	}
}

func isSingletonFlag(name string, valueFlags map[string]bool) bool {
	if repeatableFlags[name] {
		return false
	}
	if valueFlags[name] {
		return true
	}
	return flagsWithoutValues[name]
}

func isKnownFlagTokenValue(value string, valueFlags map[string]bool) bool {
	if !strings.HasPrefix(value, "--") {
		return false
	}

	name := strings.TrimPrefix(value, "--")
	if i := strings.IndexByte(name, '='); i >= 0 {
		name = name[:i]
	}
	if strings.TrimSpace(name) == "" {
		return false
	}

	if valueFlags[name] {
		return true
	}

	return flagsWithoutValues[name]
}

func (parsed parsedCommandLine) lastFlagValue(name string) (string, bool) {
	values, ok := parsed.flags[name]
	if !ok || len(values) == 0 {
		return "", false
	}

	return values[len(values)-1], true
}

func (parsed parsedCommandLine) flagValues(name string) []string {
	values, ok := parsed.flags[name]
	if !ok || len(values) == 0 {
		return nil
	}

	return append([]string(nil), values...)
}

func (parsed parsedCommandLine) hasFlag(names ...string) bool {
	for _, name := range names {
		if values, ok := parsed.flags[name]; ok && len(values) > 0 {
			if !flagsWithoutValues[name] {
				return true
			}

			last := strings.TrimSpace(values[len(values)-1])
			if last == "" {
				return true
			}

			parsedValue, err := strconv.ParseBool(last)
			if err != nil {
				return false
			}
			if parsedValue {
				return true
			}
		}
	}

	return false
}

func ensureAllowedFlags(parsed parsedCommandLine, allowed ...string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, flag := range allowed {
		allowedSet[flag] = struct{}{}
	}

	var unknown []string
	for flag := range parsed.flags {
		if _, ok := allowedSet[flag]; !ok {
			unknown = append(unknown, flag)
		}
	}

	if len(unknown) == 0 {
		return nil
	}

	sort.Strings(unknown)
	return usageErrorf("unknown flag(s) for %s: %s", parsed.method, strings.Join(unknown, ", "))
}

func rejectBlankFlagValues(parsed parsedCommandLine, names ...string) error {
	for _, name := range names {
		for _, value := range parsed.flagValues(name) {
			if strings.TrimSpace(value) == "" {
				return usageErrorf("--%s cannot be empty", name)
			}
		}
	}
	return nil
}

func validateNonNegativeIntegerFlag(parsed parsedCommandLine, name string) error {
	value, ok := parsed.lastFlagValue(name)
	if !ok {
		return nil
	}

	parsedValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsedValue < 0 {
		return usageErrorf("invalid --%s value %q: must be a non-negative integer", name, value)
	}
	if name == "limit" && parsedValue > listing.MaxPaginationLimit {
		return usageErrorf("invalid --%s value %q: must be <= %d", name, value, listing.MaxPaginationLimit)
	}

	return nil
}

func listArgs(parsed parsedCommandLine) []string {
	args := make([]string, 0, 4)

	if value, ok := parsed.lastFlagValue("limit"); ok {
		args = append(args, "--limit", value)
	}
	if value, ok := parsed.lastFlagValue("offset"); ok {
		args = append(args, "--offset", value)
	}

	return args
}

func searchArgs(parsed parsedCommandLine) []string {
	orderedFlags := []string{"name", "min-size", "max-size"}
	args := make([]string, 0)

	for _, flag := range orderedFlags {
		for _, value := range parsed.flags[flag] {
			args = append(args, "--"+flag)
			if value != "" {
				args = append(args, value)
			}
		}
	}

	if value, ok := parsed.lastFlagValue("limit"); ok {
		args = append(args, "--limit", value)
	}
	if value, ok := parsed.lastFlagValue("offset"); ok {
		args = append(args, "--offset", value)
	}

	args = append(args, parsed.positionals...)
	return args
}

func parseVerifyLevel(parsed parsedCommandLine) (verify.VerifyLevel, error) {
	selected := make([]verify.VerifyLevel, 0, 1)
	if parsed.hasFlag("fast") {
		selected = append(selected, verify.VerifyFast)
	}
	if parsed.hasFlag("standard") {
		selected = append(selected, verify.VerifyStandard)
	}
	if parsed.hasFlag("full") {
		selected = append(selected, verify.VerifyFull)
	}
	if parsed.hasFlag("deep") {
		selected = append(selected, verify.VerifyDeep)
	}

	if len(selected) > 1 {
		return verify.VerifyStandard, usageErrorf("multiple verify levels provided")
	}

	positionalLevel := ""
	if len(parsed.positionals) == 2 && parsed.positionals[0] == "system" {
		positionalLevel = parsed.positionals[1]
	}
	if len(parsed.positionals) == 3 && parsed.positionals[0] == "file" {
		positionalLevel = parsed.positionals[2]
	}

	if positionalLevel != "" {
		if len(selected) > 0 {
			return verify.VerifyStandard, usageErrorf("verify level provided both as flag and positional argument")
		}

		switch positionalLevel {
		case "fast":
			return verify.VerifyFast, nil
		case "standard":
			return verify.VerifyStandard, nil
		case "full":
			return verify.VerifyFull, nil
		case "deep":
			return verify.VerifyDeep, nil
		default:
			return verify.VerifyStandard, usageErrorf("unknown verify level: %s", positionalLevel)
		}
	}

	if len(selected) == 1 {
		return selected[0], nil
	}

	return verify.VerifyStandard, nil
}

func isVerifyLevelName(value string) bool {
	switch strings.TrimSpace(value) {
	case "fast", "standard", "full", "deep":
		return true
	default:
		return false
	}
}
