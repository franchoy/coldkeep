package maintenance

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/graph"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/retention"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/verify"
)

// isContainerFKViolation returns true when err is a foreign-key constraint
// failure from either PostgreSQL ("violates foreign key") or SQLite
// ("FOREIGN KEY constraint failed").
func isContainerFKViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "foreign key constraint") ||
		strings.Contains(msg, "violates foreign key")
}

var gcConnectDB = db.ConnectDB

var gcAdvisoryUnlock = func(ctx context.Context, conn *sql.Conn) (bool, error) {
	var unlocked bool
	err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", gcAdvisoryLockID).Scan(&unlocked)
	return unlocked, err
}

var gcPhysicalIntegrityCheck = func(dbconn *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
	return verify.CheckPhysicalFileGraphIntegrity(dbconn)
}

var gcComputeReachability = func(ctx context.Context, dbconn *sql.DB) (*retention.ReachabilitySummary, error) {
	return computeGCReachabilityFromCatalog(ctx, dbconn)
}

var gcMarkReachableChunks = func(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	return MarkReachableChunks(ctx, dbconn)
}

var gcLoadLivePackedBlockIDs = func(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	return LoadLivePackedBlockIDs(ctx, dbconn)
}

var gcLoadLiveLegacyContainerIDs = func(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	return LoadLiveLegacyContainerIDs(ctx, dbconn)
}

type livePhysicalUnits struct {
	LegacyLiveContainerIDs map[int64]struct{}
	PackedLiveBlockIDs     map[int64]struct{}
}

// GCResult contains structured metadata about a GC run.
// Non-dry-run GC is state-changing: it deletes unreferenced metadata rows and
// container files. Dry-run is read-only and only reports what would be removed.
type GCResult struct {
	DryRun                       bool     `json:"dry_run"`
	AffectedContainers           int      `json:"affected_containers"`
	ContainerFilenames           []string `json:"container_filenames"`
	BytesReclaimed               int64    `json:"bytes_reclaimed"`
	SnapshotRetainedContainers   int      `json:"snapshot_retained_containers"`
	SnapshotRetainedLogicalFiles int      `json:"snapshot_retained_logical_files"`
	RetainedCurrentOnlyLogical   int      `json:"retained_current_only_logical_files"`
	RetainedSnapshotOnlyLogical  int      `json:"retained_snapshot_only_logical_files"`
	RetainedSharedLogical        int      `json:"retained_shared_logical_files"`
}

type gcDispatchUnitKind string

const (
	gcDispatchSealedContainer gcDispatchUnitKind = "sealed_container"
	gcDispatchActiveContainer gcDispatchUnitKind = "active_container"
)

type gcDispatchUnit struct {
	Kind        gcDispatchUnitKind
	ContainerID int64
	Filename    string
}

type gcExecutionOptions struct {
	fs               fsx.FS
	dispatchObserver func(gcDispatchUnit)
	workers          int
}

func (opts gcExecutionOptions) effectiveWorkers() int {
	if opts.workers > 1 {
		return opts.workers
	}
	return 1
}

func (opts gcExecutionOptions) effectiveFS() fsx.FS {
	if opts.fs != nil {
		return opts.fs
	}
	return fsx.Default()
}

func (opts gcExecutionOptions) observeDispatch(unit gcDispatchUnit) {
	if opts.dispatchObserver != nil {
		opts.dispatchObserver(unit)
	}
}

func RunGCWithContainersDir(dryRun bool, containersDir string) error {
	_, err := RunGCWithContainersDirResult(dryRun, containersDir)
	return err
}

// RunGCWithContainersDirResult implements GC under the v1.2 audited-root model.
// It opens the global DB connection via gcConnectDB and delegates to RunGCWithDB.
// Both real and dry-run GC are subject to the same pre-flight gate.
func RunGCWithContainersDirResult(dryRun bool, containersDir string) (GCResult, error) {
	dbconn, err := gcConnectDB()
	if err != nil {
		return GCResult{}, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	return RunGCWithDB(ctx, dbconn, dryRun, containersDir)
}

// RunGCWithDB implements GC under the v1.2 audited-root model using a
// caller-provided DB connection and context. This is the DB-aware entry point
// used by the engine facade.
//
//  1. Acquire advisory lock (singleton enforcement).
//
//  2. Pre-flight: run CheckPhysicalFileGraphIntegrity. GC is unconditionally
//     refused if any of the following conditions are present:
//     - orphan physical_file rows (no matching logical_file)
//     - logical_file.ref_count mismatches vs COUNT(physical_file)
//     - negative logical_file.ref_count values
//     This guard ensures GC never reasons about container liveness on a drifted
//     graph; the audited physical_file layer is the confirmed root of truth before
//     any deletion decisions are made.
//
//  3. Identify sealed, non-quarantined containers and evaluate liveness using
//     chunk.live_ref_count and chunk.pin_count as the immediate deletion
//     criterion. This is correct because steps 1–2 guarantee the physical-root
//     graph is coherent and chunk ref counts are trustworthy inputs.
//
// If integrity issues are found at step 2, the error message directs operators
// to run 'repair ref-counts' before retrying GC.
// Both real and dry-run GC are subject to the same pre-flight gate.
func RunGCWithDB(ctx context.Context, dbconn *sql.DB, dryRun bool, containersDir string) (result GCResult, err error) {
	return RunGCWithDBWorkers(ctx, dbconn, dryRun, containersDir, 1)
}

// RunGCWithDBWorkers is the worker-aware Engine entry. Workers is an upper
// bound on concurrently executing physical-container units; values below two
// retain the serial compatibility behavior.
func RunGCWithDBWorkers(ctx context.Context, dbconn *sql.DB, dryRun bool, containersDir string, workers int) (result GCResult, err error) {
	return runGCWithDBOptions(ctx, dbconn, dryRun, containersDir, gcExecutionOptions{workers: workers})
}

func runGCWithDBOptions(ctx context.Context, dbconn *sql.DB, dryRun bool, containersDir string, opts gcExecutionOptions) (result GCResult, err error) {
	result.DryRun = dryRun

	fsys := opts.effectiveFS()

	advisoryLock, err := acquireGCAdvisoryLock(ctx, dbconn, dryRun)
	if err != nil {
		return GCResult{}, err
	}
	defer func() {
		err = errors.Join(err, advisoryLock.release())
	}()

	if err := gcIntegrityPreFlight(dbconn); err != nil {
		return GCResult{}, err
	}
	if !dryRun {
		if _, err := cleanupRootlessLogicalRecipes(ctx, dbconn); err != nil {
			return GCResult{}, fmt.Errorf("cleanup rootless logical recipes: %w", err)
		}
	}

	state, err := buildGCPreFlightState(ctx, dbconn)
	if err != nil {
		return GCResult{}, err
	}
	applyRetentionCountsToResult(state.reachability, &result)

	sealedPlan, err := materializeSealedGCPlan(ctx, dbconn)
	if err != nil {
		return GCResult{}, err
	}
	sealedResults := executeGCPlan(ctx, sealedPlan, opts, func(unit gcPlannedUnit) gcUnitResult {
		outcome, physicalBytes, unitErr := processSealedContainerForGC(
			ctx,
			dbconn,
			unit.dispatch.ContainerID,
			unit.dispatch.Filename,
			dryRun,
			state,
			containersDir,
			fsys,
		)
		return gcUnitResult{plan: unit, outcome: outcome, physicalBytes: physicalBytes, err: unitErr}
	})
	if sealedErr := aggregateGCUnitResults(sealedResults, ctx.Err(), &result); sealedErr != nil {
		return result, sealedErr
	}

	if !dryRun {
		activeCandidates, activeErr := queryFullyDeadActiveContainers(ctx, dbconn)
		if activeErr != nil {
			return result, fmt.Errorf("cleanup fully dead active containers: %w", activeErr)
		}
		activePlan := planActiveGCUnits(activeCandidates, len(sealedPlan))
		activeResults := executeGCPlan(ctx, activePlan, opts, func(unit gcPlannedUnit) gcUnitResult {
			outcome, physicalBytes, unitErr := sweepDeadActiveContainerResult(
				ctx,
				dbconn,
				containersDir,
				state.reachableChunks,
				state.liveUnits,
				fsys,
				unit.dispatch.ContainerID,
				unit.dispatch.Filename,
			)
			return gcUnitResult{plan: unit, outcome: outcome, physicalBytes: physicalBytes, err: unitErr}
		})
		if activeErr := aggregateGCUnitResults(activeResults, ctx.Err(), &result); activeErr != nil {
			return result, fmt.Errorf("cleanup fully dead active containers: %w", activeErr)
		}
	}

	return result, nil
}

// gcPreFlightState holds the snapshot-reachability and liveness data computed
// once before the sealed-container sweep loop.
type gcPreFlightState struct {
	reachableChunks map[int64]struct{}
	liveUnits       livePhysicalUnits
	reachability    *retention.ReachabilitySummary
}

// sealedContainerGCResult describes what happened when one sealed container
// was evaluated for GC.
type sealedContainerGCResult int

const (
	sealedContainerSkipped  sealedContainerGCResult = iota // rolled back, no action
	sealedContainerRetained                                // retained by snapshot safety net
	sealedContainerAffected                                // deleted (or dry-run counted)
)

type gcPlannedUnit struct {
	index    int
	dispatch gcDispatchUnit
}

type gcUnitResult struct {
	plan          gcPlannedUnit
	outcome       sealedContainerGCResult
	physicalBytes int64
	err           error
}

func executeGCPlan(ctx context.Context, plan []gcPlannedUnit, opts gcExecutionOptions, execute func(gcPlannedUnit) gcUnitResult) []gcUnitResult {
	if len(plan) == 0 {
		return nil
	}
	workers := opts.effectiveWorkers()
	if workers > len(plan) {
		workers = len(plan)
	}

	completed := make(chan gcUnitResult, workers)
	results := make([]gcUnitResult, len(plan))
	started := make([]bool, len(plan))
	next := 0
	inFlight := 0
	stopped := false

	dispatch := func(unit gcPlannedUnit) {
		localIndex := next
		started[localIndex] = true
		next++
		inFlight++
		go func() {
			opts.observeDispatch(unit.dispatch)
			completed <- execute(unit)
		}()
	}

	for inFlight < workers && next < len(plan) {
		if ctx.Err() != nil {
			stopped = true
			break
		}
		dispatch(plan[next])
	}

	for inFlight > 0 {
		unitResult := <-completed
		inFlight--
		localIndex := unitResult.plan.index - plan[0].index
		results[localIndex] = unitResult
		if unitResult.err != nil || ctx.Err() != nil {
			stopped = true
		}
		if !stopped && next < len(plan) {
			dispatch(plan[next])
		}
	}

	ordered := make([]gcUnitResult, 0, len(plan))
	for i := range results {
		if started[i] {
			ordered = append(ordered, results[i])
		}
	}
	return ordered
}

func aggregateGCUnitResults(unitResults []gcUnitResult, callerErr error, result *GCResult) error {
	var joined []error
	overflowed := false
	for _, unitResult := range unitResults {
		if unitResult.outcome == sealedContainerRetained {
			result.SnapshotRetainedContainers++
		}
		if unitResult.outcome == sealedContainerAffected {
			result.AffectedContainers++
			result.ContainerFilenames = append(result.ContainerFilenames, unitResult.plan.dispatch.Filename)
			if !overflowed {
				if result.BytesReclaimed > math.MaxInt64-unitResult.physicalBytes {
					overflowed = true
					joined = append(joined, fmt.Errorf(
						"GC byte-accounting overflow at plan index %d kind=%s container_id=%d filename=%q",
						unitResult.plan.index,
						unitResult.plan.dispatch.Kind,
						unitResult.plan.dispatch.ContainerID,
						unitResult.plan.dispatch.Filename,
					))
				} else {
					result.BytesReclaimed += unitResult.physicalBytes
				}
			}
		}
		if unitResult.err != nil {
			joined = append(joined, unitResult.err)
		}
	}
	combined := errors.Join(joined...)
	if callerErr != nil && !errors.Is(combined, callerErr) {
		combined = errors.Join(combined, callerErr)
	}
	return combined
}

// gcAdvisoryLock owns the dedicated PostgreSQL session carrying GC's
// session-level advisory lock. The connection must not return to the pool until
// a successful unlock, or until it has been discarded after uncertain cleanup.
type gcAdvisoryLock struct {
	conn *sql.Conn
}

// acquireGCAdvisoryLock enforces the SQLite/PostgreSQL backend rules and, for
// PostgreSQL, acquires the advisory lock on one dedicated session.
func acquireGCAdvisoryLock(ctx context.Context, dbconn *sql.DB, dryRun bool) (*gcAdvisoryLock, error) {
	backend := db.BackendFromDB(dbconn)
	if backend == db.BackendSQLite {
		if !dryRun {
			return nil, fmt.Errorf("live GC is not supported on the SQLite backend; run with --dry-run to inspect GC candidates")
		}
		log.Println("gc: SQLite backend detected — skipping advisory lock (dry-run only)")
		return &gcAdvisoryLock{}, nil
	}
	if backend == db.BackendPostgres && dbconn.Stats().MaxOpenConnections == 1 {
		return nil, fmt.Errorf("PostgreSQL GC requires at least two database connections so its dedicated advisory-lock session does not exhaust the pool")
	}

	conn, err := dbconn.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to reserve advisory-lock session: %w", err)
	}

	var locked bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", gcAdvisoryLockID).Scan(&locked); err != nil {
		acquireErr := fmt.Errorf("failed to attempt advisory lock: %w", err)
		return nil, errors.Join(acquireErr, discardGCAdvisoryConn(conn))
	}
	if !locked {
		return nil, errors.Join(
			fmt.Errorf("GC already running (advisory lock held)"),
			closeGCAdvisoryConn(conn),
		)
	}
	return &gcAdvisoryLock{conn: conn}, nil
}

func (lock *gcAdvisoryLock) release() error {
	if lock == nil || lock.conn == nil {
		return nil
	}
	conn := lock.conn
	lock.conn = nil

	cleanupCtx, cleanupCancel := db.NewOperationContext(context.Background())
	defer cleanupCancel()
	unlocked, unlockErr := gcAdvisoryUnlock(cleanupCtx, conn)
	if unlockErr == nil && unlocked {
		return closeGCAdvisoryConn(conn)
	}
	if unlockErr == nil {
		unlockErr = fmt.Errorf("PostgreSQL advisory unlock returned false for the owned GC lock")
	} else {
		unlockErr = fmt.Errorf("failed to release PostgreSQL GC advisory lock: %w", unlockErr)
	}
	return errors.Join(unlockErr, discardGCAdvisoryConn(conn))
}

func closeGCAdvisoryConn(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}
	if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("close PostgreSQL GC advisory-lock session: %w", err)
	}
	return nil
}

// discardGCAdvisoryConn prevents a session whose lock state is uncertain from
// returning to database/sql's reusable pool. driver.ErrBadConn is the expected
// signal used by Conn.Raw to make that connection unusable.
func discardGCAdvisoryConn(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}
	err := conn.Raw(func(any) error { return driver.ErrBadConn })
	if err != nil && !errors.Is(err, driver.ErrBadConn) && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("discard PostgreSQL GC advisory-lock session: %w", err)
	}
	return nil
}

// gcIntegrityPreFlight runs CheckPhysicalFileGraphIntegrity and returns an
// invariant error if the graph has orphans, ref-count mismatches, or negative
// ref counts. GC must be refused when any of these conditions are present.
func gcIntegrityPreFlight(dbconn *sql.DB) error {
	integrity, err := gcPhysicalIntegrityCheck(dbconn)
	if err != nil {
		if _, ok := invariants.Code(err); ok {
			return invariants.New(
				invariants.CodeGCRefusedIntegrity,
				fmt.Sprintf(
					"GC refused: physical_file graph integrity issues detected (orphan_rows=%d ref_count_mismatches=%d negative_ref_counts=%d); run 'repair ref-counts' first",
					integrity.OrphanPhysicalFileRows, integrity.LogicalRefCountMismatches, integrity.NegativeLogicalRefCounts,
				),
				err,
			)
		}
		return fmt.Errorf("GC pre-flight integrity check failed: %w", err)
	}
	if integrity.OrphanPhysicalFileRows > 0 || integrity.LogicalRefCountMismatches > 0 || integrity.NegativeLogicalRefCounts > 0 {
		return invariants.New(
			invariants.CodeGCRefusedIntegrity,
			fmt.Sprintf(
				"GC refused: physical_file graph integrity issues detected (orphan_rows=%d ref_count_mismatches=%d negative_ref_counts=%d); run 'repair ref-counts' first",
				integrity.OrphanPhysicalFileRows, integrity.LogicalRefCountMismatches, integrity.NegativeLogicalRefCounts,
			),
			nil,
		)
	}
	return nil
}

const gcRootlessRecipeCleanupLimit = 1000

// cleanupRootlessLogicalRecipes removes a bounded batch of completed logical
// recipes that have no current or snapshot root and no pinned recipe chunk.
// It runs before chunk sweep so the file_chunk -> chunk RESTRICT edge cannot
// obstruct physical reclamation. Snapshot deletion itself remains metadata-only.
func cleanupRootlessLogicalRecipes(ctx context.Context, dbconn *sql.DB) (deleted int64, err error) {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	rows, err := tx.QueryContext(ctx, `
		SELECT lf.id
		FROM logical_file lf
		WHERE lf.status = $1
		AND NOT EXISTS (
			SELECT 1 FROM physical_file pf WHERE pf.logical_file_id = lf.id
		)
		AND NOT EXISTS (
			SELECT 1 FROM snapshot_file sf WHERE sf.logical_file_id = lf.id
		)
		AND NOT EXISTS (
			SELECT 1
			FROM file_chunk fc
			JOIN chunk ch ON ch.id = fc.chunk_id
			WHERE fc.logical_file_id = lf.id AND ch.pin_count > 0
		)
		ORDER BY lf.id
		LIMIT $2`, filestate.LogicalFileCompleted, gcRootlessRecipeCleanupLimit)
	if err != nil {
		return 0, err
	}
	candidates := make([]int64, 0)
	for rows.Next() {
		var logicalFileID int64
		if err := rows.Scan(&logicalFileID); err != nil {
			_ = rows.Close()
			return 0, err
		}
		candidates = append(candidates, logicalFileID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}

	for _, logicalFileID := range candidates {
		lockLogicalQuery := db.QueryWithOptionalForUpdate(dbconn, `
			SELECT lf.id
			FROM logical_file lf
			WHERE lf.id = $1
			AND lf.status = $2
			AND NOT EXISTS (
				SELECT 1 FROM physical_file pf WHERE pf.logical_file_id = lf.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM snapshot_file sf WHERE sf.logical_file_id = lf.id
			)`)
		var lockedLogicalFileID int64
		if err := tx.QueryRowContext(ctx, lockLogicalQuery, logicalFileID, filestate.LogicalFileCompleted).Scan(&lockedLogicalFileID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return 0, err
		}

		lockChunksQuery := db.QueryWithOptionalForUpdate(dbconn, `
			SELECT ch.id, ch.pin_count
			FROM file_chunk fc
			JOIN chunk ch ON ch.id = fc.chunk_id
			WHERE fc.logical_file_id = $1
			ORDER BY ch.id, fc.chunk_order`)
		chunkRows, err := tx.QueryContext(ctx, lockChunksQuery, logicalFileID)
		if err != nil {
			return 0, err
		}
		pinned := false
		for chunkRows.Next() {
			var chunkID int64
			var pinCount int64
			if err := chunkRows.Scan(&chunkID, &pinCount); err != nil {
				_ = chunkRows.Close()
				return 0, err
			}
			if pinCount > 0 {
				pinned = true
			}
		}
		if err := chunkRows.Err(); err != nil {
			_ = chunkRows.Close()
			return 0, err
		}
		if err := chunkRows.Close(); err != nil {
			return 0, err
		}
		if pinned {
			continue
		}

		result, err := tx.ExecContext(ctx, `
			DELETE FROM logical_file
			WHERE id = $1
			AND status = $2
			AND NOT EXISTS (
				SELECT 1 FROM physical_file pf WHERE pf.logical_file_id = logical_file.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM snapshot_file sf WHERE sf.logical_file_id = logical_file.id
			)
			AND NOT EXISTS (
				SELECT 1
				FROM file_chunk fc
				JOIN chunk ch ON ch.id = fc.chunk_id
				WHERE fc.logical_file_id = logical_file.id AND ch.pin_count > 0
			)`, logicalFileID, filestate.LogicalFileCompleted)
		if err != nil {
			return 0, err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		if rowsAffected > 1 {
			return 0, fmt.Errorf("rootless logical recipe cleanup id=%d deleted %d rows", logicalFileID, rowsAffected)
		}
		deleted += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

// buildGCPreFlightState computes the snapshot-reachability summary, the
// reachable chunk set, and the live physical unit maps used throughout the GC
// container sweep.
func buildGCPreFlightState(ctx context.Context, dbconn *sql.DB) (gcPreFlightState, error) {
	reachability, err := gcComputeReachability(ctx, dbconn)
	if err != nil {
		return gcPreFlightState{}, fmt.Errorf("GC pre-flight: failed to compute reachability summary: %w", err)
	}
	reachableChunks, err := gcMarkReachableChunks(ctx, dbconn)
	if err != nil {
		return gcPreFlightState{}, fmt.Errorf("GC pre-flight: failed to mark reachable chunks: %w", err)
	}
	livePackedBlockIDs, err := gcLoadLivePackedBlockIDs(ctx, dbconn)
	if err != nil {
		return gcPreFlightState{}, fmt.Errorf("GC pre-flight: failed to load live packed blocks: %w", err)
	}
	liveLegacyContainerIDs, err := gcLoadLiveLegacyContainerIDs(ctx, dbconn)
	if err != nil {
		return gcPreFlightState{}, fmt.Errorf("GC pre-flight: failed to load live legacy containers: %w", err)
	}
	return gcPreFlightState{
		reachableChunks: reachableChunks,
		liveUnits: livePhysicalUnits{
			LegacyLiveContainerIDs: liveLegacyContainerIDs,
			PackedLiveBlockIDs:     livePackedBlockIDs,
		},
		reachability: reachability,
	}, nil
}

// applyRetentionCountsToResult populates the snapshot-retention counters in
// result from the pre-computed reachability summary.
func applyRetentionCountsToResult(reachability *retention.ReachabilitySummary, result *GCResult) {
	result.SnapshotRetainedLogicalFiles = len(reachability.SnapshotLogicalIDs)
	classification := retention.ClassifyRetention(reachability)
	result.RetainedCurrentOnlyLogical = len(classification.CurrentOnly)
	result.RetainedSnapshotOnlyLogical = len(classification.SnapshotOnly)
	result.RetainedSharedLogical = len(classification.Shared)
}

// materializeSealedGCPlan scans and closes the candidate query before any unit
// transaction begins. The returned slice is the authoritative execution order.
func materializeSealedGCPlan(ctx context.Context, dbconn *sql.DB) ([]gcPlannedUnit, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename
		FROM container WHERE quarantine = FALSE AND sealed = TRUE AND sealing = FALSE
		ORDER BY id ASC, filename ASC
	`)
	if err != nil {
		return nil, err
	}

	var units []gcDispatchUnit
	for rows.Next() {
		var unit gcDispatchUnit
		unit.Kind = gcDispatchSealedContainer
		if err := rows.Scan(&unit.ContainerID, &unit.Filename); err != nil {
			_ = rows.Close()
			return nil, err
		}
		units = append(units, unit)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	sort.Slice(units, func(i, j int) bool {
		if units[i].ContainerID != units[j].ContainerID {
			return units[i].ContainerID < units[j].ContainerID
		}
		return units[i].Filename < units[j].Filename
	})
	plan := make([]gcPlannedUnit, len(units))
	for i := range units {
		plan[i] = gcPlannedUnit{index: i, dispatch: units[i]}
	}
	return plan, nil
}

// processSealedContainerForGC evaluates and optionally deletes one sealed
// container. It owns the transaction lifecycle for that container.
func processSealedContainerForGC(ctx context.Context, dbconn *sql.DB, containerID int64, filename string, dryRun bool, state gcPreFlightState, containersDir string, fsys fsx.FS) (sealedContainerGCResult, int64, error) {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return sealedContainerSkipped, 0, err
	}

	stillEmpty, skip, err := evaluateSealedContainerEmpty(ctx, tx, dbconn, containerID, dryRun, state.liveUnits)
	if err != nil {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, err
	}
	if skip || !stillEmpty {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, nil
	}

	return checkRetentionAndCommit(ctx, tx, containerID, filename, dryRun, state.reachableChunks, containersDir, fsys)
}

// evaluateSealedContainerEmpty dispatches to the appropriate emptiness check
// based on whether the GC is running in dry-run mode.
func evaluateSealedContainerEmpty(ctx context.Context, tx *sql.Tx, dbconn *sql.DB, containerID int64, dryRun bool, liveUnits livePhysicalUnits) (stillEmpty bool, skip bool, err error) {
	if dryRun {
		stillEmpty, err = evaluateContainerEmptyDryRun(ctx, tx, containerID, liveUnits)
		if err != nil {
			return false, false, err
		}
		return stillEmpty, false, nil
	}
	stillEmpty, skip, err = evaluateContainerEmptyLive(ctx, tx, dbconn, containerID, liveUnits)
	if err != nil {
		return false, false, err
	}
	return stillEmpty, skip, nil
}

// checkRetentionAndCommit runs the snapshot-retention safety check and, if
// the container is not retained, either rolls back (dry-run) or commits the
// deletion and removes the physical file.
func checkRetentionAndCommit(ctx context.Context, tx *sql.Tx, containerID int64, filename string, dryRun bool, reachableChunks map[int64]struct{}, containersDir string, fsys fsx.FS) (sealedContainerGCResult, int64, error) {
	hasRetained, err := containerHasReachableChunks(ctx, tx, containerID, reachableChunks)
	if err != nil {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, fmt.Errorf("retention safety check for container %d: %w", containerID, err)
	}
	if hasRetained {
		_ = tx.Rollback()
		return sealedContainerRetained, 0, nil
	}
	containerPath, physicalBytes, err := inspectGCContainerFile(fsys, containersDir, filename)
	if err != nil {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, err
	}
	if dryRun {
		_ = tx.Rollback()
		return sealedContainerAffected, physicalBytes, nil
	}
	if err := commitGCContainerDeletionWithPath(ctx, tx, containerID, containerPath, fsys); err != nil {
		return sealedContainerSkipped, 0, err
	}
	return sealedContainerAffected, physicalBytes, nil
}

// evaluateContainerEmptyDryRun checks whether a container appears empty
// without acquiring row locks. Used only for dry-run GC.
func evaluateContainerEmptyDryRun(ctx context.Context, tx *sql.Tx, containerID int64, liveUnits livePhysicalUnits) (bool, error) {
	var stillEmpty bool
	err := tx.QueryRowContext(ctx, `
		SELECT
			COALESCE(sealed, false) AND NOT EXISTS (
				SELECT 1
				FROM blocks b
				JOIN chunk ch ON ch.id = b.chunk_id
				WHERE b.container_id = $1
				AND (ch.live_ref_count > 0 OR ch.pin_count > 0)
			)
		FROM container WHERE id = $1
	`, containerID).Scan(&stillEmpty)
	if err != nil {
		return false, err
	}
	hasLiveUnits, err := containerHasLivePhysicalUnits(ctx, tx, containerID, liveUnits)
	if err != nil {
		return false, fmt.Errorf("live physical unit check for container %d: %w", containerID, err)
	}
	if hasLiveUnits {
		stillEmpty = false
	}
	return stillEmpty, nil
}

// gcChunkEmptinessQuerySQLite and gcChunkEmptinessQueryPostgres are
// pre-computed constant SQL queries (parameter $1 = container_id) that
// evaluate to true when a container has no live or pinned chunks.
// The PostgreSQL variant acquires FOR UPDATE row locks on the inner SELECT.
const gcChunkEmptinessQuerySQLite = `
		WITH locked_chunks AS (
			SELECT ch.live_ref_count, ch.pin_count
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = $1
		)
		SELECT NOT EXISTS (
			SELECT 1 FROM locked_chunks WHERE live_ref_count > 0 OR pin_count > 0
		)`

const gcChunkEmptinessQueryPostgres = `
		WITH locked_chunks AS (
			SELECT ch.live_ref_count, ch.pin_count
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = $1
			FOR UPDATE
		)
		SELECT NOT EXISTS (
			SELECT 1 FROM locked_chunks WHERE live_ref_count > 0 OR pin_count > 0
		)`

// buildLockedChunkEmptinessQuery selects the appropriate pre-computed query
// constant based on whether the backend supports SELECT FOR UPDATE.
func buildLockedChunkEmptinessQuery(dbconn *sql.DB) string {
	if db.SupportsSelectForUpdate(dbconn) {
		return gcChunkEmptinessQueryPostgres
	}
	return gcChunkEmptinessQuerySQLite
}

// evaluateContainerEmptyLive checks whether a container is empty under
// row-level locks. Returns skip=true if the container no longer qualifies
// (vanished or became unsealed/quarantined between the outer query and now).
func evaluateContainerEmptyLive(ctx context.Context, tx *sql.Tx, dbconn *sql.DB, containerID int64, liveUnits livePhysicalUnits) (stillEmpty bool, skip bool, err error) {
	var isSealed, isQuarantined bool
	containerLockQuery := db.QueryWithOptionalForUpdate(dbconn, `
		SELECT COALESCE(sealed, false), COALESCE(quarantine, false)
		FROM container
		WHERE id = $1
	`)
	err = tx.QueryRowContext(ctx, containerLockQuery, containerID).Scan(&isSealed, &isQuarantined)
	if err == sql.ErrNoRows {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	if !isSealed || isQuarantined {
		return false, true, nil
	}
	chunkEmptinessQ := buildLockedChunkEmptinessQuery(dbconn)
	err = tx.QueryRowContext(ctx, chunkEmptinessQ, containerID).Scan(&stillEmpty)
	if err != nil {
		return false, false, err
	}
	hasLiveUnits, err := containerHasLivePhysicalUnits(ctx, tx, containerID, liveUnits)
	if err != nil {
		return false, false, fmt.Errorf("live physical unit check for container %d: %w", containerID, err)
	}
	if hasLiveUnits {
		stillEmpty = false
	}
	return stillEmpty, false, nil
}

func inspectGCContainerFile(fsys fsx.FS, containersDir, filename string) (string, int64, error) {
	containerPath, err := container.SafeContainerPath(containersDir, filename)
	if err != nil {
		return "", 0, fmt.Errorf("invalid container filename %q: %w", filename, err)
	}
	info, err := fsys.Stat(containerPath)
	if err != nil {
		return "", 0, fmt.Errorf("stat container file %q: %w", filename, err)
	}
	physicalBytes := info.Size()
	if physicalBytes < 0 {
		return "", 0, fmt.Errorf("stat container file %q returned negative size %d", filename, physicalBytes)
	}
	return containerPath, physicalBytes, nil
}

// commitGCContainerDeletion is retained for focused transaction-cardinality
// contracts. Ordinary GC resolves and stats the path before calling the
// path-aware helper so the physical observation occurs exactly once.
func commitGCContainerDeletion(ctx context.Context, tx *sql.Tx, containerID int64, containersDir, filename string, fsys fsx.FS) error {
	containerPath, _, err := inspectGCContainerFile(fsys, containersDir, filename)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return commitGCContainerDeletionWithPath(ctx, tx, containerID, containerPath, fsys)
}

// commitGCContainerDeletionWithPath sweeps chunk/block metadata, deletes the
// container row, commits, then removes the already-inspected physical file.
// The transaction is rolled back on every failure before commit.
func commitGCContainerDeletionWithPath(ctx context.Context, tx *sql.Tx, containerID int64, containerPath string, fsys fsx.FS) error {
	if err := SweepUnreachableChunks(ctx, tx, containerID); err != nil {
		_ = tx.Rollback()
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM container WHERE id = $1`, containerID)
	if err != nil {
		_ = tx.Rollback()
		if isContainerFKViolation(err) {
			return invariants.New(
				invariants.CodeGCFKViolation,
				fmt.Sprintf("GC: FK violation deleting container id=%d — container still has live refs; run verify to diagnose", containerID),
				err,
			)
		}
		return err
	}
	if err := db.RequireExactlyOneRow(result, "delete GC container"); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return removeContainerFileWithFS(fsys, containerPath)
}

// gcChunkQuerier is a minimal interface satisfied by *sql.Tx and *sql.DB,
// allowing containerHasReachableChunks to operate inside a transaction.
type gcChunkQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// MarkReachableChunks computes chunk reachability from snapshot-retained roots
// using the shared graph traversal engine.
func MarkReachableChunks(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	g := graph.NewService(dbconn)

	metadata, err := catalog.NewServiceFromSQL(dbconn).LoadGCPlanMetadata(ctx, catalog.GCPlanInput{})
	if err != nil {
		return nil, err
	}
	roots := make([]graph.NodeID, 0, len(metadata.Roots))
	for _, root := range metadata.Roots {
		roots = append(roots, graph.NodeID{Type: graph.EntityLogicalFile, ID: root.LogicalFileID})
	}

	return g.ReachableChunksFromRoots(ctx, roots)
}

func computeGCReachabilityFromCatalog(ctx context.Context, dbconn *sql.DB) (*retention.ReachabilitySummary, error) {
	metadata, err := catalog.NewServiceFromSQL(dbconn).LoadGCPlanMetadata(ctx, catalog.GCPlanInput{})
	if err != nil {
		return nil, err
	}
	current := make(map[int64]struct{})
	snapshot := make(map[int64]struct{})
	retained := make(map[int64]struct{}, len(metadata.Roots))
	for _, root := range metadata.Roots {
		retained[root.LogicalFileID] = struct{}{}
		if root.Current {
			current[root.LogicalFileID] = struct{}{}
		}
		if len(root.SnapshotIDs) != 0 {
			snapshot[root.LogicalFileID] = struct{}{}
		}
	}
	return &retention.ReachabilitySummary{CurrentLogicalIDs: current, SnapshotLogicalIDs: snapshot, RetainedLogicalIDs: retained}, nil
}

// LoadLivePackedBlockIDs resolves live chunks (live_ref_count > 0 OR pin_count > 0)
// to their packed storage block ids through chunk_block_refs.
func LoadLivePackedBlockIDs(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	liveChunkIDs, err := loadLiveChunkIDs(ctx, dbconn)
	if err != nil {
		return nil, err
	}
	return packedBlockIDsForChunks(ctx, dbconn, liveChunkIDs)
}

// LoadLiveLegacyContainerIDs resolves live chunks (live_ref_count > 0 OR pin_count > 0)
// to legacy container ids through blocks rows.
func LoadLiveLegacyContainerIDs(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT b.container_id
		FROM blocks b
		JOIN chunk ch ON ch.id = b.chunk_id
		WHERE ch.status = 'COMPLETED'
		AND (ch.live_ref_count > 0 OR ch.pin_count > 0)
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	containerIDs := make(map[int64]struct{})
	for rows.Next() {
		var containerID int64
		if err := rows.Scan(&containerID); err != nil {
			return nil, err
		}
		containerIDs[containerID] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return containerIDs, nil
}

// containerHasReachableChunks reports whether any chunk in containerID is in
// reachableChunkIDs. It is the snapshot-retention safety net: even when
// live_ref_count == 0, a container must not be reclaimed if its chunks are
// graph-reachable from retained roots.
func containerHasReachableChunks(ctx context.Context, q gcChunkQuerier, containerID int64, reachableChunkIDs map[int64]struct{}) (bool, error) {
	if len(reachableChunkIDs) == 0 {
		return false, nil
	}
	rows, err := q.QueryContext(ctx, `
		SELECT DISTINCT b.chunk_id
		FROM blocks b
		WHERE b.container_id = $1
	`, containerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	if retained, err := anyChunkInReachableSet(rows, reachableChunkIDs); err != nil || retained {
		return retained, err
	}

	packedRows, err := q.QueryContext(ctx, `
		SELECT DISTINCT cbr.chunk_id
		FROM storage_blocks sb
		JOIN chunk_block_refs cbr ON cbr.block_id = sb.id
		WHERE sb.container_id = $1
	`, containerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = packedRows.Close() }()

	return anyChunkInReachableSet(packedRows, reachableChunkIDs)
}

// anyChunkInReachableSet scans a single-column chunk_id result set and returns
// true as soon as any ID is present in reachableChunkIDs. The caller retains
// ownership of rows (including Close via defer).
func anyChunkInReachableSet(rows *sql.Rows, reachableChunkIDs map[int64]struct{}) (bool, error) {
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return false, err
		}
		if _, retained := reachableChunkIDs[chunkID]; retained {
			return true, nil
		}
	}
	return false, rows.Err()
}

type gcSweepExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// SweepUnreachableChunks performs the chunk/blocks sweep for one container.
// It relies on the earlier mark phase and active liveness guards.
//
// v1.8 packed-block rule:
//   - GC never rewrites packed blocks.
//   - GC never removes individual chunk bytes from packed blocks.
//   - GC only deletes whole storage_blocks when they have no live chunk refs.
//   - For retained storage_blocks, keep all chunk_block_refs for chunks
//     physically present in the block. Never delete per-chunk refs from a
//     retained block, otherwise DB metadata can diverge from encoded block
//     tables and break verification invariants.
//
// Any future block compaction/rewriting must be implemented as a separate
// feature and is intentionally out of scope here.
func SweepUnreachableChunks(ctx context.Context, execer gcSweepExecer, containerID int64) error {
	chunkIDsToDelete, err := collectLegacyChunkIDsForContainer(ctx, execer, containerID)
	if err != nil {
		return err
	}

	deletablePackedBlockIDs, err := deletablePackedBlockIDsForContainer(ctx, execer, containerID)
	if err != nil {
		return err
	}

	packedChunkIDs, err := collectPackedChunkIDsToDelete(ctx, execer, deletablePackedBlockIDs)
	if err != nil {
		return err
	}
	for chunkID := range packedChunkIDs {
		chunkIDsToDelete[chunkID] = struct{}{}
	}

	// Metadata deletion order is intentional and must remain transactional:
	// 1) delete chunk_block_refs for the dead packed block
	// 2) delete storage_blocks row
	// 3) physical bytes follow existing container lifecycle behavior
	//    (whole-container metadata+file deletion after commit in current model)
	//
	// This preserves the crash-safety invariant that no committed
	// chunk_block_ref can point to a deleted storage_block.
	for _, blockID := range deletablePackedBlockIDs {
		if err := deletePackedBlockMetadata(ctx, execer, blockID); err != nil {
			return err
		}
	}

	if _, err := execer.ExecContext(ctx, `DELETE FROM blocks WHERE container_id = $1`, containerID); err != nil {
		return err
	}

	return deleteUnreachableChunkRows(ctx, execer, chunkIDsToDelete)
}

// collectLegacyChunkIDsForContainer returns the set of chunk IDs referenced by
// legacy (non-packed) blocks rows for the given container.
func collectLegacyChunkIDsForContainer(ctx context.Context, execer gcSweepExecer, containerID int64) (map[int64]struct{}, error) {
	rows, err := execer.QueryContext(ctx, `
		SELECT b.chunk_id
		FROM blocks b
		WHERE b.container_id = $1
	`, containerID)
	if err != nil {
		return nil, err
	}
	result := make(map[int64]struct{})
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			_ = rows.Close()
			return nil, err
		}
		result[chunkID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	return result, nil
}

// collectPackedChunkIDsToDelete returns the set of chunk IDs referenced via
// chunk_block_refs for the given deletable packed block IDs.
func collectPackedChunkIDsToDelete(ctx context.Context, execer gcSweepExecer, deletablePackedBlockIDs []int64) (map[int64]struct{}, error) {
	result := make(map[int64]struct{})
	for _, blockID := range deletablePackedBlockIDs {
		rows, err := execer.QueryContext(ctx, `
			SELECT cbr.chunk_id
			FROM chunk_block_refs cbr
			WHERE cbr.block_id = $1
		`, blockID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var chunkID int64
			if err := rows.Scan(&chunkID); err != nil {
				_ = rows.Close()
				return nil, err
			}
			result[chunkID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	return result, nil
}

// deleteUnreachableChunkRows deletes each chunk in chunkIDs only when its
// live_ref_count and pin_count are both zero, preserving any chunk that gained
// a new reference since the mark phase.
func deleteUnreachableChunkRows(ctx context.Context, execer gcSweepExecer, chunkIDs map[int64]struct{}) error {
	orderedChunkIDs := make([]int64, 0, len(chunkIDs))
	for chunkID := range chunkIDs {
		orderedChunkIDs = append(orderedChunkIDs, chunkID)
	}
	sort.Slice(orderedChunkIDs, func(i, j int) bool { return orderedChunkIDs[i] < orderedChunkIDs[j] })
	for _, chunkID := range orderedChunkIDs {
		if _, err := execer.ExecContext(ctx, `
			DELETE FROM chunk
			WHERE id = $1
			AND live_ref_count = 0
			AND pin_count = 0
		`, chunkID); err != nil {
			return err
		}
	}
	return nil
}

func deletablePackedBlockIDsForContainer(ctx context.Context, q gcChunkQuerier, containerID int64) ([]int64, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT sb.id
		FROM storage_blocks sb
		WHERE sb.container_id = $1
		AND NOT EXISTS (
			SELECT 1
			FROM chunk_block_refs cbr
			JOIN chunk ch ON ch.id = cbr.chunk_id
			WHERE cbr.block_id = sb.id
			AND ch.status = 'COMPLETED'
			AND (ch.live_ref_count > 0 OR ch.pin_count > 0)
		)
		ORDER BY sb.id ASC
	`, containerID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]int64, 0)
	for rows.Next() {
		var blockID int64
		if err := rows.Scan(&blockID); err != nil {
			return nil, err
		}
		out = append(out, blockID)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func deletePackedBlockMetadata(ctx context.Context, execer gcSweepExecer, blockID int64) error {
	if _, err := execer.ExecContext(ctx, `
		DELETE FROM chunk_block_refs
		WHERE block_id = $1
	`, blockID); err != nil {
		return err
	}

	result, err := execer.ExecContext(ctx, `
		DELETE FROM storage_blocks
		WHERE id = $1
	`, blockID)
	if err != nil {
		return err
	}
	if err := db.RequireExactlyOneRow(result, "delete GC packed storage block"); err != nil {
		return err
	}

	return nil
}

// removeContainerFileWithFS deletes the physical container file through the
// filesystem seam. The metadata transaction is already committed, so failure
// is returned without attempting a synthetic metadata restore.
func removeContainerFileWithFS(fsys fsx.FS, path string) error {
	if err := fsys.Remove(path); err != nil {
		return fmt.Errorf("remove container file %q: %w", path, err)
	}
	return nil
}

// activeContainer is a minimal record used during the fully-dead active
// container cleanup pass.
type activeContainer struct {
	id       int64
	filename string
}

func planActiveGCUnits(candidates []activeContainer, baseIndex int) []gcPlannedUnit {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].id != candidates[j].id {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].filename < candidates[j].filename
	})
	plan := make([]gcPlannedUnit, len(candidates))
	for i, candidate := range candidates {
		plan[i] = gcPlannedUnit{
			index: baseIndex + i,
			dispatch: gcDispatchUnit{
				Kind:        gcDispatchActiveContainer,
				ContainerID: candidate.id,
				Filename:    candidate.filename,
			},
		}
	}
	return plan
}

// queryFullyDeadActiveContainers returns active, non-quarantined containers
// where no chunk (legacy or packed) is still live or pinned.
func queryFullyDeadActiveContainers(ctx context.Context, dbconn *sql.DB) ([]activeContainer, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT c.id, c.filename
		FROM container c
		WHERE c.sealed = FALSE AND c.quarantine = FALSE
		AND NOT EXISTS (
			SELECT 1
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = c.id
			AND (ch.live_ref_count > 0 OR ch.pin_count > 0)
		)
		AND NOT EXISTS (
			SELECT 1
			FROM storage_blocks sb
			JOIN chunk_block_refs cbr ON cbr.block_id = sb.id
			JOIN chunk ch ON ch.id = cbr.chunk_id
			WHERE sb.container_id = c.id
			AND (ch.live_ref_count > 0 OR ch.pin_count > 0)
		)
		AND EXISTS (
			SELECT 1 FROM blocks WHERE container_id = c.id
			UNION ALL
			SELECT 1 FROM storage_blocks WHERE container_id = c.id
		)
		ORDER BY c.id ASC, c.filename ASC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var candidates []activeContainer
	for rows.Next() {
		var ac activeContainer
		if err := rows.Scan(&ac.id, &ac.filename); err != nil {
			return nil, err
		}
		candidates = append(candidates, ac)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

// sweepDeadActiveContainer acquires a transaction, re-verifies that the
// container is still fully dead under lock, sweeps its chunks and blocks,
// commits, then removes the physical file.
func sweepDeadActiveContainer(ctx context.Context, dbconn *sql.DB, containersDir string, liveUnits livePhysicalUnits, fsys fsx.FS, containerID int64, filename string) error {
	_, _, err := sweepDeadActiveContainerResult(ctx, dbconn, containersDir, map[int64]struct{}{}, liveUnits, fsys, containerID, filename)
	return err
}

func sweepDeadActiveContainerResult(ctx context.Context, dbconn *sql.DB, containersDir string, reachableChunkIDs map[int64]struct{}, liveUnits livePhysicalUnits, fsys fsx.FS, containerID int64, filename string) (sealedContainerGCResult, int64, error) {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return sealedContainerSkipped, 0, err
	}

	eligible, err := verifyActiveContainerEligible(ctx, tx, dbconn, containerID)
	if err != nil {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, err
	}
	if !eligible {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, nil
	}

	stillFullyDead, err := verifyActiveContainerFullyDead(ctx, tx, dbconn, containerID, liveUnits)
	if err != nil {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, err
	}
	if !stillFullyDead {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, nil
	}

	hasRetained, err := containerHasReachableChunks(ctx, tx, containerID, reachableChunkIDs)
	if err != nil {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, fmt.Errorf("retention safety check for active container %d: %w", containerID, err)
	}
	if hasRetained {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, nil
	}

	containerPath, physicalBytes, err := inspectGCContainerFile(fsys, containersDir, filename)
	if err != nil {
		_ = tx.Rollback()
		return sealedContainerSkipped, 0, err
	}
	if err := commitGCContainerDeletionWithPath(ctx, tx, containerID, containerPath, fsys); err != nil {
		return sealedContainerSkipped, 0, err
	}
	return sealedContainerAffected, physicalBytes, nil
}

func verifyActiveContainerEligible(ctx context.Context, tx *sql.Tx, dbconn *sql.DB, containerID int64) (bool, error) {
	query := db.QueryWithOptionalForUpdate(dbconn, `
		SELECT COALESCE(sealed, false), COALESCE(quarantine, false)
		FROM container
		WHERE id = $1
	`)
	var sealed, quarantined bool
	if err := tx.QueryRowContext(ctx, query, containerID).Scan(&sealed, &quarantined); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return !sealed && !quarantined, nil
}

// verifyActiveContainerFullyDead acquires FOR UPDATE locks on the container's
// chunk rows and returns true only if none are live or pinned and no live
// physical units remain. Must be called inside an open transaction.
func verifyActiveContainerFullyDead(ctx context.Context, tx *sql.Tx, dbconn *sql.DB, containerID int64, liveUnits livePhysicalUnits) (bool, error) {
	var stillFullyDead bool
	chunkEmptinessQ := buildLockedChunkEmptinessQuery(dbconn)
	if err := tx.QueryRowContext(ctx, chunkEmptinessQ, containerID).Scan(&stillFullyDead); err != nil {
		return false, err
	}
	hasLiveUnits, err := containerHasLivePhysicalUnits(ctx, tx, containerID, liveUnits)
	if err != nil {
		return false, fmt.Errorf("live physical unit check for active container %d: %w", containerID, err)
	}
	if hasLiveUnits {
		stillFullyDead = false
	}
	return stillFullyDead, nil
}

func loadLiveChunkIDs(ctx context.Context, q gcChunkQuerier) (map[int64]struct{}, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT id
		FROM chunk
		WHERE status = 'COMPLETED'
		AND (live_ref_count > 0 OR pin_count > 0)
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	chunkIDs := make(map[int64]struct{})
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return nil, err
		}
		chunkIDs[chunkID] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return chunkIDs, nil
}

func packedBlockIDsForChunks(ctx context.Context, q gcChunkQuerier, chunkIDs map[int64]struct{}) (map[int64]struct{}, error) {
	if len(chunkIDs) == 0 {
		return map[int64]struct{}{}, nil
	}

	rows, err := q.QueryContext(ctx, `
		SELECT chunk_id, block_id
		FROM chunk_block_refs
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	blockIDs := make(map[int64]struct{})
	for rows.Next() {
		var chunkID int64
		var blockID int64
		if err := rows.Scan(&chunkID, &blockID); err != nil {
			return nil, err
		}
		if _, ok := chunkIDs[chunkID]; ok {
			blockIDs[blockID] = struct{}{}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return blockIDs, nil
}

func containerHasLivePackedBlocks(ctx context.Context, q gcChunkQuerier, containerID int64, livePackedBlockIDs map[int64]struct{}) (bool, error) {
	if len(livePackedBlockIDs) == 0 {
		return false, nil
	}

	rows, err := q.QueryContext(ctx, `
		SELECT id
		FROM storage_blocks
		WHERE container_id = $1
	`, containerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var blockID int64
		if err := rows.Scan(&blockID); err != nil {
			return false, err
		}
		if _, live := livePackedBlockIDs[blockID]; live {
			return true, nil
		}
	}

	return false, rows.Err()
}

func containerHasLivePhysicalUnits(ctx context.Context, q gcChunkQuerier, containerID int64, liveUnits livePhysicalUnits) (bool, error) {
	if _, ok := liveUnits.LegacyLiveContainerIDs[containerID]; ok {
		return true, nil
	}
	return containerHasLivePackedBlocks(ctx, q, containerID, liveUnits.PackedLiveBlockIDs)
}
