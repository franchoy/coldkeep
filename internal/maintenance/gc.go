package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/graph"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/retention"
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

var gcAdvisoryUnlock = func(ctx context.Context, dbconn *sql.DB) error {
	_, err := dbconn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", gcAdvisoryLockID)
	return err
}

var gcPhysicalIntegrityCheck = func(dbconn *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
	return verify.CheckPhysicalFileGraphIntegrity(dbconn)
}

var gcComputeReachability = func(ctx context.Context, dbconn *sql.DB) (*retention.ReachabilitySummary, error) {
	return retention.ComputeReachabilitySummary(ctx, dbconn)
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
	SnapshotRetainedContainers   int      `json:"snapshot_retained_containers"`
	SnapshotRetainedLogicalFiles int      `json:"snapshot_retained_logical_files"`
	RetainedCurrentOnlyLogical   int      `json:"retained_current_only_logical_files"`
	RetainedSnapshotOnlyLogical  int      `json:"retained_snapshot_only_logical_files"`
	RetainedSharedLogical        int      `json:"retained_shared_logical_files"`
}

func RunGCWithContainersDir(dryRun bool, containersDir string) error {
	_, err := RunGCWithContainersDirResult(dryRun, containersDir)
	return err
}

// RunGCWithContainersDirResult implements GC under the v1.2 audited-root model
// (Option A — conservative path):
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
func RunGCWithContainersDirResult(dryRun bool, containersDir string) (result GCResult, err error) {
	result.DryRun = dryRun

	dbconn, err := gcConnectDB()
	if err != nil {
		return GCResult{}, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	fsys := fsx.Default()

	// G1: SQLite does not support advisory locks or live GC (no transaction
	// isolation guarantees during deletion). Live GC on SQLite is refused
	// fail-closed. Dry-run GC on SQLite is allowed for inspection only.
	backend := db.BackendFromDB(dbconn)
	if backend == db.BackendSQLite {
		if !dryRun {
			return GCResult{}, fmt.Errorf("live GC is not supported on the SQLite backend; run with --dry-run to inspect GC candidates")
		}
		log.Println("gc: SQLite backend detected — skipping advisory lock (dry-run only)")
	} else {
		// Attempt to acquire advisory lock to ensure only one GC runs at a time
		var locked bool

		err = dbconn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", gcAdvisoryLockID).Scan(&locked)
		if err != nil {
			return GCResult{}, fmt.Errorf("failed to attempt advisory lock: %w", err)
		}

		if !locked {
			return GCResult{}, fmt.Errorf("GC already running (advisory lock held)")
		}

		defer func() {
			cleanupCtx, cleanupCancel := db.NewOperationContext(context.Background())
			defer cleanupCancel()
			unlockErr := gcAdvisoryUnlock(cleanupCtx, dbconn)
			if unlockErr != nil {
				log.Printf("warning: failed to release advisory lock: %v\n", unlockErr)
			}
		}()
	}

	// Pre-flight: refuse GC if the physical_file graph has integrity issues.
	// Proceeding on a drifted graph risks treating live blocks as unreferenced.
	integrity, err := gcPhysicalIntegrityCheck(dbconn)
	if err != nil {
		if _, ok := invariants.Code(err); ok {
			return GCResult{}, invariants.New(
				invariants.CodeGCRefusedIntegrity,
				fmt.Sprintf(
					"GC refused: physical_file graph integrity issues detected (orphan_rows=%d ref_count_mismatches=%d negative_ref_counts=%d); run 'repair ref-counts' first",
					integrity.OrphanPhysicalFileRows,
					integrity.LogicalRefCountMismatches,
					integrity.NegativeLogicalRefCounts,
				),
				err,
			)
		}
		return GCResult{}, fmt.Errorf("GC pre-flight integrity check failed: %w", err)
	}
	if integrity.OrphanPhysicalFileRows > 0 || integrity.LogicalRefCountMismatches > 0 || integrity.NegativeLogicalRefCounts > 0 {
		return GCResult{}, invariants.New(
			invariants.CodeGCRefusedIntegrity,
			fmt.Sprintf(
				"GC refused: physical_file graph integrity issues detected (orphan_rows=%d ref_count_mismatches=%d negative_ref_counts=%d); run 'repair ref-counts' first",
				integrity.OrphanPhysicalFileRows,
				integrity.LogicalRefCountMismatches,
				integrity.NegativeLogicalRefCounts,
			),
			nil,
		)
	}

	// Compute reachability summary once upfront. This is used as a safety net
	// inside the container loop to ensure snapshot-retained chunks are never
	// reclaimed, regardless of live_ref_count state.
	reachability, err := gcComputeReachability(ctx, dbconn)
	if err != nil {
		return GCResult{}, fmt.Errorf("GC pre-flight: failed to compute reachability summary: %w", err)
	}

	// Snapshot-retained logical file count is a fixed property of the retained
	// root set at the time GC runs. Populate it once, before the container loop.
	result.SnapshotRetainedLogicalFiles = len(reachability.SnapshotLogicalIDs)
	classification := retention.ClassifyRetention(reachability)
	result.RetainedCurrentOnlyLogical = len(classification.CurrentOnly)
	result.RetainedSnapshotOnlyLogical = len(classification.SnapshotOnly)
	result.RetainedSharedLogical = len(classification.Shared)

	reachableChunks, err := gcMarkReachableChunks(ctx, dbconn)
	if err != nil {
		return GCResult{}, fmt.Errorf("GC pre-flight: failed to mark reachable chunks: %w", err)
	}
	livePackedBlockIDs, err := gcLoadLivePackedBlockIDs(ctx, dbconn)
	if err != nil {
		return GCResult{}, fmt.Errorf("GC pre-flight: failed to load live packed blocks: %w", err)
	}
	liveLegacyContainerIDs, err := gcLoadLiveLegacyContainerIDs(ctx, dbconn)
	if err != nil {
		return GCResult{}, fmt.Errorf("GC pre-flight: failed to load live legacy containers: %w", err)
	}
	liveUnits := livePhysicalUnits{
		LegacyLiveContainerIDs: liveLegacyContainerIDs,
		PackedLiveBlockIDs:     livePackedBlockIDs,
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename
		FROM container WHERE quarantine = FALSE AND sealed = TRUE AND sealing = FALSE
		ORDER BY id ASC
	`)
	if err != nil {
		return GCResult{}, err
	}
	defer func() { _ = rows.Close() }()

	var affectedContainers int

	for rows.Next() {
		var containerID int64
		var filename string

		if err := rows.Scan(&containerID, &filename); err != nil {
			return GCResult{}, err
		}

		if err := ctx.Err(); err != nil {
			return GCResult{}, err
		}

		tx, err := dbconn.BeginTx(ctx, nil)
		if err != nil {
			return GCResult{}, err
		}

		var stillEmpty bool
		if dryRun {
			// Dry-run keeps a non-blocking check and reports what would be deleted.
			err = tx.QueryRowContext(ctx, `
				SELECT 
					COALESCE(sealed, false) AND NOT EXISTS (
						SELECT 1
						FROM blocks b
						JOIN chunk ch ON ch.id = b.chunk_id
						WHERE b.container_id = $1
						AND (ch.live_ref_count > 0 OR ch.pin_count > 0)
					)
				FROM container where id = $1
			`, containerID).Scan(&stillEmpty)
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, err
			}
			hasLiveUnits, err := containerHasLivePhysicalUnits(ctx, tx, containerID, liveUnits)
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, fmt.Errorf("live physical unit check for container %d: %w", containerID, err)
			}
			if hasLiveUnits {
				stillEmpty = false
			}
		} else {
			// Lock the container row first so status/metadata used for deletion is stable.
			var isSealed bool
			var isQuarantined bool
			containerLockQuery := db.QueryWithOptionalForUpdate(dbconn, `
				SELECT COALESCE(sealed, false), COALESCE(quarantine, false)
				FROM container
				WHERE id = $1
			`)
			err = tx.QueryRowContext(ctx, containerLockQuery, containerID).Scan(&isSealed, &isQuarantined)
			if err == sql.ErrNoRows {
				_ = tx.Rollback()
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, err
			}
			if !isSealed || isQuarantined {
				_ = tx.Rollback()
				continue
			}

			// Lock all chunk rows referenced by this container, then evaluate emptiness.
			chunkLockQuery := db.QueryWithOptionalForUpdate(dbconn, `
				SELECT ch.live_ref_count, ch.pin_count
				FROM blocks b
				JOIN chunk ch ON ch.id = b.chunk_id
				WHERE b.container_id = $1
			`)
			emptyContainerQuery := fmt.Sprintf(`
				WITH locked_chunks AS (
					%s
				)
				SELECT NOT EXISTS (
					SELECT 1 FROM locked_chunks WHERE live_ref_count > 0 OR pin_count > 0
				)
			`, chunkLockQuery)
			err = tx.QueryRowContext(ctx, emptyContainerQuery, containerID).Scan(&stillEmpty)
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, err
			}
			hasLiveUnits, err := containerHasLivePhysicalUnits(ctx, tx, containerID, liveUnits)
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, fmt.Errorf("live physical unit check for container %d: %w", containerID, err)
			}
			if hasLiveUnits {
				stillEmpty = false
			}
		}

		if !stillEmpty {
			_ = tx.Rollback()
			continue
		}

		// Snapshot-retention safety net: even if live_ref_count == 0, skip any
		// container whose chunks are still reachable from a retained logical file.
		hasRetained, err := containerHasReachableChunks(ctx, tx, containerID, reachableChunks)
		if err != nil {
			_ = tx.Rollback()
			return GCResult{}, fmt.Errorf("retention safety check for container %d: %w", containerID, err)
		}
		if hasRetained {
			_ = tx.Rollback()
			result.SnapshotRetainedContainers++
			continue
		}

		// If dry-run, rollback transaction and skip file deletion
		// dry-run is just simulation and count
		if dryRun {
			_ = tx.Rollback()
			affectedContainers++
			result.ContainerFilenames = append(result.ContainerFilenames, filename)
			continue
		}

		// Delete location records and then chunk rows linked to this container.
		err = SweepUnreachableChunks(ctx, tx, containerID)
		if err != nil {
			_ = tx.Rollback()
			return GCResult{}, err
		}

		// Delete container row
		_, err = tx.ExecContext(ctx, `DELETE FROM container WHERE id = $1`, containerID)
		if err != nil {
			_ = tx.Rollback()
			if isContainerFKViolation(err) {
				return GCResult{}, invariants.New(
					invariants.CodeGCFKViolation,
					fmt.Sprintf("GC: FK violation deleting container id=%d — container still has live refs; run verify to diagnose", containerID),
					err,
				)
			}
			return GCResult{}, err
		}

		if err := tx.Commit(); err != nil {
			return GCResult{}, err
		}

		// After commit, delete file from disk
		containerPath, err := container.SafeContainerPath(containersDir, filename)
		if err != nil {
			return GCResult{}, fmt.Errorf("invalid container filename %q: %w", filename, err)
		}

		removeContainerFileWithFS(fsys, containerPath)

		affectedContainers++
		result.ContainerFilenames = append(result.ContainerFilenames, filename)
	}

	if err := rows.Err(); err != nil {
		return GCResult{}, err
	}

	// Final cleanup pass: reclaim active containers that have become fully empty.
	// An active (unsealed) container where every chunk has live_ref_count = 0 and
	// pin_count = 0 can be deleted entirely (file + metadata) without violating the
	// append-only offset invariant, because we remove the whole container.
	// Partially-dead active containers (mixed live/dead chunks) are left intact;
	// they will be sealed and collected by the regular sealed-container path later.
	// Dry-run skips this to avoid side effects.
	if !dryRun {
		if err := cleanupFullyDeadActiveContainers(ctx, dbconn, containersDir, reachableChunks, liveUnits, fsys); err != nil {
			return GCResult{}, fmt.Errorf("cleanup fully dead active containers: %w", err)
		}
	}

	result.AffectedContainers = affectedContainers
	return result, nil
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

	roots, err := g.GCRoots(ctx, graph.GCRootOptions{})
	if err != nil {
		return nil, err
	}

	return g.ReachableChunksFromRoots(ctx, roots)
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
	for chunkID := range chunkIDs {
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

	if _, err := execer.ExecContext(ctx, `
		DELETE FROM storage_blocks
		WHERE id = $1
	`, blockID); err != nil {
		return err
	}

	return nil
}

// cleanupFullyDeadActiveContainers deletes active (unsealed) containers in which
// every chunk has live_ref_count = 0 and pin_count = 0. Deleting the whole container
// (both the physical file and all metadata rows) is safe because the append-only
// offset invariant is preserved by removing the container entirely — no offsets shift.
// Partially-dead containers (mixed live and dead chunks) are left intact;
// they will be handled by the regular sealed-container GC path once sealed.
// removeContainerFileWithFS deletes the physical container file through the
// filesystem seam. Errors are logged only; container DB rows are already
// committed as deleted at this point.
func removeContainerFileWithFS(fsys fsx.FS, path string) {
	if err := fsys.Remove(path); err != nil {
		log.Println("warning: failed to delete container file:", err)
	}
}

func cleanupFullyDeadActiveContainers(ctx context.Context, dbconn *sql.DB, containersDir string, reachableChunkIDs map[int64]struct{}, liveUnits livePhysicalUnits, fsys fsx.FS) error {
	// Identify active containers where no chunk is still live or pinned.
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
		ORDER BY c.id ASC
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	type activeContainer struct {
		id       int64
		filename string
	}
	var candidates []activeContainer
	for rows.Next() {
		var ac activeContainer
		if err := rows.Scan(&ac.id, &ac.filename); err != nil {
			return err
		}
		candidates = append(candidates, ac)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, ac := range candidates {
		// Snapshot-retention safety net: skip containers whose chunks are retained.
		hasRetained, err := containerHasReachableChunks(ctx, dbconn, ac.id, reachableChunkIDs)
		if err != nil {
			return fmt.Errorf("retention safety check for active container %d: %w", ac.id, err)
		}
		if hasRetained {
			continue
		}
		hasLiveUnits, err := containerHasLivePhysicalUnits(ctx, dbconn, ac.id, liveUnits)
		if err != nil {
			return fmt.Errorf("live physical unit check for active container %d: %w", ac.id, err)
		}
		if hasLiveUnits {
			continue
		}

		tx, err := dbconn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		// Lock and re-verify: no live or pinned chunks in this container.
		var stillFullyDead bool
		chunkLockQuery := db.QueryWithOptionalForUpdate(dbconn, `
			SELECT ch.live_ref_count, ch.pin_count
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = $1
		`)
		emptyQuery := fmt.Sprintf(`
			WITH locked AS (%s)
			SELECT NOT EXISTS (
				SELECT 1 FROM locked WHERE live_ref_count > 0 OR pin_count > 0
			)
		`, chunkLockQuery)
		if err := tx.QueryRowContext(ctx, emptyQuery, ac.id).Scan(&stillFullyDead); err != nil {
			_ = tx.Rollback()
			return err
		}
		hasLiveUnits, err = containerHasLivePhysicalUnits(ctx, tx, ac.id, liveUnits)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("live physical unit check for active container %d: %w", ac.id, err)
		}
		if hasLiveUnits {
			stillFullyDead = false
		}
		if !stillFullyDead {
			_ = tx.Rollback()
			continue
		}

		// Delete all blocks + chunk rows for this container.
		err = SweepUnreachableChunks(ctx, tx, ac.id)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		// Delete the container row.
		if _, err := tx.ExecContext(ctx, `DELETE FROM container WHERE id = $1`, ac.id); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		// Physical file deletion after commit.
		containerPath, err := container.SafeContainerPath(containersDir, ac.filename)
		if err != nil {
			return fmt.Errorf("invalid container filename %q: %w", ac.filename, err)
		}
		removeContainerFileWithFS(fsys, containerPath)
	}

	return nil
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
