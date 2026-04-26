package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/graph"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/retention"
	"github.com/franchoy/coldkeep/internal/verify"
)

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

	dbconn, err := db.ConnectDB()
	if err != nil {
		return GCResult{}, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

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

	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename
		FROM container WHERE quarantine = FALSE AND sealed = TRUE 
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
			return GCResult{}, err
		}

		if err := tx.Commit(); err != nil {
			return GCResult{}, err
		}

		// After commit, delete file from disk
		containerPath := filepath.Join(containersDir, filename)

		if err := os.Remove(containerPath); err != nil {
			log.Println("warning: failed to delete container file:", err)
		}

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
		if err := cleanupFullyDeadActiveContainers(ctx, dbconn, containersDir, reachableChunks); err != nil {
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

	rows, err := dbconn.QueryContext(ctx, `SELECT DISTINCT logical_file_id FROM snapshot_file ORDER BY logical_file_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	start := make([]graph.NodeID, 0)
	for rows.Next() {
		var logicalFileID int64
		if err := rows.Scan(&logicalFileID); err != nil {
			return nil, err
		}
		start = append(start, graph.NodeID{Type: graph.EntityLogicalFile, ID: logicalFileID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	reachable := make(map[int64]struct{})
	if err := g.Traverse(ctx, start, func(n graph.NodeID) error {
		if n.Type == graph.EntityChunk {
			reachable[n.ID] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return reachable, nil
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
}

// SweepUnreachableChunks performs the chunk/blocks sweep for one container.
// It relies on the earlier mark phase and active liveness guards.
func SweepUnreachableChunks(ctx context.Context, execer gcSweepExecer, containerID int64) error {
	_, err := execer.ExecContext(ctx, `
		WITH deleted_blocks AS (
			DELETE FROM blocks
			WHERE container_id = $1
			RETURNING chunk_id
		)
		DELETE FROM chunk c
		USING deleted_blocks db
		WHERE c.id = db.chunk_id
		AND c.live_ref_count = 0
		AND c.pin_count = 0
	`, containerID)
	return err
}

// cleanupFullyDeadActiveContainers deletes active (unsealed) containers in which
// every chunk has live_ref_count = 0 and pin_count = 0. Deleting the whole container
// (both the physical file and all metadata rows) is safe because the append-only
// offset invariant is preserved by removing the container entirely — no offsets shift.
// Partially-dead containers (mixed live and dead chunks) are left intact;
// they will be handled by the regular sealed-container GC path once sealed.
func cleanupFullyDeadActiveContainers(ctx context.Context, dbconn *sql.DB, containersDir string, reachableChunkIDs map[int64]struct{}) error {
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
		AND EXISTS (
			SELECT 1 FROM blocks WHERE container_id = c.id
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
		containerPath := filepath.Join(containersDir, ac.filename)
		if err := os.Remove(containerPath); err != nil {
			log.Println("warning: failed to delete fully-dead active container file:", err)
		}
	}

	return nil
}
