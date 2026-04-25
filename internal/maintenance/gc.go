package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/db"
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
		hasRetained, err := containerHasRetainedChunks(ctx, tx, containerID, reachability.RetainedLogicalIDs)
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
		_, err = tx.ExecContext(ctx, `
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

	// Final cleanup pass: reclaim dead chunks from active (unsealed) containers.
	// This ensures that accumulated dead chunks (live_ref_count = 0) from delete/restore
	// loops don't linger indefinitely in the reusable active container.
	// Dry-run skips this to avoid side effects.
	if !dryRun {
		err := cleanupDeadChunksFromActiveContainers(ctx, dbconn)
		if err != nil {
			return GCResult{}, fmt.Errorf("cleanup dead chunks from active containers: %w", err)
		}
	}

	result.AffectedContainers = affectedContainers
	return result, nil
}

// gcChunkQuerier is a minimal interface satisfied by *sql.Tx and *sql.DB,
// allowing containerHasRetainedChunks to operate inside a transaction.
type gcChunkQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// containerHasRetainedChunks reports whether any chunk in containerID is
// associated (via file_chunk) with a logical file that appears in retainedIDs.
// It is the snapshot-retention safety net: even when live_ref_count == 0, a
// container must not be reclaimed if its chunks are logically reachable.
func containerHasRetainedChunks(ctx context.Context, q gcChunkQuerier, containerID int64, retainedIDs map[int64]struct{}) (bool, error) {
	if len(retainedIDs) == 0 {
		return false, nil
	}
	rows, err := q.QueryContext(ctx, `
		SELECT DISTINCT fc.logical_file_id
		FROM blocks b
		JOIN file_chunk fc ON fc.chunk_id = b.chunk_id
		WHERE b.container_id = $1
	`, containerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return false, err
		}
		if _, retained := retainedIDs[id]; retained {
			return true, nil
		}
	}
	return false, rows.Err()
}

// cleanupDeadChunksFromActiveContainers removes chunk and blocks rows
// where live_ref_count = 0 and pin_count = 0 from unsealed, non-quarantined containers.
// This is the final cleanup pass to reclaim dead chunks that accumulated during
// delete/restore cycles but couldn't be removed from sealed containers (since GC
// only processes sealed containers). This ensures the reusable active container
// doesn't accumulate unbounded dead metadata.
func cleanupDeadChunksFromActiveContainers(ctx context.Context, dbconn *sql.DB) error {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Find all unsealed, non-quarantined active containers
	containerRows, err := tx.QueryContext(ctx, `
		SELECT id FROM container
		WHERE sealed = FALSE AND quarantine = FALSE
	`)
	if err != nil {
		return err
	}
	defer func() { _ = containerRows.Close() }()

	var containerIDs []int64
	for containerRows.Next() {
		var id int64
		if err := containerRows.Scan(&id); err != nil {
			return err
		}
		containerIDs = append(containerIDs, id)
	}
	if err := containerRows.Err(); err != nil {
		return err
	}

	// For each active container, clean up dead chunks
	for _, containerID := range containerIDs {
		// Delete blocks rows for dead chunks in this container
		_, err := tx.ExecContext(ctx, `
			WITH dead_blocks AS (
				DELETE FROM blocks
				WHERE container_id = $1
				AND chunk_id IN (
					SELECT id FROM chunk
					WHERE live_ref_count = 0 AND pin_count = 0
				)
				RETURNING chunk_id
			)
			DELETE FROM chunk
			WHERE id IN (SELECT chunk_id FROM dead_blocks)
		`, containerID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
