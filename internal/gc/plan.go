// Package gc provides GC planning logic shared between real GC execution and
// simulation. BuildPlan is a pure read-only phase: it performs the GC mark
// traversal and computes the sweep candidates without deleting anything.
//
// Real GC (internal/maintenance) calls BuildPlan then executes the sweep.
// Simulation (internal/observability) calls BuildPlan and stops there.
package gc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/graph"
)

// PlanOptions configures a GC plan computation.
type PlanOptions struct {
	// AssumeDeletedSnapshots is a set of snapshot IDs to exclude from
	// reachability roots before computing the plan. This lets callers ask
	// "what would become reclaimable if I deleted snapshot X?" without
	// actually deleting it.
	AssumeDeletedSnapshots []string
}

// ContainerImpact describes the GC impact on a single container.
type ContainerImpact struct {
	ContainerID       int64  `json:"container_id"`
	Filename          string `json:"filename"`
	TotalBytes        int64  `json:"total_bytes"`
	ReclaimableBytes  int64  `json:"reclaimable_bytes"`
	ReclaimableChunks int64  `json:"reclaimable_chunks"`
	// WouldDeleteFile is true when every chunk in the container is reclaimable,
	// meaning the container file would be fully removed by real GC.
	WouldDeleteFile bool `json:"would_delete_file"`
}

// Plan is the result of the GC mark phase. It describes what would be
// reclaimed if GC ran right now (optionally under AssumeDeletedSnapshots).
// No writes are made during plan computation.
type Plan struct {
	// TotalChunks is the total number of COMPLETED chunks in the repository.
	TotalChunks int64
	// ReachableChunks is the count of chunks reachable from live logical files
	// and snapshot roots (after applying AssumeDeletedSnapshots).
	ReachableChunks int64
	// UnreachableChunks is TotalChunks - ReachableChunks.
	UnreachableChunks int64
	// ReclaimableBytes is the sum of stored_size for unreachable chunks.
	ReclaimableBytes int64
	// AffectedContainers lists containers that contain at least one reclaimable
	// chunk. Containers where every chunk is reclaimable have WouldDeleteFile=true.
	AffectedContainers []ContainerImpact
}

// BuildPlan performs the GC mark phase and returns a Plan describing what
// would be reclaimed. It never modifies the database or filesystem.
//
// Reachability roots include:
//   - Current live logical files (from physical_file table)
//   - All snapshot roots (from snapshot_file table), excluding those in opts.AssumeDeletedSnapshots
//   - Quarantine/protection rules: only sealed, non-quarantined containers are planned for reclamation
//
// A chunk is reclaimable if and only if it is:
//   - Unreachable (not descended from any root)
//   - AND has no live references (live_ref_count == 0 AND pin_count == 0)
//
// BuildPlan is SQLite-compatible (uses ? placeholders). It does not acquire
// advisory locks — it is a read-only snapshot of reachability at one instant.
func BuildPlan(ctx context.Context, dbconn *sql.DB, opts PlanOptions) (*Plan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dbconn == nil {
		return nil, fmt.Errorf("gc.BuildPlan: nil db")
	}

	g := graph.NewService(dbconn)

	// --- Mark phase ---
	currentRoots, err := g.CurrentLogicalFileRoots(ctx)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: current logical file roots: %w", err)
	}
	snapshotRoots, err := g.SnapshotRoots(ctx, opts.AssumeDeletedSnapshots)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: snapshot roots: %w", err)
	}

	roots := make([]graph.NodeID, 0, len(currentRoots)+len(snapshotRoots))
	roots = append(roots, currentRoots...)
	roots = append(roots, snapshotRoots...)

	reachableChunkIDs, err := g.ReachableChunksFromRoots(ctx, roots)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: reachable chunks from roots: %w", err)
	}

	// --- Count phase ---
	var totalChunks int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM chunk WHERE status = 'COMPLETED'`,
	).Scan(&totalChunks); err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: count completed chunks: %w", err)
	}

	reachableCount := int64(len(reachableChunkIDs))
	unreachableCount := totalChunks - reachableCount

	// --- Sweep planning phase ---
	// Find sealed, non-quarantined containers and compute per-container impact.
	affectedContainers, reclaimableBytes, err := planContainerImpact(ctx, dbconn, reachableChunkIDs)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: plan container impact: %w", err)
	}

	plan := &Plan{
		TotalChunks:        totalChunks,
		ReachableChunks:    reachableCount,
		UnreachableChunks:  unreachableCount,
		ReclaimableBytes:   reclaimableBytes,
		AffectedContainers: affectedContainers,
	}

	return plan, nil
}

// planContainerImpact scans sealed non-quarantined containers and returns the
// per-container impact summary and total reclaimable bytes.
func planContainerImpact(ctx context.Context, dbconn *sql.DB, reachableChunkIDs map[int64]struct{}) ([]ContainerImpact, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = 1 AND quarantine = 0
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

	type containerRow struct {
		id       int64
		filename string
		size     int64
	}
	var containers []containerRow
	for rows.Next() {
		var c containerRow
		if err := rows.Scan(&c.id, &c.filename, &c.size); err != nil {
			return nil, 0, err
		}
		containers = append(containers, c)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	var affected []ContainerImpact
	var totalReclaimable int64

	for _, c := range containers {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}

		chunkRows, err := dbconn.QueryContext(ctx, `
			SELECT b.chunk_id, b.stored_size, ch.live_ref_count, ch.pin_count
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = ?
		`, c.id)
		if err != nil {
			return nil, 0, fmt.Errorf("gc.BuildPlan: query blocks for container %d: %w", c.id, err)
		}

		var totalChunks, reclaimChunks int64
		var reclaimBytes int64

		for chunkRows.Next() {
			var chunkID int64
			var storedSize int64
			var liveRefCount, pinCount int64
			if err := chunkRows.Scan(&chunkID, &storedSize, &liveRefCount, &pinCount); err != nil {
				_ = chunkRows.Close()
				return nil, 0, err
			}
			totalChunks++
			_, isReachable := reachableChunkIDs[chunkID]
			isLive := liveRefCount > 0 || pinCount > 0
			if !isReachable && !isLive {
				reclaimChunks++
				reclaimBytes += storedSize
			}
		}
		_ = chunkRows.Close()
		if err := chunkRows.Err(); err != nil {
			return nil, 0, err
		}

		if reclaimChunks == 0 {
			continue
		}

		impact := ContainerImpact{
			ContainerID:       c.id,
			Filename:          c.filename,
			TotalBytes:        c.size,
			ReclaimableBytes:  reclaimBytes,
			ReclaimableChunks: reclaimChunks,
			WouldDeleteFile:   reclaimChunks == totalChunks,
		}
		affected = append(affected, impact)
		totalReclaimable += reclaimBytes
	}

	return affected, totalReclaimable, nil
}
