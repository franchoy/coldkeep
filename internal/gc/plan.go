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
	ContainerID        int64  `json:"container_id"`
	Filename           string `json:"filename"`
	TotalBytes         int64  `json:"total_bytes"`
	LiveBytesAfterGC   int64  `json:"live_bytes_after_gc"`
	ReclaimableBytes   int64  `json:"reclaimable_bytes"`
	ReclaimableChunks  int64  `json:"reclaimable_chunks"`
	TotalChunks        int64  `json:"total_chunks"`
	FullyReclaimable   bool   `json:"fully_reclaimable"`
	RequiresCompaction bool   `json:"requires_compaction"`
}

type SimulationSummary struct {
	UnreachableChunks          int64 `json:"unreachable_chunks"`
	LogicallyReclaimableBytes  int64 `json:"logically_reclaimable_bytes"`
	PhysicallyReclaimableBytes int64 `json:"physically_reclaimable_bytes"`
	FullyReclaimableContainers int64 `json:"fully_reclaimable_containers"`
	PartiallyDeadContainers    int64 `json:"partially_dead_containers"`
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
	// ReclaimableBytes is the logical reclaimability estimate: sum of stored_size
	// for dead chunks, even in partially-dead containers.
	ReclaimableBytes int64
	// PhysicallyReclaimableBytes is what can be freed immediately with current GC
	// behavior (whole-container deletion only).
	PhysicallyReclaimableBytes int64
	// Summary provides operator-facing semantics that distinguish logical vs
	// physical reclaimability.
	Summary SimulationSummary
	// AffectedContainers lists containers that contain at least one reclaimable
	// chunk.
	AffectedContainers []ContainerImpact
}

type chunkRecord struct {
	ID           int64
	LiveRefCount int64
	PinCount     int64
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

	roots, err := g.GCRoots(ctx, graph.GCRootOptions{ExcludeSnapshots: opts.AssumeDeletedSnapshots})
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: gc roots: %w", err)
	}

	reachableChunkIDs, err := g.ReachableChunksFromRoots(ctx, roots)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: reachable chunks from roots: %w", err)
	}

	allChunks, err := loadAllCompletedChunks(ctx, dbconn)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: load completed chunks: %w", err)
	}

	unreachable := make([]chunkRecord, 0)
	var reachableCompletedCount int64
	for _, ch := range allChunks {
		if _, ok := reachableChunkIDs[ch.ID]; ok {
			reachableCompletedCount++
			continue
		}
		unreachable = append(unreachable, ch)
	}

	plan, err := buildPlanFromUnreachable(ctx, dbconn, int64(len(allChunks)), reachableCompletedCount, unreachable)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: build plan from unreachable: %w", err)
	}

	return plan, nil
}

func loadAllCompletedChunks(ctx context.Context, dbconn *sql.DB) ([]chunkRecord, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, live_ref_count, pin_count
		FROM chunk
		WHERE status = 'COMPLETED'
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]chunkRecord, 0)
	for rows.Next() {
		var ch chunkRecord
		if err := rows.Scan(&ch.ID, &ch.LiveRefCount, &ch.PinCount); err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func buildPlanFromUnreachable(ctx context.Context, dbconn *sql.DB, totalChunks, reachableChunks int64, unreachable []chunkRecord) (*Plan, error) {
	unreachableChunkIDs := make(map[int64]struct{}, len(unreachable))
	for _, ch := range unreachable {
		unreachableChunkIDs[ch.ID] = struct{}{}
	}

	affectedContainers, logicalReclaimableBytes, physicalReclaimableBytes, fullyReclaimableContainers, partiallyDeadContainers, err := planContainerImpact(ctx, dbconn, unreachableChunkIDs)
	if err != nil {
		return nil, err
	}

	return &Plan{
		TotalChunks:                totalChunks,
		ReachableChunks:            reachableChunks,
		UnreachableChunks:          int64(len(unreachable)),
		ReclaimableBytes:           logicalReclaimableBytes,
		PhysicallyReclaimableBytes: physicalReclaimableBytes,
		Summary: SimulationSummary{
			UnreachableChunks:          int64(len(unreachable)),
			LogicallyReclaimableBytes:  logicalReclaimableBytes,
			PhysicallyReclaimableBytes: physicalReclaimableBytes,
			FullyReclaimableContainers: fullyReclaimableContainers,
			PartiallyDeadContainers:    partiallyDeadContainers,
		},
		AffectedContainers: affectedContainers,
	}, nil
}

// planContainerImpact scans sealed non-quarantined containers and returns the
// per-container impact summary and total reclaimable bytes.
func planContainerImpact(ctx context.Context, dbconn *sql.DB, unreachableChunkIDs map[int64]struct{}) ([]ContainerImpact, int64, int64, int64, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = 1 AND quarantine = 0
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, 0, 0, 0, 0, err
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
			return nil, 0, 0, 0, 0, err
		}
		containers = append(containers, c)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, 0, 0, err
	}

	var affected []ContainerImpact
	var totalLogicalReclaimable int64
	var totalPhysicalReclaimable int64
	var fullyReclaimableContainers int64
	var partiallyDeadContainers int64

	for _, c := range containers {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, 0, 0, err
		}

		chunkRows, err := dbconn.QueryContext(ctx, `
			SELECT b.chunk_id, b.stored_size, ch.live_ref_count, ch.pin_count
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = ?
			AND ch.status = 'COMPLETED'
		`, c.id)
		if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("gc.BuildPlan: query blocks for container %d: %w", c.id, err)
		}

		var totalChunks, reclaimChunks int64
		var reclaimBytes int64

		for chunkRows.Next() {
			var chunkID int64
			var storedSize int64
			var liveRefCount, pinCount int64
			if err := chunkRows.Scan(&chunkID, &storedSize, &liveRefCount, &pinCount); err != nil {
				_ = chunkRows.Close()
				return nil, 0, 0, 0, 0, err
			}
			totalChunks++
			_, isUnreachable := unreachableChunkIDs[chunkID]
			isLive := liveRefCount > 0 || pinCount > 0
			if isUnreachable && !isLive {
				reclaimChunks++
				reclaimBytes += storedSize
			}
		}
		_ = chunkRows.Close()
		if err := chunkRows.Err(); err != nil {
			return nil, 0, 0, 0, 0, err
		}

		if reclaimChunks == 0 {
			continue
		}

		fullyReclaimable := totalChunks > 0 && reclaimChunks == totalChunks
		requiresCompaction := reclaimChunks > 0 && !fullyReclaimable
		liveBytesAfterGC := c.size - reclaimBytes
		if liveBytesAfterGC < 0 {
			liveBytesAfterGC = 0
		}

		impact := ContainerImpact{
			ContainerID:        c.id,
			Filename:           c.filename,
			TotalBytes:         c.size,
			LiveBytesAfterGC:   liveBytesAfterGC,
			ReclaimableBytes:   reclaimBytes,
			ReclaimableChunks:  reclaimChunks,
			TotalChunks:        totalChunks,
			FullyReclaimable:   fullyReclaimable,
			RequiresCompaction: requiresCompaction,
		}
		affected = append(affected, impact)
		totalLogicalReclaimable += reclaimBytes
		if fullyReclaimable {
			fullyReclaimableContainers++
			totalPhysicalReclaimable += c.size
		} else {
			partiallyDeadContainers++
		}
	}

	return affected, totalLogicalReclaimable, totalPhysicalReclaimable, fullyReclaimableContainers, partiallyDeadContainers, nil
}
