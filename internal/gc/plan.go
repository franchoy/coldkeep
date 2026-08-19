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
	"sort"
	"strings"

	"github.com/franchoy/coldkeep/internal/catalog"
	chunkmeta "github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/graph"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/verify"
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
	UnreachableChunks                  int64 `json:"unreachable_chunks"`
	LogicallyReclaimableBytes          int64 `json:"logically_reclaimable_bytes"`
	PhysicallyReclaimableBytes         int64 `json:"physically_reclaimable_bytes"`
	FullyReclaimableContainers         int64 `json:"fully_reclaimable_containers"`
	PartiallyDeadContainers            int64 `json:"partially_dead_containers"`
	PackedBlocksLive                   int64 `json:"packed_blocks_live"`
	PackedBlocksDead                   int64 `json:"packed_blocks_dead"`
	PackedBytesLive                    int64 `json:"packed_bytes_live"`
	PackedBytesReclaimable             int64 `json:"packed_bytes_reclaimable"`
	RetainedDeadBytesDueToPackedBlocks int64 `json:"retained_dead_bytes_due_to_packed_blocks"`
}

type Warning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
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
	Warnings           []Warning
}

type chunkRecord struct {
	ID             int64
	LiveRefCount   int64
	PinCount       int64
	ChunkerVersion string
}

type warningInputs struct {
	UnknownChunkerVersions    []string
	MissingContainerChunks    int64
	QuarantinedDeadContainers int64
	InconsistentContainers    int64
	PartiallyDeadContainers   int64
}

// BuildPlan performs the GC mark phase and returns a Plan describing what
// would be reclaimed. It never modifies the database or filesystem.
//
// BuildPlan applies the same refusal semantics as real GC before computing a
// read-only plan.
//
// Reachability roots include:
//   - Current live logical files (from physical_file table)
//   - All snapshot roots (from snapshot_file table), excluding those in opts.AssumeDeletedSnapshots
//   - Quarantine/protection rules: sealed, non-quarantined containers are
//     planned for whole-container deletion, and fully-dead active containers are
//     included when real non-dry-run GC would delete them
//
// A chunk is reclaimable if and only if it is:
//   - Unreachable (not descended from any root)
//   - AND has no live references (live_ref_count == 0 AND pin_count == 0)
//
// BuildPlan uses PostgreSQL-compatible $N placeholders. It does not acquire
// advisory locks — it is a read-only snapshot of reachability at one instant.
func BuildPlan(ctx context.Context, dbconn *sql.DB, opts PlanOptions) (*Plan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dbconn == nil {
		return nil, fmt.Errorf("gc.BuildPlan: nil db")
	}
	metadata, err := catalog.NewServiceFromSQL(dbconn).LoadGCPlanMetadata(ctx, catalog.GCPlanInput{ExcludeSnapshotIDs: opts.AssumeDeletedSnapshots})
	if err != nil {
		if catalog.IsCode(err, catalog.ErrorInvalidArgument) || catalog.IsCode(err, catalog.ErrorNotFound) {
			return nil, fmt.Errorf("gc.BuildPlan: validate assumed-deleted snapshots: %w", err)
		}
		return nil, fmt.Errorf("gc.BuildPlan: gc roots: %w", err)
	}
	if err := refuseOnIntegrityIssues(dbconn); err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: %w", err)
	}

	g := graph.NewService(dbconn)
	roots := make([]graph.NodeID, 0, len(metadata.Roots))
	for _, root := range metadata.Roots {
		roots = append(roots, graph.NodeID{Type: graph.EntityLogicalFile, ID: root.LogicalFileID})
	}

	reachableChunkIDs, err := g.ReachableChunksFromRoots(ctx, roots)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: reachable chunks from roots: %w", err)
	}

	allChunks, err := loadAllCompletedChunks(ctx, dbconn)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: load completed chunks: %w", err)
	}
	unknownVersions := make(map[string]struct{})
	liveChunkIDs := make(map[int64]struct{})
	for _, ch := range allChunks {
		if version := ch.ChunkerVersion; version != string(chunkmeta.VersionV1SimpleRolling) && version != string(chunkmeta.VersionV2FastCDC) {
			unknownVersions[version] = struct{}{}
		}
		if ch.LiveRefCount > 0 || ch.PinCount > 0 {
			liveChunkIDs[ch.ID] = struct{}{}
		}
	}

	livePackedBlockIDs, err := packedBlockIDsForChunks(ctx, dbconn, liveChunkIDs)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: live packed blocks from live chunks: %w", err)
	}
	reachablePackedBlockIDs, err := packedBlockIDsForChunks(ctx, dbconn, reachableChunkIDs)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: reachable packed blocks from roots: %w", err)
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

	plan, err := buildPlanFromUnreachable(ctx, dbconn, int64(len(allChunks)), reachableCompletedCount, reachableChunkIDs, livePackedBlockIDs, reachablePackedBlockIDs, unreachable, unknownVersions)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: build plan from unreachable: %w", err)
	}

	return plan, nil
}

func loadAllCompletedChunks(ctx context.Context, dbconn *sql.DB) ([]chunkRecord, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, live_ref_count, pin_count, chunker_version
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
		if err := rows.Scan(&ch.ID, &ch.LiveRefCount, &ch.PinCount, &ch.ChunkerVersion); err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func refuseOnIntegrityIssues(dbconn *sql.DB) error {
	integrity, err := verify.CheckPhysicalFileGraphIntegrity(dbconn)
	if err != nil {
		if _, ok := invariants.Code(err); ok {
			return invariants.New(
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
		return fmt.Errorf("GC pre-flight integrity check failed: %w", err)
	}
	if integrity.OrphanPhysicalFileRows > 0 || integrity.LogicalRefCountMismatches > 0 || integrity.NegativeLogicalRefCounts > 0 {
		return invariants.New(
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

	return nil
}

func buildPlanFromUnreachable(ctx context.Context, dbconn *sql.DB, totalChunks, reachableChunks int64, reachableChunkIDs map[int64]struct{}, livePackedBlockIDs, reachablePackedBlockIDs map[int64]struct{}, unreachable []chunkRecord, unknownVersions map[string]struct{}) (*Plan, error) {
	unreachableChunkIDs := make(map[int64]struct{}, len(unreachable))
	for _, ch := range unreachable {
		unreachableChunkIDs[ch.ID] = struct{}{}
	}

	affectedContainers, logicalReclaimableBytes, physicalReclaimableBytes, fullyReclaimableContainers, partiallyDeadContainers, inconsistentContainers, err := planContainerImpact(ctx, dbconn, unreachableChunkIDs, livePackedBlockIDs)
	if err != nil {
		return nil, err
	}
	activeContainers, activeLogicalReclaimableBytes, activePhysicalReclaimableBytes, activeFullyReclaimableContainers, activeInconsistentContainers, err := planActiveContainerImpact(ctx, dbconn, reachableChunkIDs, unreachableChunkIDs, livePackedBlockIDs, reachablePackedBlockIDs)
	if err != nil {
		return nil, err
	}
	affectedContainers = append(affectedContainers, activeContainers...)
	logicalReclaimableBytes += activeLogicalReclaimableBytes
	physicalReclaimableBytes += activePhysicalReclaimableBytes
	fullyReclaimableContainers += activeFullyReclaimableContainers
	inconsistentContainers += activeInconsistentContainers
	sort.Slice(affectedContainers, func(i, j int) bool {
		return affectedContainers[i].ContainerID < affectedContainers[j].ContainerID
	})
	missingContainerChunks, err := countUnreachableChunksWithoutContainer(ctx, dbconn, unreachableChunkIDs)
	if err != nil {
		return nil, err
	}
	quarantinedDeadContainers, err := countQuarantinedContainersWithDeadChunks(ctx, dbconn, unreachableChunkIDs)
	if err != nil {
		return nil, err
	}
	packedBlocksLive, packedBlocksDead, packedBytesLive, packedBytesReclaimable, retainedDeadBytesDueToPackedBlocks, err := computePackedSimulationMetrics(ctx, dbconn, livePackedBlockIDs)
	if err != nil {
		return nil, err
	}
	unknown := make([]string, 0, len(unknownVersions))
	for version := range unknownVersions {
		unknown = append(unknown, version)
	}
	sort.Strings(unknown)
	warnings := buildWarnings(warningInputs{
		UnknownChunkerVersions:    unknown,
		MissingContainerChunks:    missingContainerChunks,
		QuarantinedDeadContainers: quarantinedDeadContainers,
		InconsistentContainers:    inconsistentContainers,
		PartiallyDeadContainers:   partiallyDeadContainers,
	})

	return &Plan{
		TotalChunks:                totalChunks,
		ReachableChunks:            reachableChunks,
		UnreachableChunks:          int64(len(unreachable)),
		ReclaimableBytes:           logicalReclaimableBytes,
		PhysicallyReclaimableBytes: physicalReclaimableBytes,
		Summary: SimulationSummary{
			UnreachableChunks:                  int64(len(unreachable)),
			LogicallyReclaimableBytes:          logicalReclaimableBytes,
			PhysicallyReclaimableBytes:         physicalReclaimableBytes,
			FullyReclaimableContainers:         fullyReclaimableContainers,
			PartiallyDeadContainers:            partiallyDeadContainers,
			PackedBlocksLive:                   packedBlocksLive,
			PackedBlocksDead:                   packedBlocksDead,
			PackedBytesLive:                    packedBytesLive,
			PackedBytesReclaimable:             packedBytesReclaimable,
			RetainedDeadBytesDueToPackedBlocks: retainedDeadBytesDueToPackedBlocks,
		},
		AffectedContainers: affectedContainers,
		Warnings:           warnings,
	}, nil
}

func computePackedSimulationMetrics(ctx context.Context, dbconn *sql.DB, livePackedBlockIDs map[int64]struct{}) (int64, int64, int64, int64, int64, error) {
	blockRows, err := dbconn.QueryContext(ctx, `
		SELECT id, stored_size
		FROM storage_blocks
	`)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	defer func() { _ = blockRows.Close() }()

	var packedBlocksLive int64
	var packedBlocksDead int64
	var packedBytesLive int64
	var packedBytesReclaimable int64

	for blockRows.Next() {
		var blockID int64
		var storedSize int64
		if err := blockRows.Scan(&blockID, &storedSize); err != nil {
			return 0, 0, 0, 0, 0, err
		}
		if _, live := livePackedBlockIDs[blockID]; live {
			packedBlocksLive++
			packedBytesLive += storedSize
			continue
		}
		packedBlocksDead++
		packedBytesReclaimable += storedSize
	}
	if err := blockRows.Err(); err != nil {
		return 0, 0, 0, 0, 0, err
	}

	deadInLiveRows, err := dbconn.QueryContext(ctx, `
		SELECT cbr.block_id, cbr.size_in_block
		FROM chunk_block_refs cbr
		JOIN chunk ch ON ch.id = cbr.chunk_id
		WHERE ch.status = 'COMPLETED'
		AND ch.live_ref_count = 0
		AND ch.pin_count = 0
	`)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	defer func() { _ = deadInLiveRows.Close() }()

	var retainedDeadBytesDueToPackedBlocks int64
	for deadInLiveRows.Next() {
		var blockID int64
		var sizeInBlock int64
		if err := deadInLiveRows.Scan(&blockID, &sizeInBlock); err != nil {
			return 0, 0, 0, 0, 0, err
		}
		if _, live := livePackedBlockIDs[blockID]; live {
			retainedDeadBytesDueToPackedBlocks += sizeInBlock
		}
	}
	if err := deadInLiveRows.Err(); err != nil {
		return 0, 0, 0, 0, 0, err
	}

	return packedBlocksLive, packedBlocksDead, packedBytesLive, packedBytesReclaimable, retainedDeadBytesDueToPackedBlocks, nil
}

// planContainerImpact scans sealed non-quarantined containers and returns the
// per-container impact summary and total reclaimable bytes.
func planContainerImpact(ctx context.Context, dbconn *sql.DB, unreachableChunkIDs map[int64]struct{}, livePackedBlockIDs map[int64]struct{}) ([]ContainerImpact, int64, int64, int64, int64, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = TRUE AND quarantine = FALSE
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, 0, 0, 0, 0, 0, err
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
			return nil, 0, 0, 0, 0, 0, err
		}
		containers = append(containers, c)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, 0, 0, 0, err
	}

	var affected []ContainerImpact
	var totalLogicalReclaimable int64
	var totalPhysicalReclaimable int64
	var fullyReclaimableContainers int64
	var partiallyDeadContainers int64
	var inconsistentContainers int64

	for _, c := range containers {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, 0, 0, 0, err
		}

		chunkRows, err := dbconn.QueryContext(ctx, `
			SELECT b.chunk_id, b.stored_size, ch.live_ref_count, ch.pin_count
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = $1
			AND ch.status = 'COMPLETED'
		`, c.id)
		if err != nil {
			return nil, 0, 0, 0, 0, 0, fmt.Errorf("gc.BuildPlan: query blocks for container %d: %w", c.id, err)
		}

		var totalUnits, reclaimUnits int64
		var reclaimBytes int64
		var totalBlockBytes int64

		for chunkRows.Next() {
			var chunkID int64
			var storedSize int64
			var liveRefCount, pinCount int64
			if err := chunkRows.Scan(&chunkID, &storedSize, &liveRefCount, &pinCount); err != nil {
				_ = chunkRows.Close()
				return nil, 0, 0, 0, 0, 0, err
			}
			totalUnits++
			totalBlockBytes += storedSize
			_, isUnreachable := unreachableChunkIDs[chunkID]
			isLive := liveRefCount > 0 || pinCount > 0
			if isUnreachable && !isLive {
				reclaimUnits++
				reclaimBytes += storedSize
			}
		}
		_ = chunkRows.Close()
		if err := chunkRows.Err(); err != nil {
			return nil, 0, 0, 0, 0, 0, err
		}

		packedRows, err := dbconn.QueryContext(ctx, `
			SELECT id, stored_size
			FROM storage_blocks
			WHERE container_id = $1
		`, c.id)
		if err != nil {
			return nil, 0, 0, 0, 0, 0, fmt.Errorf("gc.BuildPlan: query storage_blocks for container %d: %w", c.id, err)
		}
		for packedRows.Next() {
			var blockID int64
			var storedSize int64
			if err := packedRows.Scan(&blockID, &storedSize); err != nil {
				_ = packedRows.Close()
				return nil, 0, 0, 0, 0, 0, err
			}
			totalUnits++
			totalBlockBytes += storedSize
			if _, live := livePackedBlockIDs[blockID]; !live {
				reclaimUnits++
				reclaimBytes += storedSize
			}
		}
		_ = packedRows.Close()
		if err := packedRows.Err(); err != nil {
			return nil, 0, 0, 0, 0, 0, err
		}

		if reclaimUnits == 0 {
			continue
		}

		fullyReclaimable := totalUnits > 0 && reclaimUnits == totalUnits
		requiresCompaction := reclaimUnits > 0 && !fullyReclaimable
		liveBytesAfterGC := c.size - reclaimBytes
		if liveBytesAfterGC < 0 {
			inconsistentContainers++
			liveBytesAfterGC = 0
		}
		if totalBlockBytes > c.size {
			inconsistentContainers++
		}

		impact := ContainerImpact{
			ContainerID:        c.id,
			Filename:           c.filename,
			TotalBytes:         c.size,
			LiveBytesAfterGC:   liveBytesAfterGC,
			ReclaimableBytes:   reclaimBytes,
			ReclaimableChunks:  reclaimUnits,
			TotalChunks:        totalUnits,
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

	return affected, totalLogicalReclaimable, totalPhysicalReclaimable, fullyReclaimableContainers, partiallyDeadContainers, inconsistentContainers, nil
}

func planActiveContainerImpact(ctx context.Context, dbconn *sql.DB, reachableChunkIDs, unreachableChunkIDs map[int64]struct{}, livePackedBlockIDs, reachablePackedBlockIDs map[int64]struct{}) ([]ContainerImpact, int64, int64, int64, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT c.id, c.filename, c.current_size
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
			AND ch.status = 'COMPLETED'
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
	var inconsistentContainers int64

	for _, c := range containers {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, 0, 0, err
		}

		chunkRows, err := dbconn.QueryContext(ctx, `
			SELECT b.chunk_id, b.stored_size, ch.status
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = $1
		`, c.id)
		if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("gc.BuildPlan: query blocks for active container %d: %w", c.id, err)
		}

		var totalUnits int64
		var reclaimUnits int64
		var reclaimBytes int64
		var totalBlockBytes int64
		var hasRetained bool

		for chunkRows.Next() {
			var chunkID int64
			var storedSize int64
			var status string
			if err := chunkRows.Scan(&chunkID, &storedSize, &status); err != nil {
				_ = chunkRows.Close()
				return nil, 0, 0, 0, 0, err
			}
			if _, retained := reachableChunkIDs[chunkID]; retained {
				hasRetained = true
			}
			if status != "COMPLETED" {
				continue
			}
			totalUnits++
			totalBlockBytes += storedSize
			if _, unreachable := unreachableChunkIDs[chunkID]; !unreachable {
				continue
			}
			reclaimUnits++
			reclaimBytes += storedSize
		}
		_ = chunkRows.Close()
		if err := chunkRows.Err(); err != nil {
			return nil, 0, 0, 0, 0, err
		}

		packedRows, err := dbconn.QueryContext(ctx, `
			SELECT id, stored_size
			FROM storage_blocks
			WHERE container_id = $1
		`, c.id)
		if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("gc.BuildPlan: query storage_blocks for active container %d: %w", c.id, err)
		}
		for packedRows.Next() {
			var blockID int64
			var storedSize int64
			if err := packedRows.Scan(&blockID, &storedSize); err != nil {
				_ = packedRows.Close()
				return nil, 0, 0, 0, 0, err
			}
			totalUnits++
			totalBlockBytes += storedSize
			if _, retained := reachablePackedBlockIDs[blockID]; retained {
				hasRetained = true
			}
			if _, live := livePackedBlockIDs[blockID]; live {
				continue
			}
			reclaimUnits++
			reclaimBytes += storedSize
		}
		_ = packedRows.Close()
		if err := packedRows.Err(); err != nil {
			return nil, 0, 0, 0, 0, err
		}

		if hasRetained || totalUnits == 0 || reclaimUnits != totalUnits {
			continue
		}
		if totalBlockBytes > c.size {
			inconsistentContainers++
		}

		affected = append(affected, ContainerImpact{
			ContainerID:        c.id,
			Filename:           c.filename,
			TotalBytes:         c.size,
			LiveBytesAfterGC:   0,
			ReclaimableBytes:   reclaimBytes,
			ReclaimableChunks:  reclaimUnits,
			TotalChunks:        totalUnits,
			FullyReclaimable:   true,
			RequiresCompaction: false,
		})
		totalLogicalReclaimable += reclaimBytes
		totalPhysicalReclaimable += c.size
		fullyReclaimableContainers++
	}

	return affected, totalLogicalReclaimable, totalPhysicalReclaimable, fullyReclaimableContainers, inconsistentContainers, nil
}

func countUnreachableChunksWithoutContainer(ctx context.Context, dbconn *sql.DB, unreachableChunkIDs map[int64]struct{}) (int64, error) {
	if len(unreachableChunkIDs) == 0 {
		return 0, nil
	}
	rows, err := dbconn.QueryContext(ctx, `
		SELECT ch.id
		FROM chunk ch
		WHERE ch.status = 'COMPLETED'
		AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.chunk_id = ch.id)
		AND NOT EXISTS (SELECT 1 FROM chunk_block_refs cbr WHERE cbr.chunk_id = ch.id)
	`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var count int64
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return 0, err
		}
		if _, ok := unreachableChunkIDs[chunkID]; ok {
			count++
		}
	}
	return count, rows.Err()
}

func countQuarantinedContainersWithDeadChunks(ctx context.Context, dbconn *sql.DB, unreachableChunkIDs map[int64]struct{}) (int64, error) {
	if len(unreachableChunkIDs) == 0 {
		return 0, nil
	}
	rows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT c.id, x.chunk_id
		FROM container c
		JOIN (
			SELECT b.container_id, b.chunk_id
			FROM blocks b
			UNION
			SELECT sb.container_id, cbr.chunk_id
			FROM storage_blocks sb
			JOIN chunk_block_refs cbr ON cbr.block_id = sb.id
		) x ON x.container_id = c.id
		JOIN chunk ch ON ch.id = x.chunk_id
		WHERE c.quarantine = TRUE
		AND ch.status = 'COMPLETED'
	`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	seen := make(map[int64]struct{})
	for rows.Next() {
		var containerID, chunkID int64
		if err := rows.Scan(&containerID, &chunkID); err != nil {
			return 0, err
		}
		if _, ok := unreachableChunkIDs[chunkID]; ok {
			seen[containerID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return int64(len(seen)), nil
}

func buildWarnings(inputs warningInputs) []Warning {
	warnings := make([]Warning, 0, 5)
	if inputs.QuarantinedDeadContainers > 0 {
		warnings = append(warnings, Warning{
			Code:    "QUARANTINED_CONTAINER",
			Message: fmt.Sprintf("quarantined containers are excluded from physical reclaim calculation (%d affected container(s))", inputs.QuarantinedDeadContainers),
		})
	}
	if inputs.InconsistentContainers > 0 {
		warnings = append(warnings, Warning{
			Code:    "INCONSISTENT_METADATA",
			Message: fmt.Sprintf("inconsistent container metadata found; physical reclaim calculation may be approximate (%d container(s))", inputs.InconsistentContainers),
		})
	}
	if inputs.MissingContainerChunks > 0 {
		warnings = append(warnings, Warning{
			Code:    "CHUNK_MISSING_CONTAINER",
			Message: fmt.Sprintf("completed chunks without container placement were found; physical reclaim calculation excludes them (%d chunk(s))", inputs.MissingContainerChunks),
		})
	}
	if len(inputs.UnknownChunkerVersions) > 0 {
		warnings = append(warnings, Warning{
			Code:    "UNKNOWN_CHUNKER_VERSION",
			Message: fmt.Sprintf("unknown chunker version(s) found in completed chunks: %s", strings.Join(inputs.UnknownChunkerVersions, ", ")),
		})
	}
	if inputs.PartiallyDeadContainers > 0 {
		warnings = append(warnings, Warning{
			Code:    "PARTIAL_RECLAIM_REQUIRES_COMPACTION",
			Message: fmt.Sprintf("partial container reclaim is not physically possible yet; %d container(s) require future compaction/block work", inputs.PartiallyDeadContainers),
		})
	}
	return warnings
}

func packedBlockIDsForChunks(ctx context.Context, dbconn *sql.DB, chunkIDs map[int64]struct{}) (map[int64]struct{}, error) {
	if len(chunkIDs) == 0 {
		return map[int64]struct{}{}, nil
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT chunk_id, block_id
		FROM chunk_block_refs
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	blockIDs := make(map[int64]struct{})
	for rows.Next() {
		var chunkID, blockID int64
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
