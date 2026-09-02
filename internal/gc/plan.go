// Package gc provides read-only GC planning and simulation logic. Real GC has
// an independent locked execution path in internal/maintenance.
package gc

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	chunkmeta "github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/retention"
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
	// ReclaimableBytes is the logical reclaimability estimate: sum of chunk.size
	// once per unprotected completed chunk with an eligible physical placement.
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
	Size           int64
	ChunkerVersion string
}

type warningInputs struct {
	UnknownChunkerVersions    []string
	MissingContainerChunks    int64
	QuarantinedDeadContainers int64
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
	if err := refuseOnIntegrityIssues(dbconn); err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: %w", err)
	}
	protected, err := retention.BuildProtectedStorageSet(ctx, dbconn, retention.ProtectionOptions{
		ExcludeSnapshotIDs: opts.AssumeDeletedSnapshots,
	})
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: protected storage set: %w", err)
	}

	allChunks, err := loadAllCompletedChunks(ctx, dbconn)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: load completed chunks: %w", err)
	}
	unknownVersions := make(map[string]struct{})
	for _, ch := range allChunks {
		if version := ch.ChunkerVersion; version != string(chunkmeta.VersionV1SimpleRolling) && version != string(chunkmeta.VersionV2FastCDC) {
			unknownVersions[version] = struct{}{}
		}
	}

	unprotected := make([]chunkRecord, 0)
	var reachableCompletedCount int64
	for _, ch := range allChunks {
		if _, ok := protected.GraphReachableCompletedChunkIDs[ch.ID]; ok {
			reachableCompletedCount++
		}
		if _, ok := protected.ProtectedCompletedChunkIDs[ch.ID]; !ok {
			unprotected = append(unprotected, ch)
		}
	}

	plan, err := buildPlanFromProtection(ctx, dbconn, int64(len(allChunks)), reachableCompletedCount, protected, unprotected, unknownVersions)
	if err != nil {
		return nil, fmt.Errorf("gc.BuildPlan: build plan from unreachable: %w", err)
	}

	return plan, nil
}

func loadAllCompletedChunks(ctx context.Context, dbconn *sql.DB) ([]chunkRecord, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, size, chunker_version
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
		if err := rows.Scan(&ch.ID, &ch.Size, &ch.ChunkerVersion); err != nil {
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

func buildPlanFromProtection(ctx context.Context, dbconn *sql.DB, totalChunks, reachableChunks int64, protected *retention.ProtectedStorageSet, unprotected []chunkRecord, unknownVersions map[string]struct{}) (*Plan, error) {
	unprotectedChunkIDs := make(map[int64]struct{}, len(unprotected))
	unprotectedChunkSizes := make(map[int64]int64, len(unprotected))
	for _, ch := range unprotected {
		unprotectedChunkIDs[ch.ID] = struct{}{}
		unprotectedChunkSizes[ch.ID] = ch.Size
	}

	affectedContainers, physicalReclaimableBytes, fullyReclaimableContainers, partiallyDeadContainers, err := planContainerImpact(ctx, dbconn, protected.ProtectedCompletedChunkIDs, protected.ProtectedPackedBlockIDs)
	if err != nil {
		return nil, err
	}
	sort.Slice(affectedContainers, func(i, j int) bool {
		return affectedContainers[i].ContainerID < affectedContainers[j].ContainerID
	})
	logicalReclaimableBytes, err := sumEligibleUnprotectedChunkSizes(ctx, dbconn, unprotectedChunkSizes)
	if err != nil {
		return nil, err
	}
	missingContainerChunks, err := countUnreachableChunksWithoutContainer(ctx, dbconn, unprotectedChunkIDs)
	if err != nil {
		return nil, err
	}
	quarantinedDeadContainers, err := countQuarantinedContainersWithDeadChunks(ctx, dbconn, unprotectedChunkIDs)
	if err != nil {
		return nil, err
	}
	packedBlocksLive, packedBlocksDead, packedBytesLive, packedBytesReclaimable, retainedDeadBytesDueToPackedBlocks, err := computePackedSimulationMetrics(ctx, dbconn, protected.ProtectedPackedBlockIDs, unprotectedChunkIDs)
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
		PartiallyDeadContainers:   partiallyDeadContainers,
	})

	return &Plan{
		TotalChunks:                totalChunks,
		ReachableChunks:            reachableChunks,
		UnreachableChunks:          totalChunks - reachableChunks,
		ReclaimableBytes:           logicalReclaimableBytes,
		PhysicallyReclaimableBytes: physicalReclaimableBytes,
		Summary: SimulationSummary{
			UnreachableChunks:                  totalChunks - reachableChunks,
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

func computePackedSimulationMetrics(ctx context.Context, dbconn *sql.DB, protectedBlockIDs, unprotectedChunkIDs map[int64]struct{}) (int64, int64, int64, int64, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT sb.id, sb.stored_size
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE c.quarantine = FALSE
		AND ((c.sealed = TRUE AND c.sealing = FALSE) OR c.sealed = FALSE)
		ORDER BY sb.id
	`)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	defer func() { _ = rows.Close() }()

	eligibleBlocks := make(map[int64]struct{})
	var liveBlocks, deadBlocks, liveBytes, reclaimableBytes int64
	for rows.Next() {
		var blockID, storedSize int64
		if err := rows.Scan(&blockID, &storedSize); err != nil {
			return 0, 0, 0, 0, 0, err
		}
		if storedSize <= 0 {
			return 0, 0, 0, 0, 0, fmt.Errorf("storage block %d has invalid stored_size %d", blockID, storedSize)
		}
		eligibleBlocks[blockID] = struct{}{}
		if _, protected := protectedBlockIDs[blockID]; protected {
			liveBlocks++
			liveBytes += storedSize
		} else {
			deadBlocks++
			reclaimableBytes += storedSize
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, 0, err
	}

	refRows, err := dbconn.QueryContext(ctx, `
		SELECT chunk_id, block_id, size_in_block
		FROM chunk_block_refs
		ORDER BY chunk_id
	`)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	defer func() { _ = refRows.Close() }()
	var retainedDeadBytes int64
	for refRows.Next() {
		var chunkID, blockID, sizeInBlock int64
		if err := refRows.Scan(&chunkID, &blockID, &sizeInBlock); err != nil {
			return 0, 0, 0, 0, 0, err
		}
		_, eligible := eligibleBlocks[blockID]
		_, protectedBlock := protectedBlockIDs[blockID]
		_, unprotectedChunk := unprotectedChunkIDs[chunkID]
		if eligible && protectedBlock && unprotectedChunk {
			retainedDeadBytes += sizeInBlock
		}
	}
	if err := refRows.Err(); err != nil {
		return 0, 0, 0, 0, 0, err
	}

	return liveBlocks, deadBlocks, liveBytes, reclaimableBytes, retainedDeadBytes, nil
}

// planContainerImpact classifies every physical storage unit in an eligible
// container exactly once. Stable sealed containers may be partially dead;
// active containers are reported only when every storage unit is reclaimable.
func planContainerImpact(ctx context.Context, dbconn *sql.DB, protectedChunkIDs, protectedBlockIDs map[int64]struct{}) ([]ContainerImpact, int64, int64, int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename, current_size
		FROM container
		WHERE quarantine = FALSE
		AND ((sealed = TRUE AND sealing = FALSE) OR sealed = FALSE)
		ORDER BY id
	`)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	defer func() { _ = rows.Close() }()
	type containerRow struct {
		id       int64
		filename string
		size     int64
	}
	containers := make([]containerRow, 0)
	for rows.Next() {
		var c containerRow
		if err := rows.Scan(&c.id, &c.filename, &c.size); err != nil {
			return nil, 0, 0, 0, err
		}
		if c.size < 0 {
			return nil, 0, 0, 0, fmt.Errorf("container %d has negative current_size %d", c.id, c.size)
		}
		containers = append(containers, c)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, 0, err
	}

	affected := make([]ContainerImpact, 0)
	var physicalReclaimable, fullyReclaimable, partiallyDead int64
	for _, c := range containers {
		var totalUnits, reclaimUnits, classifiedBytes, reclaimBytes int64
		legacyRows, err := dbconn.QueryContext(ctx, `
			SELECT b.chunk_id, b.stored_size, ch.status
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = $1
			AND NOT EXISTS (SELECT 1 FROM chunk_block_refs cbr WHERE cbr.chunk_id = b.chunk_id)
			ORDER BY b.id
		`, c.id)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		for legacyRows.Next() {
			var chunkID, storedSize int64
			var status string
			if err := legacyRows.Scan(&chunkID, &storedSize, &status); err != nil {
				_ = legacyRows.Close()
				return nil, 0, 0, 0, err
			}
			totalUnits++
			classifiedBytes += storedSize
			_, protected := protectedChunkIDs[chunkID]
			if status == "COMPLETED" && !protected {
				reclaimUnits++
				reclaimBytes += storedSize
			}
		}
		if err := legacyRows.Err(); err != nil {
			_ = legacyRows.Close()
			return nil, 0, 0, 0, err
		}
		_ = legacyRows.Close()

		packedRows, err := dbconn.QueryContext(ctx, `SELECT id, stored_size FROM storage_blocks WHERE container_id = $1 ORDER BY id`, c.id)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		for packedRows.Next() {
			var blockID, storedSize int64
			if err := packedRows.Scan(&blockID, &storedSize); err != nil {
				_ = packedRows.Close()
				return nil, 0, 0, 0, err
			}
			totalUnits++
			classifiedBytes += storedSize
			if _, protected := protectedBlockIDs[blockID]; !protected {
				reclaimUnits++
				reclaimBytes += storedSize
			}
		}
		if err := packedRows.Err(); err != nil {
			_ = packedRows.Close()
			return nil, 0, 0, 0, err
		}
		_ = packedRows.Close()

		if classifiedBytes > c.size {
			return nil, 0, 0, 0, fmt.Errorf("container %d classified storage bytes %d exceed current_size %d", c.id, classifiedBytes, c.size)
		}
		if totalUnits == 0 || reclaimUnits == 0 {
			continue
		}
		full := reclaimUnits == totalUnits
		liveBytesAfterGC := c.size - reclaimBytes
		if full {
			physicalReclaimable += c.size
			fullyReclaimable++
		} else {
			partiallyDead++
		}
		affected = append(affected, ContainerImpact{
			ContainerID:        c.id,
			Filename:           c.filename,
			TotalBytes:         c.size,
			LiveBytesAfterGC:   liveBytesAfterGC,
			ReclaimableBytes:   reclaimBytes,
			ReclaimableChunks:  reclaimUnits,
			TotalChunks:        totalUnits,
			FullyReclaimable:   full,
			RequiresCompaction: !full,
		})
	}
	return affected, physicalReclaimable, fullyReclaimable, partiallyDead, nil
}

func sumEligibleUnprotectedChunkSizes(ctx context.Context, dbconn *sql.DB, sizes map[int64]int64) (int64, error) {
	if len(sizes) == 0 {
		return 0, nil
	}
	rows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT placement.chunk_id
		FROM (
			SELECT b.chunk_id
			FROM blocks b
			JOIN container c ON c.id = b.container_id
			WHERE c.quarantine = FALSE
			AND ((c.sealed = TRUE AND c.sealing = FALSE) OR c.sealed = FALSE)
			UNION ALL
			SELECT cbr.chunk_id
			FROM chunk_block_refs cbr
			JOIN storage_blocks sb ON sb.id = cbr.block_id
			JOIN container c ON c.id = sb.container_id
			WHERE c.quarantine = FALSE
			AND ((c.sealed = TRUE AND c.sealing = FALSE) OR c.sealed = FALSE)
		) placement
		ORDER BY placement.chunk_id
	`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	var total int64
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return 0, err
		}
		if size, ok := sizes[chunkID]; ok {
			total += size
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return total, nil
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
	warnings := make([]Warning, 0, 4)
	if inputs.QuarantinedDeadContainers > 0 {
		warnings = append(warnings, Warning{
			Code:    "QUARANTINED_CONTAINER",
			Message: fmt.Sprintf("quarantined containers are excluded from physical reclaim calculation (%d affected container(s))", inputs.QuarantinedDeadContainers),
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
