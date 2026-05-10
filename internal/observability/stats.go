package observability

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
)

func (s *Service) Stats(ctx context.Context, opts StatsOptions) (*StatsResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.collect.start",
		Message: "collecting observability stats",
		Metadata: map[string]any{
			"include_containers": opts.IncludeContainers,
		},
	})
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("collect observability stats: observability service requires non-nil db")
	}

	raw, err := maintenance.RunStatsResultWithDB(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("collect observability stats: %w", err)
	}

	result := s.mapMaintenanceStats(raw, opts)
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.collect.repository",
		Message: "collected repository stats",
		Metadata: map[string]any{
			"active_write_chunker": result.Repository.ActiveWriteChunker,
		},
	})
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.collect.logical",
		Message: "collected logical file stats",
		Metadata: map[string]any{
			"total_files":     result.Logical.TotalFiles,
			"completed_files": result.Logical.CompletedFiles,
		},
	})
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.collect.chunks",
		Message: "collected chunk counts and chunker-version distribution",
		Metadata: map[string]any{
			"total_chunks": result.Chunks.TotalChunks,
		},
	})
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.collect.containers",
		Message: "collected container stats",
		Metadata: map[string]any{
			"total_containers": result.Containers.TotalContainers,
		},
	})
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.collect.retention",
		Message: "collected retention classification stats",
		Metadata: map[string]any{
			"snapshot_referenced_logical_files": result.Retention.SnapshotReferencedLogicalFiles,
		},
	})
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.graph.enrich",
		Message: "enriching stats with graph reachability",
	})
	if err := s.enrichStatsWithGraph(ctx, result, opts); err != nil {
		return nil, fmt.Errorf("collect observability stats: enrich with graph: %w", err)
	}

	result.Efficiency = calculateEfficiency(result)
	emitTrace(opts.Trace, TraceEvent{
		Step:    "stats.collect.complete",
		Message: "completed observability stats collection",
		Metadata: map[string]any{
			"warnings": len(result.Warnings),
		},
	})
	return result, nil
}

func (s *Service) mapMaintenanceStats(raw *maintenance.StatsResult, opts StatsOptions) *StatsResult {
	generatedAtUTC := time.Now().UTC()
	if s != nil && s.now != nil {
		generatedAtUTC = s.now()
	}

	r := &StatsResult{
		GeneratedAtUTC: generatedAtUTC,
	}
	if raw == nil {
		return r
	}

	r.Repository = RepositoryStats{
		ActiveWriteChunker: raw.ActiveWriteChunker,
	}
	r.Logical = LogicalStats{
		TotalFiles:             raw.TotalFiles,
		CompletedFiles:         raw.CompletedFiles,
		ProcessingFiles:        raw.ProcessingFiles,
		AbortedFiles:           raw.AbortedFiles,
		TotalSizeBytes:         raw.TotalLogicalSizeBytes,
		CompletedSizeBytes:     raw.CompletedSizeBytes,
		EstimatedDedupRatioPct: raw.EstimatedDedupRatioPct,
	}
	r.Chunks = ChunkStats{
		TotalChunks:      raw.TotalChunks,
		CompletedChunks:  raw.CompletedChunks,
		CompletedBytes:   raw.CompletedChunkBytes,
		CountsByVersion:  cloneInt64Map(raw.ChunkCountsByVersion),
		BytesByVersion:   cloneInt64Map(raw.ChunkBytesByVersion),
		ChunkerVersions:  buildVersionStats(raw.ChunkCountsByVersion, raw.ChunkBytesByVersion),
		TotalReferences:  raw.TotalChunkReferences,
		UniqueReferenced: raw.UniqueReferencedChunks,
	}
	r.BlockLayout = BlockLayoutStats{
		StorageBlocksCount:        raw.BlockStats.StorageBlocks,
		ChunkBlockRefsCount:       raw.BlockStats.ChunkBlockRefs,
		AvgChunksPerBlock:         raw.BlockStats.AvgChunksPerBlock,
		AvgBlockPlaintextSize:     raw.BlockStats.AvgPlaintextSize,
		AvgBlockStoredSize:        raw.BlockStats.AvgStoredSize,
		LogicalBytes:              raw.BlockStats.LogicalBytes,
		CompressedBytes:           raw.BlockStats.CompressedBytes,
		StoredBytes:               raw.BlockStats.StoredBytes,
		CompressionSizeRatio:      raw.BlockStats.CompressionSizeRatio,
		CompressionFactor:         raw.BlockStats.CompressionFactor,
		PhysicalSizeRatio:         raw.BlockStats.PhysicalSizeRatio,
		PhysicalFactor:            raw.BlockStats.PhysicalFactor,
		CompressedBlocks:          raw.BlockStats.CompressedBlocks,
		UncompressedBlocks:        raw.BlockStats.UncompressedBlocks,
		CompressionCodecBreakdown: cloneInt64Map(raw.BlockStats.CompressionCodecBreakdown),
		AvgBlockFillRatio:         raw.BlockStats.FillRatio,
		LegacyBlockCount:          raw.BlockStats.LegacyBlocks,
		PackedBlockCount:          raw.BlockStats.PackedBlocks,
		CodecDistribution:         cloneInt64Map(raw.BlockStats.CodecDistribution),
	}
	r.Containers = ContainerStats{
		TotalContainers:       raw.TotalContainers,
		HealthyContainers:     raw.HealthyContainers,
		QuarantineContainers:  raw.QuarantineContainers,
		TotalBytes:            raw.TotalContainerBytes,
		HealthyBytes:          raw.HealthyContainerBytes,
		QuarantineBytes:       raw.QuarantineContainerBytes,
		LiveBlockBytes:        raw.LiveBlockBytes,
		DeadBlockBytes:        raw.DeadBlockBytes,
		FragmentationRatioPct: raw.FragmentationRatioPct,
	}
	r.Retention = RetentionStats{
		CurrentOnlyLogicalFiles:        raw.SnapshotRetention.CurrentOnlyLogicalFiles,
		CurrentOnlyBytes:               raw.SnapshotRetention.CurrentOnlyBytes,
		SnapshotReferencedLogicalFiles: raw.SnapshotRetention.SnapshotReferencedLogicalFiles,
		SnapshotReferencedBytes:        raw.SnapshotRetention.SnapshotReferencedBytes,
		SnapshotOnlyLogicalFiles:       raw.SnapshotRetention.SnapshotOnlyLogicalFiles,
		SnapshotOnlyBytes:              raw.SnapshotRetention.SnapshotOnlyBytes,
		SharedLogicalFiles:             raw.SnapshotRetention.SharedLogicalFiles,
		SharedBytes:                    raw.SnapshotRetention.SharedBytes,
	}
	if opts.IncludeContainers {
		r.Containers.Records = mapContainerRecords(raw.Containers)
	}

	return r
}

func mapContainerRecords(in []maintenance.ContainerStatRecord) []ContainerStatRecord {
	if len(in) == 0 {
		return nil
	}

	out := make([]ContainerStatRecord, 0, len(in))
	for _, record := range in {
		out = append(out, ContainerStatRecord{
			ID:           record.ID,
			Filename:     record.Filename,
			TotalBytes:   record.TotalBytes,
			LiveBytes:    record.LiveBytes,
			DeadBytes:    record.DeadBytes,
			Quarantine:   record.Quarantine,
			LiveRatioPct: record.LiveRatioPct,
		})
	}
	return out
}

func mapStatsResult(generatedAtUTC time.Time, raw *maintenance.StatsResult) StatsResult {
	service := &Service{now: func() time.Time { return generatedAtUTC }}
	result := service.mapMaintenanceStats(raw, StatsOptions{IncludeContainers: true})
	result.Efficiency = calculateEfficiency(result)
	return *result
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	if in == nil {
		return map[string]int64{}
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (s *Service) enrichStatsWithGraph(ctx context.Context, result *StatsResult, _ StatsOptions) error {
	if result == nil {
		return nil
	}
	if err := contextErr(ctx); err != nil {
		return err
	}

	count, err := s.countSnapshots(ctx)
	if err != nil {
		return err
	}
	result.Snapshots.TotalSnapshots = count

	snapshotIDs, err := s.listSnapshotIDs(ctx)
	if err != nil {
		return err
	}
	if len(snapshotIDs) == 0 || s == nil || s.graph == nil {
		return nil
	}

	reachableChunks, err := s.graph.GetReachableChunks(ctx, snapshotIDs)
	if err != nil {
		return err
	}
	result.Graph.SnapshotReachableChunks = int64(len(reachableChunks))

	reachableBytes, err := s.sumChunkSizesByID(ctx, reachableChunks)
	if err != nil {
		return err
	}
	result.Graph.SnapshotReachableBytes = reachableBytes

	checkChunks, checkBytes, err := s.snapshotReachabilityViaSQL(ctx, snapshotIDs)
	if err != nil {
		return err
	}
	if result.Graph.SnapshotReachableChunks != checkChunks {
		result.Warnings = append(result.Warnings, ObservationWarning{
			Code: "graph_snapshot_reachable_chunks_mismatch",
			Message: fmt.Sprintf(
				"graph snapshot reachable chunks=%d differs from aggregate snapshot query=%d",
				result.Graph.SnapshotReachableChunks,
				checkChunks,
			),
		})
	}
	if result.Graph.SnapshotReachableBytes != checkBytes {
		result.Warnings = append(result.Warnings, ObservationWarning{
			Code: "graph_snapshot_reachable_bytes_mismatch",
			Message: fmt.Sprintf(
				"graph snapshot reachable bytes=%d differs from aggregate snapshot query=%d",
				result.Graph.SnapshotReachableBytes,
				checkBytes,
			),
		})
	}

	return nil
}

func (s *Service) countSnapshots(ctx context.Context) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}

	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot`).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (s *Service) listSnapshotIDs(ctx context.Context) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, `SELECT id FROM snapshot ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ids := make([]string, 0)
	for rows.Next() {
		var rawID string
		if err := rows.Scan(&rawID); err != nil {
			return nil, err
		}
		ids = append(ids, strings.TrimSpace(rawID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

func calculateEfficiency(result *StatsResult) EfficiencyStats {
	if result == nil {
		return EfficiencyStats{}
	}
	return buildEfficiencyStats(result.Logical.CompletedSizeBytes, result.Chunks.CompletedBytes, result.Containers.TotalBytes)
}

func (s *Service) sumChunkSizesByID(ctx context.Context, chunkIDs map[int64]struct{}) (int64, error) {
	if s == nil || s.db == nil || len(chunkIDs) == 0 {
		return 0, nil
	}

	const maxBatchSize = 500

	ids := make([]int64, 0, len(chunkIDs))
	for chunkID := range chunkIDs {
		ids = append(ids, chunkID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var total int64
	for start := 0; start < len(ids); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(ids) {
			end = len(ids)
		}

		batch := ids[start:end]
		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for i, chunkID := range batch {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = chunkID
		}

		query := `SELECT id, size FROM chunk WHERE id IN (` + strings.Join(placeholders, ", ") + `)`

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return 0, err
		}

		sizeByID := make(map[int64]int64, len(batch))
		for rows.Next() {
			var chunkID int64
			var size int64
			if err := rows.Scan(&chunkID, &size); err != nil {
				_ = rows.Close()
				return 0, err
			}
			sizeByID[chunkID] = size
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return 0, err
		}
		if err := rows.Close(); err != nil {
			return 0, err
		}

		for _, chunkID := range batch {
			total += sizeByID[chunkID]
		}
	}

	return total, nil
}

func (s *Service) snapshotReachabilityViaSQL(ctx context.Context, snapshotIDs []string) (int64, int64, error) {
	if s == nil || s.db == nil || len(snapshotIDs) == 0 {
		return 0, 0, nil
	}

	placeholders := make([]string, len(snapshotIDs))
	args := make([]any, len(snapshotIDs))
	for i, snapshotID := range snapshotIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = snapshotID
	}

	query := `
		WITH snapshot_totals AS (
			SELECT snapshot_id, COUNT(*) AS file_count, COALESCE(SUM(size), 0) AS total_size
			FROM snapshot_file
			WHERE snapshot_id IN (` + strings.Join(placeholders, ", ") + `)
			GROUP BY snapshot_id
		),
		unique_chunks AS (
			SELECT DISTINCT fc.chunk_id
			FROM snapshot_file sf
			JOIN snapshot_totals st ON st.snapshot_id = sf.snapshot_id
			JOIN file_chunk fc ON fc.logical_file_id = sf.logical_file_id
		)
		SELECT COUNT(*), COALESCE(SUM(c.size),0)
		FROM unique_chunks uc
		JOIN chunk c ON c.id = uc.chunk_id
	`

	var chunkCount int64
	var totalBytes int64
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&chunkCount, &totalBytes); err != nil {
		return 0, 0, err
	}

	return chunkCount, totalBytes, nil
}

func buildEfficiencyStats(logicalBytes, uniqueChunkBytes, containerBytes int64) EfficiencyStats {
	stats := EfficiencyStats{
		LogicalBytes:     logicalBytes,
		UniqueChunkBytes: uniqueChunkBytes,
		ContainerBytes:   containerBytes,
	}

	if logicalBytes > 0 && uniqueChunkBytes > 0 {
		stats.DedupRatio = float64(logicalBytes) / float64(uniqueChunkBytes)
		savings := (1.0 - float64(uniqueChunkBytes)/float64(logicalBytes)) * 100
		if savings < 0 {
			savings = 0
		}
		stats.DedupRatioPercent = savings
	}

	if uniqueChunkBytes > 0 {
		overhead := (float64(containerBytes-uniqueChunkBytes) / float64(uniqueChunkBytes)) * 100
		stats.ContainerOverheadPct = overhead
		stats.StorageOverheadPct = overhead
	}

	return stats
}

func buildVersionStats(countsByVersion, bytesByVersion map[string]int64) []VersionStat {
	if len(countsByVersion) == 0 && len(bytesByVersion) == 0 {
		return nil
	}

	allVersions := make(map[string]struct{}, len(countsByVersion)+len(bytesByVersion))
	for version := range countsByVersion {
		allVersions[version] = struct{}{}
	}
	for version := range bytesByVersion {
		allVersions[version] = struct{}{}
	}

	versions := make([]string, 0, len(allVersions))
	for version := range allVersions {
		versions = append(versions, version)
	}
	sort.Strings(versions)

	out := make([]VersionStat, 0, len(versions))
	for _, version := range versions {
		out = append(out, VersionStat{
			Version: version,
			Chunks:  countsByVersion[version],
			Bytes:   bytesByVersion[version],
		})
	}

	return out
}
