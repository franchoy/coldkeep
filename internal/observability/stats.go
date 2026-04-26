package observability

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
)

func (s *Service) Stats(ctx context.Context, opts StatsOptions) (*StatsResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("collect observability stats: observability service requires non-nil db")
	}

	raw, err := maintenance.RunStatsResultWithDB(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("collect observability stats: %w", err)
	}

	result := s.mapMaintenanceStats(raw, opts)
	if err := s.enrichStatsWithGraph(ctx, result, opts); err != nil {
		return nil, fmt.Errorf("collect observability stats: enrich with graph: %w", err)
	}

	result.Efficiency = calculateEfficiency(result)
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

	snapshotIDs, skipped, err := s.listNumericSnapshotIDs(ctx)
	if err != nil {
		return err
	}
	if skipped > 0 {
		result.Warnings = append(result.Warnings, ObservationWarning{
			Code:    "snapshot_ids_non_numeric_skipped",
			Message: fmt.Sprintf("skipped %d snapshot id(s) that are not numeric for graph reachability", skipped),
		})
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

func (s *Service) listNumericSnapshotIDs(ctx context.Context) ([]int64, int, error) {
	if s == nil || s.db == nil {
		return nil, 0, nil
	}

	rows, err := s.db.QueryContext(ctx, `SELECT id FROM snapshot ORDER BY created_at, id`)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

	ids := make([]int64, 0)
	skipped := 0
	for rows.Next() {
		var rawID string
		if err := rows.Scan(&rawID); err != nil {
			return nil, 0, err
		}

		parsed, err := strconv.ParseInt(strings.TrimSpace(rawID), 10, 64)
		if err != nil {
			skipped++
			continue
		}
		ids = append(ids, parsed)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return ids, skipped, nil
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

	var total int64
	for chunkID := range chunkIDs {
		var size int64
		err := s.db.QueryRowContext(ctx, `SELECT size FROM chunk WHERE id = ?`, chunkID).Scan(&size)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return 0, err
		}
		total += size
	}

	return total, nil
}

func (s *Service) snapshotReachabilityViaSQL(ctx context.Context, snapshotIDs []int64) (int64, int64, error) {
	if s == nil || s.db == nil || len(snapshotIDs) == 0 {
		return 0, 0, nil
	}

	uniqueChunkSizes := make(map[int64]int64)
	for _, snapshotID := range snapshotIDs {
		rows, err := s.db.QueryContext(
			ctx,
			`SELECT fc.chunk_id, c.size
			 FROM snapshot_file sf
			 JOIN file_chunk fc ON fc.logical_file_id = sf.logical_file_id
			 JOIN chunk c ON c.id = fc.chunk_id
			 WHERE sf.snapshot_id = ?`,
			strconv.FormatInt(snapshotID, 10),
		)
		if err != nil {
			return 0, 0, err
		}

		for rows.Next() {
			var chunkID int64
			var size int64
			if err := rows.Scan(&chunkID, &size); err != nil {
				_ = rows.Close()
				return 0, 0, err
			}
			if _, exists := uniqueChunkSizes[chunkID]; !exists {
				uniqueChunkSizes[chunkID] = size
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		_ = rows.Close()
	}

	var totalBytes int64
	for _, size := range uniqueChunkSizes {
		totalBytes += size
	}

	return int64(len(uniqueChunkSizes)), totalBytes, nil
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
		stats.StorageOverheadPct = (float64(containerBytes-uniqueChunkBytes) / float64(uniqueChunkBytes)) * 100
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
