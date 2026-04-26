package observability

import (
	"context"
	"fmt"
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

	return s.mapMaintenanceStats(raw, opts), nil
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
	r.Efficiency = buildEfficiencyStats(r.Logical.CompletedSizeBytes, r.Chunks.CompletedBytes, r.Containers.TotalBytes)

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
	return *service.mapMaintenanceStats(raw, StatsOptions{IncludeContainers: true})
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

func buildEfficiencyStats(logicalBytes, uniqueChunkBytes, containerBytes int64) EfficiencyStats {
	stats := EfficiencyStats{
		LogicalBytes:     logicalBytes,
		UniqueChunkBytes: uniqueChunkBytes,
		ContainerBytes:   containerBytes,
	}

	if logicalBytes > 0 {
		stats.DedupRatio = float64(uniqueChunkBytes) / float64(logicalBytes)
		stats.DedupRatioPercent = stats.DedupRatio * 100
	}

	if uniqueChunkBytes > 0 {
		stats.StorageOverheadPct = (float64(containerBytes-uniqueChunkBytes) / float64(uniqueChunkBytes)) * 100
	}

	return stats
}
