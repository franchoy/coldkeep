package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
)

func (s *Service) Stats(ctx context.Context) (StatsResult, error) {
	if err := contextErr(ctx); err != nil {
		return StatsResult{}, err
	}
	if s == nil || s.db == nil {
		return StatsResult{}, fmt.Errorf("observability service requires non-nil db")
	}

	raw, err := maintenance.RunStatsResultWithDB(ctx, s.db)
	if err != nil {
		return StatsResult{}, err
	}

	return mapStatsResult(s.now().UTC(), raw), nil
}

func mapStatsResult(generatedAtUTC time.Time, raw *maintenance.StatsResult) StatsResult {
	if raw == nil {
		return StatsResult{GeneratedAtUTC: generatedAtUTC}
	}

	records := make([]ContainerStatRecord, 0, len(raw.Containers))
	for _, record := range raw.Containers {
		records = append(records, ContainerStatRecord{
			ID:           record.ID,
			Filename:     record.Filename,
			TotalBytes:   record.TotalBytes,
			LiveBytes:    record.LiveBytes,
			DeadBytes:    record.DeadBytes,
			Quarantine:   record.Quarantine,
			LiveRatioPct: record.LiveRatioPct,
		})
	}

	return StatsResult{
		GeneratedAtUTC: generatedAtUTC,
		Repository: RepositoryStats{
			ActiveWriteChunker: raw.ActiveWriteChunker,
		},
		Logical: LogicalStats{
			TotalFiles:             raw.TotalFiles,
			CompletedFiles:         raw.CompletedFiles,
			ProcessingFiles:        raw.ProcessingFiles,
			AbortedFiles:           raw.AbortedFiles,
			TotalSizeBytes:         raw.TotalLogicalSizeBytes,
			CompletedSizeBytes:     raw.CompletedSizeBytes,
			EstimatedDedupRatioPct: raw.EstimatedDedupRatioPct,
		},
		Physical: PhysicalStats{
			TotalPhysicalFiles: 0,
		},
		Chunks: ChunkStats{
			TotalChunks:      raw.TotalChunks,
			CompletedChunks:  raw.CompletedChunks,
			CompletedBytes:   raw.CompletedChunkBytes,
			CountsByVersion:  cloneInt64Map(raw.ChunkCountsByVersion),
			BytesByVersion:   cloneInt64Map(raw.ChunkBytesByVersion),
			TotalReferences:  raw.TotalChunkReferences,
			UniqueReferenced: raw.UniqueReferencedChunks,
		},
		Containers: ContainerStats{
			TotalContainers:       raw.TotalContainers,
			HealthyContainers:     raw.HealthyContainers,
			QuarantineContainers:  raw.QuarantineContainers,
			TotalBytes:            raw.TotalContainerBytes,
			HealthyBytes:          raw.HealthyContainerBytes,
			QuarantineBytes:       raw.QuarantineContainerBytes,
			LiveBlockBytes:        raw.LiveBlockBytes,
			DeadBlockBytes:        raw.DeadBlockBytes,
			FragmentationRatioPct: raw.FragmentationRatioPct,
			Records:               records,
		},
		Snapshots: SnapshotStats{},
		Retention: RetentionStats{
			CurrentOnlyLogicalFiles:        raw.SnapshotRetention.CurrentOnlyLogicalFiles,
			CurrentOnlyBytes:               raw.SnapshotRetention.CurrentOnlyBytes,
			SnapshotReferencedLogicalFiles: raw.SnapshotRetention.SnapshotReferencedLogicalFiles,
			SnapshotReferencedBytes:        raw.SnapshotRetention.SnapshotReferencedBytes,
			SnapshotOnlyLogicalFiles:       raw.SnapshotRetention.SnapshotOnlyLogicalFiles,
			SnapshotOnlyBytes:              raw.SnapshotRetention.SnapshotOnlyBytes,
			SharedLogicalFiles:             raw.SnapshotRetention.SharedLogicalFiles,
			SharedBytes:                    raw.SnapshotRetention.SharedBytes,
		},
	}
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

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
