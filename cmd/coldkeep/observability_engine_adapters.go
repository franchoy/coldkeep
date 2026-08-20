package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/observability"
)

func statsResultFromEngine(input engine.StatsResult) *observability.StatsResult {
	versions := make([]observability.VersionStat, len(input.Chunks.ChunkerVersions))
	for i, item := range input.Chunks.ChunkerVersions {
		versions[i] = observability.VersionStat{Version: item.Version, Chunks: item.Chunks, Bytes: item.Bytes}
	}
	records := make([]observability.ContainerStatRecord, len(input.Containers.Records))
	for i, item := range input.Containers.Records {
		records[i] = observability.ContainerStatRecord{
			ID: item.ID, Filename: item.Filename, TotalBytes: item.TotalBytes,
			LiveBytes: item.LiveBytes, DeadBytes: item.DeadBytes,
			Quarantine: item.Quarantine, LiveRatioPct: item.LiveRatioPct,
		}
	}
	warnings := make([]observability.ObservationWarning, len(input.Warnings))
	for i, item := range input.Warnings {
		warnings[i] = observability.ObservationWarning{Code: item.Code, Message: item.Message}
	}
	return &observability.StatsResult{
		GeneratedAtUTC: input.GeneratedAtUTC,
		Repository:     observability.RepositoryStats{ActiveWriteChunker: input.Repository.ActiveWriteChunker},
		Logical: observability.LogicalStats{
			TotalFiles: input.Logical.TotalFiles, CompletedFiles: input.Logical.CompletedFiles,
			ProcessingFiles: input.Logical.ProcessingFiles, AbortedFiles: input.Logical.AbortedFiles,
			TotalSizeBytes: input.Logical.TotalSizeBytes, CompletedSizeBytes: input.Logical.CompletedSizeBytes,
			EstimatedDedupRatioPct: input.Logical.EstimatedDedupRatioPct,
		},
		Physical: observability.PhysicalStats{TotalPhysicalFiles: input.Physical.TotalPhysicalFiles},
		Chunks: observability.ChunkStats{
			TotalChunks: input.Chunks.TotalChunks, CompletedChunks: input.Chunks.CompletedChunks,
			CompletedBytes:  input.Chunks.CompletedBytes,
			CountsByVersion: cloneStringInt64ForCLI(input.Chunks.CountsByVersion),
			BytesByVersion:  cloneStringInt64ForCLI(input.Chunks.BytesByVersion),
			ChunkerVersions: versions, TotalReferences: input.Chunks.TotalReferences,
			UniqueReferenced: input.Chunks.UniqueReferenced,
		},
		BlockLayout: observability.BlockLayoutStats{
			StorageBlocksCount:        input.BlockLayout.StorageBlocksCount,
			ChunkBlockRefsCount:       input.BlockLayout.ChunkBlockRefsCount,
			AvgChunksPerBlock:         input.BlockLayout.AvgChunksPerBlock,
			AvgBlockPlaintextSize:     input.BlockLayout.AvgBlockPlaintextSize,
			AvgBlockStoredSize:        input.BlockLayout.AvgBlockStoredSize,
			LogicalBytes:              input.BlockLayout.LogicalBytes,
			CompressedBytes:           input.BlockLayout.CompressedBytes,
			StoredBytes:               input.BlockLayout.StoredBytes,
			CompressionSizeRatio:      input.BlockLayout.CompressionSizeRatio,
			CompressionFactor:         input.BlockLayout.CompressionFactor,
			PhysicalSizeRatio:         input.BlockLayout.PhysicalSizeRatio,
			PhysicalFactor:            input.BlockLayout.PhysicalFactor,
			CompressedBlocks:          input.BlockLayout.CompressedBlocks,
			UncompressedBlocks:        input.BlockLayout.UncompressedBlocks,
			CompressionCodecBreakdown: cloneStringInt64ForCLI(input.BlockLayout.CompressionCodecBreakdown),
			AvgBlockFillRatio:         input.BlockLayout.AvgBlockFillRatio,
			LegacyBlockCount:          input.BlockLayout.LegacyBlockCount,
			PackedBlockCount:          input.BlockLayout.PackedBlockCount,
			CodecDistribution:         cloneStringInt64ForCLI(input.BlockLayout.CodecDistribution),
		},
		Containers: observability.ContainerStats{
			TotalContainers:      input.Containers.TotalContainers,
			HealthyContainers:    input.Containers.HealthyContainers,
			QuarantineContainers: input.Containers.QuarantineContainers,
			TotalBytes:           input.Containers.TotalBytes, HealthyBytes: input.Containers.HealthyBytes,
			QuarantineBytes: input.Containers.QuarantineBytes,
			LiveBlockBytes:  input.Containers.LiveBlockBytes, DeadBlockBytes: input.Containers.DeadBlockBytes,
			FragmentationRatioPct: input.Containers.FragmentationRatioPct, Records: records,
		},
		Efficiency: observability.EfficiencyStats{
			LogicalBytes:         input.Efficiency.LogicalBytes,
			UniqueChunkBytes:     input.Efficiency.UniqueChunkBytes,
			ContainerBytes:       input.Efficiency.ContainerBytes,
			DedupRatio:           input.Efficiency.DedupRatio,
			DedupRatioPercent:    input.Efficiency.DedupRatioPercent,
			ContainerOverheadPct: input.Efficiency.ContainerOverheadPct,
			StorageOverheadPct:   input.Efficiency.StorageOverheadPct,
		},
		Snapshots: observability.SnapshotStats{TotalSnapshots: input.Snapshots.TotalSnapshots},
		Retention: observability.RetentionStats{
			CurrentOnlyLogicalFiles:        input.Retention.CurrentOnlyLogicalFiles,
			CurrentOnlyBytes:               input.Retention.CurrentOnlyBytes,
			SnapshotReferencedLogicalFiles: input.Retention.SnapshotReferencedLogicalFiles,
			SnapshotReferencedBytes:        input.Retention.SnapshotReferencedBytes,
			SnapshotOnlyLogicalFiles:       input.Retention.SnapshotOnlyLogicalFiles,
			SnapshotOnlyBytes:              input.Retention.SnapshotOnlyBytes,
			SharedLogicalFiles:             input.Retention.SharedLogicalFiles,
			SharedBytes:                    input.Retention.SharedBytes,
		},
		Graph: observability.GraphStats{
			SnapshotReachableChunks: input.Graph.SnapshotReachableChunks,
			SnapshotReachableBytes:  input.Graph.SnapshotReachableBytes,
		},
		Warnings: warnings,
	}
}

func inspectResultFromEngine(input engine.InspectResult) (*observability.InspectResult, error) {
	summary, err := engineValuesToAny(input.Summary)
	if err != nil {
		return nil, fmt.Errorf("project inspect summary: %w", err)
	}
	metadata, err := engineValuesToAny(input.Metadata)
	if err != nil {
		return nil, fmt.Errorf("project inspect metadata: %w", err)
	}
	relations := make([]observability.Relation, len(input.Relations))
	for i, item := range input.Relations {
		relationMetadata, err := engineValuesToAny(item.Metadata)
		if err != nil {
			return nil, fmt.Errorf("project inspect relation %d: %w", i, err)
		}
		relations[i] = observability.Relation{
			Type: item.Type, Direction: observability.RelationDirection(item.Direction),
			TargetType: observability.EntityType(item.TargetType), TargetID: item.TargetID,
			Metadata: relationMetadata,
		}
	}
	warnings := make([]observability.ObservationWarning, len(input.Warnings))
	for i, item := range input.Warnings {
		warnings[i] = observability.ObservationWarning{Code: item.Code, Message: item.Message}
	}
	return &observability.InspectResult{
		GeneratedAtUTC: input.GeneratedAtUTC,
		EntityType:     observability.EntityType(input.Entity),
		EntityID:       input.EntityID,
		Summary:        summary,
		Metadata:       metadata,
		Relations:      relations,
		Warnings:       warnings,
	}, nil
}

func replayEngineTrace(options observability.TraceOptions, events []engine.TraceEvent) error {
	if !options.Enabled || options.Sink == nil {
		return nil
	}
	for _, item := range events {
		metadata, err := engineValuesToAny(item.Metadata)
		if err != nil {
			return fmt.Errorf("replay trace event %q: %w", item.Step, err)
		}
		options.Sink.Event(observability.TraceEvent{
			Step: item.Step, Entity: item.Entity, EntityID: item.EntityID,
			Message: item.Message, Metadata: metadata,
		})
	}
	return nil
}

func engineValuesToAny(input map[string]engine.Value) (map[string]any, error) {
	if len(input) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(input))
	for key, item := range input {
		value, err := engineValueToAny(item)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		out[key] = value
	}
	return out, nil
}

func engineValueToAny(input engine.Value) (any, error) {
	switch input.Kind {
	case engine.ValueNull:
		return nil, nil
	case engine.ValueBoolean:
		return input.Boolean, nil
	case engine.ValueString:
		return input.String, nil
	case engine.ValueInteger:
		if value, err := strconv.ParseInt(input.Integer, 10, 64); err == nil {
			return value, nil
		}
		if _, err := strconv.ParseUint(input.Integer, 10, 64); err == nil {
			return json.Number(input.Integer), nil
		}
		if _, err := json.Number(input.Integer).Float64(); err != nil {
			return nil, fmt.Errorf("invalid integer %q", input.Integer)
		}
		return json.Number(input.Integer), nil
	case engine.ValueDecimal:
		value, err := strconv.ParseFloat(input.Decimal, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid decimal %q", input.Decimal)
		}
		return value, nil
	case engine.ValueObject:
		return engineValuesToAny(input.Object)
	case engine.ValueArray:
		items := make([]any, len(input.Array))
		for i, item := range input.Array {
			value, err := engineValueToAny(item)
			if err != nil {
				return nil, fmt.Errorf("array item %d: %w", i, err)
			}
			items[i] = value
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unknown engine value kind %q", input.Kind)
	}
}

func cloneStringInt64ForCLI(input map[string]int64) map[string]int64 {
	if input == nil {
		return nil
	}
	out := make(map[string]int64, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
