package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/franchoy/coldkeep/internal/observability"
)

type observabilityTraceCollector struct {
	events []TraceEvent
	err    error
}

func (c *observabilityTraceCollector) Event(event observability.TraceEvent) {
	if c == nil || c.err != nil {
		return
	}
	metadata, err := valuesFromMap(event.Metadata)
	if err != nil {
		c.err = fmt.Errorf("convert trace event %q metadata: %w", event.Step, err)
		return
	}
	c.events = append(c.events, TraceEvent{
		Step:     event.Step,
		Entity:   event.Entity,
		EntityID: event.EntityID,
		Message:  event.Message,
		Metadata: metadata,
	})
}

func traceOptions(enabled bool) (observability.TraceOptions, *observabilityTraceCollector) {
	collector := &observabilityTraceCollector{}
	if !enabled {
		return observability.TraceOptions{}, collector
	}
	return observability.TraceOptions{Enabled: true, Sink: collector}, collector
}

func statsFromObservability(raw *observability.StatsResult, trace []TraceEvent) StatsResult {
	if raw == nil {
		return StatsResult{Trace: cloneTraceEvents(trace)}
	}
	versions := make([]StatsVersion, len(raw.Chunks.ChunkerVersions))
	for i, item := range raw.Chunks.ChunkerVersions {
		versions[i] = StatsVersion{Version: item.Version, Chunks: item.Chunks, Bytes: item.Bytes}
	}
	records := make([]StatsContainerRecord, len(raw.Containers.Records))
	for i, item := range raw.Containers.Records {
		records[i] = StatsContainerRecord{
			ID: item.ID, Filename: item.Filename, TotalBytes: item.TotalBytes,
			LiveBytes: item.LiveBytes, DeadBytes: item.DeadBytes,
			Quarantine: item.Quarantine, LiveRatioPct: item.LiveRatioPct,
		}
	}
	warnings := make([]OperationWarning, len(raw.Warnings))
	for i, item := range raw.Warnings {
		warnings[i] = OperationWarning{Code: item.Code, Message: item.Message}
	}
	return StatsResult{
		GeneratedAtUTC: raw.GeneratedAtUTC,
		Repository:     StatsRepository{ActiveWriteChunker: raw.Repository.ActiveWriteChunker},
		Logical: StatsLogical{
			TotalFiles: raw.Logical.TotalFiles, CompletedFiles: raw.Logical.CompletedFiles,
			ProcessingFiles: raw.Logical.ProcessingFiles, AbortedFiles: raw.Logical.AbortedFiles,
			TotalSizeBytes: raw.Logical.TotalSizeBytes, CompletedSizeBytes: raw.Logical.CompletedSizeBytes,
			EstimatedDedupRatioPct: raw.Logical.EstimatedDedupRatioPct,
		},
		Physical: StatsPhysical{TotalPhysicalFiles: raw.Physical.TotalPhysicalFiles},
		Chunks: StatsChunks{
			TotalChunks: raw.Chunks.TotalChunks, CompletedChunks: raw.Chunks.CompletedChunks,
			CompletedBytes:  raw.Chunks.CompletedBytes,
			CountsByVersion: cloneStringInt64Map(raw.Chunks.CountsByVersion),
			BytesByVersion:  cloneStringInt64Map(raw.Chunks.BytesByVersion),
			ChunkerVersions: versions, TotalReferences: raw.Chunks.TotalReferences,
			UniqueReferenced: raw.Chunks.UniqueReferenced,
		},
		BlockLayout: StatsBlockLayout{
			StorageBlocksCount:        raw.BlockLayout.StorageBlocksCount,
			ChunkBlockRefsCount:       raw.BlockLayout.ChunkBlockRefsCount,
			AvgChunksPerBlock:         raw.BlockLayout.AvgChunksPerBlock,
			AvgBlockPlaintextSize:     raw.BlockLayout.AvgBlockPlaintextSize,
			AvgBlockStoredSize:        raw.BlockLayout.AvgBlockStoredSize,
			LogicalBytes:              raw.BlockLayout.LogicalBytes,
			CompressedBytes:           raw.BlockLayout.CompressedBytes,
			StoredBytes:               raw.BlockLayout.StoredBytes,
			CompressionSizeRatio:      raw.BlockLayout.CompressionSizeRatio,
			CompressionFactor:         raw.BlockLayout.CompressionFactor,
			PhysicalSizeRatio:         raw.BlockLayout.PhysicalSizeRatio,
			PhysicalFactor:            raw.BlockLayout.PhysicalFactor,
			CompressedBlocks:          raw.BlockLayout.CompressedBlocks,
			UncompressedBlocks:        raw.BlockLayout.UncompressedBlocks,
			CompressionCodecBreakdown: cloneStringInt64Map(raw.BlockLayout.CompressionCodecBreakdown),
			AvgBlockFillRatio:         raw.BlockLayout.AvgBlockFillRatio,
			LegacyBlockCount:          raw.BlockLayout.LegacyBlockCount,
			PackedBlockCount:          raw.BlockLayout.PackedBlockCount,
			CodecDistribution:         cloneStringInt64Map(raw.BlockLayout.CodecDistribution),
		},
		Containers: StatsContainers{
			TotalContainers:      raw.Containers.TotalContainers,
			HealthyContainers:    raw.Containers.HealthyContainers,
			QuarantineContainers: raw.Containers.QuarantineContainers,
			TotalBytes:           raw.Containers.TotalBytes, HealthyBytes: raw.Containers.HealthyBytes,
			QuarantineBytes: raw.Containers.QuarantineBytes,
			LiveBlockBytes:  raw.Containers.LiveBlockBytes, DeadBlockBytes: raw.Containers.DeadBlockBytes,
			FragmentationRatioPct: raw.Containers.FragmentationRatioPct, Records: records,
		},
		Efficiency: StatsEfficiency{
			LogicalBytes: raw.Efficiency.LogicalBytes, UniqueChunkBytes: raw.Efficiency.UniqueChunkBytes,
			ContainerBytes: raw.Efficiency.ContainerBytes, DedupRatio: raw.Efficiency.DedupRatio,
			DedupRatioPercent:    raw.Efficiency.DedupRatioPercent,
			ContainerOverheadPct: raw.Efficiency.ContainerOverheadPct,
			StorageOverheadPct:   raw.Efficiency.StorageOverheadPct,
		},
		Snapshots: StatsSnapshots{TotalSnapshots: raw.Snapshots.TotalSnapshots},
		Retention: StatsRetention{
			CurrentOnlyLogicalFiles:        raw.Retention.CurrentOnlyLogicalFiles,
			CurrentOnlyBytes:               raw.Retention.CurrentOnlyBytes,
			SnapshotReferencedLogicalFiles: raw.Retention.SnapshotReferencedLogicalFiles,
			SnapshotReferencedBytes:        raw.Retention.SnapshotReferencedBytes,
			SnapshotOnlyLogicalFiles:       raw.Retention.SnapshotOnlyLogicalFiles,
			SnapshotOnlyBytes:              raw.Retention.SnapshotOnlyBytes,
			SharedLogicalFiles:             raw.Retention.SharedLogicalFiles,
			SharedBytes:                    raw.Retention.SharedBytes,
		},
		Graph: StatsGraph{
			SnapshotReachableChunks: raw.Graph.SnapshotReachableChunks,
			SnapshotReachableBytes:  raw.Graph.SnapshotReachableBytes,
		},
		Warnings: warnings,
		Trace:    cloneTraceEvents(trace),
	}
}

func inspectFromObservability(raw *observability.InspectResult, trace []TraceEvent) (InspectResult, error) {
	if raw == nil {
		return InspectResult{Trace: cloneTraceEvents(trace)}, nil
	}
	summary, err := valuesFromMap(raw.Summary)
	if err != nil {
		return InspectResult{}, fmt.Errorf("convert inspect summary: %w", err)
	}
	metadata, err := valuesFromMap(raw.Metadata)
	if err != nil {
		return InspectResult{}, fmt.Errorf("convert inspect metadata: %w", err)
	}
	relations := make([]InspectRelation, len(raw.Relations))
	for i, item := range raw.Relations {
		relationMetadata, err := valuesFromMap(item.Metadata)
		if err != nil {
			return InspectResult{}, fmt.Errorf("convert inspect relation %d metadata: %w", i, err)
		}
		relations[i] = InspectRelation{
			Type: item.Type, Direction: RelationDirection(item.Direction),
			TargetType: InspectEntity(item.TargetType), TargetID: item.TargetID,
			Metadata: relationMetadata,
		}
	}
	warnings := make([]OperationWarning, len(raw.Warnings))
	for i, item := range raw.Warnings {
		warnings[i] = OperationWarning{Code: item.Code, Message: item.Message}
	}
	return InspectResult{
		GeneratedAtUTC: raw.GeneratedAtUTC,
		Entity:         InspectEntity(raw.EntityType), EntityID: raw.EntityID,
		Summary: summary, Metadata: metadata, Relations: relations,
		Warnings: warnings, Trace: cloneTraceEvents(trace),
	}, nil
}

func valuesFromMap(input map[string]any) (map[string]Value, error) {
	if len(input) == 0 {
		return nil, nil
	}
	out := make(map[string]Value, len(input))
	for key, item := range input {
		value, err := valueFromAny(item)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		out[key] = value
	}
	return out, nil
}

func valueFromAny(input any) (Value, error) {
	if input == nil {
		return Value{Kind: ValueNull}, nil
	}
	if number, ok := input.(json.Number); ok {
		if _, err := number.Int64(); err == nil {
			return Value{Kind: ValueInteger, Integer: string(number)}, nil
		}
		if _, err := number.Float64(); err != nil {
			return Value{}, fmt.Errorf("invalid JSON number %q", number)
		}
		return Value{Kind: ValueDecimal, Decimal: string(number)}, nil
	}

	value := reflect.ValueOf(input)
	switch value.Kind() {
	case reflect.Bool:
		return Value{Kind: ValueBoolean, Boolean: value.Bool()}, nil
	case reflect.String:
		return Value{Kind: ValueString, String: value.String()}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Value{Kind: ValueInteger, Integer: strconv.FormatInt(value.Int(), 10)}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Value{Kind: ValueInteger, Integer: strconv.FormatUint(value.Uint(), 10)}, nil
	case reflect.Float32, reflect.Float64:
		floating := value.Float()
		if math.IsNaN(floating) || math.IsInf(floating, 0) {
			return Value{}, fmt.Errorf("non-finite decimal")
		}
		return Value{Kind: ValueDecimal, Decimal: strconv.FormatFloat(floating, 'g', -1, value.Type().Bits())}, nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return Value{}, fmt.Errorf("map key type %s is not string", value.Type().Key())
		}
		object := make(map[string]Value, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			item, err := valueFromAny(iterator.Value().Interface())
			if err != nil {
				return Value{}, fmt.Errorf("object field %q: %w", iterator.Key().String(), err)
			}
			object[iterator.Key().String()] = item
		}
		return Value{Kind: ValueObject, Object: object}, nil
	case reflect.Slice, reflect.Array:
		items := make([]Value, value.Len())
		for i := 0; i < value.Len(); i++ {
			item, err := valueFromAny(value.Index(i).Interface())
			if err != nil {
				return Value{}, fmt.Errorf("array item %d: %w", i, err)
			}
			items[i] = item
		}
		return Value{Kind: ValueArray, Array: items}, nil
	case reflect.Interface:
		return valueFromAny(value.Interface())
	default:
		return Value{}, fmt.Errorf("unsupported dynamic value type %T", input)
	}
}

func cloneStringInt64Map(input map[string]int64) map[string]int64 {
	if input == nil {
		return nil
	}
	out := make(map[string]int64, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneTraceEvents(input []TraceEvent) []TraceEvent {
	if input == nil {
		return nil
	}
	return append([]TraceEvent(nil), input...)
}
