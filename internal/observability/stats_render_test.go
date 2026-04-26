package observability

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRenderStatsJSONWritesResultPayload(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Repository: RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical:    LogicalStats{TotalFiles: 3, CompletedSizeBytes: 1000},
		Chunks: ChunkStats{
			CompletedBytes:   400,
			TotalReferences:  10,
			UniqueReferenced: 7,
			ChunkerVersions: []VersionStat{
				{Version: "unknown", Chunks: 1, Bytes: 9},
				{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
			},
		},
		Efficiency: EfficiencyStats{DedupRatio: 2.5, DedupRatioPercent: 60.0, ContainerOverheadPct: 5.0, StorageOverheadPct: 5.0},
	}

	if err := RenderStatsJSON(&buf, input); err != nil {
		t.Fatalf("RenderStatsJSON: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if got, _ := decoded["repository"].(map[string]any)["active_write_chunker"].(string); got != "v2-fastcdc" {
		t.Fatalf("unexpected active_write_chunker: %q", got)
	}
	eff, ok := decoded["efficiency"].(map[string]any)
	if !ok {
		t.Fatalf("missing efficiency object: %+v", decoded)
	}
	if got, ok := eff["dedup_ratio"].(float64); !ok || got != 2.5 {
		t.Fatalf("unexpected efficiency.dedup_ratio: %v", eff["dedup_ratio"])
	}
	if got, ok := eff["dedup_ratio_percent"].(float64); !ok || got != 60.0 {
		t.Fatalf("unexpected efficiency.dedup_ratio_percent: %v", eff["dedup_ratio_percent"])
	}
	if got, ok := eff["container_overhead_pct"].(float64); !ok || got != 5.0 {
		t.Fatalf("unexpected efficiency.container_overhead_pct: %v", eff["container_overhead_pct"])
	}
	if got, ok := eff["storage_overhead_pct"].(float64); !ok || got != 5.0 {
		t.Fatalf("unexpected efficiency.storage_overhead_pct: %v", eff["storage_overhead_pct"])
	}
}

func TestRenderStatsHumanPrintsChunkerVersionsInSortedOrder(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Repository: RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical: LogicalStats{
			TotalFiles:         128,
			CompletedFiles:     128,
			TotalSizeBytes:     42 * 1024 * 1024 * 1024,
			CompletedSizeBytes: 42 * 1024 * 1024 * 1024,
		},
		Physical: PhysicalStats{TotalPhysicalFiles: 140},
		Chunks: ChunkStats{
			TotalChunks:      892104,
			CompletedChunks:  892104,
			CompletedBytes:   18 * 1024 * 1024 * 1024,
			TotalReferences:  1902220,
			UniqueReferenced: 892104,
			ChunkerVersions: []VersionStat{
				{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
				{Version: "unknown", Chunks: 1, Bytes: 9},
				{Version: "v1-simple-rolling", Chunks: 3, Bytes: 30},
			},
		},
		Efficiency: EfficiencyStats{ContainerOverheadPct: 3.1, StorageOverheadPct: 3.1},
		Containers: ContainerStats{
			TotalContainers:       320,
			HealthyContainers:     320,
			QuarantineContainers:  0,
			TotalBytes:            19 * 1024 * 1024 * 1024,
			LiveBlockBytes:        18 * 1024 * 1024 * 1024,
			DeadBlockBytes:        0,
			FragmentationRatioPct: 0,
			Records: []ContainerStatRecord{
				{ID: 2, Filename: "container_000002.ck", TotalBytes: 64 * 1024 * 1024, LiveBytes: 64 * 1024 * 1024, DeadBytes: 0, Quarantine: false},
				{ID: 1, Filename: "container_000001.ck", TotalBytes: 64 * 1024 * 1024, LiveBytes: 64 * 1024 * 1024, DeadBytes: 0, Quarantine: false},
			},
		},
		Snapshots: SnapshotStats{TotalSnapshots: 12},
		Retention: RetentionStats{CurrentOnlyLogicalFiles: 20, SnapshotOnlyLogicalFiles: 8, SharedLogicalFiles: 100},
	}

	if err := RenderStatsHuman(&buf, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}

	output := buf.String()
	idxUnknown := strings.Index(output, "unknown:")
	idxV1 := strings.Index(output, "v1-simple-rolling:")
	idxV2 := strings.Index(output, "v2-fastcdc:")
	if idxUnknown == -1 || idxV1 == -1 || idxV2 == -1 {
		t.Fatalf("expected all chunker version labels in output, got:\n%s", output)
	}
	if !(idxUnknown < idxV1 && idxV1 < idxV2) {
		t.Fatalf("expected sorted order unknown < v1-simple-rolling < v2-fastcdc, got:\n%s", output)
	}

	for _, want := range []string{
		"Coldkeep repository stats",
		"Repository",
		"Logical data",
		"Physical data",
		"Chunks",
		"Efficiency",
		"Containers",
		"Snapshots",
		"Retention",
		"Chunker versions",
		"Container details",
		"active write chunker: v2-fastcdc",
		"files:               128",
		"completed files:     128",
		"physical files:      140",
		"total chunks:        892,104",
		"references:          1,902,220",
		"container overhead:  3.1%",
		"current-only files:  20",
		"snapshot-only files: 8",
		"shared files:        100",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}

	firstDetail := strings.Index(output, "container_000001.ck")
	secondDetail := strings.Index(output, "container_000002.ck")
	if firstDetail == -1 || secondDetail == -1 {
		t.Fatalf("expected both container detail rows, got:\n%s", output)
	}
	if firstDetail >= secondDetail {
		t.Fatalf("expected deterministic container detail sort by id, got:\n%s", output)
	}
}
