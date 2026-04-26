package observability

import (
	"bytes"
	"encoding/json"
	"reflect"
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

func TestRenderStatsHumanIsDeterministic(t *testing.T) {
	input := &StatsResult{
		Repository: RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical:    LogicalStats{TotalFiles: 10, CompletedFiles: 10, TotalSizeBytes: 1024},
		Chunks: ChunkStats{TotalChunks: 3, CompletedChunks: 3, CompletedBytes: 512, ChunkerVersions: []VersionStat{
			{Version: "v2-fastcdc", Chunks: 2, Bytes: 200},
			{Version: "unknown", Chunks: 1, Bytes: 50},
		}},
	}

	var first bytes.Buffer
	if err := RenderStatsHuman(&first, input); err != nil {
		t.Fatalf("RenderStatsHuman first: %v", err)
	}
	var second bytes.Buffer
	if err := RenderStatsHuman(&second, input); err != nil {
		t.Fatalf("RenderStatsHuman second: %v", err)
	}

	if first.String() != second.String() {
		t.Fatalf("expected deterministic human output\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}
}

func TestRenderStatsJSONRoundTrips(t *testing.T) {
	input := &StatsResult{
		Repository: RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical:    LogicalStats{TotalFiles: 5, CompletedFiles: 5, CompletedSizeBytes: 1000},
		Chunks:     ChunkStats{ChunkerVersions: []VersionStat{{Version: "v2-fastcdc", Chunks: 5, Bytes: 1000}}},
		Efficiency: EfficiencyStats{DedupRatio: 2.0, DedupRatioPercent: 50.0, ContainerOverheadPct: 3.0, StorageOverheadPct: 3.0},
	}

	var buf bytes.Buffer
	if err := RenderStatsJSON(&buf, input); err != nil {
		t.Fatalf("RenderStatsJSON: %v", err)
	}

	var decoded StatsResult
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal roundtrip: %v", err)
	}

	if decoded.Repository.ActiveWriteChunker != input.Repository.ActiveWriteChunker {
		t.Fatalf("repository chunker mismatch: got=%q want=%q", decoded.Repository.ActiveWriteChunker, input.Repository.ActiveWriteChunker)
	}
	if !reflect.DeepEqual(decoded.Chunks.ChunkerVersions, input.Chunks.ChunkerVersions) {
		t.Fatalf("chunker_versions mismatch: got=%+v want=%+v", decoded.Chunks.ChunkerVersions, input.Chunks.ChunkerVersions)
	}
}

func TestRenderStatsHumanSortsChunkerVersions(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Chunks: ChunkStats{ChunkerVersions: []VersionStat{
			{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
			{Version: "unknown", Chunks: 1, Bytes: 9},
			{Version: "v1-simple-rolling", Chunks: 3, Bytes: 30},
		}},
	}

	if err := RenderStatsHuman(&buf, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}
	out := buf.String()
	idxUnknown := strings.Index(out, "unknown:")
	idxV1 := strings.Index(out, "v1-simple-rolling:")
	idxV2 := strings.Index(out, "v2-fastcdc:")
	if !(idxUnknown < idxV1 && idxV1 < idxV2) {
		t.Fatalf("expected sorted chunker version order, got:\n%s", out)
	}
}

func TestRenderStatsHumanHidesContainerDetailsByDefault(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Containers: ContainerStats{Records: nil},
	}

	if err := RenderStatsHuman(&buf, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}
	if strings.Contains(buf.String(), "Container details") {
		t.Fatalf("expected no container details section by default, got:\n%s", buf.String())
	}
}

func TestRenderStatsHumanShowsContainerDetailsWhenRequested(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Containers: ContainerStats{Records: []ContainerStatRecord{{ID: 1, Filename: "container_000001.ck", TotalBytes: 64, LiveBytes: 64, DeadBytes: 0, Quarantine: false}}},
	}

	if err := RenderStatsHuman(&buf, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Container details") {
		t.Fatalf("expected container details section, got:\n%s", out)
	}
	if !strings.Contains(out, "container_000001.ck") {
		t.Fatalf("expected container row, got:\n%s", out)
	}
}

func TestRenderStatsHumanAndJSONUseSameEfficiencyFields(t *testing.T) {
	input := &StatsResult{
		Efficiency: EfficiencyStats{
			DedupRatio:           2.29,
			DedupRatioPercent:    56.3,
			ContainerOverheadPct: 3.1,
			StorageOverheadPct:   3.1,
		},
	}

	var human bytes.Buffer
	if err := RenderStatsHuman(&human, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}
	humanOut := human.String()
	for _, want := range []string{"dedup ratio:         2.29x", "dedup savings:       56.3%", "container overhead:  3.1%"} {
		if !strings.Contains(humanOut, want) {
			t.Fatalf("expected human output to contain %q, got:\n%s", want, humanOut)
		}
	}

	var jsonBuf bytes.Buffer
	if err := RenderStatsJSON(&jsonBuf, input); err != nil {
		t.Fatalf("RenderStatsJSON: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(jsonBuf.Bytes(), &decoded); err != nil {
		t.Fatalf("decode json output: %v", err)
	}
	eff, ok := decoded["efficiency"].(map[string]any)
	if !ok {
		t.Fatalf("missing efficiency object in json output: %+v", decoded)
	}
	if got, _ := eff["dedup_ratio"].(float64); got != 2.29 {
		t.Fatalf("expected dedup_ratio 2.29, got %v", eff["dedup_ratio"])
	}
	if got, _ := eff["dedup_ratio_percent"].(float64); got != 56.3 {
		t.Fatalf("expected dedup_ratio_percent 56.3, got %v", eff["dedup_ratio_percent"])
	}
	if got, _ := eff["container_overhead_pct"].(float64); got != 3.1 {
		t.Fatalf("expected container_overhead_pct 3.1, got %v", eff["container_overhead_pct"])
	}
}
