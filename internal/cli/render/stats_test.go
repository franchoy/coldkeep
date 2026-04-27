package render

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
)

func TestRenderStatsJSONWritesResultPayload(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical:    observability.LogicalStats{TotalFiles: 3, CompletedSizeBytes: 1000},
		Chunks: observability.ChunkStats{
			CompletedBytes:   400,
			TotalReferences:  10,
			UniqueReferenced: 7,
			ChunkerVersions: []observability.VersionStat{
				{Version: "unknown", Chunks: 1, Bytes: 9},
				{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
			},
		},
		Efficiency: observability.EfficiencyStats{DedupRatio: 2.5, DedupRatioPercent: 60.0, ContainerOverheadPct: 5.0, StorageOverheadPct: 5.0},
	}

	if err := (JSONRenderer{}).RenderStats(&buf, input); err != nil {
		t.Fatalf("RenderStatsJSON: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if got, _ := decoded["type"].(string); got != "stats" {
		t.Fatalf("unexpected type: %v", decoded["type"])
	}
	meta, ok := decoded["meta"].(map[string]any)
	if !ok {
		t.Fatalf("missing meta object: %+v", decoded)
	}
	if got, _ := meta["version"].(string); got != "v1.6" {
		t.Fatalf("unexpected meta.version: %v", meta["version"])
	}
	if got, _ := meta["exact"].(bool); !got {
		t.Fatalf("unexpected meta.exact: %v", meta["exact"])
	}
	if _, ok := decoded["generated_at_utc"].(string); !ok {
		t.Fatalf("missing generated_at_utc: %+v", decoded)
	}
	if warnings, ok := decoded["warnings"].([]any); !ok || len(warnings) != 0 {
		t.Fatalf("expected empty warnings array, got: %v", decoded["warnings"])
	}
	data, ok := decoded["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data object: %+v", decoded)
	}
	if got, _ := data["repository"].(map[string]any)["active_write_chunker"].(string); got != "v2-fastcdc" {
		t.Fatalf("unexpected active_write_chunker: %q", got)
	}
	eff, ok := data["efficiency"].(map[string]any)
	if !ok {
		t.Fatalf("missing efficiency object: %+v", data)
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
		Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical: observability.LogicalStats{
			TotalFiles:         128,
			CompletedFiles:     128,
			TotalSizeBytes:     42 * 1024 * 1024 * 1024,
			CompletedSizeBytes: 42 * 1024 * 1024 * 1024,
		},
		Physical: observability.PhysicalStats{TotalPhysicalFiles: 140},
		Chunks: observability.ChunkStats{
			TotalChunks:      892104,
			CompletedChunks:  892104,
			CompletedBytes:   18 * 1024 * 1024 * 1024,
			TotalReferences:  1902220,
			UniqueReferenced: 892104,
			ChunkerVersions: []observability.VersionStat{
				{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
				{Version: "unknown", Chunks: 1, Bytes: 9},
				{Version: "v1-simple-rolling", Chunks: 3, Bytes: 30},
			},
		},
		Efficiency: observability.EfficiencyStats{ContainerOverheadPct: 3.1, StorageOverheadPct: 3.1},
		Containers: observability.ContainerStats{
			TotalContainers:       320,
			HealthyContainers:     320,
			QuarantineContainers:  0,
			TotalBytes:            19 * 1024 * 1024 * 1024,
			LiveBlockBytes:        18 * 1024 * 1024 * 1024,
			DeadBlockBytes:        0,
			FragmentationRatioPct: 0,
			Records: []observability.ContainerStatRecord{
				{ID: 2, Filename: "container_000002.ck", TotalBytes: 64 * 1024 * 1024, LiveBytes: 64 * 1024 * 1024, DeadBytes: 0, Quarantine: false},
				{ID: 1, Filename: "container_000001.ck", TotalBytes: 64 * 1024 * 1024, LiveBytes: 64 * 1024 * 1024, DeadBytes: 0, Quarantine: false},
			},
		},
		Snapshots: observability.SnapshotStats{TotalSnapshots: 12},
		Retention: observability.RetentionStats{CurrentOnlyLogicalFiles: 20, SnapshotOnlyLogicalFiles: 8, SharedLogicalFiles: 100},
	}

	if err := (HumanRenderer{}).RenderStats(&buf, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}

	output := buf.String()
	idxUnknown := strings.Index(output, "version: unknown")
	idxV1 := strings.Index(output, "version: v1-simple-rolling")
	idxV2 := strings.Index(output, "version: v2-fastcdc")
	if idxUnknown == -1 || idxV1 == -1 || idxV2 == -1 {
		t.Fatalf("expected all chunker version labels in output, got:\n%s", output)
	}
	if !(idxUnknown < idxV1 && idxV1 < idxV2) {
		t.Fatalf("expected sorted order unknown < v1-simple-rolling < v2-fastcdc, got:\n%s", output)
	}

	for _, want := range []string{
		"Coldkeep stats",
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
		"active_write_chunker: v2-fastcdc",
		"files: 128",
		"completed_files: 128",
		"physical_files: 140",
		"total_chunks: 892,104",
		"references: 1,902,220",
		"container_overhead: 3.1%",
		"current_only_files: 20",
		"snapshot_only_files: 8",
		"shared_files: 100",
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
		Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical:    observability.LogicalStats{TotalFiles: 10, CompletedFiles: 10, TotalSizeBytes: 1024},
		Chunks: observability.ChunkStats{TotalChunks: 3, CompletedChunks: 3, CompletedBytes: 512, ChunkerVersions: []observability.VersionStat{
			{Version: "v2-fastcdc", Chunks: 2, Bytes: 200},
			{Version: "unknown", Chunks: 1, Bytes: 50},
		}},
	}

	var first bytes.Buffer
	if err := (HumanRenderer{}).RenderStats(&first, input); err != nil {
		t.Fatalf("RenderStatsHuman first: %v", err)
	}
	var second bytes.Buffer
	if err := (HumanRenderer{}).RenderStats(&second, input); err != nil {
		t.Fatalf("RenderStatsHuman second: %v", err)
	}

	if first.String() != second.String() {
		t.Fatalf("expected deterministic human output\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}
}

func TestRenderStatsJSONRoundTrips(t *testing.T) {
	input := &StatsResult{
		Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
		Logical:    observability.LogicalStats{TotalFiles: 5, CompletedFiles: 5, CompletedSizeBytes: 1000},
		Chunks:     observability.ChunkStats{ChunkerVersions: []observability.VersionStat{{Version: "v2-fastcdc", Chunks: 5, Bytes: 1000}}},
		Efficiency: observability.EfficiencyStats{DedupRatio: 2.0, DedupRatioPercent: 50.0, ContainerOverheadPct: 3.0, StorageOverheadPct: 3.0},
	}

	var buf bytes.Buffer
	if err := (JSONRenderer{}).RenderStats(&buf, input); err != nil {
		t.Fatalf("RenderStatsJSON: %v", err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal roundtrip: %v", err)
	}
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data payload: %+v", envelope)
	}

	encodedData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal envelope data: %v", err)
	}

	var decoded StatsResult
	if err := json.Unmarshal(encodedData, &decoded); err != nil {
		t.Fatalf("unmarshal stats data: %v", err)
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
		Chunks: observability.ChunkStats{ChunkerVersions: []observability.VersionStat{
			{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
			{Version: "unknown", Chunks: 1, Bytes: 9},
			{Version: "v1-simple-rolling", Chunks: 3, Bytes: 30},
		}},
	}

	if err := (HumanRenderer{}).RenderStats(&buf, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}
	out := buf.String()
	idxUnknown := strings.Index(out, "version: unknown")
	idxV1 := strings.Index(out, "version: v1-simple-rolling")
	idxV2 := strings.Index(out, "version: v2-fastcdc")
	if !(idxUnknown < idxV1 && idxV1 < idxV2) {
		t.Fatalf("expected sorted chunker version order, got:\n%s", out)
	}
}

func TestRenderStatsHumanHidesContainerDetailsByDefault(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Containers: observability.ContainerStats{Records: nil},
	}

	if err := (HumanRenderer{}).RenderStats(&buf, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}
	if strings.Contains(buf.String(), "Container details") {
		t.Fatalf("expected no container details section by default, got:\n%s", buf.String())
	}
}

func TestRenderStatsHumanShowsContainerDetailsWhenRequested(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Containers: observability.ContainerStats{Records: []observability.ContainerStatRecord{{ID: 1, Filename: "container_000001.ck", TotalBytes: 64, LiveBytes: 64, DeadBytes: 0, Quarantine: false}}},
	}

	if err := (HumanRenderer{}).RenderStats(&buf, input); err != nil {
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
		Efficiency: observability.EfficiencyStats{
			DedupRatio:           2.29,
			DedupRatioPercent:    56.3,
			ContainerOverheadPct: 3.1,
			StorageOverheadPct:   3.1,
		},
	}

	var human bytes.Buffer
	if err := (HumanRenderer{}).RenderStats(&human, input); err != nil {
		t.Fatalf("RenderStatsHuman: %v", err)
	}
	humanOut := human.String()
	for _, want := range []string{"dedup_ratio: 2.29x", "dedup_savings: 56.3%", "container_overhead: 3.1%"} {
		if !strings.Contains(humanOut, want) {
			t.Fatalf("expected human output to contain %q, got:\n%s", want, humanOut)
		}
	}

	var jsonBuf bytes.Buffer
	if err := (JSONRenderer{}).RenderStats(&jsonBuf, input); err != nil {
		t.Fatalf("RenderStatsJSON: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(jsonBuf.Bytes(), &decoded); err != nil {
		t.Fatalf("decode json output: %v", err)
	}
	data, ok := decoded["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data object in json output: %+v", decoded)
	}
	eff, ok := data["efficiency"].(map[string]any)
	if !ok {
		t.Fatalf("missing efficiency object in json output: %+v", data)
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
