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
		Logical:    LogicalStats{TotalFiles: 3},
		Chunks: ChunkStats{
			TotalReferences:  10,
			UniqueReferenced: 7,
			ChunkerVersions: []VersionStat{
				{Version: "unknown", Chunks: 1, Bytes: 9},
				{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
			},
		},
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
}

func TestRenderStatsHumanPrintsChunkerVersionsInSortedOrder(t *testing.T) {
	var buf bytes.Buffer
	input := &StatsResult{
		Chunks: ChunkStats{
			ChunkerVersions: []VersionStat{
				{Version: "v2-fastcdc", Chunks: 2, Bytes: 20},
				{Version: "unknown", Chunks: 1, Bytes: 9},
				{Version: "v1-simple-rolling", Chunks: 3, Bytes: 30},
			},
		},
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
}
