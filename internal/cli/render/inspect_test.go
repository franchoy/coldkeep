package render

import (
	"bytes"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
)

func TestRenderInspectHumanChunkExampleLayout(t *testing.T) {
	r := &InspectResult{
		EntityType: observability.EntityChunk,
		EntityID:   "123",
		Summary: map[string]any{
			"size_bytes":      int64(4096),
			"chunker_version": "v2-fastcdc",
			"container_id":    int64(2),
		},
		Relations: []observability.Relation{
			{
				Type:       "referenced_by",
				Direction:  observability.RelationIncoming,
				TargetType: observability.EntityLogicalFile,
				TargetID:   "45",
			},
		},
	}

	var buf bytes.Buffer
	if err := (HumanRenderer{}).RenderInspect(&buf, r); err != nil {
		t.Fatalf("RenderInspectHuman: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"Chunk 123",
		"Summary",
		"size:",
		"4.0 KiB",
		"chunker version:",
		"v2-fastcdc",
		"container:",
		"2",
		"Referenced by",
		"Logical file:",
		"45",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderInspectHumanLogicalFileExampleLayout(t *testing.T) {
	r := &InspectResult{
		EntityType: observability.EntityLogicalFile,
		EntityID:   "45",
		Summary: map[string]any{
			"original_name":        "photo.jpg",
			"chunk_count":          int64(12),
			"chunker_version":      "v2-fastcdc",
			"avg_chunk_size_bytes": 2048.0,
		},
		Relations: []observability.Relation{
			{Type: "references", Direction: observability.RelationOutgoing, TargetType: observability.EntityChunk, TargetID: "123"},
			{Type: "references", Direction: observability.RelationOutgoing, TargetType: observability.EntityChunk, TargetID: "124"},
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "10"},
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "11"},
		},
	}

	var buf bytes.Buffer
	if err := (HumanRenderer{}).RenderInspect(&buf, r); err != nil {
		t.Fatalf("RenderInspectHuman: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"Logical file 45",
		"Summary",
		"name:",
		"photo.jpg",
		"chunks:",
		"12",
		"chunker version:",
		"v2-fastcdc",
		"References",
		"Chunk:",
		"123",
		"124",
		"Referenced by",
		"Snapshot:",
		"10",
		"11",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}
