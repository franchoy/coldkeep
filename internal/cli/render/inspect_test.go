package render

import (
	"bytes"
	"encoding/json"
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
		"Inspect chunk 123",
		"Summary",
		"size:",
		"4.0 KiB",
		"chunker_version:",
		"v2-fastcdc",
		"container:",
		"2",
		"Referenced by",
		"relation: logical file 45",
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
		"Inspect logical file 45",
		"Summary",
		"name:",
		"photo.jpg",
		"chunks:",
		"12",
		"chunker_version:",
		"v2-fastcdc",
		"References",
		"relation: chunk 123",
		"123",
		"124",
		"Referenced by",
		"relation: snapshot 10",
		"10",
		"11",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderInspectJSONIsDeterministic(t *testing.T) {
	r := &InspectResult{
		EntityType: observability.EntityChunk,
		EntityID:   "123",
		Relations: []observability.Relation{
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "11"},
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "10"},
		},
		Warnings: []observability.ObservationWarning{
			{Code: "B", Message: "b"},
			{Code: "A", Message: "a"},
		},
	}

	var first bytes.Buffer
	if err := (JSONRenderer{}).RenderInspect(&first, r); err != nil {
		t.Fatalf("RenderInspectJSON first: %v", err)
	}
	var second bytes.Buffer
	if err := (JSONRenderer{}).RenderInspect(&second, r); err != nil {
		t.Fatalf("RenderInspectJSON second: %v", err)
	}

	if first.String() != second.String() {
		t.Fatalf("expected deterministic inspect JSON output\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(first.Bytes(), &payload); err != nil {
		t.Fatalf("decode inspect json: %v", err)
	}
	data := payload["data"].(map[string]any)
	relations := data["relations"].([]any)
	firstRel := relations[0].(map[string]any)
	if got, _ := firstRel["target_id"].(string); got != "10" {
		t.Fatalf("expected sorted relations by target_id, got %v", got)
	}
}
