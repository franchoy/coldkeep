package render

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
)

func TestRenderSimulationHumanIncludesNoMutationFooter(t *testing.T) {
	r := &SimulationResult{
		Kind: "gc",
		GC: &observability.GCSimulationResult{
			Kind: "gc",
			Summary: observability.GCSimulationSummary{
				ReachableChunks:            10,
				UnreachableChunks:          2,
				LogicallyReclaimableBytes:  1024,
				PhysicallyReclaimableBytes: 2048,
			},
		},
	}

	var buf bytes.Buffer
	if err := (HumanRenderer{}).RenderSimulation(&buf, r); err != nil {
		t.Fatalf("RenderSimulation human: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "GC simulation") {
		t.Fatalf("expected simulation title, got: %s", out)
	}
	if !strings.Contains(out, "Result") || !strings.Contains(out, "changed: false") {
		t.Fatalf("expected read-only footer, got: %s", out)
	}
}

func TestRenderSimulationJSONUsesStableEnvelope(t *testing.T) {
	r := &SimulationResult{
		Kind: "gc",
		GC: &observability.GCSimulationResult{
			Kind: "gc",
			Summary: observability.GCSimulationSummary{
				ReachableChunks: 7,
			},
		},
	}

	var buf bytes.Buffer
	if err := (JSONRenderer{}).RenderSimulation(&buf, r); err != nil {
		t.Fatalf("RenderSimulation json: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal simulation json: %v", err)
	}
	if payload["type"] != "simulation" {
		t.Fatalf("unexpected type: %v", payload["type"])
	}
	if _, ok := payload["generated_at_utc"].(string); !ok {
		t.Fatalf("missing generated_at_utc: %v", payload)
	}
	meta, ok := payload["meta"].(map[string]any)
	if !ok {
		t.Fatalf("missing meta object: %v", payload)
	}
	if got, _ := meta["version"].(string); got != "v1.6" {
		t.Fatalf("unexpected meta.version: %v", meta["version"])
	}
	if got, _ := meta["exact"].(bool); got != false {
		t.Fatalf("unexpected meta.exact: %v", meta["exact"])
	}
	if warnings, ok := payload["warnings"].([]any); !ok || len(warnings) != 0 {
		t.Fatalf("expected empty warnings array, got %v", payload["warnings"])
	}
	if _, ok := payload["data"].(map[string]any); !ok {
		t.Fatalf("expected data object, got: %T", payload["data"])
	}
}
