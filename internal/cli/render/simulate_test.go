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
	if !strings.Contains(out, "Result") || !strings.Contains(out, "changed: false") || !strings.Contains(out, "state_change: none") || !strings.Contains(out, "note: no repository state changed") {
		t.Fatalf("expected read-only footer, got: %s", out)
	}
}

func TestRenderSimulationHumanFormatsContainerImpactBytes(t *testing.T) {
	r := &SimulationResult{
		Kind: "gc",
		GC: &observability.GCSimulationResult{
			Kind: "gc",
			Containers: []observability.ContainerSimulationImpact{{
				ContainerID:       7,
				Filename:          "c007.bin",
				ReclaimableBytes:  1536,
				LiveBytesAfterGC:  512,
				ReclaimableChunks: 2,
				TotalChunks:       4,
			}},
		},
	}

	var buf bytes.Buffer
	if err := (HumanRenderer{}).RenderSimulation(&buf, r); err != nil {
		t.Fatalf("RenderSimulation human: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "reclaimable_bytes: 1.5 KiB") {
		t.Fatalf("expected human-readable reclaimable bytes, got: %s", out)
	}
	if !strings.Contains(out, "live_bytes_after_gc: 512 B") {
		t.Fatalf("expected human-readable live bytes after gc, got: %s", out)
	}
}

func TestRenderSimulationHumanIncludesPackedMetrics(t *testing.T) {
	r := &SimulationResult{
		Kind: "gc",
		GC: &observability.GCSimulationResult{
			Kind: "gc",
			Summary: observability.GCSimulationSummary{
				PackedBlocksLive:                   3,
				PackedBlocksDead:                   2,
				PackedBytesLive:                    3 * 1024 * 1024,
				PackedBytesReclaimable:             2 * 1024 * 1024,
				RetainedDeadBytesDueToPackedBlocks: 512 * 1024,
			},
		},
	}

	var buf bytes.Buffer
	if err := (HumanRenderer{}).RenderSimulation(&buf, r); err != nil {
		t.Fatalf("RenderSimulation human: %v", err)
	}
	out := buf.String()
	for _, expected := range []string{
		"Packed",
		"packed_blocks_live: 3",
		"packed_blocks_dead: 2",
		"packed_bytes_live: 3 MiB",
		"packed_bytes_reclaimable: 2 MiB",
		"retained_dead_bytes_due_to_packed_blocks: 0 MiB",
	} {
		if !strings.Contains(out, expected) {
			t.Fatalf("expected %q in output, got: %s", expected, out)
		}
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
	if got, _ := meta["version"].(string); got != "v1.7" {
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

func TestRenderSimulationJSONIsDeterministic(t *testing.T) {
	r := &SimulationResult{
		Kind: "gc",
		GC: &observability.GCSimulationResult{
			Kind: "gc",
			Assumptions: observability.GCSimulationAssumptions{
				DeletedSnapshots: []string{"s2", "s1"},
			},
			Containers: []observability.ContainerSimulationImpact{
				{ContainerID: 2, Filename: "b"},
				{ContainerID: 1, Filename: "a"},
			},
			Warnings: []observability.ObservationWarning{
				{Code: "B", Message: "b"},
				{Code: "A", Message: "a"},
			},
		},
	}

	var first bytes.Buffer
	if err := (JSONRenderer{}).RenderSimulation(&first, r); err != nil {
		t.Fatalf("RenderSimulationJSON first: %v", err)
	}
	var second bytes.Buffer
	if err := (JSONRenderer{}).RenderSimulation(&second, r); err != nil {
		t.Fatalf("RenderSimulationJSON second: %v", err)
	}

	if first.String() != second.String() {
		t.Fatalf("expected deterministic simulation JSON output\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(first.Bytes(), &payload); err != nil {
		t.Fatalf("decode simulation json: %v", err)
	}
	data := payload["data"].(map[string]any)
	gc := data["gc"].(map[string]any)
	assumptions := gc["assumptions"].(map[string]any)
	deleted := assumptions["deleted_snapshots"].([]any)
	if len(deleted) != 2 || deleted[0] != "s1" || deleted[1] != "s2" {
		t.Fatalf("expected sorted deleted snapshots, got %v", deleted)
	}
	containers := gc["containers"].([]any)
	firstContainer := containers[0].(map[string]any)
	if got, _ := firstContainer["container_id"].(float64); int(got) != 1 {
		t.Fatalf("expected sorted containers by id, got %v", firstContainer["container_id"])
	}
}
