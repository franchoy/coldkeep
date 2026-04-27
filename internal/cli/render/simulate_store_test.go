package render

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRenderSimulateStoreHumanLayout(t *testing.T) {
	r := &SimulateStoreReport{
		Subcommand:        "store",
		Path:              "/tmp/input.bin",
		Files:             2,
		Chunks:            5,
		Containers:        1,
		LogicalSizeBytes:  2097152,
		PhysicalSizeBytes: 1048576,
		DedupRatioPct:     50,
	}

	var buf bytes.Buffer
	if err := RenderSimulateStoreHuman(&buf, r); err != nil {
		t.Fatalf("RenderSimulateStoreHuman: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"[SIMULATE] subcommand=store path=/tmp/input.bin",
		"Files:          2",
		"Chunks:         5",
		"Containers:     1",
		"Logical size:   2097152 bytes (2.00 MB)",
		"Physical size:  1048576 bytes (1.00 MB)",
		"Dedup savings:  50.00%",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderSimulateStoreJSONContract(t *testing.T) {
	r := &SimulateStoreReport{Subcommand: "store-folder", Path: "/tmp/data", Files: 1}

	var buf bytes.Buffer
	if err := RenderSimulateStoreJSON(&buf, r); err != nil {
		t.Fatalf("RenderSimulateStoreJSON: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &payload); err != nil {
		t.Fatalf("parse JSON payload: %v output=%q", err, buf.String())
	}
	if got, _ := payload["status"].(string); got != "ok" {
		t.Fatalf("status mismatch: got=%v payload=%v", payload["status"], payload)
	}
	if got, _ := payload["command"].(string); got != "simulate" {
		t.Fatalf("command mismatch: got=%v payload=%v", payload["command"], payload)
	}
	if got, _ := payload["simulated"].(bool); !got {
		t.Fatalf("simulated mismatch: got=%v payload=%v", payload["simulated"], payload)
	}
	data, _ := payload["data"].(map[string]any)
	if data == nil {
		t.Fatalf("missing data object: payload=%v", payload)
	}
	if got, _ := data["subcommand"].(string); got != "store-folder" {
		t.Fatalf("subcommand mismatch: got=%v payload=%v", data["subcommand"], payload)
	}
}

func TestRenderSimulateStoreJSONIsDeterministic(t *testing.T) {
	r := &SimulateStoreReport{
		Subcommand:        "store",
		Path:              "/tmp/in",
		Files:             3,
		Chunks:            3,
		Containers:        1,
		LogicalSizeBytes:  300,
		PhysicalSizeBytes: 200,
		DedupRatioPct:     33.333,
	}

	var first bytes.Buffer
	if err := RenderSimulateStoreJSON(&first, r); err != nil {
		t.Fatalf("RenderSimulateStoreJSON first: %v", err)
	}
	var second bytes.Buffer
	if err := RenderSimulateStoreJSON(&second, r); err != nil {
		t.Fatalf("RenderSimulateStoreJSON second: %v", err)
	}
	if first.String() != second.String() {
		t.Fatalf("expected deterministic simulate-store JSON output\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}
}
