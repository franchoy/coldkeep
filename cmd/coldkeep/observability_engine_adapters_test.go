package main

import (
	"encoding/json"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestEngineValueToAnyPreservesLargeIntegerJSONToken(t *testing.T) {
	value, err := engineValueToAny(engine.Value{Kind: engine.ValueInteger, Integer: "18446744073709551615"})
	if err != nil {
		t.Fatalf("engineValueToAny: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if got, want := string(encoded), "18446744073709551615"; got != want {
		t.Fatalf("integer token changed: got=%s want=%s", got, want)
	}
}

func TestStatsEngineCompatibilityProjectionPreservesFields(t *testing.T) {
	projected := statsResultFromEngine(engine.StatsResult{
		Logical: engine.StatsLogical{TotalFiles: 3, TotalSizeBytes: 99},
		Chunks: engine.StatsChunks{
			CountsByVersion: map[string]int64{"v2-fastcdc": 2},
			ChunkerVersions: []engine.StatsVersion{{Version: "v2-fastcdc", Chunks: 2, Bytes: 99}},
		},
		Warnings: []engine.OperationWarning{{Code: "warning", Message: "detail"}},
	})
	if projected.Logical.TotalFiles != 3 || projected.Logical.TotalSizeBytes != 99 {
		t.Fatalf("logical projection changed: %+v", projected.Logical)
	}
	if projected.Chunks.CountsByVersion["v2-fastcdc"] != 2 || len(projected.Chunks.ChunkerVersions) != 1 {
		t.Fatalf("chunk projection changed: %+v", projected.Chunks)
	}
	if len(projected.Warnings) != 1 || projected.Warnings[0].Code != "warning" {
		t.Fatalf("warning projection changed: %+v", projected.Warnings)
	}
}
