package main

import (
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
)

func TestStatsCommandHumanWithContainersShowsContainerDetails(t *testing.T) {
	originalRunStats := runObservabilityStatsPhase
	t.Cleanup(func() { runObservabilityStatsPhase = originalRunStats })

	runObservabilityStatsPhase = func(opts observability.StatsOptions) (*observability.StatsResult, error) {
		if !opts.IncludeContainers {
			t.Fatal("expected IncludeContainers=true when --containers is set")
		}
		return &observability.StatsResult{
			Repository: observability.RepositoryStats{ActiveWriteChunker: "v2-fastcdc"},
			Containers: observability.ContainerStats{
				TotalContainers: 1,
				TotalBytes:      1024,
				Records: []observability.ContainerStatRecord{{
					ID:         7,
					Filename:   "container_000007.ck",
					TotalBytes: 1024,
					LiveBytes:  768,
					DeadBytes:  256,
				}},
			},
		}, nil
	}

	output := captureStdout(t, func() {
		if err := runStatsCommand(parsedCommandLine{method: "stats", flags: map[string][]string{"containers": {""}}}, outputModeText); err != nil {
			t.Fatalf("runStatsCommand with --containers returned error: %v", err)
		}
	})

	for _, want := range []string{"Container details", "container_id: 7", "file: container_000007.ck", "size: 1.0 KiB", "live: 768 B", "dead: 256 B"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestRunInspectCommandChunkTextReverseShowsReferencedBySection(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		if entity != observability.EntityChunk {
			t.Fatalf("unexpected entity: %s", entity)
		}
		if id != "77" {
			t.Fatalf("unexpected id: %s", id)
		}
		if !opts.Reverse {
			t.Fatal("expected reverse=true")
		}
		return &observability.InspectResult{
			EntityType: observability.EntityChunk,
			EntityID:   "77",
			Summary: map[string]any{
				"size_bytes":      int64(2048),
				"chunker_version": "v2-fastcdc",
			},
			Relations: []observability.Relation{{
				Type:       "referenced_by",
				Direction:  observability.RelationIncoming,
				TargetType: observability.EntityLogicalFile,
				TargetID:   "42",
			}},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"chunk", "77"},
			flags:       map[string][]string{"reverse": {""}},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runInspectCommand chunk reverse text returned error: %v", err)
		}
	})

	for _, want := range []string{"Inspect chunk 77", "Summary", "size: 2.0 KiB", "Referenced by", "relation: logical file 42"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestRunInspectCommandContainerDeepTextShowsDetailedSummary(t *testing.T) {
	originalInspect := runObservabilityInspectPhase
	t.Cleanup(func() { runObservabilityInspectPhase = originalInspect })

	runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
		if entity != observability.EntityContainer {
			t.Fatalf("unexpected entity: %s", entity)
		}
		if id != "5" {
			t.Fatalf("unexpected id: %s", id)
		}
		if !opts.Deep {
			t.Fatal("expected deep=true")
		}
		if opts.Limit != 20 {
			t.Fatalf("expected limit=20, got %d", opts.Limit)
		}
		return &observability.InspectResult{
			EntityType: observability.EntityContainer,
			EntityID:   "5",
			Summary: map[string]any{
				"filename":          "ctr_5.bin",
				"size_bytes":        int64(4096),
				"chunk_count":       int64(12),
				"stored_size_bytes": int64(2048),
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runInspectCommand(parsedCommandLine{
			method:      "inspect",
			positionals: []string{"container", "5"},
			flags:       map[string][]string{"deep": {""}, "limit": {"20"}},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runInspectCommand container deep text returned error: %v", err)
		}
	})

	for _, want := range []string{"Inspect container 5", "Summary", "filename: ctr_5.bin", "size: 4.0 KiB", "chunks: 12", "stored_size_bytes: 2.0 KiB"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}
}
