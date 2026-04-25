package benchmark

import (
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
)

func BenchmarkDefaultDatasets(b *testing.B) {
	registry, err := chunk.NewDefaultRegistry()
	if err != nil {
		b.Fatalf("NewDefaultRegistry: %v", err)
	}

	datasets := DefaultDatasets()
	versions := []chunk.Version{chunk.VersionV1SimpleRolling, chunk.VersionV2FastCDC}

	for _, version := range versions {
		version := version
		for _, dataset := range datasets {
			dataset := dataset
			b.Run(string(version)+"/"+dataset.Name, func(b *testing.B) {
				workDir := b.TempDir()
				chunker := registry.MustGet(version)

				if _, err := RunDataset(workDir, chunker, dataset); err != nil {
					b.Fatalf("warmup RunDataset: %v", err)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					runs, err := RunDataset(workDir, chunker, dataset)
					if err != nil {
						b.Fatalf("RunDataset: %v", err)
					}
					if len(runs) != 1+len(dataset.Mutations) {
						b.Fatalf("unexpected run count: got=%d want=%d", len(runs), 1+len(dataset.Mutations))
					}
					for _, run := range runs {
						if run.TotalBytes == 0 {
							b.Fatalf("unexpected zero-byte run for %s/%s", run.DatasetName, run.VariantName)
						}
					}
				}
			})
		}
	}
}

func TestCompareRunsRejectsMismatchedInputs(t *testing.T) {
	_, err := CompareRuns(
		RunResult{DatasetName: "a", Chunker: chunk.VersionV1SimpleRolling},
		RunResult{DatasetName: "b", Chunker: chunk.VersionV1SimpleRolling},
	)
	if err == nil {
		t.Fatal("expected dataset mismatch error")
	}

	_, err = CompareRuns(
		RunResult{DatasetName: "same", Chunker: chunk.VersionV1SimpleRolling},
		RunResult{DatasetName: "same", Chunker: chunk.VersionV2FastCDC},
	)
	if err == nil {
		t.Fatal("expected chunker mismatch error")
	}
}
