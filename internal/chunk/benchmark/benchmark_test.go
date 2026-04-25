package benchmark

import (
	"bytes"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
)

func TestGenerateBaseDeterministic(t *testing.T) {
	first := GenerateBase(1234, 1024)
	second := GenerateBase(1234, 1024)
	third := GenerateBase(5678, 1024)

	if !bytes.Equal(first, second) {
		t.Fatal("expected GenerateBase to be deterministic for the same seed")
	}
	if bytes.Equal(first, third) {
		t.Fatal("expected GenerateBase to differ for different seeds")
	}
}

func TestModifyAtOffsetsOnlyChangesRequestedRegions(t *testing.T) {
	base := bytes.Repeat([]byte{0xAA}, 32*1024)
	modified := ModifyAtOffsets(base, []int{4096, 16384})

	if bytes.Equal(base, modified) {
		t.Fatal("expected modified data to differ from base")
	}
	if !bytes.Equal(base[:4096], modified[:4096]) {
		t.Fatal("expected prefix before first mutation to remain unchanged")
	}
	if !bytes.Equal(base[8192:16384], modified[8192:16384]) {
		t.Fatal("expected region between mutations to remain unchanged")
	}
	if !bytes.Equal(base[20480:], modified[20480:]) {
		t.Fatal("expected suffix after fixed mutation windows to remain unchanged")
	}
}

func TestShiftDataPrependsPrefix(t *testing.T) {
	base := []byte("payload")
	prefix := []byte("prefix-")
	shifted := ShiftData(base, prefix)

	if !bytes.Equal(shifted[:len(prefix)], prefix) {
		t.Fatal("expected prefix at start of shifted data")
	}
	if !bytes.Equal(shifted[len(prefix):], base) {
		t.Fatal("expected original payload after inserted prefix")
	}
}

func TestDefaultDatasetsExposeRequestedCorpus(t *testing.T) {
	datasets := DefaultDatasets()
	if len(datasets) != 4 {
		t.Fatalf("expected 4 default datasets, got %d", len(datasets))
	}

	gotNames := make([]string, 0, len(datasets))
	for _, dataset := range datasets {
		gotNames = append(gotNames, dataset.Name)
		if len(dataset.Base.Data) == 0 {
			t.Fatalf("dataset %q has empty base data", dataset.Name)
		}
	}

	wantNames := []string{"random-base", "slight-modifications", "shifted-data", "structured-data"}
	for index, want := range wantNames {
		if gotNames[index] != want {
			t.Fatalf("dataset[%d] mismatch: got=%q want=%q names=%v", index, gotNames[index], want, gotNames)
		}
	}
}

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
