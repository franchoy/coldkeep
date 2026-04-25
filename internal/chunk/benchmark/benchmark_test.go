package benchmark

import (
	"bytes"
	"math"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/chunk/fastcdc"
	"github.com/franchoy/coldkeep/internal/chunk/simplecdc"
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

func TestRunChunkerReturnsDeterministicGenericResult(t *testing.T) {
	data := GenerateBase(1234, 256*1024)
	chunker := fastcdc.Chunker{}

	first := RunChunker(chunker, data)
	second := RunChunker(chunker, data)

	if first.Version != chunk.VersionV2FastCDC {
		t.Fatalf("unexpected version: got=%q want=%q", first.Version, chunk.VersionV2FastCDC)
	}
	if first.ChunkCount == 0 {
		t.Fatal("expected non-zero chunk count")
	}
	if first.TotalSize != int64(len(data)) {
		t.Fatalf("unexpected total size: got=%d want=%d", first.TotalSize, len(data))
	}
	if len(first.ChunkHashes) != first.ChunkCount {
		t.Fatalf("chunk hash count mismatch: got=%d want=%d", len(first.ChunkHashes), first.ChunkCount)
	}
	if len(first.Chunks) != first.ChunkCount {
		t.Fatalf("chunk record count mismatch: got=%d want=%d", len(first.Chunks), first.ChunkCount)
	}
	if !bytes.Equal([]byte(joinHashes(first.ChunkHashes)), []byte(joinHashes(second.ChunkHashes))) {
		t.Fatal("expected deterministic chunk hashes for repeated runs")
	}
}

func TestCompareChunkCountsReportsV1VsV2(t *testing.T) {
	comparison, err := CompareChunkCounts(
		Result{Version: chunk.VersionV1SimpleRolling, ChunkCount: 12, TotalSize: 4096},
		Result{Version: chunk.VersionV2FastCDC, ChunkCount: 8, TotalSize: 4096},
	)
	if err != nil {
		t.Fatalf("CompareChunkCounts: %v", err)
	}
	if comparison.LeftChunkCount != 12 || comparison.RightChunkCount != 8 {
		t.Fatalf("unexpected chunk counts: %+v", comparison)
	}
	if comparison.ChunkCountDelta != -4 {
		t.Fatalf("unexpected chunk count delta: %+v", comparison)
	}
}

func TestCompareReuseComputesSharedChunksOverBaseTotal(t *testing.T) {
	comparison, err := CompareReuse(
		Result{
			Version:    chunk.VersionV2FastCDC,
			ChunkCount: 3,
			Chunks: []ChunkRecord{
				{Hash: "a", Size: 10},
				{Hash: "b", Size: 10},
				{Hash: "c", Size: 10},
			},
		},
		Result{
			Version:    chunk.VersionV2FastCDC,
			ChunkCount: 3,
			Chunks: []ChunkRecord{
				{Hash: "a", Size: 10},
				{Hash: "x", Size: 10},
				{Hash: "c", Size: 10},
			},
		},
	)
	if err != nil {
		t.Fatalf("CompareReuse: %v", err)
	}
	if comparison.SharedChunks != 2 {
		t.Fatalf("expected 2 shared chunks, got %+v", comparison)
	}
	if math.Abs(comparison.ReuseRatioPct-((2.0/3.0)*100)) > 0.0001 {
		t.Fatalf("unexpected reuse ratio: %+v", comparison)
	}
}

func TestCompareBoundaryStabilityUsesReuseAfterShiftNotOffsets(t *testing.T) {
	comparison, err := CompareBoundaryStability(
		Result{
			Version:    chunk.VersionV2FastCDC,
			ChunkCount: 2,
			Chunks: []ChunkRecord{
				{Hash: "same-a", Offset: 0, Size: 10},
				{Hash: "same-b", Offset: 10, Size: 10},
			},
		},
		Result{
			Version:    chunk.VersionV2FastCDC,
			ChunkCount: 2,
			Chunks: []ChunkRecord{
				{Hash: "same-a", Offset: 5, Size: 10},
				{Hash: "same-b", Offset: 15, Size: 10},
			},
		},
	)
	if err != nil {
		t.Fatalf("CompareBoundaryStability: %v", err)
	}
	if comparison.SharedChunks != 2 {
		t.Fatalf("expected shifted data to preserve 2 shared chunks, got %+v", comparison)
	}
	if comparison.ReuseAfterShiftPct != 100 {
		t.Fatalf("expected reuse_after_shift=100%%, got %+v", comparison)
	}
}

func TestExpectedBehaviorV2ReuseAtLeastV1OnSmallEdits(t *testing.T) {
	base := GenerateStructuredLogData(9001, 18000)
	modified := ModifyAtOffsets(base, []int{32 * 1024, 192 * 1024, 512 * 1024})

	v1Base := RunChunker(simplecdc.New(), base)
	v1Modified := RunChunker(simplecdc.New(), modified)
	v2Base := RunChunker(fastcdc.Chunker{}, base)
	v2Modified := RunChunker(fastcdc.Chunker{}, modified)

	assertCoverageInvariant(t, base, v1Base)
	assertCoverageInvariant(t, modified, v1Modified)
	assertCoverageInvariant(t, base, v2Base)
	assertCoverageInvariant(t, modified, v2Modified)

	v1Reuse, err := CompareReuse(v1Base, v1Modified)
	if err != nil {
		t.Fatalf("CompareReuse v1: %v", err)
	}
	v2Reuse, err := CompareReuse(v2Base, v2Modified)
	if err != nil {
		t.Fatalf("CompareReuse v2: %v", err)
	}

	if v2Reuse.ReuseRatioPct < v1Reuse.ReuseRatioPct {
		t.Fatalf("expected v2 reuse >= v1 reuse on small edits: v1=%.2f v2=%.2f", v1Reuse.ReuseRatioPct, v2Reuse.ReuseRatioPct)
	}
}

func TestExpectedBehaviorV2ReuseAfterShiftAtLeastV1(t *testing.T) {
	base := GenerateBase(4444, 2*1024*1024)
	shifted := ShiftData(base, bytes.Repeat([]byte("shift-prefix|"), 512))

	v1Base := RunChunker(simplecdc.New(), base)
	v1Shifted := RunChunker(simplecdc.New(), shifted)
	v2Base := RunChunker(fastcdc.Chunker{}, base)
	v2Shifted := RunChunker(fastcdc.Chunker{}, shifted)

	assertCoverageInvariant(t, base, v1Base)
	assertCoverageInvariant(t, shifted, v1Shifted)
	assertCoverageInvariant(t, base, v2Base)
	assertCoverageInvariant(t, shifted, v2Shifted)

	v1Stability, err := CompareBoundaryStability(v1Base, v1Shifted)
	if err != nil {
		t.Fatalf("CompareBoundaryStability v1: %v", err)
	}
	v2Stability, err := CompareBoundaryStability(v2Base, v2Shifted)
	if err != nil {
		t.Fatalf("CompareBoundaryStability v2: %v", err)
	}

	if v2Stability.ReuseAfterShiftPct < v1Stability.ReuseAfterShiftPct {
		t.Fatalf("expected v2 reuse_after_shift >= v1 reuse_after_shift: v1=%.2f v2=%.2f", v1Stability.ReuseAfterShiftPct, v2Stability.ReuseAfterShiftPct)
	}
}

func TestFastCDCBetterThanV1_SmallModifications(t *testing.T) {
	base := GenerateStructuredLogData(9001, 18000)
	modified := ModifyAtOffsets(base, []int{32 * 1024, 192 * 1024, 512 * 1024})

	v1Base := RunChunker(simplecdc.New(), base)
	v1Modified := RunChunker(simplecdc.New(), modified)
	v2Base := RunChunker(fastcdc.Chunker{}, base)
	v2Modified := RunChunker(fastcdc.Chunker{}, modified)

	v1Reuse, err := CompareReuse(v1Base, v1Modified)
	if err != nil {
		t.Fatalf("CompareReuse v1: %v", err)
	}
	v2Reuse, err := CompareReuse(v2Base, v2Modified)
	if err != nil {
		t.Fatalf("CompareReuse v2: %v", err)
	}

	if v2Reuse.ReuseRatioPct < v1Reuse.ReuseRatioPct {
		t.Fatalf("expected fastcdc reuse >= v1 reuse on small modifications: v1=%.2f v2=%.2f", v1Reuse.ReuseRatioPct, v2Reuse.ReuseRatioPct)
	}
}

func TestFastCDCBetterThanV1_ShiftedData(t *testing.T) {
	base := GenerateBase(4444, 2*1024*1024)
	shifted := ShiftData(base, bytes.Repeat([]byte("shift-prefix|"), 512))

	v1Base := RunChunker(simplecdc.New(), base)
	v1Shifted := RunChunker(simplecdc.New(), shifted)
	v2Base := RunChunker(fastcdc.Chunker{}, base)
	v2Shifted := RunChunker(fastcdc.Chunker{}, shifted)

	v1Stability, err := CompareBoundaryStability(v1Base, v1Shifted)
	if err != nil {
		t.Fatalf("CompareBoundaryStability v1: %v", err)
	}
	v2Stability, err := CompareBoundaryStability(v2Base, v2Shifted)
	if err != nil {
		t.Fatalf("CompareBoundaryStability v2: %v", err)
	}

	if v2Stability.ReuseAfterShiftPct < v1Stability.ReuseAfterShiftPct {
		t.Fatalf("expected fastcdc reuse_after_shift >= v1 reuse_after_shift: v1=%.2f v2=%.2f", v1Stability.ReuseAfterShiftPct, v2Stability.ReuseAfterShiftPct)
	}
}

func TestChunkerDeterminism(t *testing.T) {
	data := GenerateBase(777, 3*1024*1024)
	chunkers := []chunk.Chunker{simplecdc.New(), fastcdc.Chunker{}}

	for _, c := range chunkers {
		first := RunChunker(c, data)
		second := RunChunker(c, data)

		if first.Version != second.Version {
			t.Fatalf("version mismatch for %q: first=%q second=%q", c.Version(), first.Version, second.Version)
		}
		if first.ChunkCount != second.ChunkCount {
			t.Fatalf("chunk count drift for %q: first=%d second=%d", c.Version(), first.ChunkCount, second.ChunkCount)
		}
		if first.TotalSize != second.TotalSize {
			t.Fatalf("total size drift for %q: first=%d second=%d", c.Version(), first.TotalSize, second.TotalSize)
		}
		if len(first.ChunkHashes) != len(second.ChunkHashes) {
			t.Fatalf("hash count drift for %q: first=%d second=%d", c.Version(), len(first.ChunkHashes), len(second.ChunkHashes))
		}
		for i := range first.ChunkHashes {
			if first.ChunkHashes[i] != second.ChunkHashes[i] {
				t.Fatalf("chunk hash drift for %q at index %d: first=%q second=%q", c.Version(), i, first.ChunkHashes[i], second.ChunkHashes[i])
			}
		}
	}
}

func TestChunkerDeterminism_RunDatasetTwice(t *testing.T) {
	dataset := Dataset{
		Name: "determinism-check",
		Base: Variant{Name: "base", Data: GenerateBase(1111, 2*1024*1024)},
		Mutations: []Variant{
			{Name: "modified", Data: ModifyAtOffsets(GenerateBase(1111, 2*1024*1024), []int{64 * 1024, 512 * 1024})},
			{Name: "shifted", Data: ShiftData(GenerateBase(1111, 2*1024*1024), bytes.Repeat([]byte("prefix|"), 128))},
		},
	}

	chunkers := []chunk.Chunker{simplecdc.New(), fastcdc.Chunker{}}
	for _, c := range chunkers {
		firstRuns, err := RunDataset(t.TempDir(), c, dataset)
		if err != nil {
			t.Fatalf("RunDataset first pass for %q: %v", c.Version(), err)
		}
		secondRuns, err := RunDataset(t.TempDir(), c, dataset)
		if err != nil {
			t.Fatalf("RunDataset second pass for %q: %v", c.Version(), err)
		}
		if len(firstRuns) != len(secondRuns) {
			t.Fatalf("run count drift for %q: first=%d second=%d", c.Version(), len(firstRuns), len(secondRuns))
		}

		for i := range firstRuns {
			first := firstRuns[i]
			second := secondRuns[i]
			if first.Version != second.Version || first.DatasetName != second.DatasetName || first.VariantName != second.VariantName {
				t.Fatalf("metadata drift for %q run[%d]: first=%+v second=%+v", c.Version(), i, first, second)
			}
			if first.ChunkCount != second.ChunkCount || first.TotalSize != second.TotalSize {
				t.Fatalf("summary drift for %q run[%d]: first_count=%d second_count=%d first_size=%d second_size=%d", c.Version(), i, first.ChunkCount, second.ChunkCount, first.TotalSize, second.TotalSize)
			}
			if len(first.Chunks) != len(second.Chunks) {
				t.Fatalf("chunk list length drift for %q run[%d]: first=%d second=%d", c.Version(), i, len(first.Chunks), len(second.Chunks))
			}
			for j := range first.Chunks {
				if first.Chunks[j] != second.Chunks[j] {
					t.Fatalf("chunk drift for %q run[%d] chunk[%d]: first=%+v second=%+v", c.Version(), i, j, first.Chunks[j], second.Chunks[j])
				}
			}
		}
	}
}

func TestChunkCoverage(t *testing.T) {
	chunkers := []chunk.Chunker{simplecdc.New(), fastcdc.Chunker{}}

	for _, dataset := range DefaultDatasets() {
		for _, variant := range dataset.Variants() {
			for _, c := range chunkers {
				result := RunChunker(c, variant.Data)
				assertCoverageInvariant(t, variant.Data, result)
			}
		}
	}
}

func TestValidateCoverageInvariantsRejectsGapOverlapAndOutOfOrder(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		err := ValidateCoverageInvariants(20, Result{
			TotalSize: 20,
			Chunks: []ChunkRecord{
				{Offset: 0, Size: 10},
				{Offset: 12, Size: 8},
			},
		})
		if err == nil {
			t.Fatal("expected gap error")
		}
	})

	t.Run("overlap", func(t *testing.T) {
		err := ValidateCoverageInvariants(20, Result{
			TotalSize: 20,
			Chunks: []ChunkRecord{
				{Offset: 0, Size: 10},
				{Offset: 8, Size: 12},
			},
		})
		if err == nil {
			t.Fatal("expected overlap error")
		}
	})

	t.Run("out_of_order", func(t *testing.T) {
		err := ValidateCoverageInvariants(20, Result{
			TotalSize: 20,
			Chunks: []ChunkRecord{
				{Offset: 10, Size: 10},
				{Offset: 0, Size: 10},
			},
		})
		if err == nil {
			t.Fatal("expected out-of-order error")
		}
	})
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
						if run.TotalSize == 0 {
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
		RunResult{DatasetName: "a", Version: chunk.VersionV1SimpleRolling},
		RunResult{DatasetName: "b", Version: chunk.VersionV1SimpleRolling},
	)
	if err == nil {
		t.Fatal("expected dataset mismatch error")
	}

	_, err = CompareRuns(
		RunResult{DatasetName: "same", Version: chunk.VersionV1SimpleRolling},
		RunResult{DatasetName: "same", Version: chunk.VersionV2FastCDC},
	)
	if err == nil {
		t.Fatal("expected chunker mismatch error")
	}
}

func joinHashes(hashes []string) string {
	if len(hashes) == 0 {
		return ""
	}
	joined := hashes[0]
	for _, hash := range hashes[1:] {
		joined += "," + hash
	}
	return joined
}

func assertCoverageInvariant(t *testing.T, data []byte, result Result) {
	t.Helper()
	if err := ValidateCoverageInvariants(int64(len(data)), result); err != nil {
		t.Fatalf("coverage invariant failed: %v", err)
	}
}
