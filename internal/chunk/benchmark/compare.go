package benchmark

import (
	"fmt"
	"strings"
)

// ChunkCountComparison compares chunk counts for the same bytes across two chunkers.
type ChunkCountComparison struct {
	LeftVersion     string
	RightVersion    string
	LeftChunkCount  int
	RightChunkCount int
	LeftTotalSize   int64
	RightTotalSize  int64
	ChunkCountDelta int
}

// ReuseComparison reports how many base chunks still exist after a mutation.
type ReuseComparison struct {
	Version       string
	SharedChunks  int
	TotalChunks   int
	ReuseRatioPct float64
}

// BoundaryStabilityComparison reports shift resilience as chunk reuse after a shift.
type BoundaryStabilityComparison struct {
	Version            string
	SharedChunks       int
	TotalChunks        int
	ReuseAfterShiftPct float64
}

// Comparison summarizes one dataset baseline/candidate comparison for reporting.
type Comparison struct {
	DatasetName           string
	Chunker               string
	BaseVariant           string
	CandidateVariant      string
	BaseChunkCount        int
	CandidateChunkCount   int
	SharedChunks          int
	ReuseRatioPct         float64
	ReuseAfterShiftPct    float64
	BaseTotalBytes        int64
	CandidateTotalBytes   int64
	BaseAvgChunkSize      float64
	CandidateAvgChunkSize float64
}

// CompareChunkCounts compares chunk counts between two chunker results for the same bytes.
func CompareChunkCounts(left, right Result) (ChunkCountComparison, error) {
	if left.TotalSize != right.TotalSize {
		return ChunkCountComparison{}, fmt.Errorf("cannot compare chunk counts for different total sizes: %d vs %d", left.TotalSize, right.TotalSize)
	}
	return ChunkCountComparison{
		LeftVersion:     string(left.Version),
		RightVersion:    string(right.Version),
		LeftChunkCount:  left.ChunkCount,
		RightChunkCount: right.ChunkCount,
		LeftTotalSize:   left.TotalSize,
		RightTotalSize:  right.TotalSize,
		ChunkCountDelta: right.ChunkCount - left.ChunkCount,
	}, nil
}

// CompareReuse computes shared chunk reuse between a base and modified result.
func CompareReuse(base, candidate Result) (ReuseComparison, error) {
	if base.Version != candidate.Version {
		return ReuseComparison{}, fmt.Errorf("cannot compare reuse across different chunkers: %q vs %q", base.Version, candidate.Version)
	}
	shared := countSharedChunks(base.Chunks, candidate.Chunks)
	comparison := ReuseComparison{
		Version:      string(base.Version),
		SharedChunks: shared,
		TotalChunks:  base.ChunkCount,
	}
	if base.ChunkCount > 0 {
		comparison.ReuseRatioPct = float64(shared) / float64(base.ChunkCount) * 100
	}
	return comparison, nil
}

// CompareBoundaryStability computes shift resilience as chunk reuse after a shift.
func CompareBoundaryStability(base, shifted Result) (BoundaryStabilityComparison, error) {
	if base.Version != shifted.Version {
		return BoundaryStabilityComparison{}, fmt.Errorf("cannot compare boundary stability across different chunkers: %q vs %q", base.Version, shifted.Version)
	}
	shared := countSharedChunks(base.Chunks, shifted.Chunks)
	comparison := BoundaryStabilityComparison{
		Version:      string(base.Version),
		SharedChunks: shared,
		TotalChunks:  base.ChunkCount,
	}
	if base.ChunkCount > 0 {
		comparison.ReuseAfterShiftPct = float64(shared) / float64(base.ChunkCount) * 100
	}
	return comparison, nil
}

// CompareRuns computes reporting metrics for one baseline/candidate pair.
func CompareRuns(base, candidate RunResult) (Comparison, error) {
	if base.DatasetName != candidate.DatasetName {
		return Comparison{}, fmt.Errorf("cannot compare different datasets: %q vs %q", base.DatasetName, candidate.DatasetName)
	}
	if base.Version != candidate.Version {
		return Comparison{}, fmt.Errorf("cannot compare different chunkers: %q vs %q", base.Version, candidate.Version)
	}

	baseResult := Result{Version: base.Version, ChunkCount: base.ChunkCount, TotalSize: base.TotalSize, Chunks: base.Chunks}
	candidateResult := Result{Version: candidate.Version, ChunkCount: candidate.ChunkCount, TotalSize: candidate.TotalSize, Chunks: candidate.Chunks}
	reuse, err := CompareReuse(baseResult, candidateResult)
	if err != nil {
		return Comparison{}, err
	}

	comparison := Comparison{
		DatasetName:           base.DatasetName,
		Chunker:               string(base.Version),
		BaseVariant:           base.VariantName,
		CandidateVariant:      candidate.VariantName,
		BaseChunkCount:        base.ChunkCount,
		CandidateChunkCount:   candidate.ChunkCount,
		SharedChunks:          reuse.SharedChunks,
		ReuseRatioPct:         reuse.ReuseRatioPct,
		BaseTotalBytes:        base.TotalSize,
		CandidateTotalBytes:   candidate.TotalSize,
		BaseAvgChunkSize:      base.AvgChunkSize,
		CandidateAvgChunkSize: candidate.AvgChunkSize,
	}

	if strings.Contains(strings.ToLower(candidate.VariantName), "shift") {
		boundary, err := CompareBoundaryStability(baseResult, candidateResult)
		if err != nil {
			return Comparison{}, err
		}
		comparison.ReuseAfterShiftPct = boundary.ReuseAfterShiftPct
	}

	return comparison, nil
}

func countSharedChunks(left, right []ChunkRecord) int {
	counts := make(map[string]int, len(left))
	for _, chunk := range left {
		counts[chunkKey(chunk)]++
	}

	shared := 0
	for _, chunk := range right {
		key := chunkKey(chunk)
		if counts[key] == 0 {
			continue
		}
		counts[key]--
		shared++
	}
	return shared
}

func chunkKey(chunk ChunkRecord) string {
	return chunk.Hash + "/" + fmt.Sprint(chunk.Size)
}
