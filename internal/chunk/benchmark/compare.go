package benchmark

import "fmt"

// Comparison summarizes how a mutated variant relates to a baseline variant.
type Comparison struct {
	DatasetName           string
	Chunker               string
	BaseVariant           string
	CandidateVariant      string
	BaseChunkCount        int
	CandidateChunkCount   int
	ReusedChunkCount      int
	ReuseRatioPct         float64
	StableBoundaryCount   int
	BoundaryStabilityPct  float64
	BaseTotalBytes        int64
	CandidateTotalBytes   int64
	BaseAvgChunkSize      float64
	CandidateAvgChunkSize float64
}

// CompareRuns computes simple, repeatable metrics for one baseline/candidate pair.
func CompareRuns(base, candidate RunResult) (Comparison, error) {
	if base.DatasetName != candidate.DatasetName {
		return Comparison{}, fmt.Errorf("cannot compare different datasets: %q vs %q", base.DatasetName, candidate.DatasetName)
	}
	if base.Chunker != candidate.Chunker {
		return Comparison{}, fmt.Errorf("cannot compare different chunkers: %q vs %q", base.Chunker, candidate.Chunker)
	}

	comparison := Comparison{
		DatasetName:           base.DatasetName,
		Chunker:               string(base.Chunker),
		BaseVariant:           base.VariantName,
		CandidateVariant:      candidate.VariantName,
		BaseChunkCount:        base.ChunkCount,
		CandidateChunkCount:   candidate.ChunkCount,
		BaseTotalBytes:        base.TotalBytes,
		CandidateTotalBytes:   candidate.TotalBytes,
		BaseAvgChunkSize:      base.AvgChunkSize,
		CandidateAvgChunkSize: candidate.AvgChunkSize,
	}

	comparison.ReusedChunkCount = countSharedChunks(base.Chunks, candidate.Chunks)
	if base.ChunkCount > 0 {
		comparison.ReuseRatioPct = float64(comparison.ReusedChunkCount) / float64(base.ChunkCount) * 100
	}

	comparison.StableBoundaryCount = countSharedBoundaries(base.Chunks, candidate.Chunks)
	if len(base.Chunks) > 0 {
		comparison.BoundaryStabilityPct = float64(comparison.StableBoundaryCount) / float64(len(base.Chunks)) * 100
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

func countSharedBoundaries(left, right []ChunkRecord) int {
	seen := make(map[int64]int, len(left))
	for _, chunk := range left {
		seen[chunk.Offset]++
	}

	shared := 0
	for _, chunk := range right {
		if seen[chunk.Offset] == 0 {
			continue
		}
		seen[chunk.Offset]--
		shared++
	}
	return shared
}

func chunkKey(chunk ChunkRecord) string {
	return chunk.Hash + "/" + fmt.Sprint(chunk.Size)
}
