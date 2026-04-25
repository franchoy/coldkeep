package benchmark

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/chunk"
)

// ChunkRecord is one emitted chunk summarized for comparison work.
type ChunkRecord struct {
	Hash   string
	Offset int64
	Size   int64
}

// RunResult is the deterministic chunking output for one dataset variant.
type RunResult struct {
	DatasetName  string
	VariantName  string
	Chunker      chunk.Version
	ChunkCount   int
	TotalBytes   int64
	AvgChunkSize float64
	Chunks       []ChunkRecord
}

// RunChunker executes one chunker on one dataset variant by writing the
// variant bytes to a deterministic temp file in workDir.
func RunChunker(workDir string, chunker chunk.Chunker, datasetName string, variant Variant) (RunResult, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return RunResult{}, fmt.Errorf("create benchmark work dir: %w", err)
	}

	path := filepath.Join(workDir, datasetName+"-"+variant.Name+".bin")
	if err := os.WriteFile(path, variant.Data, 0o600); err != nil {
		return RunResult{}, fmt.Errorf("write benchmark input %q: %w", path, err)
	}

	results, err := chunker.ChunkFile(path)
	if err != nil {
		return RunResult{}, fmt.Errorf("chunk file %q with %s: %w", path, chunker.Version(), err)
	}

	run := RunResult{
		DatasetName: datasetName,
		VariantName: variant.Name,
		Chunker:     chunker.Version(),
		ChunkCount:  len(results),
		Chunks:      make([]ChunkRecord, 0, len(results)),
	}
	for _, item := range results {
		run.TotalBytes += item.Info.Size
		run.Chunks = append(run.Chunks, ChunkRecord{
			Hash:   item.Info.Hash,
			Offset: item.Info.Offset,
			Size:   item.Info.Size,
		})
	}
	if run.ChunkCount > 0 {
		run.AvgChunkSize = float64(run.TotalBytes) / float64(run.ChunkCount)
	}

	return run, nil
}

// RunDataset executes one chunker across the baseline and mutation set for a dataset.
func RunDataset(workDir string, chunker chunk.Chunker, dataset Dataset) ([]RunResult, error) {
	variants := dataset.Variants()
	runs := make([]RunResult, 0, len(variants))
	for _, variant := range variants {
		run, err := RunChunker(workDir, chunker, dataset.Name, variant)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// RunRegistry executes each registered chunker version against all default datasets.
func RunRegistry(workDir string, registry *chunk.Registry, versions []chunk.Version, datasets []Dataset) (map[chunk.Version][]RunResult, error) {
	results := make(map[chunk.Version][]RunResult, len(versions))
	for _, version := range versions {
		chunker := registry.MustGet(version)
		for _, dataset := range datasets {
			runs, err := RunDataset(filepath.Join(workDir, string(version)), chunker, dataset)
			if err != nil {
				return nil, err
			}
			results[version] = append(results[version], runs...)
		}
	}
	return results, nil
}
