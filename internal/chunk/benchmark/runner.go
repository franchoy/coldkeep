package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk"
)

// Result is the generic chunker output summary used by the benchmark harness.
type Result struct {
	Version     chunk.Version
	ChunkCount  int
	TotalSize   int64
	ChunkHashes []string
	Chunks      []ChunkRecord
}

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
	Version      chunk.Version
	ChunkCount   int
	TotalSize    int64
	AvgChunkSize float64
	Chunks       []ChunkRecord
}

// RunChunker executes one chunker on in-memory data and returns a deterministic
// summary result. It panics on unexpected file-system or chunker execution
// failures because the benchmark harness treats such failures as fatal.
func RunChunker(c chunk.Chunker, data []byte) Result {
	result, err := runChunkerWithError(c, data)
	if err != nil {
		panic(err)
	}
	return result
}

// RunDataset executes one chunker across the baseline and mutation set for a dataset.
func RunDataset(workDir string, chunker chunk.Chunker, dataset Dataset) ([]RunResult, error) {
	variants := dataset.Variants()
	runs := make([]RunResult, 0, len(variants))
	for _, variant := range variants {
		run, err := runDatasetVariant(workDir, chunker, dataset.Name, variant)
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

func runChunkerWithError(c chunk.Chunker, data []byte) (Result, error) {
	workDir, err := os.MkdirTemp("", "coldkeep-chunk-benchmark-")
	if err != nil {
		return Result{}, fmt.Errorf("create benchmark temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	path := filepath.Join(workDir, "input.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return Result{}, fmt.Errorf("write benchmark input %q: %w", path, err)
	}

	results, err := c.ChunkFile(path)
	if err != nil {
		return Result{}, fmt.Errorf("chunk file %q with %s: %w", path, c.Version(), err)
	}

	run := Result{
		Version:     c.Version(),
		ChunkCount:  len(results),
		ChunkHashes: make([]string, 0, len(results)),
		Chunks:      make([]ChunkRecord, 0, len(results)),
	}
	for _, item := range results {
		hash := normalizedChunkHash(item)
		run.TotalSize += item.Info.Size
		run.ChunkHashes = append(run.ChunkHashes, hash)
		run.Chunks = append(run.Chunks, ChunkRecord{
			Hash:   hash,
			Offset: item.Info.Offset,
			Size:   item.Info.Size,
		})
	}

	return run, nil
}

func runDatasetVariant(workDir string, chunker chunk.Chunker, datasetName string, variant Variant) (RunResult, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return RunResult{}, fmt.Errorf("create benchmark work dir: %w", err)
	}

	path := filepath.Join(workDir, datasetName+"-"+variant.Name+".bin")
	if err := os.WriteFile(path, variant.Data, 0o600); err != nil {
		return RunResult{}, fmt.Errorf("write benchmark input %q: %w", path, err)
	}

	raw, err := chunker.ChunkFile(path)
	if err != nil {
		return RunResult{}, fmt.Errorf("chunk file %q with %s: %w", path, chunker.Version(), err)
	}

	run := RunResult{
		DatasetName: datasetName,
		VariantName: variant.Name,
		Version:     chunker.Version(),
		ChunkCount:  len(raw),
		Chunks:      make([]ChunkRecord, 0, len(raw)),
	}
	for _, item := range raw {
		hash := normalizedChunkHash(item)
		run.TotalSize += item.Info.Size
		run.Chunks = append(run.Chunks, ChunkRecord{
			Hash:   hash,
			Offset: item.Info.Offset,
			Size:   item.Info.Size,
		})
	}
	if run.ChunkCount > 0 {
		run.AvgChunkSize = float64(run.TotalSize) / float64(run.ChunkCount)
	}

	return run, nil
}

// ValidateCoverageInvariants enforces chunk coverage invariants for one run:
// total chunk sizes equal file size, no overlaps, no gaps, and strict ordering.
func ValidateCoverageInvariants(fileSize int64, result Result) error {
	if fileSize < 0 {
		return fmt.Errorf("file size must be non-negative: %d", fileSize)
	}
	if result.TotalSize != fileSize {
		return fmt.Errorf("total size mismatch: chunks=%d file=%d", result.TotalSize, fileSize)
	}

	var cursor int64
	for i, c := range result.Chunks {
		if c.Size <= 0 {
			return fmt.Errorf("chunk %d has non-positive size: %d", i, c.Size)
		}
		if c.Offset < 0 {
			return fmt.Errorf("chunk %d has negative offset: %d", i, c.Offset)
		}
		if c.Offset != cursor {
			if c.Offset > cursor {
				return fmt.Errorf("gap before chunk %d: got offset=%d want=%d", i, c.Offset, cursor)
			}
			return fmt.Errorf("overlap or out-of-order chunk %d: got offset=%d want=%d", i, c.Offset, cursor)
		}
		cursor += c.Size
	}
	if cursor != fileSize {
		return fmt.Errorf("coverage mismatch: covered=%d file=%d", cursor, fileSize)
	}

	return nil
}

func normalizedChunkHash(item chunk.Result) string {
	if trimmed := strings.TrimSpace(item.Info.Hash); trimmed != "" {
		return trimmed
	}
	sum := sha256.Sum256(item.Data)
	return hex.EncodeToString(sum[:])
}
