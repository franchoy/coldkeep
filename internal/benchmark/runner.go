package benchmark

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/execution"
)

// BenchmarkCase defines one benchmark scenario execution unit.
type BenchmarkCase struct {
	Name      string
	Run       func(ctx BenchmarkContext) error
	Execution execution.Options
}

// BenchmarkContext contains per-case isolated paths.
type BenchmarkContext struct {
	RepoPath string
	DataPath string
}

// Result captures one benchmark case execution outcome.
type Result struct {
	Name      string
	Duration  time.Duration
	Metrics   Metrics
	Execution execution.Options
	ExecStats execution.ExecutionStats
	Success   bool
	Error     string
}

type ioDebugProcessRecord struct {
	ContainerAppendCount   int64 `json:"container_append_count"`
	FsyncCount             int64 `json:"fsync_count"`
	ContainerOpenCount     int64 `json:"container_open_count"`
	ContainerCloseCount    int64 `json:"container_close_count"`
	BytesWritten           int64 `json:"bytes_written"`
	BytesRead              int64 `json:"bytes_read"`
	SnapshotMetadataWrites int64 `json:"snapshot_metadata_write_count"`
}

// RunBenchmark executes benchmark cases sequentially with isolated temp paths.
func RunBenchmark(cases []BenchmarkCase) ([]Result, error) {
	results := make([]Result, 0, len(cases))
	for index, bc := range cases {
		if strings.TrimSpace(bc.Name) == "" {
			return results, fmt.Errorf("benchmark case at index %d has empty name", index)
		}
		if bc.Run == nil {
			return results, fmt.Errorf("benchmark case %q has nil run function", bc.Name)
		}

		ctx, cleanup, err := newBenchmarkContext()
		if err != nil {
			return results, fmt.Errorf("create context for benchmark case %q: %w", bc.Name, err)
		}

		ioCountersPath := filepath.Join(ctx.RepoPath, fmt.Sprintf(".io-debug-%s.jsonl", strings.ReplaceAll(bc.Name, " ", "_")))
		metrics, runErr := Measure(func() error {
			_ = os.Remove(ioCountersPath)

			prevPath, hadPath := os.LookupEnv("COLDKEEP_IO_COUNTERS_FILE")
			if err := os.Setenv("COLDKEEP_IO_COUNTERS_FILE", ioCountersPath); err != nil {
				return fmt.Errorf("set io debug env: %w", err)
			}
			defer func() {
				if hadPath {
					_ = os.Setenv("COLDKEEP_IO_COUNTERS_FILE", prevPath)
				} else {
					_ = os.Unsetenv("COLDKEEP_IO_COUNTERS_FILE")
				}
			}()

			return bc.Run(ctx)
		})

		ioStats, ioErr := readAggregatedIOCounters(ioCountersPath)
		if ioErr != nil {
			return results, fmt.Errorf("read io counters for benchmark case %q: %w", bc.Name, ioErr)
		}

		cleanupErr := cleanup()

		result := Result{
			Name:      bc.Name,
			Duration:  metrics.Duration,
			Metrics:   metrics,
			Execution: bc.Execution,
			ExecStats: execution.ExecutionStats{
				TotalFilesProcessed:    metrics.FilesProcessed,
				TotalBytesProcessed:    metrics.BytesProcessed,
				WorkersUsed:            bc.Execution.StoreFolderWorkers,
				ContainerAppendCount:   ioStats.ContainerAppendCount,
				FsyncCount:             ioStats.FsyncCount,
				ContainerOpenCount:     ioStats.ContainerOpenCount,
				ContainerCloseCount:    ioStats.ContainerCloseCount,
				BytesWritten:           ioStats.BytesWritten,
				BytesRead:              ioStats.BytesRead,
				SnapshotMetadataWrites: ioStats.SnapshotMetadataWrites,
			},
			Success: runErr == nil && cleanupErr == nil,
		}
		if runErr != nil {
			result.Error = runErr.Error()
		}
		if cleanupErr != nil {
			if result.Error == "" {
				result.Error = cleanupErr.Error()
			} else {
				result.Error = result.Error + "; " + cleanupErr.Error()
			}
		}
		results = append(results, result)

		if runErr != nil {
			return results, fmt.Errorf("run benchmark case %q: %w", bc.Name, runErr)
		}
		if cleanupErr != nil {
			return results, fmt.Errorf("cleanup benchmark case %q context: %w", bc.Name, cleanupErr)
		}
	}

	return results, nil
}

func readAggregatedIOCounters(path string) (ioDebugProcessRecord, error) {
	var out ioDebugProcessRecord
	if strings.TrimSpace(path) == "" {
		return out, nil
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return out, err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec ioDebugProcessRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return out, fmt.Errorf("parse io debug record: %w", err)
		}
		out.ContainerAppendCount += rec.ContainerAppendCount
		out.FsyncCount += rec.FsyncCount
		out.ContainerOpenCount += rec.ContainerOpenCount
		out.ContainerCloseCount += rec.ContainerCloseCount
		out.BytesWritten += rec.BytesWritten
		out.BytesRead += rec.BytesRead
		out.SnapshotMetadataWrites += rec.SnapshotMetadataWrites
	}
	if err := scanner.Err(); err != nil {
		return out, err
	}

	return out, nil
}

func newBenchmarkContext() (BenchmarkContext, func() error, error) {
	root, err := os.MkdirTemp("", "coldkeep-benchmark-")
	if err != nil {
		return BenchmarkContext{}, nil, err
	}

	repoPath := filepath.Join(root, "repo")
	dataPath := filepath.Join(root, "data")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		_ = os.RemoveAll(root)
		return BenchmarkContext{}, nil, err
	}
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		_ = os.RemoveAll(root)
		return BenchmarkContext{}, nil, err
	}

	cleanup := func() error {
		if err := os.RemoveAll(root); err != nil {
			return fmt.Errorf("remove benchmark temp root %q: %w", root, err)
		}
		return nil
	}

	return BenchmarkContext{RepoPath: repoPath, DataPath: dataPath}, cleanup, nil
}
