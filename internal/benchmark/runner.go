package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BenchmarkCase defines one benchmark scenario execution unit.
type BenchmarkCase struct {
	Name string
	Run  func(ctx BenchmarkContext) error
}

// BenchmarkContext contains per-case isolated paths.
type BenchmarkContext struct {
	RepoPath string
	DataPath string
}

// Result captures one benchmark case execution outcome.
type Result struct {
	Name     string
	Duration time.Duration
	Success  bool
	Error    string
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

		started := time.Now()
		runErr := bc.Run(ctx)
		duration := time.Since(started)
		cleanupErr := cleanup()

		result := Result{
			Name:     bc.Name,
			Duration: duration,
			Success:  runErr == nil && cleanupErr == nil,
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
