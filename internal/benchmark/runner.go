package benchmark

import (
	"bufio"
	"encoding/json"
	"errors"
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
	ExtraEnv map[string]string
}

// CaseEnvironmentFactory creates per-case environment overrides and cleanup.
// It runs before the case timer starts.
type CaseEnvironmentFactory func(caseName string) (map[string]string, func() error, error)

// FinalStateObserver captures benchmark-only evidence after a case finishes and
// before its external resources and temporary paths are cleaned up. The raw
// message must encode one sanitized JSON object.
type FinalStateObserver func(caseName string, ctx BenchmarkContext) (json.RawMessage, error)

// Result captures one benchmark case execution outcome.
type Result struct {
	Name      string
	Duration  time.Duration
	Metrics   Metrics
	Execution execution.Options
	ExecStats execution.ExecutionStats
	// DiagnosticFinalState is separately versioned diagnostic evidence. It does
	// not change the surrounding benchmark report schema.
	DiagnosticFinalState json.RawMessage
	Success              bool
	Error                string
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
	return runBenchmark(cases, nil, nil)
}

// RunBenchmarkWithEnvironmentFactory executes cases with per-case external
// resources, such as isolated benchmark databases.
func RunBenchmarkWithEnvironmentFactory(cases []BenchmarkCase, factory CaseEnvironmentFactory) ([]Result, error) {
	if factory == nil {
		return nil, fmt.Errorf("case environment factory cannot be nil")
	}
	return runBenchmark(cases, factory, nil)
}

// RunBenchmarkWithEnvironmentFactoryAndObserver executes cases with per-case
// external resources and captures final state before any cleanup runs.
func RunBenchmarkWithEnvironmentFactoryAndObserver(
	cases []BenchmarkCase,
	factory CaseEnvironmentFactory,
	observer FinalStateObserver,
) ([]Result, error) {
	if factory == nil {
		return nil, fmt.Errorf("case environment factory cannot be nil")
	}
	if observer == nil {
		return nil, fmt.Errorf("final state observer cannot be nil")
	}
	return runBenchmark(cases, factory, observer)
}

func runBenchmark(cases []BenchmarkCase, factory CaseEnvironmentFactory, observer FinalStateObserver) ([]Result, error) {
	results := make([]Result, 0, len(cases))
	for index, bc := range cases {
		if err := validateBenchmarkCase(index, bc); err != nil {
			return results, err
		}
		result, completed, err := runBenchmarkCase(bc, factory, observer)
		if completed {
			results = append(results, result)
		}
		if err != nil {
			return results, err
		}
	}

	return results, nil
}

func validateBenchmarkCase(index int, bc BenchmarkCase) error {
	if strings.TrimSpace(bc.Name) == "" {
		return fmt.Errorf("benchmark case at index %d has empty name", index)
	}
	if bc.Run == nil {
		return fmt.Errorf("benchmark case %q has nil run function", bc.Name)
	}
	return nil
}

func runBenchmarkCase(
	bc BenchmarkCase,
	factory CaseEnvironmentFactory,
	observer FinalStateObserver,
) (Result, bool, error) {
	ctx, cleanup, externalCleanup, err := prepareBenchmarkCase(bc.Name, factory)
	if err != nil {
		return Result{}, false, err
	}

	ioCountersPath := benchmarkIOCountersPath(ctx, bc.Name)
	metrics, runErr := measureBenchmarkCase(bc, ctx, ioCountersPath)
	ioStats, ioErr := readAggregatedIOCounters(ioCountersPath)
	diagnosticFinalState, observerErr := observeBenchmarkFinalState(observer, bc.Name, ctx)
	cleanupErr := cleanupBenchmarkCase(externalCleanup, cleanup)
	if ioErr != nil {
		return Result{}, false, benchmarkIOCountersError(bc.Name, ioErr, cleanupErr)
	}

	operationErr := errors.Join(runErr, observerErr)
	result := benchmarkCaseResult(bc, metrics, ioStats, diagnosticFinalState, operationErr, cleanupErr)
	return result, true, benchmarkCaseError(bc.Name, operationErr, cleanupErr)
}

func prepareBenchmarkCase(
	caseName string,
	factory CaseEnvironmentFactory,
) (BenchmarkContext, func() error, func() error, error) {
	ctx, cleanup, err := newBenchmarkContext()
	if err != nil {
		return BenchmarkContext{}, nil, nil, fmt.Errorf("create context for benchmark case %q: %w", caseName, err)
	}
	externalCleanup := func() error { return nil }
	if factory == nil {
		return ctx, cleanup, externalCleanup, nil
	}

	extraEnv, cleanupExternal, factoryErr := factory(caseName)
	if factoryErr != nil {
		_ = cleanup()
		return BenchmarkContext{}, nil, nil, fmt.Errorf("create environment for benchmark case %q: %w", caseName, factoryErr)
	}
	ctx.ExtraEnv = extraEnv
	if cleanupExternal != nil {
		externalCleanup = cleanupExternal
	}
	return ctx, cleanup, externalCleanup, nil
}

func benchmarkIOCountersPath(ctx BenchmarkContext, caseName string) string {
	return filepath.Join(ctx.RepoPath, fmt.Sprintf(".io-debug-%s.jsonl", strings.ReplaceAll(caseName, " ", "_")))
}

func measureBenchmarkCase(bc BenchmarkCase, ctx BenchmarkContext, ioCountersPath string) (Metrics, error) {
	return Measure(func() error {
		_ = os.Remove(ioCountersPath)
		prevPath, hadPath := os.LookupEnv("COLDKEEP_IO_COUNTERS_FILE")
		if err := os.Setenv("COLDKEEP_IO_COUNTERS_FILE", ioCountersPath); err != nil {
			return fmt.Errorf("set io debug env: %w", err)
		}
		defer restoreBenchmarkIOCountersPath(prevPath, hadPath)
		return bc.Run(ctx)
	})
}

func restoreBenchmarkIOCountersPath(previous string, existed bool) {
	if existed {
		_ = os.Setenv("COLDKEEP_IO_COUNTERS_FILE", previous)
		return
	}
	_ = os.Unsetenv("COLDKEEP_IO_COUNTERS_FILE")
}

func observeBenchmarkFinalState(
	observer FinalStateObserver,
	caseName string,
	ctx BenchmarkContext,
) (json.RawMessage, error) {
	if observer == nil {
		return nil, nil
	}
	diagnosticFinalState, err := observer(caseName, ctx)
	if err != nil {
		return diagnosticFinalState, err
	}
	if !json.Valid(diagnosticFinalState) {
		return diagnosticFinalState, fmt.Errorf("observer returned invalid JSON")
	}
	var object map[string]any
	if err := json.Unmarshal(diagnosticFinalState, &object); err != nil || object == nil {
		return diagnosticFinalState, fmt.Errorf("observer must return a JSON object")
	}
	return diagnosticFinalState, nil
}

func cleanupBenchmarkCase(externalCleanup, cleanup func() error) error {
	externalCleanupErr := externalCleanup()
	cleanupErr := cleanup()
	if externalCleanupErr != nil && cleanupErr != nil {
		return errors.Join(externalCleanupErr, cleanupErr)
	}
	if externalCleanupErr != nil {
		return externalCleanupErr
	}
	return cleanupErr
}

func benchmarkIOCountersError(caseName string, ioErr, cleanupErr error) error {
	if cleanupErr != nil {
		ioErr = errors.Join(ioErr, cleanupErr)
	}
	return fmt.Errorf("read io counters for benchmark case %q: %w", caseName, ioErr)
}

func benchmarkCaseResult(
	bc BenchmarkCase,
	metrics Metrics,
	ioStats ioDebugProcessRecord,
	diagnosticFinalState json.RawMessage,
	operationErr error,
	cleanupErr error,
) Result {
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
		DiagnosticFinalState: diagnosticFinalState,
		Success:              operationErr == nil && cleanupErr == nil,
	}
	if operationErr != nil {
		result.Error = operationErr.Error()
	}
	if cleanupErr != nil {
		if result.Error == "" {
			result.Error = cleanupErr.Error()
		} else {
			result.Error += "; " + cleanupErr.Error()
		}
	}
	return result
}

func benchmarkCaseError(caseName string, operationErr, cleanupErr error) error {
	if operationErr != nil {
		return fmt.Errorf("run benchmark case %q: %w", caseName, operationErr)
	}
	if cleanupErr != nil {
		return fmt.Errorf("cleanup benchmark case %q context: %w", caseName, cleanupErr)
	}
	return nil
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
