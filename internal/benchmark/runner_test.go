package benchmark

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/execution"
)

func TestRunBenchmarkRunsCasesInOrderWithIsolatedContexts(t *testing.T) {
	var seen []string
	cases := []BenchmarkCase{
		{
			Name:      "first",
			Execution: execution.Options{StoreFolderWorkers: 4, PipelineDepth: 1, Deterministic: true},
			Run: func(ctx BenchmarkContext) error {
				if ctx.RepoPath == "" || ctx.DataPath == "" {
					t.Fatalf("expected repo/data paths in context")
				}
				if _, err := os.Stat(ctx.RepoPath); err != nil {
					t.Fatalf("repo path missing: %v", err)
				}
				if _, err := os.Stat(ctx.DataPath); err != nil {
					t.Fatalf("data path missing: %v", err)
				}
				seen = append(seen, ctx.RepoPath+"|"+ctx.DataPath)
				return nil
			},
		},
		{
			Name:      "second",
			Execution: execution.Options{StoreFolderWorkers: 4, PipelineDepth: 1, Deterministic: true},
			Run: func(ctx BenchmarkContext) error {
				seen = append(seen, ctx.RepoPath+"|"+ctx.DataPath)
				return nil
			},
		},
	}

	results, err := RunBenchmark(cases)
	if err != nil {
		t.Fatalf("RunBenchmark returned error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("result length mismatch: got=%d want=2", len(results))
	}
	if results[0].Name != "first" || results[1].Name != "second" {
		t.Fatalf("unexpected result order: %+v", results)
	}
	if !results[0].Success || !results[1].Success {
		t.Fatalf("expected successful results, got: %+v", results)
	}
	if results[0].Execution.StoreFolderWorkers != 4 || results[1].Execution.StoreFolderWorkers != 4 {
		t.Fatalf("expected execution workers to be attached to results, got: %+v", results)
	}
	if results[0].ExecStats.WorkersUsed != 4 || results[1].ExecStats.WorkersUsed != 4 {
		t.Fatalf("expected exec stats workers to be attached to results, got: %+v", results)
	}

	if len(seen) != 2 {
		t.Fatalf("seen context count mismatch: got=%d want=2", len(seen))
	}
	if seen[0] == seen[1] {
		t.Fatalf("expected unique contexts per case, got=%q", seen[0])
	}
}

func TestRunBenchmarkStopsOnCaseErrorAndReturnsPartialResults(t *testing.T) {
	cases := []BenchmarkCase{
		{
			Name:      "ok",
			Execution: execution.Options{StoreFolderWorkers: 2, PipelineDepth: 1, Deterministic: true},
			Run: func(ctx BenchmarkContext) error {
				return os.WriteFile(filepath.Join(ctx.DataPath, "ok.txt"), []byte("ok"), 0o600)
			},
		},
		{
			Name:      "fail",
			Execution: execution.Options{StoreFolderWorkers: 2, PipelineDepth: 1, Deterministic: true},
			Run: func(ctx BenchmarkContext) error {
				return os.ErrPermission
			},
		},
		{
			Name:      "never-runs",
			Execution: execution.Options{StoreFolderWorkers: 2, PipelineDepth: 1, Deterministic: true},
			Run: func(ctx BenchmarkContext) error {
				t.Fatal("unexpected execution of case after failure")
				return nil
			},
		},
	}

	results, err := RunBenchmark(cases)
	if err == nil {
		t.Fatal("expected error from failed benchmark case")
	}
	if len(results) != 2 {
		t.Fatalf("expected partial results with 2 entries, got=%d", len(results))
	}
	if !results[0].Success {
		t.Fatalf("expected first result success, got=%+v", results[0])
	}
	if results[1].Success {
		t.Fatalf("expected second result failure, got=%+v", results[1])
	}
	if results[1].Error == "" {
		t.Fatalf("expected second result to include error text, got=%+v", results[1])
	}
	if results[0].ExecStats.WorkersUsed != 2 || results[1].ExecStats.WorkersUsed != 2 {
		t.Fatalf("expected exec stats workers to survive partial results, got=%+v", results)
	}
}

func TestRunBenchmarkRejectsInvalidCases(t *testing.T) {
	_, err := RunBenchmark([]BenchmarkCase{{Name: "", Execution: execution.Options{StoreFolderWorkers: 1, PipelineDepth: 1, Deterministic: true}, Run: func(BenchmarkContext) error { return nil }}})
	if err == nil {
		t.Fatal("expected error for empty case name")
	}

	_, err = RunBenchmark([]BenchmarkCase{{Name: "nil", Execution: execution.Options{StoreFolderWorkers: 1, PipelineDepth: 1, Deterministic: true}, Run: nil}})
	if err == nil {
		t.Fatal("expected error for nil case run function")
	}
}
