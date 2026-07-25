package benchmark

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestRunBenchmarkWithEnvironmentFactoryScopesAndCleansEachCase(t *testing.T) {
	var created []string
	var cleaned []string
	cases := []BenchmarkCase{
		{
			Name:      "first",
			Execution: execution.Options{StoreFolderWorkers: 1, PipelineDepth: 1, Deterministic: true},
			Run: func(ctx BenchmarkContext) error {
				if got := ctx.ExtraEnv["DB_NAME"]; got != "db_first" {
					t.Fatalf("first DB_NAME=%q", got)
				}
				return nil
			},
		},
		{
			Name:      "second",
			Execution: execution.Options{StoreFolderWorkers: 1, PipelineDepth: 1, Deterministic: true},
			Run: func(ctx BenchmarkContext) error {
				if got := ctx.ExtraEnv["DB_NAME"]; got != "db_second" {
					t.Fatalf("second DB_NAME=%q", got)
				}
				return nil
			},
		},
	}

	_, err := RunBenchmarkWithEnvironmentFactory(cases, func(caseName string) (map[string]string, func() error, error) {
		created = append(created, caseName)
		return map[string]string{"DB_NAME": "db_" + caseName}, func() error {
			cleaned = append(cleaned, caseName)
			return nil
		}, nil
	})
	if err != nil {
		t.Fatalf("RunBenchmarkWithEnvironmentFactory: %v", err)
	}
	if got := len(created); got != 2 {
		t.Fatalf("created=%v", created)
	}
	if got := len(cleaned); got != 2 {
		t.Fatalf("cleaned=%v", cleaned)
	}
	for index := range created {
		if created[index] != cleaned[index] {
			t.Fatalf("cleanup order mismatch: created=%v cleaned=%v", created, cleaned)
		}
	}
}

func TestRunBenchmarkWithEnvironmentFactoryRejectsNilFactory(t *testing.T) {
	if _, err := RunBenchmarkWithEnvironmentFactory(nil, nil); err == nil {
		t.Fatal("expected nil factory error")
	}
}

func TestRunBenchmarkWithEnvironmentFactoryCleansAfterCaseFailure(t *testing.T) {
	cleaned := false
	cases := []BenchmarkCase{{
		Name: "failing",
		Run: func(BenchmarkContext) error {
			return errors.New("case failed")
		},
	}}
	results, err := RunBenchmarkWithEnvironmentFactory(
		cases,
		func(string) (map[string]string, func() error, error) {
			return map[string]string{"DB_NAME": "db_failing"}, func() error {
				cleaned = true
				return nil
			}, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "case failed") {
		t.Fatalf("expected case failure, got results=%+v err=%v", results, err)
	}
	if !cleaned {
		t.Fatal("expected external cleanup after case failure")
	}
}
