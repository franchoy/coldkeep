package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/coordination"
	"github.com/franchoy/coldkeep/internal/recovery"
)

func TestRunCLIRepositoryLeaseOrdering(t *testing.T) {
	trace := &cliLifecycleTrace{}
	runtime := newTestCLIRuntime(t, trace)
	runtime.dispatch = func(parsed parsedCommandLine, mode cliOutputMode) error {
		trace.add("db open")
		defer trace.add("db cleanup")
		trace.add("command")
		fmt.Fprintln(os.Stdout, "coordinated success")
		return nil
	}

	stdout, stderr, code := captureRuntimeCLI(t, []string{"stats"}, runtime)
	if code != exitSuccess {
		t.Fatalf("runCLIWithRuntime code=%d stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "coordinated success") {
		t.Fatalf("stdout=%q want coordinated success", stdout)
	}
	trace.require(t, []string{
		"lease acquire", "recovery", "db open", "command", "db cleanup", "lease release", "render success",
	})
}

func TestExecuteCLILeaseAcquisitionFailureShortCircuitsRuntime(t *testing.T) {
	busyErr := fmt.Errorf("%w: held by another process", coordination.ErrRepositoryBusy)
	trace := &cliLifecycleTrace{}
	runtime := newTestCLIRuntime(t, trace)
	spoolDirectory := t.TempDir()
	runtime.newOutputSpool = testOutputSpoolFactory(spoolDirectory)
	runtime.newCoordinator = func() coordination.Coordinator {
		return &fakeCLICoordinator{
			acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
				trace.add("lease acquire")
				return nil, busyErr
			},
		}
	}
	parsed := parsedCommandLine{method: "stats", flags: map[string][]string{}}

	err := executeCLICommand(
		[]string{"stats"},
		parsed,
		outputModeText,
		repositoryCoordinationPolicyFor(parsed),
		runtime,
	)
	if !errors.Is(err, coordination.ErrRepositoryBusy) {
		t.Fatalf("executeCLICommand error=%v want ErrRepositoryBusy", err)
	}
	trace.require(t, []string{"lease acquire"})
	requireDirectoryEmpty(t, spoolDirectory)
}

func TestExecuteCLISpoolCreationFailureShortCircuitsBeforeLease(t *testing.T) {
	spoolErr := errors.New("spool creation failure")
	trace := &cliLifecycleTrace{}
	runtime := newTestCLIRuntime(t, trace)
	runtime.newOutputSpool = func() (*coordinatedOutputSpool, error) {
		return nil, spoolErr
	}
	parsed := parsedCommandLine{method: "stats", flags: map[string][]string{}}

	err := executeCLICommand(
		[]string{"stats"},
		parsed,
		outputModeText,
		repositoryCoordinationPolicyFor(parsed),
		runtime,
	)
	if !errors.Is(err, spoolErr) {
		t.Fatalf("executeCLICommand error=%v want errors.Is(%v)", err, spoolErr)
	}
	trace.require(t, nil)
}

func TestRunCLIRepositoryCoordinationBypassesNonRepositoryPaths(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantCode int
		want     []string
	}{
		{name: "help", args: []string{"help"}, wantCode: exitSuccess, want: []string{"dispatch", "render success"}},
		{name: "version", args: []string{"version"}, wantCode: exitSuccess, want: []string{"dispatch", "render success"}},
		{name: "init", args: []string{"init"}, wantCode: exitSuccess, want: []string{"dispatch", "render success"}},
		{name: "simulate", args: []string{"simulate", "gc"}, wantCode: exitSuccess, want: []string{"dispatch", "render success"}},
		{name: "benchmark", args: []string{"benchmark", "run"}, wantCode: exitSuccess, want: []string{"dispatch", "render success"}},
		{name: "unknown command", args: []string{"unknown"}, wantCode: exitUsage, want: []string{"dispatch"}},
		{name: "command help", args: []string{"store", "--help"}, wantCode: exitSuccess, want: []string{"dispatch", "render success"}},
		{name: "invalid syntax", args: []string{"stats", "--output"}, wantCode: exitUsage},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			if test.name == "unknown command" {
				runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
					trace.add("dispatch")
					return usageErrorf("unknown command")
				}
			}
			stdout, stderr, code := captureRuntimeCLI(t, test.args, runtime)
			_ = stdout
			_ = stderr
			if code != test.wantCode {
				t.Fatalf("runCLIWithRuntime code=%d want=%d", code, test.wantCode)
			}
			trace.require(t, test.want)
		})
	}
}

func TestExecuteCLIRecoveryFailureRemainsDiagnosticAndReleasesLease(t *testing.T) {
	recoveryErr := errors.New("startup recovery failure")
	trace := &cliLifecycleTrace{}
	runtime := newTestCLIRuntime(t, trace)
	runtime.recover = func(cliOutputMode) (recovery.Report, error) {
		trace.add("recovery")
		return recovery.Report{}, recoveryErr
	}
	runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
		trace.add("db open")
		trace.add("command")
		return nil
	}
	parsed := parsedCommandLine{method: "stats", flags: map[string][]string{}}

	err := executeCLICommand(
		[]string{"stats"},
		parsed,
		outputModeText,
		repositoryCoordinationPolicyFor(parsed),
		runtime,
	)
	if err != nil {
		t.Fatalf("diagnostic startup recovery error became fatal: %v", err)
	}
	trace.require(t, []string{"lease acquire", "recovery", "db open", "command", "lease release"})
}

func TestExecuteCLIRuntimeFailuresReleaseLeaseAndPreserveErrors(t *testing.T) {
	dbErr := errors.New("database open failure")
	commandErr := errors.New("command failure")
	releaseErr := errors.New("lease release failure")

	tests := []struct {
		name       string
		operation  error
		release    error
		wantErrors []error
	}{
		{name: "database failure", operation: dbErr, wantErrors: []error{dbErr}},
		{name: "command failure", operation: commandErr, wantErrors: []error{commandErr}},
		{name: "release failure", release: releaseErr, wantErrors: []error{releaseErr}},
		{
			name:       "operation and release failures",
			operation:  commandErr,
			release:    releaseErr,
			wantErrors: []error{commandErr, releaseErr},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			runtime.newCoordinator = func() coordination.Coordinator {
				return &fakeCLICoordinator{
					acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
						trace.add("lease acquire")
						return &fakeCLILease{releaseFn: func() error {
							trace.add("lease release")
							return test.release
						}}, nil
					},
				}
			}
			runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
				trace.add("db open")
				defer trace.add("db cleanup")
				trace.add("command")
				return test.operation
			}
			parsed := parsedCommandLine{method: "stats", flags: map[string][]string{}}

			err := executeCLICommand(
				[]string{"stats"},
				parsed,
				outputModeText,
				repositoryCoordinationPolicyFor(parsed),
				runtime,
			)
			for _, wantErr := range test.wantErrors {
				if !errors.Is(err, wantErr) {
					t.Fatalf("executeCLICommand error=%v want errors.Is(%v)", err, wantErr)
				}
			}
			trace.require(t, []string{
				"lease acquire", "recovery", "db open", "command", "db cleanup", "lease release",
			})
		})
	}
}

func TestRunCLIReleaseFailureSuppressesSpooledSuccessAndSuccessRendering(t *testing.T) {
	releaseErr := errors.New("lease release failure")
	trace := &cliLifecycleTrace{}
	runtime := newTestCLIRuntime(t, trace)
	runtime.newCoordinator = func() coordination.Coordinator {
		return &fakeCLICoordinator{
			acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
				trace.add("lease acquire")
				return &fakeCLILease{releaseFn: func() error {
					trace.add("lease release")
					return releaseErr
				}}, nil
			},
		}
	}
	runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
		trace.add("command")
		fmt.Fprintln(os.Stdout, "must not be rendered")
		return nil
	}

	stdout, _, code := captureRuntimeCLI(t, []string{"stats"}, runtime)
	if code != exitGeneral {
		t.Fatalf("runCLIWithRuntime code=%d want=%d", code, exitGeneral)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("stdout=%q want empty after release failure", stdout)
	}
	trace.require(t, []string{"lease acquire", "recovery", "command", "lease release"})
}

func TestRunCLICoordinatedOutputDecisionMatrix(t *testing.T) {
	operationErr := errors.New("operation failure")
	releaseErr := errors.New("lease release failure")

	tests := []struct {
		name           string
		operationErr   error
		releaseErr     error
		wantCode       int
		wantPayload    bool
		wantSuccess    bool
		wantStderrText []string
	}{
		{
			name:        "operation success release success",
			wantCode:    exitSuccess,
			wantPayload: true,
			wantSuccess: true,
		},
		{
			name:           "operation success release failure",
			releaseErr:     releaseErr,
			wantCode:       exitGeneral,
			wantStderrText: []string{"repository coordination failed"},
		},
		{
			name:           "operation failure release success",
			operationErr:   operationErr,
			wantCode:       exitGeneral,
			wantPayload:    true,
			wantStderrText: []string{operationErr.Error()},
		},
		{
			name:           "operation failure release failure",
			operationErr:   operationErr,
			releaseErr:     releaseErr,
			wantCode:       exitGeneral,
			wantPayload:    true,
			wantStderrText: []string{operationErr.Error()},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			spoolDirectory := t.TempDir()
			runtime.newOutputSpool = testOutputSpoolFactory(spoolDirectory)
			runtime.newCoordinator = func() coordination.Coordinator {
				return &fakeCLICoordinator{
					acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
						trace.add("lease acquire")
						return &fakeCLILease{releaseFn: func() error {
							trace.add("lease release")
							return test.releaseErr
						}}, nil
					},
				}
			}
			runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
				trace.add("command")
				fmt.Fprintln(os.Stdout, "command payload")
				return test.operationErr
			}

			stdout, stderr, code := captureRuntimeCLI(t, []string{"stats"}, runtime)
			if code != test.wantCode {
				t.Fatalf("runCLIWithRuntime code=%d want=%d stderr=%q", code, test.wantCode, stderr)
			}
			if got := strings.Contains(stdout, "command payload"); got != test.wantPayload {
				t.Fatalf("payload present=%v want=%v stdout=%q", got, test.wantPayload, stdout)
			}
			for _, want := range test.wantStderrText {
				if !strings.Contains(stderr, want) {
					t.Fatalf("stderr=%q does not contain %q", stderr, want)
				}
			}
			trace.mu.Lock()
			successRendered := false
			for _, event := range trace.events {
				if event == "render success" {
					successRendered = true
				}
			}
			trace.mu.Unlock()
			if successRendered != test.wantSuccess {
				t.Fatalf("success rendered=%v want=%v", successRendered, test.wantSuccess)
			}
			requireDirectoryEmpty(t, spoolDirectory)
		})
	}
}

func TestRunCLICoordinatedBatchPartialFailureOutput(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		outputMode cliOutputMode
	}{
		{name: "human", args: []string{"repair", "ref-counts"}, outputMode: outputModeText},
		{name: "json", args: []string{"repair", "ref-counts", "--output", "json"}, outputMode: outputModeJSON},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			runtime.dispatch = func(parsed parsedCommandLine, mode cliOutputMode) error {
				if mode != test.outputMode {
					t.Fatalf("output mode=%q want=%q", mode, test.outputMode)
				}
				return emitBatchCommandReport("repair", partialFailureBatchReport(), mode)
			}

			stdout, stderr, code := captureRuntimeCLI(t, test.args, runtime)
			if code == exitSuccess {
				t.Fatalf("runCLIWithRuntime unexpectedly succeeded stdout=%q stderr=%q", stdout, stderr)
			}
			if test.outputMode == outputModeJSON {
				var payload map[string]any
				if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &payload); err != nil {
					t.Fatalf("decode batch JSON stdout=%q: %v", stdout, err)
				}
				if got, _ := payload["status"].(string); got != "partial_failure" {
					t.Fatalf("batch JSON status=%q want=partial_failure payload=%v", got, payload)
				}
			} else {
				for _, want := range []string{"[REPAIR]", "✔ id=12", "✖ id=18", "Summary:"} {
					if !strings.Contains(stdout, want) {
						t.Fatalf("human batch stdout=%q does not contain %q", stdout, want)
					}
				}
			}
		})
	}
}

func TestExecuteCLIBatchOperationAndReleaseFailuresReplayReportAndPreserveErrors(t *testing.T) {
	releaseErr := errors.New("lease release failure")
	trace := &cliLifecycleTrace{}
	runtime := newTestCLIRuntime(t, trace)
	runtime.newCoordinator = func() coordination.Coordinator {
		return &fakeCLICoordinator{
			acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
				return &fakeCLILease{releaseFn: func() error { return releaseErr }}, nil
			},
		}
	}
	var operationErr error
	runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
		operationErr = emitBatchCommandReport("repair", partialFailureBatchReport(), outputModeText)
		return operationErr
	}
	parsed := parsedCommandLine{method: "repair", positionals: []string{"ref-counts"}, flags: map[string][]string{}}

	var executeErr error
	stdout := captureStdout(t, func() {
		executeErr = executeCLICommand(
			[]string{"repair", "ref-counts"},
			parsed,
			outputModeText,
			repositoryCoordinationPolicyFor(parsed),
			runtime,
		)
	})
	if !strings.Contains(stdout, "Summary:") {
		t.Fatalf("batch report was not replayed, stdout=%q", stdout)
	}
	if operationErr == nil || !errors.Is(executeErr, operationErr) {
		t.Fatalf("execute error=%v does not preserve operation error=%v", executeErr, operationErr)
	}
	if !errors.Is(executeErr, releaseErr) {
		t.Fatalf("execute error=%v does not preserve release error=%v", executeErr, releaseErr)
	}
}

func TestRunCLIReleaseOnlyFailureSuppressesJSONSuccessPayload(t *testing.T) {
	releaseErr := errors.New("lease release failure")
	trace := &cliLifecycleTrace{}
	runtime := newTestCLIRuntime(t, trace)
	runtime.newCoordinator = func() coordination.Coordinator {
		return &fakeCLICoordinator{
			acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
				return &fakeCLILease{releaseFn: func() error { return releaseErr }}, nil
			},
		}
	}
	runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
		fmt.Fprintln(os.Stdout, `{"status":"ok","command":"stats"}`)
		return nil
	}

	stdout, stderr, code := captureRuntimeCLI(t, []string{"stats", "--output", "json"}, runtime)
	if code != exitGeneral {
		t.Fatalf("runCLIWithRuntime code=%d want=%d stderr=%q", code, exitGeneral, stderr)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("stdout=%q want empty after release-only failure", stdout)
	}
	if !strings.Contains(stderr, "repository coordination failed") {
		t.Fatalf("stderr=%q does not contain stable coordination error", stderr)
	}
}

func partialFailureBatchReport() batch.Report {
	return batch.NewReport(batch.OperationRepair, false, []batch.ItemResult{
		{ID: 12, Status: batch.ResultSuccess, Message: "repaired"},
		{ID: 18, Status: batch.ResultFailed, Message: "forced failure"},
	})
}

func newTestCLIRuntime(t *testing.T, trace *cliLifecycleTrace) cliRuntime {
	t.Helper()
	identity, err := coordination.ResolveIdentity(t.TempDir())
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	return cliRuntime{
		newCoordinator: func() coordination.Coordinator {
			return &fakeCLICoordinator{
				acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
					trace.add("lease acquire")
					return &fakeCLILease{releaseFn: func() error {
						trace.add("lease release")
						return nil
					}}, nil
				},
			}
		},
		newOutputSpool: testOutputSpoolFactory(t.TempDir()),
		resolveIdentity: func(string) (coordination.Identity, error) {
			return identity, nil
		},
		newOwner: coordination.NewOwner,
		recover: func(cliOutputMode) (recovery.Report, error) {
			trace.add("recovery")
			return recovery.Report{}, nil
		},
		dispatch: func(parsedCommandLine, cliOutputMode) error {
			trace.add("dispatch")
			return nil
		},
		renderSuccess: func(parsedCommandLine, cliOutputMode) {
			trace.add("render success")
		},
		now: func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
}

func newDefaultCommandTestRuntime(t *testing.T) cliRuntime {
	t.Helper()
	identity, err := coordination.ResolveIdentity(t.TempDir())
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	return cliRuntime{
		newCoordinator: func() coordination.Coordinator {
			return &fakeCLICoordinator{
				acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
					return &fakeCLILease{releaseFn: func() error { return nil }}, nil
				},
			}
		},
		newOutputSpool:  testOutputSpoolFactory(t.TempDir()),
		resolveIdentity: func(string) (coordination.Identity, error) { return identity, nil },
		newOwner:        coordination.NewOwner,
		recover:         runStartupRecoveryWithOptionalLogBuffering,
		dispatch:        dispatchCLICommand,
		renderSuccess:   printCLISuccess,
		now:             time.Now,
	}
}

func testOutputSpoolFactory(directory string) func() (*coordinatedOutputSpool, error) {
	return func() (*coordinatedOutputSpool, error) {
		return newCoordinatedOutputSpool(directory)
	}
}

func requireDirectoryEmpty(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read spool directory: %v", err)
	}
	if len(entries) != 0 {
		paths := make([]string, 0, len(entries))
		for _, entry := range entries {
			paths = append(paths, filepath.Join(directory, entry.Name()))
		}
		t.Fatalf("spool directory not empty after lifecycle: %v", paths)
	}
}

func captureRuntimeCLI(t *testing.T, args []string, runtime cliRuntime) (stdout string, stderr string, code int) {
	t.Helper()
	stderr = captureStderr(t, func() {
		stdout = captureStdout(t, func() {
			code = runCLIWithRuntime(args, runtime)
		})
	})
	return stdout, stderr, code
}

type fakeCLICoordinator struct {
	acquireFn func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error)
}

func (coordinator *fakeCLICoordinator) Acquire(
	ctx context.Context,
	identity coordination.Identity,
	request coordination.Request,
) (coordination.Lease, error) {
	return coordinator.acquireFn(ctx, identity, request)
}

type fakeCLILease struct {
	releaseOnce sync.Once
	releaseFn   func() error
	releaseErr  error
}

func (lease *fakeCLILease) Release() error {
	lease.releaseOnce.Do(func() {
		lease.releaseErr = lease.releaseFn()
	})
	return lease.releaseErr
}

type cliLifecycleTrace struct {
	mu     sync.Mutex
	events []string
}

func (trace *cliLifecycleTrace) add(event string) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.events = append(trace.events, event)
}

func (trace *cliLifecycleTrace) require(t *testing.T, want []string) {
	t.Helper()
	trace.mu.Lock()
	got := append([]string(nil), trace.events...)
	trace.mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("trace=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("trace=%v want=%v", got, want)
		}
	}
}
