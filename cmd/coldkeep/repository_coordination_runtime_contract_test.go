package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/coordination"
)

func TestRunCLIRestoreVerifyAndGCRuntimeWithinLease(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		operation coordination.Operation
		event     string
	}{
		{name: "restore", args: []string{"restore", "42"}, operation: coordination.OperationRestore, event: "restore runtime"},
		{name: "verify", args: []string{"verify", "system"}, operation: coordination.OperationVerify, event: "verify runtime"},
		{name: "gc dry run", args: []string{"gc", "--dry-run"}, operation: coordination.OperationGarbageCollect, event: "gc dry-run runtime"},
		{name: "gc live", args: []string{"gc"}, operation: coordination.OperationGarbageCollect, event: "gc live runtime"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			var acquiredOperation coordination.Operation
			runtime.newCoordinator = func() coordination.Coordinator {
				return &fakeCLICoordinator{
					acquireFn: func(_ context.Context, _ coordination.Identity, request coordination.Request) (coordination.Lease, error) {
						acquiredOperation = request.Operation
						trace.add("lease acquire")
						return &fakeCLILease{releaseFn: func() error {
							trace.add("lease release")
							return nil
						}}, nil
					},
				}
			}
			runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
				trace.add(test.event)
				defer trace.add(test.event + " cleanup")
				return nil
			}

			_, stderr, code := captureRuntimeCLI(t, test.args, runtime)
			if code != exitSuccess {
				t.Fatalf("exit=%d want=%d stderr=%q", code, exitSuccess, stderr)
			}
			if acquiredOperation != test.operation {
				t.Fatalf("acquired operation=%q want=%q", acquiredOperation, test.operation)
			}
			trace.require(t, []string{
				"lease acquire",
				"recovery",
				test.event,
				test.event + " cleanup",
				"lease release",
				"render success",
			})
		})
	}
}

func TestRunCLIRestoreVerifyAndGCFailuresCleanUpBeforeLeaseRelease(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		event string
	}{
		{name: "restore", args: []string{"restore", "42"}, event: "restore runtime"},
		{name: "verify", args: []string{"verify", "system"}, event: "verify runtime"},
		{name: "gc dry run", args: []string{"gc", "--dry-run"}, event: "gc dry-run runtime"},
		{name: "gc live", args: []string{"gc"}, event: "gc live runtime"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operationErr := fmt.Errorf("%s failed", test.event)
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
				trace.add(test.event)
				defer trace.add(test.event + " cleanup")
				return operationErr
			}

			_, stderr, code := captureRuntimeCLI(t, test.args, runtime)
			if code != exitGeneral {
				t.Fatalf("exit=%d want=%d stderr=%q", code, exitGeneral, stderr)
			}
			if !strings.Contains(stderr, operationErr.Error()) {
				t.Fatalf("stderr=%q does not contain operation failure", stderr)
			}
			trace.require(t, []string{
				"lease acquire",
				"recovery",
				test.event,
				test.event + " cleanup",
				"lease release",
			})
		})
	}
}

func TestExecuteCLIVerifyOperationAndReleaseFailuresRemainJoined(t *testing.T) {
	verifyErr := errors.New("verify runtime failure")
	releaseErr := errors.New("verify lease release failure")
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
		trace.add("verify runtime")
		defer trace.add("verify runtime cleanup")
		return verifyErr
	}
	parsed := parsedCommandLine{method: "verify", positionals: []string{"system"}, flags: map[string][]string{}}

	err := executeCLICommand(
		[]string{"verify", "system"},
		parsed,
		outputModeText,
		repositoryCoordinationPolicyFor(parsed),
		runtime,
	)
	if !errors.Is(err, verifyErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("joined error=%v does not preserve verify and release failures", err)
	}
	trace.require(t, []string{
		"lease acquire",
		"recovery",
		"verify runtime",
		"verify runtime cleanup",
		"lease release",
	})
}
