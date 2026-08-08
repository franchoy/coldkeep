//go:build linux

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/coordination"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/recovery"
)

func TestRunCLIRealCoordinatorWrapsRestoreVerifyAndGCOnLinux(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		operation coordination.Operation
	}{
		{name: "restore", args: []string{"restore", "42"}, operation: coordination.OperationRestore},
		{name: "verify", args: []string{"verify", "system"}, operation: coordination.OperationVerify},
		{name: "gc dry run", args: []string{"gc", "--dry-run"}, operation: coordination.OperationGarbageCollect},
		{name: "gc live", args: []string{"gc"}, operation: coordination.OperationGarbageCollect},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repositoryPath := t.TempDir()
			identity, err := coordination.ResolveIdentity(repositoryPath)
			if err != nil {
				t.Fatalf("ResolveIdentity: %v", err)
			}
			ownerPath := filepath.Join(repositoryPath, coordination.ControlDirectoryName, coordination.OwnerMetadataName)
			lockPath := filepath.Join(repositoryPath, coordination.ControlDirectoryName, coordination.LockArtifactName)
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			runtime.newCoordinator = coordination.NewCoordinator
			runtime.resolveIdentity = func(string) (coordination.Identity, error) { return identity, nil }
			runtime.recover = func(cliOutputMode) (recovery.Report, error) {
				trace.add("recovery")
				return recovery.Report{}, nil
			}
			runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
				trace.add("command runtime")
				defer trace.add("command cleanup")
				data, readErr := os.ReadFile(ownerPath)
				if readErr != nil {
					return readErr
				}
				owner, decodeErr := coordination.DecodeOwner(data)
				if decodeErr != nil {
					return decodeErr
				}
				if owner.Operation != test.operation {
					return fmt.Errorf("owner operation=%q want=%q", owner.Operation, test.operation)
				}
				return nil
			}

			_, stderr, code := captureRuntimeCLI(t, test.args, runtime)
			if code != exitSuccess {
				t.Fatalf("runCLIWithRuntime code=%d stderr=%q", code, stderr)
			}
			trace.require(t, []string{"recovery", "command runtime", "command cleanup", "render success"})
			if info, statErr := os.Lstat(lockPath); statErr != nil {
				t.Fatalf("persistent repository.lock missing: %v", statErr)
			} else if !info.Mode().IsRegular() {
				t.Fatalf("repository.lock mode=%v want regular", info.Mode())
			}
			if _, statErr := os.Lstat(ownerPath); !os.IsNotExist(statErr) {
				t.Fatalf("owner metadata exists after release, stat err=%v", statErr)
			}

			lease, acquireErr := coordination.NewCoordinator().Acquire(context.Background(), identity, mustTestOwnerRequest(t, identity, test.operation))
			if acquireErr != nil {
				t.Fatalf("reacquire after command cleanup: %v", acquireErr)
			}
			if releaseErr := lease.Release(); releaseErr != nil {
				t.Fatalf("release reacquired lease: %v", releaseErr)
			}
		})
	}
}

func mustTestOwnerRequest(t *testing.T, identity coordination.Identity, operation coordination.Operation) coordination.Request {
	t.Helper()
	owner, err := coordination.NewOwner(operation, identity, "test", time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	return coordination.Request{Operation: operation, Mode: coordination.ModeExclusive, Owner: owner}
}

func TestRunCLIProductionLeaseWrapsRecoveryAndCommandOnLinux(t *testing.T) {
	repositoryPath := t.TempDir()
	controlDirectory := filepath.Join(repositoryPath, coordination.ControlDirectoryName)
	lockPath := filepath.Join(controlDirectory, coordination.LockArtifactName)
	ownerPath := filepath.Join(controlDirectory, coordination.OwnerMetadataName)

	originalContainersDir := container.ContainersDir
	originalRecovery := startupRecoveryPhase
	originalStats := runObservabilityStatsPhase
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
		startupRecoveryPhase = originalRecovery
		runObservabilityStatsPhase = originalStats
	})
	container.ContainersDir = repositoryPath

	assertPublishedOwner := func(stage string) {
		t.Helper()
		data, err := os.ReadFile(ownerPath)
		if err != nil {
			t.Fatalf("%s: read owner metadata: %v", stage, err)
		}
		owner, err := coordination.DecodeOwner(data)
		if err != nil {
			t.Fatalf("%s: decode owner metadata: %v", stage, err)
		}
		if owner.Operation != coordination.OperationStats {
			t.Fatalf("%s: owner operation=%q want=%q", stage, owner.Operation, coordination.OperationStats)
		}
	}
	startupRecoveryPhase = func(string) (recovery.Report, error) {
		assertPublishedOwner("recovery")
		return recovery.Report{}, nil
	}
	runObservabilityStatsPhase = func(observability.StatsOptions) (*observability.StatsResult, error) {
		assertPublishedOwner("command")
		return &observability.StatsResult{}, nil
	}

	stdout, stderr, code := captureProductionCLI(t, []string{"stats"})
	if code != exitSuccess {
		t.Fatalf("runCLI code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if info, err := os.Lstat(lockPath); err != nil {
		t.Fatalf("persistent repository.lock missing: %v", err)
	} else if !info.Mode().IsRegular() {
		t.Fatalf("repository.lock mode=%v want regular", info.Mode())
	}
	if _, err := os.Lstat(ownerPath); !os.IsNotExist(err) {
		t.Fatalf("owner metadata exists after runCLI, stat err=%v", err)
	}
}

func captureProductionCLI(t *testing.T, args []string) (stdout string, stderr string, code int) {
	t.Helper()
	stderr = captureStderr(t, func() {
		stdout = captureStdout(t, func() {
			code = runCLI(args)
		})
	})
	return stdout, stderr, code
}
