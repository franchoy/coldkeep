//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/coordination"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/recovery"
)

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
