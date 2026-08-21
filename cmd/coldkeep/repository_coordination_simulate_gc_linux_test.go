//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/coordination"
	"github.com/franchoy/coldkeep/internal/recovery"
)

const (
	phase9SimulateGCHolderEnv  = "COLDKEEP_PHASE9_SIMULATE_GC_HOLDER"
	phase9SimulateGCRepository = "COLDKEEP_PHASE9_SIMULATE_GC_REPOSITORY"
)

func TestPhase9SimulateGCIndependentHolderProcess(t *testing.T) {
	if os.Getenv(phase9SimulateGCHolderEnv) == "1" {
		runPhase9SimulateGCHolder(t)
		return
	}

	repositoryPath := t.TempDir()
	identity, err := coordination.ResolveIdentity(repositoryPath)
	if err != nil {
		t.Fatalf("resolve repository identity: %v", err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	command := exec.Command(executable, "-test.run=^TestPhase9SimulateGCIndependentHolderProcess$") // #nosec G204 -- executable is the current Go test binary.
	command.Env = append(os.Environ(), phase9SimulateGCHolderEnv+"=1", phase9SimulateGCRepository+"="+repositoryPath)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start holder process: %v", err)
	}
	waited := false
	t.Cleanup(func() {
		if waited {
			return
		}
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	ready, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(ready) != "ready" {
		t.Fatalf("holder readiness=%q err=%v stderr=%q", ready, err, stderr.String())
	}

	var recoveryCalled, dispatchCalled bool
	runtime := newTestCLIRuntime(t, &cliLifecycleTrace{})
	runtime.newCoordinator = coordination.NewCoordinator
	runtime.resolveIdentity = func(string) (coordination.Identity, error) { return identity, nil }
	runtime.recover = func(cliOutputMode) (recovery.Report, error) {
		recoveryCalled = true
		return recovery.Report{}, nil
	}
	runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
		dispatchCalled = true
		return nil
	}

	_, busyStderr, code := captureRuntimeCLI(t, []string{"simulate", "gc"}, runtime)
	if code == exitSuccess || !strings.Contains(strings.ToLower(busyStderr), "busy") {
		t.Fatalf("contended simulate gc code=%d stderr=%q", code, busyStderr)
	}
	if recoveryCalled || dispatchCalled {
		t.Fatalf("contended simulate gc reached recovery=%t dispatch=%t before ownership", recoveryCalled, dispatchCalled)
	}

	if _, err := fmt.Fprintln(stdin, "release"); err != nil {
		t.Fatalf("signal holder release: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("holder process exit: %v stderr=%q", err, stderr.String())
	}
	waited = true

	recoveryCalled, dispatchCalled = false, false
	_, retryStderr, code := captureRuntimeCLI(t, []string{"simulate", "gc"}, runtime)
	if code != exitSuccess {
		t.Fatalf("simulate gc after holder release code=%d stderr=%q", code, retryStderr)
	}
	if !recoveryCalled || !dispatchCalled {
		t.Fatalf("reacquired simulate gc recovery=%t dispatch=%t", recoveryCalled, dispatchCalled)
	}
}

func runPhase9SimulateGCHolder(t *testing.T) {
	repositoryPath := os.Getenv(phase9SimulateGCRepository)
	identity, err := coordination.ResolveIdentity(repositoryPath)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := coordination.NewOwner(coordination.OperationStore, identity, "phase9-holder", time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := coordination.NewCoordinator().Acquire(context.Background(), identity, coordination.Request{
		Operation: coordination.OperationStore,
		Mode:      coordination.ModeExclusive,
		Owner:     owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}
