//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
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
	holder := startPhase9HolderProcess(t, executable, repositoryPath)
	holder.requireReady(t)
	runtime, probe := newPhase9SimulateGCRuntime(t, identity)
	requirePhase9ContendedRuntime(t, runtime, probe)
	holder.release(t)
	probe.reset()
	requirePhase9UncontendedRuntime(t, runtime, probe)
}

type phase9HolderProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  *bufio.Reader
	stderr  bytes.Buffer
	waited  bool
}

func startPhase9HolderProcess(t *testing.T, executable, repositoryPath string) *phase9HolderProcess {
	t.Helper()
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
	holder := &phase9HolderProcess{command: command, stdin: stdin, stdout: bufio.NewReader(stdout)}
	command.Stderr = &holder.stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start holder process: %v", err)
	}
	t.Cleanup(func() {
		if holder.waited {
			return
		}
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	return holder
}

func (p *phase9HolderProcess) requireReady(t *testing.T) {
	t.Helper()
	ready, err := p.stdout.ReadString('\n')
	if err != nil || strings.TrimSpace(ready) != "ready" {
		t.Fatalf("holder readiness=%q err=%v stderr=%q", ready, err, p.stderr.String())
	}
}

func (p *phase9HolderProcess) release(t *testing.T) {
	t.Helper()
	if _, err := fmt.Fprintln(p.stdin, "release"); err != nil {
		t.Fatalf("signal holder release: %v", err)
	}
	if err := p.command.Wait(); err != nil {
		t.Fatalf("holder process exit: %v stderr=%q", err, p.stderr.String())
	}
	p.waited = true
}

type phase9RuntimeProbe struct {
	recoveryCalled bool
	dispatchCalled bool
}

func (p *phase9RuntimeProbe) reset() {
	p.recoveryCalled = false
	p.dispatchCalled = false
}

func newPhase9SimulateGCRuntime(t *testing.T, identity coordination.Identity) (cliRuntime, *phase9RuntimeProbe) {
	t.Helper()
	probe := &phase9RuntimeProbe{}
	runtime := newTestCLIRuntime(t, &cliLifecycleTrace{})
	runtime.newCoordinator = coordination.NewCoordinator
	runtime.resolveIdentity = func(string) (coordination.Identity, error) { return identity, nil }
	runtime.recover = func(cliOutputMode) (recovery.Report, error) {
		probe.recoveryCalled = true
		return recovery.Report{}, nil
	}
	runtime.dispatch = func(parsedCommandLine, cliOutputMode) error {
		probe.dispatchCalled = true
		return nil
	}
	return runtime, probe
}

func requirePhase9ContendedRuntime(t *testing.T, runtime cliRuntime, probe *phase9RuntimeProbe) {
	t.Helper()
	_, busyStderr, code := captureRuntimeCLI(t, []string{"simulate", "gc"}, runtime)
	if code == exitSuccess || !strings.Contains(strings.ToLower(busyStderr), "busy") {
		t.Fatalf("contended simulate gc code=%d stderr=%q", code, busyStderr)
	}
	if probe.recoveryCalled || probe.dispatchCalled {
		t.Fatalf("contended simulate gc reached recovery=%t dispatch=%t before ownership", probe.recoveryCalled, probe.dispatchCalled)
	}
}

func requirePhase9UncontendedRuntime(t *testing.T, runtime cliRuntime, probe *phase9RuntimeProbe) {
	t.Helper()
	_, retryStderr, code := captureRuntimeCLI(t, []string{"simulate", "gc"}, runtime)
	if code != exitSuccess {
		t.Fatalf("simulate gc after holder release code=%d stderr=%q", code, retryStderr)
	}
	if !probe.recoveryCalled || !probe.dispatchCalled {
		t.Fatalf("reacquired simulate gc recovery=%t dispatch=%t", probe.recoveryCalled, probe.dispatchCalled)
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
