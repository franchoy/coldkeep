package coordination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var testOwnerStart = time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)

type fakeLease struct {
	releaseErr   error
	releaseCalls int
	released     bool
	onRelease    func()
}

func (l *fakeLease) Release() error {
	l.releaseCalls++
	if l.released {
		return nil
	}
	if l.releaseErr == nil {
		l.released = true
	}
	if l.onRelease != nil {
		l.onRelease()
		l.onRelease = nil
	}
	return l.releaseErr
}

type fakeCoordinator struct {
	lease        Lease
	acquireErr   error
	acquireCalls int
	acquired     bool
}

func (c *fakeCoordinator) Acquire(context.Context, Identity, Request) (Lease, error) {
	c.acquireCalls++
	if c.acquireErr != nil {
		return nil, c.acquireErr
	}
	c.acquired = true
	return c.lease, nil
}

func TestWithLeaseLifecycle(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	lease := &fakeLease{}
	coordinator := &fakeCoordinator{lease: lease}
	callbackCalled := false

	err := WithLease(context.Background(), coordinator, identity, mustRequest(t, identity, OperationStore), func() error {
		callbackCalled = true
		if !coordinator.acquired {
			t.Fatal("callback ran before acquisition")
		}
		if lease.releaseCalls != 0 {
			t.Fatal("lease released before callback completed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithLease: %v", err)
	}
	if !callbackCalled || coordinator.acquireCalls != 1 || lease.releaseCalls != 1 {
		t.Fatalf("unexpected lifecycle callback=%v acquire=%d release=%d", callbackCalled, coordinator.acquireCalls, lease.releaseCalls)
	}
}

func TestWithLeaseDoesNotRunAfterAcquireFailure(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	coordinator := &fakeCoordinator{acquireErr: ErrRepositoryBusy}
	callbackCalled := false

	err := WithLease(context.Background(), coordinator, identity, mustRequest(t, identity, OperationVerify), func() error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("expected busy classification, got %v", err)
	}
	if callbackCalled {
		t.Fatal("callback ran after acquisition failure")
	}
}

func TestWithLeaseCombinesOperationAndReleaseErrors(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	operationErr := errors.New("operation failed")
	releaseErr := errors.New("release failed")
	lease := &fakeLease{releaseErr: releaseErr}

	err := WithLease(context.Background(), &fakeCoordinator{lease: lease}, identity, mustRequest(t, identity, OperationRestore), func() error {
		return operationErr
	})
	if !errors.Is(err, operationErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("expected joined operation and release errors, got %v", err)
	}
	if !strings.HasPrefix(err.Error(), operationErr.Error()+"\n") {
		t.Fatalf("operation error was not first in joined error: %v", err)
	}
	if lease.releaseCalls != 1 {
		t.Fatalf("expected one release, got %d", lease.releaseCalls)
	}
}

func TestWithLeaseReturnsOperationErrorAfterReleaseSuccess(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	operationErr := errors.New("operation failed")
	lease := &fakeLease{}

	err := WithLease(context.Background(), &fakeCoordinator{lease: lease}, identity, mustRequest(t, identity, OperationRemove), func() error {
		return operationErr
	})
	if !errors.Is(err, operationErr) {
		t.Fatalf("expected operation error, got %v", err)
	}
	if lease.releaseCalls != 1 {
		t.Fatalf("expected one release, got %d", lease.releaseCalls)
	}
}

func TestWithLeaseReturnsReleaseErrorAfterOperationSuccess(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	releaseErr := errors.New("release failed")
	err := WithLease(context.Background(), &fakeCoordinator{
		lease: &fakeLease{releaseErr: releaseErr},
	}, identity, mustRequest(t, identity, OperationGarbageCollect), func() error {
		return nil
	})
	if !errors.Is(err, releaseErr) {
		t.Fatalf("expected release error, got %v", err)
	}
}

func TestValidateRequestAcceptsSimulateGCOperation(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	if err := ValidateRequest(identity, mustRequest(t, identity, OperationSimulateGC)); err != nil {
		t.Fatalf("ValidateRequest simulate gc: %v", err)
	}
}

func TestLeaseContractReleaseIsIdempotentAfterSuccess(t *testing.T) {
	cleanupCalls := 0
	lease := &fakeLease{onRelease: func() {
		cleanupCalls++
	}}

	if err := lease.Release(); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if cleanupCalls != 1 {
		t.Fatalf("cleanup calls=%d want=1", cleanupCalls)
	}
	if lease.releaseCalls != 2 {
		t.Fatalf("release calls=%d want=2", lease.releaseCalls)
	}
}

func TestWithLeasePreservesContextCancellation(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	coordinator := &fakeCoordinator{lease: &fakeLease{}}

	err := WithLease(ctx, coordinator, identity, mustRequest(t, identity, OperationList), func() error {
		t.Fatal("callback must not run for a cancelled context")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if coordinator.acquireCalls != 0 {
		t.Fatalf("cancelled context reached coordinator %d times", coordinator.acquireCalls)
	}
}

func TestWithLeasePreservesExpiredDeadline(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	coordinator := &fakeCoordinator{lease: &fakeLease{}}

	err := WithLease(ctx, coordinator, identity, mustRequest(t, identity, OperationInspect), func() error {
		t.Fatal("callback must not run for an expired context")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline classification, got %v", err)
	}
	if coordinator.acquireCalls != 0 {
		t.Fatalf("expired context reached coordinator %d times", coordinator.acquireCalls)
	}
}

func TestWithLeaseRejectsUnsupportedModeAndInvalidInputs(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	coordinator := &fakeCoordinator{lease: &fakeLease{}}

	request := mustRequest(t, identity, OperationStats)
	request.Mode = Mode("shared")
	err := WithLease(context.Background(), coordinator, identity, request, func() error { return nil })
	if !errors.Is(err, ErrRepositoryLockUnsupported) {
		t.Fatalf("expected unsupported classification, got %v", err)
	}

	if err := WithLease(context.Background(), nil, identity, mustRequest(t, identity, OperationStats), func() error { return nil }); err == nil {
		t.Fatal("expected nil coordinator error")
	}
}

func TestValidateRequestRejectsMismatchedOwner(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	request := mustRequest(t, identity, OperationStore)
	request.Owner.Operation = OperationRemove
	if err := ValidateRequest(identity, request); err == nil {
		t.Fatal("expected owner operation mismatch")
	}

	request = mustRequest(t, identity, OperationStore)
	request.Owner.IdentityHash = strings.Repeat("0", sha256HexLength)
	if err := ValidateRequest(identity, request); err == nil {
		t.Fatal("expected owner identity mismatch")
	}
}

func TestCoordinationValidateRequestRejectsBlankAndUnknownOperations(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	for _, operation := range []Operation{"", "unknown"} {
		request := mustRequest(t, identity, OperationStore)
		request.Operation = operation
		request.Owner.Operation = operation
		if err := ValidateRequest(identity, request); err == nil {
			t.Fatalf("expected operation %q to fail", operation)
		}
	}
}

type nonReentrantFakeCoordinator struct {
	mu   sync.Mutex
	held map[string]bool
}

func (c *nonReentrantFakeCoordinator) Acquire(_ context.Context, identity Identity, _ Request) (Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.held == nil {
		c.held = make(map[string]bool)
	}
	if c.held[identity.Hash] {
		return nil, fmt.Errorf("%w: %s", ErrNestedRepositoryAcquisition, identity.Hash)
	}
	c.held[identity.Hash] = true
	return &fakeLease{onRelease: func() {
		c.mu.Lock()
		delete(c.held, identity.Hash)
		c.mu.Unlock()
	}}, nil
}

func TestCoordinatorContractRejectsNestedIdentityAndSeparatesRepositories(t *testing.T) {
	coordinator := &nonReentrantFakeCoordinator{}
	firstIdentity := mustIdentity(t, t.TempDir())
	secondIdentity := mustIdentity(t, t.TempDir())
	request := mustRequest(t, firstIdentity, OperationStore)

	first, err := coordinator.Acquire(context.Background(), firstIdentity, request)
	if err != nil {
		t.Fatalf("acquire first identity: %v", err)
	}
	if _, err := coordinator.Acquire(context.Background(), firstIdentity, request); !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatalf("expected nested acquisition error, got %v", err)
	}
	secondRequest := mustRequest(t, secondIdentity, OperationStore)
	second, err := coordinator.Acquire(context.Background(), secondIdentity, secondRequest)
	if err != nil {
		t.Fatalf("independent repository should not contend: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release second identity: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release first identity: %v", err)
	}
	if _, err := coordinator.Acquire(context.Background(), firstIdentity, request); err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
}

func TestCoordinatorContractTreatsAliasAsNestedIdentity(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "repository")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("create target: %v", err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	direct := mustIdentity(t, target)
	throughAlias := mustIdentity(t, alias)
	coordinator := &nonReentrantFakeCoordinator{}
	request := mustRequest(t, direct, OperationStore)
	lease, err := coordinator.Acquire(context.Background(), direct, request)
	if err != nil {
		t.Fatalf("acquire direct identity: %v", err)
	}
	aliasRequest := mustRequest(t, throughAlias, OperationStore)
	if _, err := coordinator.Acquire(context.Background(), throughAlias, aliasRequest); !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatalf("expected alias acquisition to be nested, got %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("release direct identity: %v", err)
	}
}

func mustIdentity(t *testing.T, path string) Identity {
	t.Helper()
	identity, err := ResolveIdentity(path)
	if err != nil {
		t.Fatalf("ResolveIdentity(%q): %v", path, err)
	}
	return identity
}

func mustRequest(t *testing.T, identity Identity, operation Operation) Request {
	t.Helper()
	owner, err := NewOwner(operation, identity, "1.13.11", testOwnerStart)
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	return Request{
		Operation: operation,
		Mode:      ModeExclusive,
		Owner:     owner,
	}
}
