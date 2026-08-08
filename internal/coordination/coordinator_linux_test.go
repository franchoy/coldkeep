//go:build linux

package coordination

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestProductionCoordinatorIntegratedLinuxLifecycle(t *testing.T) {
	repositoryPath := t.TempDir()
	identity, request := mustProductionRequest(t, repositoryPath, OperationStore, time.Unix(1_700_000_000, 0))
	coordinator := NewCoordinator()

	lease, err := coordinator.Acquire(context.Background(), identity, request)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	t.Cleanup(func() { _ = lease.Release() })

	prepared, err := PrepareControlNamespace(repositoryPath)
	if err != nil {
		t.Fatalf("PrepareControlNamespace: %v", err)
	}
	if info, err := os.Lstat(prepared.LockArtifactPath); err != nil {
		t.Fatalf("lstat repository.lock: %v", err)
	} else if !info.Mode().IsRegular() {
		t.Fatalf("repository.lock mode=%v want regular", info.Mode())
	}
	publishedOwner, err := readOwnerMetadata(prepared)
	if err != nil {
		t.Fatalf("readOwnerMetadata: %v", err)
	}
	if publishedOwner != request.Owner {
		t.Fatalf("published owner=%+v want=%+v", publishedOwner, request.Owner)
	}

	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if info, err := os.Lstat(prepared.LockArtifactPath); err != nil {
		t.Fatalf("persistent repository.lock missing: %v", err)
	} else if !info.Mode().IsRegular() {
		t.Fatalf("repository.lock mode after release=%v want regular", info.Mode())
	}
	if _, err := os.Lstat(prepared.OwnerMetadataPath); !os.IsNotExist(err) {
		t.Fatalf("owner metadata exists after release, stat err=%v", err)
	}

	reacquired, err := NewCoordinator().Acquire(context.Background(), identity, request)
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatalf("release reacquired Lease: %v", err)
	}
}

func TestProductionCoordinatorsShareProcessRegistryAndProtectSuccessor(t *testing.T) {
	repositoryPath := t.TempDir()
	identity, firstRequest := mustProductionRequest(t, repositoryPath, OperationStore, time.Unix(1_700_000_000, 0))
	_, successorRequest := mustProductionRequest(t, repositoryPath, OperationRestore, time.Unix(1_700_000_001, 0))
	firstCoordinator := NewCoordinator()
	secondCoordinator := NewCoordinator()

	firstLease, err := firstCoordinator.Acquire(context.Background(), identity, firstRequest)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	t.Cleanup(func() { _ = firstLease.Release() })

	nestedLease, err := secondCoordinator.Acquire(context.Background(), identity, successorRequest)
	if nestedLease != nil {
		_ = nestedLease.Release()
		t.Fatal("nested production acquisition returned a Lease")
	}
	if !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatalf("nested Acquire error=%v want ErrNestedRepositoryAcquisition", err)
	}
	if errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("nested Acquire error=%v unexpectedly classified Busy", err)
	}

	if err := firstLease.Release(); err != nil {
		t.Fatalf("release first Lease: %v", err)
	}
	successorLease, err := secondCoordinator.Acquire(context.Background(), identity, successorRequest)
	if err != nil {
		t.Fatalf("successor Acquire: %v", err)
	}
	t.Cleanup(func() { _ = successorLease.Release() })

	if err := firstLease.Release(); err != nil {
		t.Fatalf("stale first Release: %v", err)
	}
	prepared, err := PrepareControlNamespace(repositoryPath)
	if err != nil {
		t.Fatalf("PrepareControlNamespace: %v", err)
	}
	publishedOwner, err := readOwnerMetadata(prepared)
	if err != nil {
		t.Fatalf("read successor owner metadata: %v", err)
	}
	if publishedOwner != successorRequest.Owner {
		t.Fatalf("owner after stale release=%+v want successor=%+v", publishedOwner, successorRequest.Owner)
	}

	thirdLease, err := NewCoordinator().Acquire(context.Background(), identity, firstRequest)
	if thirdLease != nil {
		_ = thirdLease.Release()
		t.Fatal("stale release freed successor process reservation")
	}
	if !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatalf("Acquire after stale release error=%v want ErrNestedRepositoryAcquisition", err)
	}
	if err := successorLease.Release(); err != nil {
		t.Fatalf("release successor Lease: %v", err)
	}
}

func TestProductionCoordinatorAllowsDifferentRepositories(t *testing.T) {
	firstIdentity, firstRequest := mustProductionRequest(t, t.TempDir(), OperationStore, time.Unix(1_700_000_000, 0))
	secondIdentity, secondRequest := mustProductionRequest(t, t.TempDir(), OperationStore, time.Unix(1_700_000_000, 0))

	firstLease, err := NewCoordinator().Acquire(context.Background(), firstIdentity, firstRequest)
	if err != nil {
		t.Fatalf("acquire first repository: %v", err)
	}
	t.Cleanup(func() { _ = firstLease.Release() })
	secondLease, err := NewCoordinator().Acquire(context.Background(), secondIdentity, secondRequest)
	if err != nil {
		t.Fatalf("acquire second repository: %v", err)
	}
	t.Cleanup(func() { _ = secondLease.Release() })

	if err := secondLease.Release(); err != nil {
		t.Fatalf("release second repository: %v", err)
	}
	if err := firstLease.Release(); err != nil {
		t.Fatalf("release first repository: %v", err)
	}
}

func mustProductionRequest(
	t *testing.T,
	repositoryPath string,
	operation Operation,
	startedAt time.Time,
) (Identity, Request) {
	t.Helper()
	identity, err := ResolveIdentity(repositoryPath)
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	owner, err := NewOwner(operation, identity, "test-version", startedAt)
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	return identity, Request{Operation: operation, Mode: ModeExclusive, Owner: owner}
}
