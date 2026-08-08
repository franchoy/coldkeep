package coordination

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var productionProcessRegistry processRegistry

// NewCoordinator returns the production repository coordinator. All instances
// share one process registry so same-process acquisition is non-reentrant even
// when callers construct separate coordinators.
func NewCoordinator() Coordinator {
	return newRepositoryCoordinator(coordinatorDependencies{
		prepare:       PrepareControlNamespace,
		reserve:       reserveProductionProcessIdentity,
		acquireNative: acquireNativeLockResource,
		publishOwner:  publishOwnerMetadata,
		removeOwner:   removeOwnerMetadata,
	})
}

func reserveProductionProcessIdentity(identity Identity) (processReservationResource, error) {
	return productionProcessRegistry.reserve(identity)
}

type processReservationResource interface {
	release()
}

type nativeLockResource interface {
	release() error
}

type coordinatorDependencies struct {
	prepare       func(string) (PreparedControlNamespace, error)
	reserve       func(Identity) (processReservationResource, error)
	acquireNative func(PreparedControlNamespace) (nativeLockResource, error)
	publishOwner  func(PreparedControlNamespace, Owner) error
	removeOwner   func(PreparedControlNamespace) error
}

type repositoryCoordinator struct {
	dependencies coordinatorDependencies
}

func newRepositoryCoordinator(dependencies coordinatorDependencies) *repositoryCoordinator {
	return &repositoryCoordinator{dependencies: dependencies}
}

func acquireNativeLockResource(prepared PreparedControlNamespace) (nativeLockResource, error) {
	return acquireNativeLock(prepared)
}

func (coordinator *repositoryCoordinator) Acquire(
	ctx context.Context,
	identity Identity,
	request Request,
) (Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateRequest(identity, request); err != nil {
		return nil, err
	}

	prepared, err := coordinator.dependencies.prepare(identity.CanonicalPath)
	if err != nil {
		return nil, err
	}
	if prepared.Identity != identity {
		return nil, fmt.Errorf("%w: prepared repository identity does not match acquisition identity", ErrRepositoryIdentityInvalid)
	}

	reservation, err := coordinator.dependencies.reserve(prepared.Identity)
	if err != nil {
		return nil, err
	}

	nativeLock, err := coordinator.dependencies.acquireNative(prepared)
	if err != nil {
		reservation.release()
		return nil, err
	}

	if err := coordinator.dependencies.publishOwner(prepared, request.Owner); err != nil {
		nativeReleaseErr := nativeLock.release()
		reservation.release()
		if nativeReleaseErr != nil {
			return nil, errors.Join(err, nativeReleaseErr)
		}
		return nil, err
	}

	return &repositoryLease{
		prepared:    prepared,
		reservation: reservation,
		nativeLock:  nativeLock,
		removeOwner: coordinator.dependencies.removeOwner,
	}, nil
}

type repositoryLease struct {
	prepared    PreparedControlNamespace
	reservation processReservationResource
	nativeLock  nativeLockResource
	removeOwner func(PreparedControlNamespace) error

	releaseOnce sync.Once
	releaseErr  error
}

func (lease *repositoryLease) Release() error {
	if lease == nil {
		return nil
	}
	lease.releaseOnce.Do(func() {
		ownerRemovalErr := lease.removeOwner(lease.prepared)
		nativeReleaseErr := lease.nativeLock.release()
		lease.reservation.release()

		switch {
		case nativeReleaseErr != nil && ownerRemovalErr != nil:
			lease.releaseErr = errors.Join(nativeReleaseErr, ownerRemovalErr)
		case nativeReleaseErr != nil:
			lease.releaseErr = nativeReleaseErr
		default:
			// Owner metadata is diagnostic. Its removal failure alone must not
			// turn a successfully released native lease into an operation error.
			lease.releaseErr = nil
		}
	})
	return lease.releaseErr
}
