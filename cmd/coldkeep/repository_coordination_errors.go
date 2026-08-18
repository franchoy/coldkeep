package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/franchoy/coldkeep/internal/coordination"
)

const (
	publicCodeRepositoryBusy            = "REPOSITORY_BUSY"
	publicCodeRepositoryLockUnsupported = "REPOSITORY_LOCK_UNSUPPORTED"
	publicCodePermissionDenied          = "PERMISSION_DENIED"
	publicCodeCanceled                  = "CANCELED"
	publicCodeDeadlineExceeded          = "DEADLINE_EXCEEDED"
)

// repositoryCoordinationFailure marks an error produced while acquiring or
// releasing the outer repository lease. It preserves the original cause for
// errors.Is/errors.As while allowing generic I/O failures to use a safe CLI
// message rather than exposing repository paths or native-lock details.
type repositoryCoordinationFailure struct {
	err error
}

func (failure *repositoryCoordinationFailure) Error() string {
	return failure.err.Error()
}

func (failure *repositoryCoordinationFailure) Unwrap() error {
	return failure.err
}

func markRepositoryCoordinationFailure(err error) error {
	if err == nil {
		return nil
	}
	var marked *repositoryCoordinationFailure
	if errors.As(err, &marked) {
		return err
	}
	return &repositoryCoordinationFailure{err: err}
}

// cliRepositoryCoordinator keeps the coordination stage visible to the CLI
// classifier without changing the internal coordination contract.
type cliRepositoryCoordinator struct {
	delegate coordination.Coordinator
}

func (coordinator cliRepositoryCoordinator) Acquire(
	ctx context.Context,
	identity coordination.Identity,
	request coordination.Request,
) (coordination.Lease, error) {
	if coordinator.delegate == nil {
		return nil, markRepositoryCoordinationFailure(fmt.Errorf("repository coordinator is unavailable"))
	}
	lease, err := coordinator.delegate.Acquire(ctx, identity, request)
	if err != nil {
		return nil, markRepositoryCoordinationFailure(err)
	}
	if lease == nil {
		return nil, nil
	}
	return &cliRepositoryLease{delegate: lease}, nil
}

type cliRepositoryLease struct {
	delegate coordination.Lease
}

func (lease *cliRepositoryLease) Release() error {
	if lease == nil || lease.delegate == nil {
		return nil
	}
	return markRepositoryCoordinationFailure(lease.delegate.Release())
}

// stableCLIError maps established coordination and runtime causes into the
// existing cliError representation. Existing explicit cliError ownership wins,
// which preserves the operation-first precedence of joined operation/release
// failures produced by coordination.WithLease.
func stableCLIError(err error) error {
	if err == nil {
		return nil
	}
	if joinedError, ok := stableJoinedCLIError(err); ok {
		return joinedError
	}

	var existing *cliError
	if errors.As(err, &existing) {
		return err
	}

	if classified, ok := stableSentinelCLIError(err); ok {
		return classified
	}

	var coordinationFailure *repositoryCoordinationFailure
	if errors.As(err, &coordinationFailure) {
		return observabilityWrappedError(exitGeneral, "INTERNAL", "repository coordination failed", err)
	}

	return err
}

func stableJoinedCLIError(err error) (error, bool) {
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || !hasStableCLIClassification(err) {
		return nil, false
	}
	causes := joined.Unwrap()
	if len(causes) == 0 {
		return nil, false
	}
	primary := stableCLIError(causes[0])
	var classified *cliError
	if errors.As(primary, &classified) {
		return &cliError{code: classified.code, msg: classified.msg, err: err, publicCode: classified.publicCode}, true
	}
	return observabilityWrappedError(exitGeneral, "INTERNAL", strings.TrimSpace(causes[0].Error()), err), true
}

func stableSentinelCLIError(err error) (error, bool) {
	switch {
	case errors.Is(err, coordination.ErrRepositoryIdentityInvalid):
		return observabilityWrappedError(exitUsage, "INVALID_ARGUMENT", "repository identity is invalid", err), true
	case errors.Is(err, coordination.ErrRepositoryBusy):
		return observabilityWrappedError(exitGeneral, publicCodeRepositoryBusy, "repository is busy", err), true
	case errors.Is(err, coordination.ErrRepositoryLockUnsupported):
		return observabilityWrappedError(exitGeneral, publicCodeRepositoryLockUnsupported, "repository locking is unsupported", err), true
	case errors.Is(err, coordination.ErrNestedRepositoryAcquisition):
		return observabilityWrappedError(exitGeneral, "INTERNAL", "repository coordination failed", err), true
	case errors.Is(err, context.Canceled):
		return observabilityWrappedError(exitGeneral, publicCodeCanceled, "operation canceled", err), true
	case errors.Is(err, context.DeadlineExceeded):
		return observabilityWrappedError(exitGeneral, publicCodeDeadlineExceeded, "operation deadline exceeded", err), true
	case errors.Is(err, fs.ErrPermission):
		return observabilityWrappedError(exitGeneral, publicCodePermissionDenied, "permission denied", err), true
	}
	return nil, false
}

func hasStableCLIClassification(err error) bool {
	if err == nil {
		return false
	}
	if stableSentinelCLIClassification(err) {
		return true
	}
	var coordinationFailure *repositoryCoordinationFailure
	return errors.As(err, &coordinationFailure)
}

func stableSentinelCLIClassification(err error) bool {
	_, ok := stableSentinelCLIError(err)
	return ok
}
