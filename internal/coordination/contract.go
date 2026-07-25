// Package coordination defines the repository coordination contract.
//
// Phase 11 deliberately contains no operating-system lock implementation.
// Native acquisition and CLI integration belong to Phase 12, while independent
// process and live-GC contention proof belongs to Phase 13.
package coordination

import (
	"context"
	"errors"
	"fmt"
)

// Mode identifies a repository coordination compatibility mode.
type Mode string

const (
	// ModeExclusive is the only v1.13.11 coordination mode. Every participating
	// repository operation conflicts with every other participating operation.
	ModeExclusive Mode = "exclusive"
)

// Operation is a stable, path-free identifier used for policy and diagnostics.
type Operation string

const (
	OperationStore           Operation = "store"
	OperationStoreFolder     Operation = "store-folder"
	OperationRestore         Operation = "restore"
	OperationRemove          Operation = "remove"
	OperationRepair          Operation = "repair"
	OperationGarbageCollect  Operation = "gc"
	OperationStats           Operation = "stats"
	OperationInspect         Operation = "inspect"
	OperationList            Operation = "list"
	OperationSearch          Operation = "search"
	OperationVerify          Operation = "verify"
	OperationDoctor          Operation = "doctor"
	OperationConfigGet       Operation = "config-get"
	OperationConfigSet       Operation = "config-set"
	OperationSnapshotCreate  Operation = "snapshot-create"
	OperationSnapshotDelete  Operation = "snapshot-delete"
	OperationSnapshotRestore Operation = "snapshot-restore"
	OperationSnapshotList    Operation = "snapshot-list"
	OperationSnapshotShow    Operation = "snapshot-show"
	OperationSnapshotStats   Operation = "snapshot-stats"
	OperationSnapshotDiff    Operation = "snapshot-diff"
	OperationStartupRecovery Operation = "startup-recovery"
	OperationSchemaBootstrap Operation = "schema-bootstrap"
	OperationSchemaMigration Operation = "schema-migration"
)

// Request describes one fail-fast exclusive acquisition.
type Request struct {
	Operation Operation
	Mode      Mode
	Owner     Owner
}

// Lease is the explicit ownership token returned by a Coordinator.
//
// Release implementations must be idempotent. Finalizers are prohibited;
// callers retain explicit lifecycle ownership.
type Lease interface {
	Release() error
}

// Coordinator acquires one repository-wide lease.
//
// Phase 12 implementations must be fail-fast and non-reentrant. A second
// acquisition of the same identity while held must return
// ErrNestedRepositoryAcquisition rather than reference-counting ownership.
type Coordinator interface {
	Acquire(context.Context, Identity, Request) (Lease, error)
}

// WithLease runs fn while holding a lease and centralizes the required release
// and error-combination lifecycle. It does not implement lock acquisition.
func WithLease(
	ctx context.Context,
	coordinator Coordinator,
	identity Identity,
	request Request,
	fn func() error,
) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if coordinator == nil {
		return fmt.Errorf("coordination: coordinator is required")
	}
	if fn == nil {
		return fmt.Errorf("coordination: operation callback is required")
	}
	if err := ValidateRequest(identity, request); err != nil {
		return err
	}

	lease, err := coordinator.Acquire(ctx, identity, request)
	if err != nil {
		return err
	}
	if lease == nil {
		return fmt.Errorf("coordination: coordinator returned a nil lease")
	}

	defer func() {
		releaseErr := lease.Release()
		switch {
		case err != nil && releaseErr != nil:
			err = errors.Join(err, releaseErr)
		case releaseErr != nil:
			err = releaseErr
		}
	}()

	return fn()
}

// ValidateRequest freezes the relationship between identity, operation, mode,
// and diagnostic owner metadata before native acquisition.
func ValidateRequest(identity Identity, request Request) error {
	if err := ValidateIdentity(identity); err != nil {
		return err
	}
	if request.Mode != ModeExclusive {
		return fmt.Errorf("%w: unsupported mode %q", ErrRepositoryLockUnsupported, request.Mode)
	}
	if !isCanonicalOperation(request.Operation) {
		return fmt.Errorf("coordination: unsupported operation %q", request.Operation)
	}
	if err := ValidateOwner(request.Owner); err != nil {
		return err
	}
	if request.Owner.Operation != request.Operation {
		return fmt.Errorf("coordination: owner operation %q does not match request %q", request.Owner.Operation, request.Operation)
	}
	if request.Owner.Mode != request.Mode {
		return fmt.Errorf("coordination: owner mode %q does not match request %q", request.Owner.Mode, request.Mode)
	}
	if request.Owner.IdentityHash != identity.Hash {
		return fmt.Errorf("coordination: owner identity does not match request identity")
	}
	return nil
}

func isCanonicalOperation(operation Operation) bool {
	switch operation {
	case OperationStore,
		OperationStoreFolder,
		OperationRestore,
		OperationRemove,
		OperationRepair,
		OperationGarbageCollect,
		OperationStats,
		OperationInspect,
		OperationList,
		OperationSearch,
		OperationVerify,
		OperationDoctor,
		OperationConfigGet,
		OperationConfigSet,
		OperationSnapshotCreate,
		OperationSnapshotDelete,
		OperationSnapshotRestore,
		OperationSnapshotList,
		OperationSnapshotShow,
		OperationSnapshotStats,
		OperationSnapshotDiff,
		OperationStartupRecovery,
		OperationSchemaBootstrap,
		OperationSchemaMigration:
		return true
	default:
		return false
	}
}
