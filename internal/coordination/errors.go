package coordination

import "errors"

var (
	// ErrRepositoryBusy reports that another holder owns the repository lease.
	ErrRepositoryBusy = errors.New("repository is busy")
	// ErrRepositoryLockUnsupported reports that repository coordination is not
	// supported by the active platform or filesystem.
	ErrRepositoryLockUnsupported = errors.New("repository coordination is unsupported")
	// ErrRepositoryIdentityInvalid reports an invalid repository namespace.
	ErrRepositoryIdentityInvalid = errors.New("repository identity is invalid")
	// ErrNestedRepositoryAcquisition reports a repeated acquisition of an
	// already-held repository identity.
	ErrNestedRepositoryAcquisition = errors.New("nested repository acquisition is not allowed")
)
