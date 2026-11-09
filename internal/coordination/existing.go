package coordination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var ErrPreparationRequired = errors.New("coordination preparation required")
var ErrOwnershipLost = errors.New("repository ownership lost")

// ExistingLease shares the legacy native lock and process registry. It never
// prepares artifacts or changes diagnostic owner metadata. Check detects local
// namespace substitution; this remains cooperative exclusion, not fencing
// against arbitrary filesystem/SQL writers.
type ExistingLease interface {
	Lease
	Check() error
}

type existingLease struct {
	mu          sync.Mutex
	identity    Identity
	paths       []string
	infos       []os.FileInfo
	native      *nativeLockHandle
	reservation processReservationResource
	closed      bool
	lost        bool
	releaseErr  error
}

// AcquireExisting refuses an unprepared repository without creating anything.
// Its caller must close all operation resources before Release.
func AcquireExisting(ctx context.Context, identity Identity) (ExistingLease, error) {
	if ctx == nil {
		return nil, errors.New("coordination: context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateIdentity(identity); err != nil {
		return nil, err
	}
	control := filepath.Join(identity.CanonicalPath, ControlDirectoryName)
	paths := []string{identity.CanonicalPath, control, filepath.Join(control, LockArtifactName)}
	infos := make([]os.FileInfo, 0, len(paths))
	for i, path := range paths {
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			return nil, ErrPreparationRequired
		}
		if err != nil {
			return nil, fmt.Errorf("%w: inspect existing coordination material", ErrRepositoryIdentityInvalid)
		}
		if info.Mode()&os.ModeSymlink != 0 || (i < 2 && !info.IsDir()) || (i == 2 && !info.Mode().IsRegular()) {
			return nil, fmt.Errorf("%w: unsafe existing coordination material", ErrRepositoryIdentityInvalid)
		}
		infos = append(infos, info)
	}
	if info, err := os.Lstat(filepath.Join(control, OwnerMetadataName)); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: unsafe owner metadata", ErrRepositoryIdentityInvalid)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	reservation, err := reserveProductionProcessIdentity(identity)
	if err != nil {
		return nil, err
	}
	native, err := acquireExistingNativeLockPlatform(paths[2])
	if err != nil {
		reservation.release()
		return nil, err
	}
	l := &existingLease{identity: identity, paths: paths, infos: infos, native: native, reservation: reservation}
	if native.matchesArtifact != nil && !native.matchesArtifact(infos[2]) {
		return nil, errors.Join(ErrOwnershipLost, l.Release())
	}
	if err := errors.Join(ctx.Err(), l.Check()); err != nil {
		return nil, errors.Join(err, l.Release())
	}
	return l, nil
}

func (l *existingLease) check() error {
	if l.closed || l.lost {
		return ErrOwnershipLost
	}
	for i, path := range l.paths {
		info, err := os.Lstat(path)
		if err != nil || !os.SameFile(l.infos[i], info) || info.Mode() != l.infos[i].Mode() {
			l.lost = true
			return ErrOwnershipLost
		}
	}
	return nil
}

func (l *existingLease) Check() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.check()
}

func (l *existingLease) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.releaseErr = errors.Join(l.check(), l.native.release())
		l.reservation.release()
		l.closed = true
	}
	return l.releaseErr
}
