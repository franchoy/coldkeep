package coordination

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

// PrepareExisting explicitly prepares coordination for an existing legacy root.
// Stop legacy writers/maintenance first. This is not repair of a lost live lock.
// It never creates the payload root or changes owner metadata or existing locks.
func PrepareExisting(ctx context.Context, payloadRoot string) (err error) {
	if ctx == nil {
		return ErrRepositoryIdentityInvalid
	}
	if err = ctx.Err(); err != nil {
		return
	}
	id, err := ResolveIdentity(payloadRoot)
	if err != nil {
		return err
	}
	info, err := os.Lstat(id.CanonicalPath)
	if err != nil || !info.IsDir() {
		return ErrRepositoryIdentityInvalid
	}
	reservation, err := reserveProductionProcessIdentity(id)
	if err != nil {
		return err
	}
	defer reservation.release()
	root, err := os.OpenRoot(id.CanonicalPath)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	if err = root.Mkdir(ControlDirectoryName, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	control, err := root.Lstat(ControlDirectoryName)
	if err != nil || !control.IsDir() {
		return ErrRepositoryIdentityInvalid
	}
	owner, err := root.Lstat(filepath.Join(ControlDirectoryName, OwnerMetadataName))
	if err == nil && !owner.Mode().IsRegular() {
		return ErrRepositoryIdentityInvalid
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	lockPath := filepath.Join(id.CanonicalPath, ControlDirectoryName, LockArtifactName)
	var native *nativeLockHandle
	if _, e := os.Lstat(lockPath); e == nil {
		native, err = acquireExistingNativeLockPlatform(lockPath)
	} else if os.IsNotExist(e) {
		native, err = acquireNativeLockPlatform(lockPath)
	} else {
		return e
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, native.release()) }()
	lock, err := os.Lstat(lockPath)
	if err != nil {
		return err
	}
	if !native.matchesArtifact(lock) {
		return ErrOwnershipLost
	}
	for path, want := range map[string]os.FileInfo{id.CanonicalPath: info, filepath.Join(id.CanonicalPath, ControlDirectoryName): control, lockPath: lock} {
		got, e := os.Lstat(path)
		if e != nil || !os.SameFile(want, got) || want.Mode() != got.Mode() {
			return ErrOwnershipLost
		}
	}
	final, e := ResolveIdentity(payloadRoot)
	if e != nil || final != id {
		return ErrOwnershipLost
	}
	return ctx.Err()
}
