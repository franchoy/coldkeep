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
	id, info, err := prepareExistingIdentity(ctx, payloadRoot)
	if err != nil {
		return err
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
	control, err := prepareExistingControl(root)
	if err != nil {
		return err
	}
	lockPath := filepath.Join(id.CanonicalPath, ControlDirectoryName, LockArtifactName)
	native, err := prepareExistingNativeLock(lockPath)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, native.release()) }()
	return checkPreparedExisting(ctx, payloadRoot, id, info, control, lockPath, native)
}

func prepareExistingIdentity(ctx context.Context, payloadRoot string) (Identity, os.FileInfo, error) {
	if ctx == nil {
		return Identity{}, nil, ErrRepositoryIdentityInvalid
	}
	if err := ctx.Err(); err != nil {
		return Identity{}, nil, err
	}
	id, err := ResolveIdentity(payloadRoot)
	if err != nil {
		return Identity{}, nil, err
	}
	info, err := os.Lstat(id.CanonicalPath)
	if err != nil || !info.IsDir() {
		return Identity{}, nil, ErrRepositoryIdentityInvalid
	}
	return id, info, nil
}

func prepareExistingControl(root *os.Root) (os.FileInfo, error) {
	if err := root.Mkdir(ControlDirectoryName, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	control, err := root.Lstat(ControlDirectoryName)
	if err != nil || !control.IsDir() {
		return nil, ErrRepositoryIdentityInvalid
	}
	if err = checkExistingPreparationOwner(root); err != nil {
		return nil, err
	}
	return control, nil
}

func checkExistingPreparationOwner(root *os.Root) error {
	owner, err := root.Lstat(filepath.Join(ControlDirectoryName, OwnerMetadataName))
	if err == nil && !owner.Mode().IsRegular() {
		return ErrRepositoryIdentityInvalid
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func prepareExistingNativeLock(lockPath string) (*nativeLockHandle, error) {
	if _, err := os.Lstat(lockPath); err == nil {
		return acquireExistingNativeLockPlatform(lockPath)
	} else if os.IsNotExist(err) {
		return acquireNativeLockPlatform(lockPath)
	} else {
		return nil, err
	}
}

func checkPreparedExisting(ctx context.Context, payloadRoot string, id Identity, info, control os.FileInfo, lockPath string, native *nativeLockHandle) error {
	lock, err := os.Lstat(lockPath)
	if err != nil {
		return err
	}
	if !native.matchesArtifact(lock) {
		return ErrOwnershipLost
	}
	if err = checkExistingPreparationArtifacts(map[string]os.FileInfo{
		id.CanonicalPath: info,
		filepath.Join(id.CanonicalPath, ControlDirectoryName): control,
		lockPath: lock,
	}); err != nil {
		return err
	}
	final, err := ResolveIdentity(payloadRoot)
	if err != nil || final != id {
		return ErrOwnershipLost
	}
	return ctx.Err()
}

func checkExistingPreparationArtifacts(want map[string]os.FileInfo) error {
	for path, expected := range want {
		got, err := os.Lstat(path)
		if err != nil || !os.SameFile(expected, got) || expected.Mode() != got.Mode() {
			return ErrOwnershipLost
		}
	}
	return nil
}
