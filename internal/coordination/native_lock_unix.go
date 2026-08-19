//go:build linux || darwin

package coordination

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func acquireNativeLockPlatform(lockPath string) (*nativeLockHandle, error) {
	if err := inspectNativeLockArtifact(lockPath); err != nil {
		return nil, err
	}

	fd, err := unix.Open(lockPath, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("coordination: open native repository lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), LockArtifactName)
	if file == nil {
		primary := fmt.Errorf("coordination: create native repository lock file handle")
		if closeErr := unix.Close(fd); closeErr != nil {
			primary = errors.Join(primary, fmt.Errorf("coordination: close native repository lock descriptor: %w", closeErr))
		}
		return nil, primary
	}

	info, err := file.Stat()
	if err != nil {
		return nil, closeNativeLockFileAfterError(file, fmt.Errorf("coordination: inspect opened native repository lock: %w", err))
	}
	if !info.Mode().IsRegular() {
		return nil, closeNativeLockFileAfterError(file, fmt.Errorf("coordination: native repository lock must be a regular file"))
	}

	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, closeNativeLockFileAfterError(file, mapNativeFlockError(err))
	}

	return &nativeLockHandle{
		releaseFn: func() error {
			return releaseNativeLockFile(file, fd)
		},
	}, nil
}

func inspectNativeLockArtifact(lockPath string) error {
	info, err := os.Lstat(lockPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("coordination: inspect native repository lock: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("coordination: native repository lock must be a regular file")
	}
	return nil
}

func mapNativeFlockError(err error) error {
	switch {
	case errors.Is(err, unix.EWOULDBLOCK), errors.Is(err, unix.EAGAIN):
		return fmt.Errorf("%w: native flock: %w", ErrRepositoryBusy, err)
	case errors.Is(err, unix.ENOSYS), errors.Is(err, unix.ENOTSUP), errors.Is(err, unix.EOPNOTSUPP):
		return fmt.Errorf("%w: native flock: %w", ErrRepositoryLockUnsupported, err)
	default:
		return fmt.Errorf("coordination: acquire native repository lock: %w", err)
	}
}

func closeNativeLockFileAfterError(file *os.File, primary error) error {
	if closeErr := file.Close(); closeErr != nil {
		return errors.Join(primary, fmt.Errorf("coordination: close native repository lock after failed acquisition: %w", closeErr))
	}
	return primary
}

func releaseNativeLockFile(file *os.File, fd int) error {
	var releaseErr error
	if err := unix.Flock(fd, unix.LOCK_UN); err != nil {
		releaseErr = fmt.Errorf("coordination: unlock native repository lock: %w", err)
	}
	if err := file.Close(); err != nil {
		releaseErr = errors.Join(releaseErr, fmt.Errorf("coordination: close native repository lock: %w", err))
	}
	return releaseErr
}
