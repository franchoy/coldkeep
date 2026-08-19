//go:build windows

package coordination

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

const (
	windowsNativeLockBytesLow  = 1
	windowsNativeLockBytesHigh = 0
)

func acquireNativeLockPlatform(lockPath string) (*nativeLockHandle, error) {
	if err := inspectWindowsNativeLockArtifact(lockPath); err != nil {
		return nil, err
	}

	path, err := windows.UTF16PtrFromString(lockPath)
	if err != nil {
		return nil, fmt.Errorf("coordination: encode native repository lock path: %w", err)
	}
	handle, err := windows.CreateFile(
		path,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_ALWAYS,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("coordination: open native repository lock: %w", err)
	}

	if err := validateWindowsNativeLockHandle(handle); err != nil {
		return nil, closeWindowsNativeLockHandleAfterError(handle, err)
	}

	overlapped := &windows.Overlapped{}
	if err := windows.LockFileEx(
		handle,
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		windowsNativeLockBytesLow,
		windowsNativeLockBytesHigh,
		overlapped,
	); err != nil {
		return nil, closeWindowsNativeLockHandleAfterError(handle, mapWindowsNativeLockError(err))
	}

	return &nativeLockHandle{
		releaseFn: func() error {
			return releaseWindowsNativeLockHandle(handle)
		},
	}, nil
}

func inspectWindowsNativeLockArtifact(lockPath string) error {
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

func validateWindowsNativeLockHandle(handle windows.Handle) error {
	fileType, err := windows.GetFileType(handle)
	if err != nil {
		return fmt.Errorf("coordination: inspect native repository lock type: %w", err)
	}
	if fileType != windows.FILE_TYPE_DISK {
		return fmt.Errorf("coordination: native repository lock must be a disk file")
	}

	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return fmt.Errorf("coordination: inspect native repository lock attributes: %w", err)
	}
	if info.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
		return fmt.Errorf("coordination: native repository lock must be a regular non-reparse file")
	}
	return nil
}

func mapWindowsNativeLockError(err error) error {
	switch {
	case errors.Is(err, windows.ERROR_LOCK_VIOLATION):
		return fmt.Errorf("%w: native LockFileEx: %w", ErrRepositoryBusy, err)
	case errors.Is(err, windows.ERROR_NOT_SUPPORTED):
		return fmt.Errorf("%w: native LockFileEx: %w", ErrRepositoryLockUnsupported, err)
	default:
		return fmt.Errorf("coordination: acquire native repository lock: %w", err)
	}
}

func closeWindowsNativeLockHandleAfterError(handle windows.Handle, primary error) error {
	if closeErr := windows.CloseHandle(handle); closeErr != nil {
		return errors.Join(primary, fmt.Errorf("coordination: close native repository lock after failed acquisition: %w", closeErr))
	}
	return primary
}

func releaseWindowsNativeLockHandle(handle windows.Handle) error {
	var releaseErr error
	overlapped := &windows.Overlapped{}
	if err := windows.UnlockFileEx(
		handle,
		0,
		windowsNativeLockBytesLow,
		windowsNativeLockBytesHigh,
		overlapped,
	); err != nil {
		releaseErr = fmt.Errorf("coordination: unlock native repository lock: %w", err)
	}
	if err := windows.CloseHandle(handle); err != nil {
		releaseErr = errors.Join(releaseErr, fmt.Errorf("coordination: close native repository lock: %w", err))
	}
	return releaseErr
}
