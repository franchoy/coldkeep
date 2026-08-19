//go:build !linux && !darwin && !windows

package coordination

import "fmt"

func acquireNativeLockPlatform(string) (*nativeLockHandle, error) {
	return nil, fmt.Errorf("%w: native repository locking is unavailable on this platform", ErrRepositoryLockUnsupported)
}
