package coordination

import "sync"

// nativeLockHandle retains the platform lock resource until release. It is a
// low-level primitive and does not include process reservation or owner data.
type nativeLockHandle struct {
	releaseOnce sync.Once
	releaseFn   func() error
	releaseErr  error
}

func acquireNativeLock(prepared PreparedControlNamespace) (*nativeLockHandle, error) {
	if err := validatePreparedControlNamespace(prepared); err != nil {
		return nil, err
	}
	return acquireNativeLockPlatform(prepared.LockArtifactPath)
}

func (handle *nativeLockHandle) release() error {
	if handle == nil {
		return nil
	}
	handle.releaseOnce.Do(func() {
		if handle.releaseFn != nil {
			handle.releaseErr = handle.releaseFn()
		}
	})
	return handle.releaseErr
}
