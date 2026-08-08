//go:build windows

package coordination

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsNativeLockCreatesPersistentArtifact(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	if _, err := os.Lstat(prepared.LockArtifactPath); !os.IsNotExist(err) {
		t.Fatalf("lock artifact exists before acquisition, stat err=%v", err)
	}

	handle := mustAcquireWindowsNativeLock(t, prepared)
	info, err := os.Lstat(prepared.LockArtifactPath)
	if err != nil {
		t.Fatalf("lstat acquired lock artifact: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("lock artifact mode=%v want regular", info.Mode())
	}
	if err := handle.release(); err != nil {
		t.Fatalf("release native lock: %v", err)
	}
	if info, err := os.Lstat(prepared.LockArtifactPath); err != nil {
		t.Fatalf("persistent lock artifact missing after release: %v", err)
	} else if !info.Mode().IsRegular() {
		t.Fatalf("persistent lock artifact mode=%v want regular", info.Mode())
	}
}

func TestWindowsNativeLockPreservesExistingContents(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	wantContents := []byte("persistent Windows lock artifact contents\r\n")
	if err := os.WriteFile(prepared.LockArtifactPath, wantContents, 0o600); err != nil {
		t.Fatalf("write existing lock artifact: %v", err)
	}

	handle := mustAcquireWindowsNativeLock(t, prepared)
	if err := handle.release(); err != nil {
		t.Fatalf("release native lock: %v", err)
	}
	gotContents, err := os.ReadFile(prepared.LockArtifactPath)
	if err != nil {
		t.Fatalf("read existing lock artifact: %v", err)
	}
	if !bytes.Equal(gotContents, wantContents) {
		t.Fatalf("existing lock artifact contents=%q want=%q", gotContents, wantContents)
	}
}

func TestWindowsNativeLockPreservesEmptyArtifactLength(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	if err := os.WriteFile(prepared.LockArtifactPath, nil, 0o600); err != nil {
		t.Fatalf("create empty lock artifact: %v", err)
	}

	handle := mustAcquireWindowsNativeLock(t, prepared)
	if err := handle.release(); err != nil {
		t.Fatalf("release native lock: %v", err)
	}
	info, err := os.Stat(prepared.LockArtifactPath)
	if err != nil {
		t.Fatalf("stat empty lock artifact: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("empty lock artifact size=%d want=0", info.Size())
	}
}

func TestWindowsNativeLockRejectsDirectoryArtifact(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	if err := os.Mkdir(prepared.LockArtifactPath, 0o700); err != nil {
		t.Fatalf("create lock artifact directory: %v", err)
	}
	if handle, err := acquireNativeLock(prepared); err == nil {
		_ = handle.release()
		t.Fatal("expected directory lock artifact rejection")
	}
	if info, err := os.Lstat(prepared.LockArtifactPath); err != nil {
		t.Fatalf("lstat rejected directory: %v", err)
	} else if !info.IsDir() {
		t.Fatalf("lock artifact mode=%v want directory", info.Mode())
	}
}

func TestWindowsNativeLockRejectsSymlinkArtifact(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	outsidePath := filepath.Join(t.TempDir(), "outside-lock")
	outsideContents := []byte("outside target remains unchanged")
	if err := os.WriteFile(outsidePath, outsideContents, 0o600); err != nil {
		t.Fatalf("write outside target: %v", err)
	}
	if err := os.Symlink(outsidePath, prepared.LockArtifactPath); err != nil {
		t.Skipf("Windows file symlink creation unavailable: %v", err)
	}

	if handle, err := acquireNativeLock(prepared); err == nil {
		_ = handle.release()
		t.Fatal("expected symlink lock artifact rejection")
	}
	info, err := os.Lstat(prepared.LockArtifactPath)
	if err != nil {
		t.Fatalf("lstat rejected symlink: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("lock artifact mode=%v want symlink", info.Mode())
	}
	gotContents, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatalf("read outside target: %v", err)
	}
	if !bytes.Equal(gotContents, outsideContents) {
		t.Fatalf("outside target contents=%q want=%q", gotContents, outsideContents)
	}
}

func TestWindowsNativeLockContentionAndReacquire(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	holder := mustAcquireWindowsNativeLock(t, prepared)

	contender, err := acquireNativeLock(prepared)
	if contender != nil {
		_ = contender.release()
		t.Fatal("contending native acquisition returned a handle")
	}
	if !errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("contending native acquisition error=%v want ErrRepositoryBusy", err)
	}
	if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		t.Fatalf("contending native acquisition error=%v want ERROR_LOCK_VIOLATION", err)
	}

	if err := holder.release(); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	reacquired := mustAcquireWindowsNativeLock(t, prepared)
	if err := reacquired.release(); err != nil {
		t.Fatalf("release reacquired native lock: %v", err)
	}
}

func TestWindowsNativeLockReleaseIsIdempotentAndCannotDamageSuccessor(t *testing.T) {
	const releasers = 32
	prepared := mustPreparedControlNamespace(t)
	first := mustAcquireWindowsNativeLock(t, prepared)
	errorsByRelease := make(chan error, releasers)
	var workers sync.WaitGroup
	workers.Add(releasers)
	for range releasers {
		go func() {
			defer workers.Done()
			errorsByRelease <- first.release()
		}()
	}
	workers.Wait()
	close(errorsByRelease)
	for err := range errorsByRelease {
		if err != nil {
			t.Fatalf("concurrent release: %v", err)
		}
	}

	successor := mustAcquireWindowsNativeLock(t, prepared)
	if err := first.release(); err != nil {
		t.Fatalf("stale release of first native lock: %v", err)
	}
	contender, err := acquireNativeLock(prepared)
	if contender != nil {
		_ = contender.release()
		t.Fatal("stale release unlocked successor")
	}
	if !errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("contention after stale release error=%v want ErrRepositoryBusy", err)
	}
	if err := successor.release(); err != nil {
		t.Fatalf("release successor native lock: %v", err)
	}
}

func TestWindowsNativeLockAllowsDifferentRepositories(t *testing.T) {
	firstPrepared := mustPreparedControlNamespace(t)
	secondPrepared := mustPreparedControlNamespace(t)
	first := mustAcquireWindowsNativeLock(t, firstPrepared)
	second := mustAcquireWindowsNativeLock(t, secondPrepared)
	if err := second.release(); err != nil {
		t.Fatalf("release second repository lock: %v", err)
	}
	if err := first.release(); err != nil {
		t.Fatalf("release first repository lock: %v", err)
	}
}

func TestWindowsNativeLockConcurrentContention(t *testing.T) {
	const competitors = 32
	prepared := mustPreparedControlNamespace(t)
	holder := mustAcquireWindowsNativeLock(t, prepared)
	start := make(chan struct{})
	results := make(chan windowsNativeLockResult, competitors)
	var workers sync.WaitGroup
	workers.Add(competitors)
	for range competitors {
		go func() {
			defer workers.Done()
			<-start
			handle, err := acquireNativeLock(prepared)
			results <- windowsNativeLockResult{handle: handle, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	for result := range results {
		if result.handle != nil {
			_ = result.handle.release()
			t.Fatal("concurrent contender acquired held native lock")
		}
		if !errors.Is(result.err, ErrRepositoryBusy) {
			t.Fatalf("concurrent contention error=%v want ErrRepositoryBusy", result.err)
		}
		if !errors.Is(result.err, windows.ERROR_LOCK_VIOLATION) {
			t.Fatalf("concurrent contention error=%v want ERROR_LOCK_VIOLATION", result.err)
		}
	}
	if err := holder.release(); err != nil {
		t.Fatalf("release contention holder: %v", err)
	}
	reacquired := mustAcquireWindowsNativeLock(t, prepared)
	if err := reacquired.release(); err != nil {
		t.Fatalf("release post-contention native lock: %v", err)
	}
}

func TestWindowsNativeLockErrorMapping(t *testing.T) {
	for name, test := range map[string]struct {
		err     error
		want    error
		notWant []error
	}{
		"busy": {
			err: windows.ERROR_LOCK_VIOLATION, want: ErrRepositoryBusy,
			notWant: []error{ErrRepositoryLockUnsupported},
		},
		"unsupported": {
			err: windows.ERROR_NOT_SUPPORTED, want: ErrRepositoryLockUnsupported,
			notWant: []error{ErrRepositoryBusy},
		},
		"sharing": {
			err: windows.ERROR_SHARING_VIOLATION, want: windows.ERROR_SHARING_VIOLATION,
			notWant: []error{ErrRepositoryBusy, ErrRepositoryLockUnsupported},
		},
		"permission": {
			err: windows.ERROR_ACCESS_DENIED, want: windows.ERROR_ACCESS_DENIED,
			notWant: []error{ErrRepositoryBusy, ErrRepositoryLockUnsupported},
		},
		"unexpected": {
			err: windows.ERROR_INVALID_DATA, want: windows.ERROR_INVALID_DATA,
			notWant: []error{ErrRepositoryBusy, ErrRepositoryLockUnsupported},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := mapWindowsNativeLockError(test.err)
			if !errors.Is(err, test.want) {
				t.Fatalf("mapped error=%v want errors.Is(%v)", err, test.want)
			}
			for _, notWant := range test.notWant {
				if errors.Is(err, notWant) {
					t.Fatalf("mapped error=%v unexpectedly matches %v", err, notWant)
				}
			}
		})
	}
}

func mustAcquireWindowsNativeLock(t *testing.T, prepared PreparedControlNamespace) *nativeLockHandle {
	t.Helper()
	handle, err := acquireNativeLock(prepared)
	if err != nil {
		t.Fatalf("acquireNativeLock: %v", err)
	}
	if handle == nil {
		t.Fatal("acquireNativeLock returned nil handle")
	}
	return handle
}

type windowsNativeLockResult struct {
	handle *nativeLockHandle
	err    error
}
