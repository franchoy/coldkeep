//go:build linux || darwin

package coordination

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNativeLockCreatesPersistentArtifact(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	if _, err := os.Lstat(prepared.LockArtifactPath); !os.IsNotExist(err) {
		t.Fatalf("lock artifact exists before acquisition, stat err=%v", err)
	}

	handle := mustAcquireNativeLock(t, prepared)
	info, err := os.Lstat(prepared.LockArtifactPath)
	if err != nil {
		t.Fatalf("lstat acquired lock artifact: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("lock artifact mode=%v want regular", info.Mode())
	}
	if mode := info.Mode().Perm(); mode&^os.FileMode(0o600) != 0 {
		t.Fatalf("new lock artifact mode=%#o exceeds requested 0600", mode)
	}
	if _, err := os.Lstat(prepared.OwnerMetadataPath); !os.IsNotExist(err) {
		t.Fatalf("native acquisition created owner metadata, stat err=%v", err)
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

func TestNativeLockPreservesExistingPermissionsAndContents(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	wantContents := []byte("persistent lock artifact contents\n")
	if err := os.WriteFile(prepared.LockArtifactPath, wantContents, 0o600); err != nil {
		t.Fatalf("write existing lock artifact: %v", err)
	}
	if err := os.Chmod(prepared.LockArtifactPath, 0o640); err != nil {
		t.Fatalf("set existing lock artifact mode: %v", err)
	}

	handle := mustAcquireNativeLock(t, prepared)
	if err := handle.release(); err != nil {
		t.Fatalf("release native lock: %v", err)
	}

	info, err := os.Stat(prepared.LockArtifactPath)
	if err != nil {
		t.Fatalf("stat existing lock artifact: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o640 {
		t.Fatalf("existing lock artifact mode=%#o want=0640", mode)
	}
	gotContents, err := os.ReadFile(prepared.LockArtifactPath)
	if err != nil {
		t.Fatalf("read existing lock artifact: %v", err)
	}
	if !bytes.Equal(gotContents, wantContents) {
		t.Fatalf("existing lock artifact contents=%q want=%q", gotContents, wantContents)
	}
}

func TestNativeLockRejectsUnsafeArtifacts(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		prepared := mustPreparedControlNamespace(t)
		outsidePath := t.TempDir() + "/outside-lock"
		outsideContents := []byte("outside target remains unchanged")
		if err := os.WriteFile(outsidePath, outsideContents, 0o600); err != nil {
			t.Fatalf("write outside target: %v", err)
		}
		if err := os.Symlink(outsidePath, prepared.LockArtifactPath); err != nil {
			t.Skipf("symlink creation unavailable: %v", err)
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
	})

	t.Run("directory", func(t *testing.T) {
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
	})

	t.Run("fifo", func(t *testing.T) {
		prepared := mustPreparedControlNamespace(t)
		if err := unix.Mkfifo(prepared.LockArtifactPath, 0o600); err != nil {
			t.Skipf("FIFO creation unavailable: %v", err)
		}
		if handle, err := acquireNativeLock(prepared); err == nil {
			_ = handle.release()
			t.Fatal("expected FIFO lock artifact rejection")
		}
		if info, err := os.Lstat(prepared.LockArtifactPath); err != nil {
			t.Fatalf("lstat rejected FIFO: %v", err)
		} else if info.Mode()&os.ModeNamedPipe == 0 {
			t.Fatalf("lock artifact mode=%v want FIFO", info.Mode())
		}
	})
}

func TestNativeLockContentionAndReacquire(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	holder := mustAcquireNativeLock(t, prepared)

	contender, err := acquireNativeLock(prepared)
	if contender != nil {
		_ = contender.release()
		t.Fatal("contending native acquisition returned a handle")
	}
	if !errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("contending native acquisition error=%v want ErrRepositoryBusy", err)
	}

	if err := holder.release(); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	reacquired := mustAcquireNativeLock(t, prepared)
	if err := reacquired.release(); err != nil {
		t.Fatalf("release reacquired native lock: %v", err)
	}
}

func TestNativeLockReleaseIsIdempotentAndCannotDamageSuccessor(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	first := mustAcquireNativeLock(t, prepared)
	if err := first.release(); err != nil {
		t.Fatalf("release first native lock: %v", err)
	}
	if err := first.release(); err != nil {
		t.Fatalf("second release of first native lock: %v", err)
	}

	successor := mustAcquireNativeLock(t, prepared)
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

func TestNativeLockAllowsDifferentRepositories(t *testing.T) {
	firstPrepared := mustPreparedControlNamespace(t)
	secondPrepared := mustPreparedControlNamespace(t)
	first := mustAcquireNativeLock(t, firstPrepared)
	second := mustAcquireNativeLock(t, secondPrepared)
	if err := second.release(); err != nil {
		t.Fatalf("release second repository lock: %v", err)
	}
	if err := first.release(); err != nil {
		t.Fatalf("release first repository lock: %v", err)
	}
}

func TestNativeLockConcurrentContention(t *testing.T) {
	const competitors = 32
	prepared := mustPreparedControlNamespace(t)
	holder := mustAcquireNativeLock(t, prepared)
	start := make(chan struct{})
	results := make(chan nativeLockResult, competitors)
	var workers sync.WaitGroup
	workers.Add(competitors)
	for range competitors {
		go func() {
			defer workers.Done()
			<-start
			handle, err := acquireNativeLock(prepared)
			results <- nativeLockResult{handle: handle, err: err}
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
	}
	if err := holder.release(); err != nil {
		t.Fatalf("release contention holder: %v", err)
	}
	reacquired := mustAcquireNativeLock(t, prepared)
	if err := reacquired.release(); err != nil {
		t.Fatalf("release post-contention native lock: %v", err)
	}
}

func TestNativeFlockErrorMapping(t *testing.T) {
	for name, test := range map[string]struct {
		err     error
		want    error
		notWant error
	}{
		"busy":        {err: unix.EWOULDBLOCK, want: ErrRepositoryBusy, notWant: ErrRepositoryLockUnsupported},
		"unsupported": {err: unix.ENOSYS, want: ErrRepositoryLockUnsupported, notWant: ErrRepositoryBusy},
		"permission":  {err: unix.EACCES, want: unix.EACCES, notWant: ErrRepositoryLockUnsupported},
		"unexpected":  {err: unix.EIO, want: unix.EIO, notWant: ErrRepositoryLockUnsupported},
	} {
		t.Run(name, func(t *testing.T) {
			err := mapNativeFlockError(test.err)
			if !errors.Is(err, test.want) {
				t.Fatalf("mapped error=%v want errors.Is(%v)", err, test.want)
			}
			if errors.Is(err, test.notWant) {
				t.Fatalf("mapped error=%v unexpectedly matches %v", err, test.notWant)
			}
		})
	}
}

func mustAcquireNativeLock(t *testing.T, prepared PreparedControlNamespace) *nativeLockHandle {
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

type nativeLockResult struct {
	handle *nativeLockHandle
	err    error
}
