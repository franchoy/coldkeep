//go:build linux || darwin

package coordination

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
)

func TestNativeLockExistingPreservesReadOnlyArtifact(t *testing.T) {
	prepared, before, contents := nativeLockExistingReadOnlyFixture(t)
	handle := mustAcquireNativeExistingQuality(t, prepared, before)
	if err := handle.release(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(prepared.LockArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || before.Mode() != after.Mode() {
		t.Fatal("existing-only acquisition changed the persistent inode or mode")
	}
	got, err := os.ReadFile(prepared.LockArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, contents) {
		t.Fatalf("existing-only acquisition changed bytes: %q", got)
	}
}

func TestNativeLockExistingMissingRefusesCreation(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	handle, err := acquireExistingNativeLockPlatform(prepared.LockArtifactPath)
	if handle != nil {
		_ = handle.release()
		t.Fatal("missing existing-only lock returned ownership")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing existing-only lock: %v", err)
	}
	if _, err := os.Lstat(prepared.LockArtifactPath); !os.IsNotExist(err) {
		t.Fatalf("existing-only acquisition created lock: %v", err)
	}
	if _, err := os.Lstat(prepared.OwnerMetadataPath); !os.IsNotExist(err) {
		t.Fatalf("existing-only acquisition created owner metadata: %v", err)
	}
}

func TestNativeLockExistingContentionAndReacquire(t *testing.T) {
	prepared, before, _ := nativeLockExistingReadOnlyFixture(t)
	holder := mustAcquireNativeExistingQuality(t, prepared, before)
	contender, err := acquireExistingNativeLockPlatform(prepared.LockArtifactPath)
	if contender != nil {
		_ = contender.release()
		t.Fatal("existing-only contender acquired held lock")
	}
	if !errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("existing-only contention: %v", err)
	}
	if err := holder.release(); err != nil {
		t.Fatal(err)
	}
	successor := mustAcquireNativeExistingQuality(t, prepared, before)
	if err := successor.release(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeLockExistingLeaseCancellationAndLoss(t *testing.T) {
	_, prepared := existingLeaseTestControl(t)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := existingLeaseContentionContext{
		Context: parent, cancel: cancel, lockPath: prepared.LockArtifactPath,
		beforeCancel: func() error { return os.Chmod(prepared.LockArtifactPath, 0o400) },
	}
	lease, err := AcquireExisting(ctx, prepared.Identity)
	if lease != nil {
		_ = lease.Release()
		t.Fatal("canceled lost lease returned ownership")
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("final cancellation must retain physical ownership loss: %v", err)
	}
	checkExistingLeaseReacquires(t, prepared.Identity)
}

func nativeLockExistingReadOnlyFixture(t *testing.T) (PreparedControlNamespace, os.FileInfo, []byte) {
	t.Helper()
	prepared := mustPreparedControlNamespace(t)
	contents := []byte("read-only persistent native lock\n")
	if err := os.WriteFile(prepared.LockArtifactPath, contents, 0o400); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(prepared.LockArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode().Perm() != 0o400 {
		t.Fatalf("fixture mode: %v", before.Mode())
	}
	return prepared, before, contents
}

func mustAcquireNativeExistingQuality(t *testing.T, prepared PreparedControlNamespace, before os.FileInfo) *nativeLockHandle {
	t.Helper()
	handle, err := acquireExistingNativeLockPlatform(prepared.LockArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.release() })
	if handle.matchesArtifact == nil || !handle.matchesArtifact(before) {
		t.Fatal("existing-only handle did not retain the same persistent inode")
	}
	return handle
}
