//go:build linux || darwin || windows

package coordination

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExistingLeaseContextAdmission(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, name := range []string{"nil-invalid", "canceled-invalid", "canceled-missing", "canceled-unprepared"} {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			identity := existingLeaseAdmissionIdentity(t, parent, name)
			ctx, want := canceled, context.Canceled
			if name == "nil-invalid" {
				ctx, want = nil, nil
			}
			lease, err := AcquireExisting(ctx, identity)
			checkExistingLeaseAdmissionError(t, lease, err, want)
			entries, err := os.ReadDir(parent)
			if err != nil || len(entries) != 0 {
				t.Fatalf("context refusal changed owned root: %v, %v", entries, err)
			}
		})
	}
}

func existingLeaseAdmissionIdentity(t *testing.T, parent, name string) Identity {
	t.Helper()
	switch name {
	case "nil-invalid", "canceled-invalid":
		return Identity{CanonicalPath: parent}
	case "canceled-missing":
		parent = filepath.Join(parent, "missing")
	}
	identity, err := ResolveIdentity(parent)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func checkExistingLeaseAdmissionError(t *testing.T, lease ExistingLease, err, want error) {
	t.Helper()
	if lease != nil {
		_ = lease.Release()
		t.Fatal("context refusal returned ownership")
	}
	if want == nil {
		if err == nil || err.Error() != "coordination: context required" {
			t.Fatalf("nil context error: %v", err)
		}
		if errors.Is(err, ErrRepositoryIdentityInvalid) {
			t.Fatal("nil context was converted to an identity error")
		}
		return
	}
	if !errors.Is(err, want) {
		t.Fatalf("context admission: got %v, want %v", err, want)
	}
}

func TestExistingLeaseNativeFailureReleasesReservation(t *testing.T) {
	_, prepared := existingLeaseTestControl(t)
	holder, err := acquireExistingNativeLockPlatform(prepared.LockArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.release() })
	lease, err := AcquireExisting(context.Background(), prepared.Identity)
	if lease != nil {
		_ = lease.Release()
		t.Fatal("native contention returned ownership")
	}
	if !errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("native acquisition failure: %v", err)
	}
	if err := holder.release(); err != nil {
		t.Fatal(err)
	}
	checkExistingLeaseReacquires(t, prepared.Identity)
}

// Observe actual native ownership, independently of the number of Err calls.
type existingLeaseContentionContext struct {
	context.Context
	cancel       context.CancelFunc
	lockPath     string
	beforeCancel func() error
}

func (c existingLeaseContentionContext) Err() error {
	handle, err := acquireExistingNativeLockPlatform(c.lockPath)
	if errors.Is(err, ErrRepositoryBusy) {
		var changeErr error
		if c.beforeCancel != nil {
			changeErr = c.beforeCancel()
		}
		c.cancel()
		return errors.Join(c.Context.Err(), changeErr)
	}
	if err != nil {
		return err
	}
	return errors.Join(c.Context.Err(), handle.release())
}

func TestExistingLeaseFinalCancellationReleasesOwnership(t *testing.T) {
	_, prepared := existingLeaseTestControl(t)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := existingLeaseContentionContext{Context: parent, cancel: cancel, lockPath: prepared.LockArtifactPath}
	lease, err := AcquireExisting(ctx, prepared.Identity)
	if lease != nil {
		_ = lease.Release()
		t.Fatal("final cancellation returned ownership")
	}
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("final cancellation of unchanged lock: %v", err)
	}
	checkExistingLeaseReacquires(t, prepared.Identity)
}
