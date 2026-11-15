package coordination

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareExistingContextAdmission(t *testing.T) {
	for _, name := range []string{"nil", "canceled-existing", "canceled-missing"} {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			root := parent
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			want := context.Canceled
			if name == "nil" {
				ctx, want = nil, ErrRepositoryIdentityInvalid
			}
			if name == "canceled-missing" {
				root = filepath.Join(parent, "missing")
			}
			if err := PrepareExisting(ctx, root); !errors.Is(err, want) {
				t.Fatalf("context admission: got %v, want %v", err, want)
			}
			entries, err := os.ReadDir(parent)
			if err != nil || len(entries) != 0 {
				t.Fatalf("context refusal changed owned root: %v, %v", entries, err)
			}
		})
	}
}

func TestPrepareExistingRefusalCleanup(t *testing.T) {
	for _, kind := range []string{"control-symlink", "owner-symlink"} {
		t.Run(kind, func(t *testing.T) {
			root := unsafePreparationTestRoot(t, kind)
			if err := PrepareExisting(context.Background(), root); !errors.Is(err, ErrRepositoryIdentityInvalid) {
				t.Fatalf("unsafe fixture refusal: %v", err)
			}
			restorePreparationTestFixture(t, root, kind)
			if err := PrepareExisting(context.Background(), root); err != nil {
				t.Fatalf("prepare after owned fixture correction: %v", err)
			}
			checkPreparationReacquisition(t, root)
		})
	}
}

func restorePreparationTestFixture(t *testing.T, root, kind string) {
	t.Helper()
	path := filepath.Join(root, ControlDirectoryName)
	if kind == "owner-symlink" {
		path = filepath.Join(path, OwnerMetadataName)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if kind == "control-symlink" {
		if err := os.Rename(path+".saved", path); err != nil {
			t.Fatal(err)
		}
	}
}

// Cancellation follows actual artifact creation, independently of Err call count.
type preparationArtifactContext struct {
	context.Context
	cancel   context.CancelFunc
	lockPath string
}

func (c preparationArtifactContext) Err() error {
	if _, err := os.Lstat(c.lockPath); err == nil {
		c.cancel()
	}
	return c.Context.Err()
}

func TestPrepareExistingFinalCancellationReleasesOwnership(t *testing.T) {
	root := t.TempDir()
	lock := filepath.Join(root, ControlDirectoryName, LockArtifactName)
	if _, err := os.Lstat(lock); !os.IsNotExist(err) {
		t.Fatalf("fixture must start without a lock: %v", err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := preparationArtifactContext{Context: parent, cancel: cancel, lockPath: lock}
	if err := PrepareExisting(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("final cancellation: %v", err)
	}
	before, err := os.Lstat(lock)
	if err != nil || !before.Mode().IsRegular() {
		t.Fatalf("persistent lock missing after cancellation: %v", err)
	}
	checkPreparationReacquisition(t, root)
	after, err := os.Lstat(lock)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("reacquisition replaced the persistent lock: %v", err)
	}
}

func checkPreparationReacquisition(t *testing.T, root string) {
	t.Helper()
	id, err := ResolveIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := AcquireExisting(context.Background(), id)
	if err != nil {
		t.Fatalf("ownership retained after preparation refusal: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}
