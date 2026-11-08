//go:build linux || darwin || windows

package coordination

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestExistingLeasePreparationAndMetadata(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	id, e := ResolveIdentity(root)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = AcquireExisting(context.Background(), id); !errors.Is(e, ErrPreparationRequired) {
		t.Fatal(e)
	}
	if _, e = os.Stat(root); !os.IsNotExist(e) {
		t.Fatal("created missing root")
	}
	if e = os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	if _, e = AcquireExisting(context.Background(), id); !errors.Is(e, ErrPreparationRequired) {
		t.Fatal(e)
	}
	prepared, e := PrepareControlNamespace(root)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = AcquireExisting(context.Background(), id); !errors.Is(e, ErrPreparationRequired) {
		t.Fatal(e)
	}
	if _, e = os.Stat(prepared.LockArtifactPath); !os.IsNotExist(e) {
		t.Fatal("created lock")
	}
	if e = os.WriteFile(prepared.LockArtifactPath, []byte("persistent lock bytes"), 0400); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(prepared.OwnerMetadataPath, []byte("stale owner bytes"), 0600); e != nil {
		t.Fatal(e)
	}
	before, e := os.Stat(prepared.LockArtifactPath)
	if e != nil {
		t.Fatal(e)
	}
	l, e := AcquireExisting(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	if e = l.Check(); e != nil {
		t.Fatal(e)
	}
	if e = l.Release(); e != nil {
		t.Fatal(e)
	}
	if e = l.Release(); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(l.Check(), ErrOwnershipLost) {
		t.Fatal("closed lease reusable")
	}
	after, e := os.Stat(prepared.LockArtifactPath)
	if e != nil {
		t.Fatal(e)
	}
	if !os.SameFile(before, after) {
		t.Fatal("lock replaced")
	}
	for path, want := range map[string]string{prepared.LockArtifactPath: "persistent lock bytes", prepared.OwnerMetadataPath: "stale owner bytes"} {
		b, e := os.ReadFile(path)
		if e != nil || string(b) != want {
			t.Fatalf("changed %s", path)
		}
	}
}

func TestExistingLeaseLegacyContention(t *testing.T) {
	if mode := os.Getenv("CK_EXISTING_CHILD"); mode != "" {
		root := os.Getenv("CK_EXISTING_ROOT")
		id, request := mustProductionRequest(t, root, OperationGarbageCollect, time.Now())
		var l Lease
		var e error
		if mode == "legacy" {
			l, e = NewCoordinator().Acquire(context.Background(), id, request)
		} else {
			l, e = AcquireExisting(context.Background(), id)
		}
		if l != nil {
			_ = l.Release()
		}
		if !errors.Is(e, ErrRepositoryBusy) {
			t.Fatalf("child contention=%v", e)
		}
		return
	}
	root := t.TempDir()
	id, req := mustProductionRequest(t, root, OperationStore, time.Now())
	setup, e := NewCoordinator().Acquire(context.Background(), id, req)
	if e != nil {
		t.Fatal(e)
	}
	if e = setup.Release(); e != nil {
		t.Fatal(e)
	}
	for _, holder := range []string{"existing", "legacy"} {
		t.Run(holder, func(t *testing.T) {
			var l Lease
			var e error
			if holder == "existing" {
				l, e = AcquireExisting(context.Background(), id)
			} else {
				l, e = NewCoordinator().Acquire(context.Background(), id, req)
			}
			if e != nil {
				t.Fatal(e)
			}
			defer func() {
				if e := l.Release(); e != nil {
					t.Fatal(e)
				}
			}()
			if _, e = AcquireExisting(context.Background(), id); !errors.Is(e, ErrNestedRepositoryAcquisition) {
				t.Fatal(e)
			}
			if _, e = NewCoordinator().Acquire(context.Background(), id, req); !errors.Is(e, ErrNestedRepositoryAcquisition) {
				t.Fatal(e)
			}
			alias := filepath.Join(t.TempDir(), "alias")
			if e = os.Symlink(root, alias); e == nil {
				aliasID, e := ResolveIdentity(alias)
				if e != nil {
					t.Fatal(e)
				}
				if aliasID != id {
					t.Fatal("different alias domain")
				}
				if _, e = AcquireExisting(context.Background(), aliasID); !errors.Is(e, ErrNestedRepositoryAcquisition) {
					t.Fatal(e)
				}
			}
			for _, mode := range []string{"existing", "legacy"} {
				cmd := exec.Command(os.Args[0], "-test.run=^TestExistingLeaseLegacyContention$")
				cmd.Env = append(os.Environ(), "CK_EXISTING_CHILD="+mode, "CK_EXISTING_ROOT="+root)
				if out, e := cmd.CombinedOutput(); e != nil {
					t.Fatalf("%s child %v %s", mode, e, out)
				}
			}
		})
	}
}

func TestExistingLeaseUnsafeAndLost(t *testing.T) {
	for _, kind := range []string{"control-symlink", "lock-symlink", "lock-directory", "owner-symlink", "lost"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			p, e := PrepareControlNamespace(root)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(p.LockArtifactPath, nil, 0600); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "control-symlink":
				if e = os.Rename(p.ControlDirectory, p.ControlDirectory+".moved"); e == nil {
					e = os.Symlink(p.ControlDirectory+".moved", p.ControlDirectory)
				}
			case "lock-symlink":
				if e = os.Remove(p.LockArtifactPath); e == nil {
					e = os.Symlink(filepath.Join(t.TempDir(), "other"), p.LockArtifactPath)
				}
			case "lock-directory":
				if e = os.Remove(p.LockArtifactPath); e == nil {
					e = os.Mkdir(p.LockArtifactPath, 0700)
				}
			case "owner-symlink":
				e = os.Symlink(filepath.Join(t.TempDir(), "other"), p.OwnerMetadataPath)
			}
			if e != nil {
				t.Fatal(e)
			}
			l, e := AcquireExisting(context.Background(), p.Identity)
			if kind != "lost" {
				if e == nil {
					_ = l.Release()
					t.Fatal("unsafe artifact admitted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = os.Rename(p.LockArtifactPath, p.LockArtifactPath+".moved"); e != nil {
				_ = l.Release()
				t.Skipf("platform denies rename of locked artifact: %v", e)
			}
			if e = os.WriteFile(p.LockArtifactPath, nil, 0600); e != nil {
				t.Fatal(e)
			}
			if !errors.Is(l.Check(), ErrOwnershipLost) || !errors.Is(l.Release(), ErrOwnershipLost) {
				t.Fatal("lost namespace accepted")
			}
		})
	}
}
