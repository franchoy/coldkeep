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

func TestPrepareExisting(t *testing.T) {
	root := filepath.Join(t.TempDir(), "legacy")
	if e := PrepareExisting(context.Background(), root); e == nil {
		t.Fatal("created missing root")
	}
	if _, e := os.Stat(root); !os.IsNotExist(e) {
		t.Fatal("missing root changed")
	}
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	id, e := ResolveIdentity(root)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = AcquireExisting(context.Background(), id); !errors.Is(e, ErrPreparationRequired) {
		t.Fatal(e)
	}
	if e = PrepareExisting(context.Background(), root); e != nil {
		t.Fatal(e)
	}
	lock := filepath.Join(root, ControlDirectoryName, LockArtifactName)
	owner := filepath.Join(root, ControlDirectoryName, OwnerMetadataName)
	if e = os.WriteFile(lock, []byte("persistent bytes"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(owner, []byte("diagnostic owner"), 0600); e != nil {
		t.Fatal(e)
	}
	before, e := os.Stat(lock)
	if e != nil {
		t.Fatal(e)
	}
	if e = PrepareExisting(context.Background(), root); e != nil {
		t.Fatal(e)
	}
	after, e := os.Stat(lock)
	if e != nil || !os.SameFile(before, after) {
		t.Fatal("lock replaced")
	}
	for path, want := range map[string]string{lock: "persistent bytes", owner: "diagnostic owner"} {
		b, e := os.ReadFile(path)
		if e != nil || string(b) != want {
			t.Fatal("preparation changed existing bytes")
		}
	}
	l, e := AcquireExisting(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	if e = PrepareExisting(context.Background(), root); !errors.Is(e, ErrNestedRepositoryAcquisition) {
		t.Fatal(e)
	}
	if e = l.Release(); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"control-symlink", "lock-symlink", "lock-directory", "owner-symlink"} {
		t.Run(kind, func(t *testing.T) {
			r := t.TempDir()
			if e = PrepareExisting(context.Background(), r); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(r, ControlDirectoryName, LockArtifactName)
			switch kind {
			case "control-symlink":
				path = filepath.Join(r, ControlDirectoryName)
			case "owner-symlink":
				path = filepath.Join(r, ControlDirectoryName, OwnerMetadataName)
			}
			if kind != "owner-symlink" {
				if e = os.Rename(path, path+".saved"); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "lock-directory" {
				e = os.Mkdir(path, 0700)
			} else {
				e = os.Symlink(t.TempDir(), path)
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = PrepareExisting(context.Background(), r); e == nil {
				t.Fatal("unsafe preparation admitted")
			}
		})
	}
}

func TestPrepareExistingChildContention(t *testing.T) {
	if root := os.Getenv("CK_PREPARE_CHILD_ROOT"); root != "" {
		if e := PrepareExisting(context.Background(), root); !errors.Is(e, ErrRepositoryBusy) {
			t.Fatalf("native preparation contention: %v", e)
		}
		return
	}
	root := t.TempDir()
	if e := PrepareExisting(context.Background(), root); e != nil {
		t.Fatal(e)
	}
	id, req := mustProductionRequest(t, root, OperationStore, time.Now())
	for _, legacy := range []bool{false, true} {
		var l Lease
		var e error
		if legacy {
			l, e = NewCoordinator().Acquire(context.Background(), id, req)
		} else {
			l, e = AcquireExisting(context.Background(), id)
		}
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPrepareExistingChildContention$")
		cmd.Env = append(os.Environ(), "CK_PREPARE_CHILD_ROOT="+root)
		out, childErr := cmd.CombinedOutput()
		cancel()
		releaseErr := l.Release()
		if childErr != nil || releaseErr != nil {
			t.Fatalf("child=%v release=%v %s", childErr, releaseErr, out)
		}
	}
	if e := PrepareExisting(context.Background(), root); e != nil {
		t.Fatal(e)
	}
}
