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
	root, id := prepareExistingTestRoot(t)
	lock, owner, before := prepareExistingTestArtifacts(t, root)
	if err := PrepareExisting(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(lock)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("lock replaced")
	}
	for path, want := range map[string]string{lock: "persistent bytes", owner: "diagnostic owner"} {
		b, err := os.ReadFile(path)
		if err != nil || string(b) != want {
			t.Fatal("preparation changed existing bytes")
		}
	}
	checkNestedPreparationRefused(t, root, id)
	for _, kind := range []string{"control-symlink", "lock-symlink", "lock-directory", "owner-symlink"} {
		t.Run(kind, func(t *testing.T) {
			root := unsafePreparationTestRoot(t, kind)
			if err := PrepareExisting(context.Background(), root); err == nil {
				t.Fatal("unsafe preparation admitted")
			}
		})
	}
}

func prepareExistingTestRoot(t *testing.T) (string, Identity) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "legacy")
	if err := PrepareExisting(context.Background(), root); err == nil {
		t.Fatal("created missing root")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("missing root changed")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	id, err := ResolveIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AcquireExisting(context.Background(), id); !errors.Is(err, ErrPreparationRequired) {
		t.Fatal(err)
	}
	return root, id
}

func prepareExistingTestArtifacts(t *testing.T, root string) (string, string, os.FileInfo) {
	t.Helper()
	if err := PrepareExisting(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(root, ControlDirectoryName, LockArtifactName)
	owner := filepath.Join(root, ControlDirectoryName, OwnerMetadataName)
	if err := os.WriteFile(lock, []byte("persistent bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(owner, []byte("diagnostic owner"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(lock)
	if err != nil {
		t.Fatal(err)
	}
	return lock, owner, before
}

func checkNestedPreparationRefused(t *testing.T, root string, id Identity) {
	t.Helper()
	lease, err := AcquireExisting(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err = PrepareExisting(context.Background(), root); !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatal(err)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func unsafePreparationTestRoot(t *testing.T, kind string) string {
	t.Helper()
	root := t.TempDir()
	if err := PrepareExisting(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ControlDirectoryName, LockArtifactName)
	switch kind {
	case "control-symlink":
		path = filepath.Join(root, ControlDirectoryName)
	case "owner-symlink":
		path = filepath.Join(root, ControlDirectoryName, OwnerMetadataName)
	}
	if kind != "owner-symlink" {
		if err := os.Rename(path, path+".saved"); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	if kind == "lock-directory" {
		err = os.Mkdir(path, 0700)
	} else {
		err = os.Symlink(t.TempDir(), path)
	}
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestPrepareExistingChildContention(t *testing.T) {
	if prepareExistingChildMode(t) {
		return
	}
	root := t.TempDir()
	if err := PrepareExisting(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	id, req := mustProductionRequest(t, root, OperationStore, time.Now())
	for _, legacy := range []bool{false, true} {
		var lease Lease
		var err error
		if legacy {
			lease, err = NewCoordinator().Acquire(context.Background(), id, req)
		} else {
			lease, err = AcquireExisting(context.Background(), id)
		}
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPrepareExistingChildContention$")
		cmd.Env = append(os.Environ(), "CK_PREPARE_CHILD_ROOT="+root)
		out, childErr := cmd.CombinedOutput()
		cancel()
		releaseErr := lease.Release()
		checkPreparationChildResult(t, childErr, releaseErr, out)
		t.Logf("preparation child contention passed: legacy=%t", legacy)
	}
	if err := PrepareExisting(context.Background(), root); err != nil {
		t.Fatal(err)
	}
}

func prepareExistingChildMode(t *testing.T) bool {
	t.Helper()
	root := os.Getenv("CK_PREPARE_CHILD_ROOT")
	if root == "" {
		return false
	}
	if err := PrepareExisting(context.Background(), root); !errors.Is(err, ErrRepositoryBusy) {
		t.Fatalf("native preparation contention: %v", err)
	}
	return true
}

func checkPreparationChildResult(t *testing.T, childErr, releaseErr error, out []byte) {
	t.Helper()
	if childErr != nil || releaseErr != nil {
		t.Fatalf("child=%v release=%v %s", childErr, releaseErr, out)
	}
}
