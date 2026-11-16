//go:build linux

package recoveryexport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func watchMutations(t *testing.T, root string) func() bool {
	t.Helper()
	fd, e := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	must(t, e)
	t.Cleanup(func() { must(t, unix.Close(fd)) })
	must(t, filepath.WalkDir(root, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		_, e = unix.InotifyAddWatch(fd, path, unix.IN_CREATE|unix.IN_DELETE|unix.IN_MODIFY|unix.IN_ATTRIB|unix.IN_MOVED_FROM|unix.IN_MOVED_TO|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF)
		return e
	}))
	return func() bool {
		b := make([]byte, 65536)
		n, e := unix.Read(fd, b)
		if e != nil && !errors.Is(e, unix.EAGAIN) {
			t.Fatal(e)
		}
		return n > 0
	}
}

func TestNoTransientSourceMutation(t *testing.T) {
	f := newFixture(t)
	changed := watchMutations(t, f.root)
	r, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, noConsume)
	must(t, e)
	if !r.Complete || changed() {
		t.Fatal("source mutation event during passive inventory")
	}
	// Prove the observer detects create/repair-and-undo, not only final bytes.
	p := filepath.Join(f.root, "transient-probe")
	must(t, os.WriteFile(p, []byte("x"), 0600))
	must(t, os.Remove(p))
	if !changed() {
		t.Fatal("mutation observer missed create-and-undo")
	}
}
