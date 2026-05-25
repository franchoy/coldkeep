package fsx

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func TestOSFSOpenFileWriteReadSyncClose(t *testing.T) {
	t.Parallel()

	fsys := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile create failed: %v", err)
	}

	written, err := f.Write([]byte("coldkeep"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if written != len("coldkeep") {
		t.Fatalf("Write length mismatch: got %d want %d", written, len("coldkeep"))
	}

	if err := f.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := fsys.Open(path)
	if err != nil {
		t.Fatalf("Open read failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Fatalf("Close read handle failed: %v", err)
		}
	}()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(got) != "coldkeep" {
		t.Fatalf("content mismatch: got %q want %q", string(got), "coldkeep")
	}
}

func TestOSFSStatMkdirAllRenameRemove(t *testing.T) {
	t.Parallel()

	fsys := Default()
	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b")
	src := filepath.Join(nested, "source.txt")
	dst := filepath.Join(nested, "dest.txt")

	if err := fsys.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	f, err := fsys.OpenFile(src, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile source failed: %v", err)
	}
	if _, err := f.Write([]byte("payload")); err != nil {
		t.Fatalf("Write source failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close source failed: %v", err)
	}

	info, err := fsys.Stat(src)
	if err != nil {
		t.Fatalf("Stat source failed: %v", err)
	}
	if info.Size() != int64(len("payload")) {
		t.Fatalf("Stat size mismatch: got %d want %d", info.Size(), len("payload"))
	}

	if err := fsys.Rename(src, dst); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if _, err := fsys.Stat(src); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat old path error mismatch: got %v want fs.ErrNotExist", err)
	}
	if _, err := fsys.Stat(dst); err != nil {
		t.Fatalf("Stat destination failed: %v", err)
	}

	if err := fsys.Remove(dst); err != nil {
		t.Fatalf("Remove destination failed: %v", err)
	}
	if _, err := fsys.Stat(dst); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat removed path error mismatch: got %v want fs.ErrNotExist", err)
	}
}

func TestOSFSReadDirAndWalkDir(t *testing.T) {
	t.Parallel()

	fsys := Default()
	dir := t.TempDir()

	for _, rel := range []string{
		"root.txt",
		filepath.Join("sub", "child.txt"),
	} {
		path := filepath.Join(dir, rel)
		if err := fsys.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll for %s failed: %v", rel, err)
		}
		f, err := fsys.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatalf("OpenFile for %s failed: %v", rel, err)
		}
		if _, err := f.Write([]byte(rel)); err != nil {
			t.Fatalf("Write for %s failed: %v", rel, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close for %s failed: %v", rel, err)
		}
	}

	entries, err := fsys.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	slices.Sort(names)

	if !reflect.DeepEqual(names, []string{"root.txt", "sub"}) {
		t.Fatalf("ReadDir names mismatch: got %#v", names)
	}

	var walked []string
	err = fsys.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		walked = append(walked, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir failed: %v", err)
	}

	slices.Sort(walked)
	want := []string{
		".",
		"root.txt",
		"sub",
		filepath.Join("sub", "child.txt"),
	}
	slices.Sort(want)

	if !reflect.DeepEqual(walked, want) {
		t.Fatalf("WalkDir paths mismatch:\n got: %#v\nwant: %#v", walked, want)
	}
}

func TestOSFSPreservesMissingPathErrors(t *testing.T) {
	t.Parallel()

	fsys := Default()
	missing := filepath.Join(t.TempDir(), "missing")

	if _, err := fsys.Open(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open missing error mismatch: got %v want fs.ErrNotExist", err)
	}
	if _, err := fsys.Stat(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat missing error mismatch: got %v want fs.ErrNotExist", err)
	}
	if err := fsys.Remove(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Remove missing error mismatch: got %v want fs.ErrNotExist", err)
	}
	if _, err := fsys.ReadDir(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadDir missing error mismatch: got %v want fs.ErrNotExist", err)
	}
	if err := fsys.WalkDir(missing, func(_ string, _ fs.DirEntry, err error) error {
		return err
	}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("WalkDir missing error mismatch: got %v want fs.ErrNotExist", err)
	}
}
