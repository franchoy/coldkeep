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

// mustNoErr fails the test immediately if err is non-nil.
func mustNoErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// runFSReadWriteTest exercises OpenFile/Write/Sync/Close/Open/ReadAll on fsys.
func runFSReadWriteTest(t *testing.T, fsys FS, content string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	mustNoErr(t, err, "OpenFile create")

	written, err := f.Write([]byte(content))
	mustNoErr(t, err, "Write")
	if written != len(content) {
		t.Fatalf("Write length mismatch: got %d want %d", written, len(content))
	}

	mustNoErr(t, f.Sync(), "Sync")
	mustNoErr(t, f.Close(), "Close write handle")

	r, err := fsys.Open(path)
	mustNoErr(t, err, "Open read")
	defer func() {
		if err := r.Close(); err != nil {
			t.Fatalf("Close read handle: %v", err)
		}
	}()

	got, err := io.ReadAll(r)
	mustNoErr(t, err, "ReadAll")
	if string(got) != content {
		t.Fatalf("content mismatch: got %q want %q", string(got), content)
	}
}

// runFSMetadataTest exercises MkdirAll/Stat/Rename/Remove on fsys.
func runFSMetadataTest(t *testing.T, fsys FS) {
	t.Helper()
	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b")
	src := filepath.Join(nested, "source.txt")
	dst := filepath.Join(nested, "dest.txt")

	mustNoErr(t, fsys.MkdirAll(nested, 0o755), "MkdirAll")

	f, err := fsys.OpenFile(src, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	mustNoErr(t, err, "OpenFile source")
	_, err = f.Write([]byte("payload"))
	mustNoErr(t, err, "Write source")
	mustNoErr(t, f.Close(), "Close source")

	info, err := fsys.Stat(src)
	mustNoErr(t, err, "Stat source")
	if info.Size() != int64(len("payload")) {
		t.Fatalf("Stat size mismatch: got %d want %d", info.Size(), len("payload"))
	}

	mustNoErr(t, fsys.Rename(src, dst), "Rename")

	if _, err := fsys.Stat(src); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat old path after rename: got %v want fs.ErrNotExist", err)
	}
	_, err = fsys.Stat(dst)
	mustNoErr(t, err, "Stat destination after rename")

	mustNoErr(t, fsys.Remove(dst), "Remove")

	if _, err := fsys.Stat(dst); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat removed path: got %v want fs.ErrNotExist", err)
	}
}

// runFSReadDirWalkDirTest exercises ReadDir and WalkDir on fsys.
func runFSReadDirWalkDirTest(t *testing.T, fsys FS) {
	t.Helper()
	dir := t.TempDir()

	for _, rel := range []string{"root.txt", filepath.Join("sub", "child.txt")} {
		path := filepath.Join(dir, rel)
		mustNoErr(t, fsys.MkdirAll(filepath.Dir(path), 0o755), "MkdirAll for "+rel)
		f, err := fsys.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		mustNoErr(t, err, "OpenFile for "+rel)
		_, err = f.Write([]byte(rel))
		mustNoErr(t, err, "Write for "+rel)
		mustNoErr(t, f.Close(), "Close for "+rel)
	}

	entries, err := fsys.ReadDir(dir)
	mustNoErr(t, err, "ReadDir")

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	if !reflect.DeepEqual(names, []string{"root.txt", "sub"}) {
		t.Fatalf("ReadDir names mismatch: got %#v", names)
	}

	var walked []string
	err = fsys.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(dir, path)
		if relErr != nil {
			return relErr
		}
		walked = append(walked, rel)
		return nil
	})
	mustNoErr(t, err, "WalkDir")

	slices.Sort(walked)
	want := []string{".", "root.txt", "sub", filepath.Join("sub", "child.txt")}
	slices.Sort(want)
	if !reflect.DeepEqual(walked, want) {
		t.Fatalf("WalkDir paths mismatch:\n got: %#v\nwant: %#v", walked, want)
	}
}
