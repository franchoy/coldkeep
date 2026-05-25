package fsx

import (
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
)

func TestOSFSOpenFileWriteReadSyncClose(t *testing.T) {
	t.Parallel()
	runFSReadWriteTest(t, Default(), "coldkeep")
}

func TestOSFSStatMkdirAllRenameRemove(t *testing.T) {
	t.Parallel()
	runFSMetadataTest(t, Default())
}

func TestOSFSReadDirAndWalkDir(t *testing.T) {
	t.Parallel()
	runFSReadDirWalkDirTest(t, Default())
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
