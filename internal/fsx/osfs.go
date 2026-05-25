package fsx

import (
	"io/fs"
	"os"
	"path/filepath"
)

// OSFS is an FS implementation backed directly by the local operating system.
//
// It must preserve standard library behavior exactly. It must not rewrite paths,
// retry operations, normalize errors, swallow errors, or add fault-injection
// behavior.
type OSFS struct{}

// Default returns the default OS-backed filesystem implementation.
func Default() OSFS {
	return OSFS{}
}

// Open delegates to os.Open.
func (OSFS) Open(name string) (File, error) {
	return os.Open(name) // #nosec G304 -- intentional OS pass-through; caller is responsible for path safety
}

// OpenFile delegates to os.OpenFile.
func (OSFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm) // #nosec G304 -- intentional OS pass-through; caller is responsible for path safety
}

// Stat delegates to os.Stat.
func (OSFS) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

// MkdirAll delegates to os.MkdirAll.
func (OSFS) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}

// Rename delegates to os.Rename.
func (OSFS) Rename(oldpath string, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// Remove delegates to os.Remove.
func (OSFS) Remove(name string) error {
	return os.Remove(name)
}

// ReadDir delegates to os.ReadDir.
func (OSFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

// WalkDir delegates to filepath.WalkDir.
func (OSFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	return filepath.WalkDir(root, fn)
}
