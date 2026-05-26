package fsx

import "io/fs"

// NoopFS is a behavior-preserving wrapper around an FS.
//
// It exists to provide an explicit seam for later deterministic wrappers while
// preserving the wrapped filesystem's behavior exactly in v1.10.8.
type NoopFS struct {
	base FS
}

// NewNoop returns a behavior-preserving filesystem wrapper around base.
func NewNoop(base FS) NoopFS {
	return NoopFS{base: base}
}

// Open delegates to the wrapped filesystem.
func (n NoopFS) Open(name string) (File, error) {
	return n.base.Open(name)
}

// OpenFile delegates to the wrapped filesystem.
func (n NoopFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	return n.base.OpenFile(name, flag, perm)
}

// Stat delegates to the wrapped filesystem.
func (n NoopFS) Stat(name string) (fs.FileInfo, error) {
	return n.base.Stat(name)
}

// MkdirAll delegates to the wrapped filesystem.
func (n NoopFS) MkdirAll(path string, perm fs.FileMode) error {
	return n.base.MkdirAll(path, perm)
}

// Rename delegates to the wrapped filesystem.
func (n NoopFS) Rename(oldpath string, newpath string) error {
	return n.base.Rename(oldpath, newpath)
}

// Remove delegates to the wrapped filesystem.
func (n NoopFS) Remove(name string) error {
	return n.base.Remove(name)
}

// ReadDir delegates to the wrapped filesystem.
func (n NoopFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return n.base.ReadDir(name)
}

// WalkDir delegates to the wrapped filesystem.
func (n NoopFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	return n.base.WalkDir(root, fn)
}
