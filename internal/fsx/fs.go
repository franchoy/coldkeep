package fsx

import (
	"io"
	"io/fs"
)

// FS is Coldkeep's minimal filesystem abstraction.
//
// It intentionally covers only the filesystem operations identified by the
// v1.10.8 seam classification work. It is not a general virtual filesystem,
// path policy layer, repository layout abstraction, or fault-injection API.
type FS interface {
	Open(name string) (File, error)
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	Stat(name string) (fs.FileInfo, error)
	MkdirAll(path string, perm fs.FileMode) error
	Rename(oldpath string, newpath string) error
	Remove(name string) error
	ReadDir(name string) ([]fs.DirEntry, error)
	WalkDir(root string, fn fs.WalkDirFunc) error
}

// File is the minimal file handle abstraction required by FS.
//
// The methods mirror the behavior needed by current Coldkeep read, write,
// close, durability-sync, random-read, seek, and truncate paths.
// All methods are satisfied by *os.File.
type File interface {
	io.Reader
	io.Writer
	io.Closer
	io.ReaderAt
	io.Seeker
	Sync() error
	Truncate(size int64) error
}
