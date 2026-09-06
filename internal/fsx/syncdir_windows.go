//go:build windows

package fsx

import "os"

func syncDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{Op: "syncdir", Path: path, Err: os.ErrInvalid}
	}
	// Windows does not provide a portable directory-fsync primitive. The staged
	// file itself is flushed and closed before reaching this compatibility seam.
	return nil
}
