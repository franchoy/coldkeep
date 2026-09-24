package fsx

import (
	"fmt"
	"os"
)

// SyncDir makes completed directory-entry changes durable on platforms that
// expose directory fsync. It is the durability boundary used before a staged
// repair container can become READY.
func SyncDir(path string) error {
	if path == "" {
		return fmt.Errorf("directory path must not be empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{Op: "syncdir", Path: path, Err: os.ErrInvalid}
	}
	return syncDir(path)
}
