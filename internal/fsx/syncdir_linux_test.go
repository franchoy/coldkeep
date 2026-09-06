//go:build linux

package fsx

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncDirLinuxFlushesDirectoryAndRejectsRegularFile(t *testing.T) {
	dir := t.TempDir()
	if err := SyncDir(dir); err != nil {
		t.Fatalf("sync directory: %v", err)
	}
	path := filepath.Join(dir, "file")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := SyncDir(path); err == nil {
		t.Fatal("directory sync accepted a regular file")
	}
}
