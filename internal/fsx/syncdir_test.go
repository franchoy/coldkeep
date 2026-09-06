package fsx

import (
	"path/filepath"
	"testing"
)

func TestSyncDirRejectsEmptyAndMissingPaths(t *testing.T) {
	if err := SyncDir(""); err == nil {
		t.Fatal("SyncDir accepted an empty path")
	}
	if err := SyncDir(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("SyncDir accepted a missing directory")
	}
}
