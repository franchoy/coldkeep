package storage

import (
	"path/filepath"
	"testing"
)

// realTempDir resolves OS-managed temp-root symlinks (for example,
// /var -> /private/var on macOS) so success-path restore tests can create
// destinations that are genuinely symlink-free.
func realTempDir(t *testing.T) string {
	t.Helper()

	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp dir symlinks: %v", err)
	}
	return dir
}
