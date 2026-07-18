package storage

import (
	"testing"
)

// realTempDir preserves the native lexical temp path form so restore tests
// exercise real-world outer aliases such as macOS /var -> /private/var.
func realTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}
