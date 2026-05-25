package maintenance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/fsx"
)

// TestGCDeleteSeamDefaultFSPreservesDeleteBehavior verifies that the default
// OS-backed filesystem seam correctly deletes container files through the GC
// delete path.
func TestGCDeleteSeamDefaultFSPreservesDeleteBehavior(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "dead-default.bin")
	if err := os.WriteFile(path, []byte("phase8-gc-seam-default"), 0600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	removeContainerFileWithFS(fsx.Default(), path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file deleted by default FS seam, stat err=%v", err)
	}
}

// TestGCDeleteSeamNoopFSMatchesDefaultBehavior verifies that wrapping the
// OS-backed filesystem with NoopFS produces byte-identical delete behavior,
// confirming the GC physical-delete seam delegates correctly through the noop
// wrapper.
func TestGCDeleteSeamNoopFSMatchesDefaultBehavior(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "dead-noop.bin")
	if err := os.WriteFile(path, []byte("phase8-gc-seam-noop"), 0600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	removeContainerFileWithFS(fsx.NewNoop(fsx.Default()), path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file deleted by noop FS seam, stat err=%v", err)
	}
}
