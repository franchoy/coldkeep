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

// TestGCFilesystemEquivalenceDefaultAndNoop is a head-to-head equivalence
// test: it creates two files in the same temp dir, deletes one via
// fsx.Default() and the other via fsx.NewNoop(fsx.Default()), then asserts
// both are gone, proving the GC physical-delete seam is behavior-preserving
// regardless of which FS implementation backs it.
func TestGCFilesystemEquivalenceDefaultAndNoop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	pathDefault := filepath.Join(dir, "dead-equiv-default.bin")
	if err := os.WriteFile(pathDefault, []byte("phase9-gc-equivalence-default"), 0600); err != nil {
		t.Fatalf("write file (default): %v", err)
	}

	pathNoop := filepath.Join(dir, "dead-equiv-noop.bin")
	if err := os.WriteFile(pathNoop, []byte("phase9-gc-equivalence-noop"), 0600); err != nil {
		t.Fatalf("write file (noop): %v", err)
	}

	removeContainerFileWithFS(fsx.Default(), pathDefault)
	removeContainerFileWithFS(fsx.NewNoop(fsx.Default()), pathNoop)

	// head-to-head equivalence: both seam implementations must produce the
	// same observable outcome (file absent).
	if _, errD := os.Stat(pathDefault); !os.IsNotExist(errD) {
		t.Fatalf("default FS seam: expected file gone, stat err=%v", errD)
	}
	if _, errN := os.Stat(pathNoop); !os.IsNotExist(errN) {
		t.Fatalf("noop FS seam: expected file gone, stat err=%v", errN)
	}
}
