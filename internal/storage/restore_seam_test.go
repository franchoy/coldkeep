package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/fsx"
)

// TestRestoreSeamDefaultFSPreservesRestoredBytes verifies that using the
// default OS-backed filesystem seam (opts.fs == nil → fsx.Default()) produces
// the same restored bytes as the pre-seam behavior.
func TestRestoreSeamDefaultFSPreservesRestoredBytes(t *testing.T) {
	t.Parallel()

	repo := NewTestRepository(t)

	content := []byte("coldkeep-phase6-seam-default")
	srcFile := filepath.Join(t.TempDir(), "source.txt")
	if err := os.WriteFile(srcFile, content, 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "restored.txt")

	restoreResult, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		storeResult.FileID,
		outPath,
		RestoreOptions{Overwrite: true},
	)
	if err != nil {
		t.Fatalf("restore with default fs: %v", err)
	}

	got, err := os.ReadFile(restoreResult.OutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("restored content mismatch: got %q want %q", got, content)
	}
	if restoreResult.RestoredHash != storeResult.FileHash {
		t.Fatalf("hash mismatch: restored=%s stored=%s", restoreResult.RestoredHash, storeResult.FileHash)
	}
}

// TestRestoreSeamNoopFSMatchesDefaultBehavior verifies that wrapping the
// OS-backed filesystem with NoopFS produces byte-identical output to the
// default restore path, confirming the seam is behavior-preserving.
func TestRestoreSeamNoopFSMatchesDefaultBehavior(t *testing.T) {
	t.Parallel()

	repo := NewTestRepository(t)

	content := []byte("coldkeep-phase6-seam-noop")
	srcFile := filepath.Join(t.TempDir(), "source.txt")
	if err := os.WriteFile(srcFile, content, 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	outDefault := filepath.Join(t.TempDir(), "restored-default.txt")
	defaultResult, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		storeResult.FileID,
		outDefault,
		RestoreOptions{Overwrite: true},
	)
	if err != nil {
		t.Fatalf("restore with default fs: %v", err)
	}

	outNoop := filepath.Join(t.TempDir(), "restored-noop.txt")
	noopResult, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		storeResult.FileID,
		outNoop,
		RestoreOptions{Overwrite: true, fs: fsx.NewNoop(fsx.Default())},
	)
	if err != nil {
		t.Fatalf("restore with noop fs: %v", err)
	}

	if noopResult.RestoredHash != defaultResult.RestoredHash {
		t.Fatalf("hash mismatch: noop=%s default=%s", noopResult.RestoredHash, defaultResult.RestoredHash)
	}

	defaultBytes, err := os.ReadFile(defaultResult.OutputPath)
	if err != nil {
		t.Fatalf("read default restored file: %v", err)
	}
	noopBytes, err := os.ReadFile(noopResult.OutputPath)
	if err != nil {
		t.Fatalf("read noop restored file: %v", err)
	}
	if !bytes.Equal(noopBytes, defaultBytes) {
		t.Fatalf("content mismatch between default and noop restore: default=%d bytes noop=%d bytes", len(defaultBytes), len(noopBytes))
	}
}
