package storage

import (
	"bytes"
	"io/fs"
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
	if err := os.WriteFile(filepath.Clean(srcFile), content, 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	outDir := realTempDir(t)
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

	got, err := fs.ReadFile(os.DirFS(outDir), "restored.txt")
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
	mustNoErr(t, os.WriteFile(filepath.Clean(srcFile), content, 0o600), "write source file")

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	mustNoErr(t, err, "store file")

	outDefaultDir := realTempDir(t)
	outDefault := filepath.Join(outDefaultDir, "restored-default.txt")
	defaultResult, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		storeResult.FileID,
		outDefault,
		RestoreOptions{Overwrite: true},
	)
	mustNoErr(t, err, "restore with default fs")

	outNoopDir := realTempDir(t)
	outNoop := filepath.Join(outNoopDir, "restored-noop.txt")
	noopResult, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		storeResult.FileID,
		outNoop,
		RestoreOptions{Overwrite: true, fs: fsx.NewNoop(fsx.Default())},
	)
	mustNoErr(t, err, "restore with noop fs")

	if noopResult.RestoredHash != defaultResult.RestoredHash {
		t.Fatalf("hash mismatch: noop=%s default=%s", noopResult.RestoredHash, defaultResult.RestoredHash)
	}

	defaultBytes, err := fs.ReadFile(os.DirFS(outDefaultDir), "restored-default.txt")
	mustNoErr(t, err, "read default restored file")
	noopBytes, err := fs.ReadFile(os.DirFS(outNoopDir), "restored-noop.txt")
	mustNoErr(t, err, "read noop restored file")
	if !bytes.Equal(noopBytes, defaultBytes) {
		t.Fatalf("content mismatch between default and noop restore: default=%d bytes noop=%d bytes", len(defaultBytes), len(noopBytes))
	}
}
