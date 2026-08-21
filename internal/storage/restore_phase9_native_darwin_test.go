//go:build darwin

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// The name extends an existing cross-platform selector. Combined with the
// production-source proof, it executes both frozen Darwin publication
// primitives on the hosted filesystem without changing CI configuration.
func TestRestoreWithTrustedRootAllowsOuterAliasForExactOutputPathPhase9DarwinPrimitiveProof(t *testing.T) {
	root := t.TempDir()
	parentFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(parentFD)

	if err := os.WriteFile(filepath.Join(root, "rename-source"), []byte("rename"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := unix.RenameatxNp(parentFD, "rename-source", parentFD, "rename-target", unix.RENAME_EXCL); err != nil {
		t.Fatalf("RenameatxNp(RENAME_EXCL): %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "rename-target")); err != nil || string(got) != "rename" {
		t.Fatalf("exclusive rename bytes=%q err=%v", got, err)
	}

	if err := os.WriteFile(filepath.Join(root, "link-source"), []byte("link"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := unix.Linkat(parentFD, "link-source", parentFD, "link-target", 0); err != nil {
		t.Fatalf("Linkat atomic fallback primitive: %v", err)
	}
	if err := unix.Unlinkat(parentFD, "link-source", 0); err != nil {
		t.Fatalf("remove linked temporary name: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "link-target")); err != nil || string(got) != "link" {
		t.Fatalf("atomic link bytes=%q err=%v", got, err)
	}
}
