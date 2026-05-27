package pathsafe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMetadataPolicyDoesNotAssumeGlobalCaseSensitivity exercises case-distinct
// file creation and records host filesystem behavior.
//
// The policy point is that case sensitivity is host/filesystem-dependent.
// This test observes actual behavior rather than asserting universal semantics.
// Linux CI is typically case-sensitive; macOS HFS+/APFS may not be.
func TestMetadataPolicyDoesNotAssumeGlobalCaseSensitivity(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	lower := filepath.Join(dir, "case.txt")
	upper := filepath.Join(dir, "CASE.txt")

	if err := os.WriteFile(lower, []byte("lower"), 0o600); err != nil {
		t.Fatalf("write lower: %v", err)
	}
	if err := os.WriteFile(upper, []byte("upper"), 0o600); err != nil {
		// On case-insensitive filesystems this may fail or overwrite.
		// The policy point is that host behavior is observed, not assumed.
		t.Logf("host filesystem may be case-insensitive or case-colliding: %v", err)
		return
	}

	lowerBytes, err := os.ReadFile(lower) // #nosec G304 — path built from t.TempDir() + hardcoded literal; no user input
	if err != nil {
		t.Fatalf("read lower: %v", err)
	}
	upperBytes, err := os.ReadFile(upper) // #nosec G304 — path built from t.TempDir() + hardcoded literal; no user input
	if err != nil {
		t.Fatalf("read upper: %v", err)
	}

	if string(lowerBytes) == string(upperBytes) {
		t.Logf("host filesystem may not distinguish case as expected")
	}
}

// TestMetadataPolicyTreatsPermissionBitsAsHostDependent verifies that a file
// created with explicit permission bits has non-zero permissions on the host.
//
// Coldkeep stores and restores POSIX mode bits on Linux. This test documents
// that permission bits are meaningful on the current host. Cross-platform
// equivalence on macOS and Windows is not asserted here.
func TestMetadataPolicyTreatsPermissionBitsAsHostDependent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "perm.txt")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}

	if info.Mode().Perm() == 0 {
		t.Fatalf("unexpected zero permissions for test file")
	}
}

// TestMetadataPolicyTreatsTimestampsAsHostDependent verifies that a newly
// created file has a non-zero modification time on the current host.
//
// Coldkeep stores and restores mtime on Linux via os.Chtimes. This test
// documents that timestamps are observable on the host filesystem. Timestamp
// precision and equivalence across platforms are not asserted here.
func TestMetadataPolicyTreatsTimestampsAsHostDependent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "time.txt")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}

	if info.ModTime().IsZero() {
		t.Fatalf("mod time should not be zero")
	}
}

// TestMetadataPolicyDoesNotUseSymlinkAssumption documents that Coldkeep does
// not assume portable symlink creation. Symlinks are resolved at store time
// and rejected at restore time via pathsafe. This test observes host symlink
// behavior and skips gracefully if symlink creation is unavailable.
func TestMetadataPolicyDoesNotUseSymlinkAssumption(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "target.txt")
	link := filepath.Join(dir, "link.txt")

	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}

	if err := os.Symlink(target, link); err != nil {
		t.Logf("symlink creation is host/platform/permission dependent: %v", err)
		return
	}

	resolved, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if !strings.Contains(resolved, "target.txt") {
		t.Fatalf("unexpected symlink target: %q", resolved)
	}
}
