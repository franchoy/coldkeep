package pathsafe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContainsNUL(t *testing.T) {
	if ContainsNUL("safe/path") {
		t.Fatalf("expected safe/path to not contain NUL")
	}
	if !ContainsNUL("safe/\x00path") {
		t.Fatalf("expected NUL-containing path to be detected")
	}
}

func TestIsWindowsDrivePath(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "drive with backslash", in: `C:\\tmp\\a.txt`, want: true},
		{name: "drive with slash", in: "D:/tmp/a.txt", want: true},
		{name: "drive only", in: "E:", want: true},
		{name: "not drive", in: "a:b", want: false},
		{name: "relative", in: "tmp/a.txt", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsWindowsDrivePath(tt.in)
			if got != tt.want {
				t.Fatalf("IsWindowsDrivePath(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestValidateStoredRelativePathAcceptsSafeRelativePaths(t *testing.T) {
	cases := []string{
		"file.txt",
		"dir/file.txt",
		"dir/sub/file.txt",
		"./dir/file.txt",
		"dir\\sub\\file.txt",
	}

	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			if err := ValidateStoredRelativePath(tc); err != nil {
				t.Fatalf("expected %q to be accepted: %v", tc, err)
			}
		})
	}
}

func TestValidateStoredRelativePathRejectsUnsafePaths(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"../evil",
		"a/../../evil",
		"a\\..\\..\\evil",
		"/absolute",
		`C:\\evil`,
		"C:/evil",
		`\\\\server\\share\\evil`,
		"safe/\x00bad",
		".",
	}

	for _, tc := range cases {
		name := strings.ReplaceAll(tc, "\x00", "<NUL>")
		if name == "" {
			name = "<empty>"
		}
		t.Run(name, func(t *testing.T) {
			if err := ValidateStoredRelativePath(tc); err == nil {
				t.Fatalf("expected %q to be rejected", tc)
			}
		})
	}
}

func TestValidateSafeFileNameAcceptsOpaqueNames(t *testing.T) {
	cases := []string{
		"container_123.bin",
		"blockhashabcdef",
		"manifest.json",
	}

	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			if err := ValidateSafeFileName(tc); err != nil {
				t.Fatalf("expected %q to be accepted: %v", tc, err)
			}
		})
	}
}

func TestValidateSafeFileNameRejectsUnsafeNames(t *testing.T) {
	cases := []string{
		"",
		"   ",
		".",
		"..",
		"a/b",
		`a\\b`,
		"/abs",
		`C:\\tmp\\x`,
		`\\\\server\\share\\x`,
		"safe\x00bad",
	}

	for _, tc := range cases {
		name := strings.ReplaceAll(tc, "\x00", "<NUL>")
		if name == "" {
			name = "<empty>"
		}
		t.Run(name, func(t *testing.T) {
			if err := ValidateSafeFileName(tc); err == nil {
				t.Fatalf("expected %q to be rejected", tc)
			}
		})
	}
}

func TestSafeJoinKeepsPathsUnderRoot(t *testing.T) {
	root := t.TempDir()

	joined, err := SafeJoin(root, "dir/file.txt")
	if err != nil {
		t.Fatalf("SafeJoin returned error: %v", err)
	}

	rel, err := filepath.Rel(root, joined)
	if err != nil {
		t.Fatalf("filepath.Rel returned error: %v", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("joined path escaped root: root=%q joined=%q rel=%q", root, joined, rel)
	}
}

func TestSafeJoinRejectsEscapeAttemptsAndPrefixConfusion(t *testing.T) {
	root := t.TempDir()

	cases := []string{
		"../evil.txt",
		"a/../../evil.txt",
		"..\\evil.txt",
		"/absolute/evil.txt",
		`C:\\evil.txt`,
	}

	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			if _, err := SafeJoin(root, tc); err == nil {
				t.Fatalf("expected SafeJoin to reject %q", tc)
			}
		})
	}

	prefixCandidate := filepath.Join(root+"2", "evil.txt")
	rel, err := filepath.Rel(root, prefixCandidate)
	if err != nil {
		t.Fatalf("filepath.Rel returned error for prefix confusion probe: %v", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return
	}
	t.Fatalf("prefix confusion probe expected outside root, got rel=%q", rel)
}

func TestValidatePathHasNoSymlinkComponentsRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	linkPath := filepath.Join(root, "link")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	err := ValidatePathHasNoSymlinkComponents(filepath.Join(linkPath, "escaped.txt"))
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection error, got: %v", err)
	}
}

func TestValidatePathHasNoSymlinkComponentsRejectsSymlinkTarget(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "sentinel.bin")
	if err := os.WriteFile(outside, []byte("sentinel"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	linkPath := filepath.Join(root, "target-link.bin")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	err := ValidatePathHasNoSymlinkComponents(linkPath)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink target rejection, got: %v", err)
	}
}

func TestValidateWritePathUnderTrustedRootRejectsSymlinkedParent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	linkPath := filepath.Join(root, "link")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	targetPath := filepath.Join(linkPath, "nested", "file.bin")
	err := ValidateWritePathUnderTrustedRoot(root, targetPath)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected trusted-root symlink rejection, got: %v", err)
	}
}

func TestValidateWritePathUnderTrustedRootAllowsOuterAlias(t *testing.T) {
	realParent := t.TempDir()
	aliasLink := filepath.Join(t.TempDir(), "outer-link")
	if err := os.Symlink(realParent, aliasLink); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	realRoot := filepath.Join(realParent, "trusted")
	if err := os.MkdirAll(realRoot, 0o700); err != nil {
		t.Fatalf("mkdir real trusted root: %v", err)
	}
	aliasRoot := filepath.Join(aliasLink, "trusted")
	targetPath := filepath.Join(aliasRoot, "nested", "file.bin")

	if err := ValidateWritePathUnderTrustedRoot(aliasRoot, targetPath); err != nil {
		t.Fatalf("expected outer alias above trusted root to be allowed, got: %v", err)
	}
}

func TestNearestExistingAncestorDirFindsParentForMissingSuffix(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nested", "missing", "file.bin")

	got, err := NearestExistingAncestorDir(path)
	if err != nil {
		t.Fatalf("NearestExistingAncestorDir returned error: %v", err)
	}
	if got != root {
		t.Fatalf("nearest ancestor mismatch: got=%q want=%q", got, root)
	}
}

func TestNearestExistingAncestorDirSkipsExistingSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	linkPath := filepath.Join(root, "linked-parent")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	got, err := NearestExistingAncestorDir(filepath.Join(linkPath, "restored.bin"))
	if err != nil {
		t.Fatalf("NearestExistingAncestorDir returned error: %v", err)
	}
	if got != root {
		t.Fatalf("nearest ancestor mismatch: got=%q want=%q", got, root)
	}
}

func TestValidateTrustedRootPathRejectsSymlinkRoot(t *testing.T) {
	realRoot := t.TempDir()
	symlinkRoot := filepath.Join(t.TempDir(), "trusted-link")
	if err := os.Symlink(realRoot, symlinkRoot); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	_, err := ValidateTrustedRootPath(symlinkRoot)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink trusted-root rejection, got: %v", err)
	}
}

func TestValidateTrustedRootPathAllowsOuterAlias(t *testing.T) {
	realParent := t.TempDir()
	aliasLink := filepath.Join(t.TempDir(), "outer-link")
	if err := os.Symlink(realParent, aliasLink); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	realRoot := filepath.Join(realParent, "trusted")
	if err := os.MkdirAll(realRoot, 0o755); err != nil {
		t.Fatalf("mkdir real trusted root: %v", err)
	}
	aliasRoot := filepath.Join(aliasLink, "trusted")

	got, err := ValidateTrustedRootPath(aliasRoot)
	if err != nil {
		t.Fatalf("expected outer alias trusted root to be allowed, got: %v", err)
	}
	if got != filepath.Clean(aliasRoot) {
		t.Fatalf("trusted root mismatch: got=%q want=%q", got, filepath.Clean(aliasRoot))
	}
}

func TestValidatePathHasNoSymlinkComponentsAllowsMissingSuffix(t *testing.T) {
	// Use EvalSymlinks so the path does not traverse OS-managed symlinks
	// (e.g. /var -> /private/var on macOS) before reaching the missing suffix.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "nested", "missing", "file.txt")

	if err := ValidatePathHasNoSymlinkComponents(target); err != nil {
		t.Fatalf("expected missing suffix path to be allowed, got: %v", err)
	}
}
