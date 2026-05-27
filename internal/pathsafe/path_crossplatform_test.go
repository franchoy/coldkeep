package pathsafe

import (
	"path/filepath"
	"strings"
	"testing"
)

// pathNormalizationCases drives TestPathNormalizationDocumentsHostSemantics.
// All inputs must produce a non-empty result from filepath.Clean.
var pathNormalizationCases = []struct {
	name  string
	input string
	notes string
}{
	{"simple relative", "a/b/c.txt", "portable relative path"},
	{"nested relative", "dir/sub/file.bin", "portable nested relative path"},
	{"dot segment cleaned", "./a/../b/file.txt", "filepath.Clean resolves dot segments on host"},
	{"repeated separators", "a//b//c.txt", "filepath.Clean collapses repeated slashes on host"},
	{"trailing separator", "a/b/", "filepath.Clean strips trailing separator on host"},
	{"absolute path", "/a/b/c.txt", "absolute path preserved by filepath.Clean on host"},
	{"parent traversal one level", "../outside", "filepath.Clean keeps traversal; safety validation must reject this"},
	{"parent traversal multi level", "../../escape", "filepath.Clean keeps traversal; safety validation must reject this"},
	{"windows backslash path", `C:\coldkeep\data.bin`, "on Linux filepath.Clean treats backslash as literal; Windows CI required for true Windows semantics"},
	{"windows drive with slash", "C:/coldkeep/data.bin", "on Linux filepath.Clean does not strip drive prefix; Windows CI required"},
	{"mixed separators", `a\b/c.txt`, "on Linux filepath.Clean treats backslash as literal character"},
}

// TestPathNormalizationDocumentsHostSemantics exercises filepath.Clean against
// platform-sensitive path forms and records host behavior.
//
// These tests document current host semantics on Linux.  They do not assert
// Windows behavior; true Windows semantics require Windows CI.
func TestPathNormalizationDocumentsHostSemantics(t *testing.T) {
	t.Parallel()

	for _, tc := range pathNormalizationCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := filepath.Clean(tc.input); got == "" {
				t.Fatalf("filepath.Clean(%q) returned empty; notes: %s", tc.input, tc.notes)
			}
		})
	}
}

// isWindowsDrivePathCases drives TestIsWindowsDrivePathCrossplatformForms.
var isWindowsDrivePathCases = []struct {
	name  string
	input string
	want  bool
	notes string
}{
	{"uppercase drive backslash", `C:\coldkeep\data.bin`, true, "Windows-style drive path detected cross-platform"},
	{"uppercase drive forward slash", "D:/coldkeep/data.bin", true, "Windows-style drive path with forward slash"},
	{"lowercase drive", "d:/data.bin", true, "lowercase drive letter is a valid Windows drive prefix"},
	{"drive letter only", "E:", true, "bare drive letter without path"},
	{"relative path not drive", "a/b/c.txt", false, "portable relative path must not be mistaken for drive path"},
	{"colon not at position 1", "ab:c", false, "colon not in drive position"},
	{"unix absolute", "/a/b", false, "POSIX absolute path is not a Windows drive path"},
	{"empty string", "", false, "empty input"},
}

// TestIsWindowsDrivePathCrossplatformForms verifies IsWindowsDrivePath
// correctly identifies Windows drive patterns on any host OS.
func TestIsWindowsDrivePathCrossplatformForms(t *testing.T) {
	t.Parallel()

	for _, tc := range isWindowsDrivePathCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := IsWindowsDrivePath(tc.input); got != tc.want {
				t.Fatalf("IsWindowsDrivePath(%q) = %v, want %v; notes: %s",
					tc.input, got, tc.want, tc.notes)
			}
		})
	}
}

// TestValidateStoredRelativePathCrossplatformForms asserts that
// ValidateStoredRelativePath rejects platform-sensitive dangerous forms
// regardless of the host OS.
func TestValidateStoredRelativePathCrossplatformForms(t *testing.T) {
	t.Parallel()

	rejected := []struct {
		name  string
		input string
		notes string
	}{
		{
			name:  "windows drive backslash",
			input: `C:\coldkeep\data.bin`,
			notes: "must be rejected even on Linux to prevent cross-platform confusion",
		},
		{
			name:  "windows drive forward slash",
			input: "C:/coldkeep/data.bin",
			notes: "Windows drive path with forward slash must be rejected",
		},
		{
			name:  "unc path double backslash",
			input: `\\server\share\file.txt`,
			notes: "UNC paths must be rejected",
		},
		{
			name:  "parent traversal",
			input: "../escape.txt",
			notes: "traversal must be rejected on all platforms",
		},
		{
			name:  "deeply nested traversal",
			input: "a/../../escape.txt",
			notes: "deep traversal must be rejected on all platforms",
		},
		{
			name:  "absolute unix path",
			input: "/etc/passwd",
			notes: "absolute path must be rejected",
		},
		{
			name:  "nul byte",
			input: "safe/\x00evil",
			notes: "NUL byte injection must be rejected on all platforms",
		},
	}

	for _, tc := range rejected {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateStoredRelativePath(tc.input); err == nil {
				t.Fatalf("ValidateStoredRelativePath(%q) should be rejected; notes: %s",
					tc.input, tc.notes)
			}
		})
	}

	accepted := []struct {
		name  string
		input string
		notes string
	}{
		{
			name:  "simple relative",
			input: "file.txt",
			notes: "plain filename is safe",
		},
		{
			name:  "nested relative",
			input: "dir/sub/file.bin",
			notes: "portable nested relative path is safe",
		},
		{
			name:  "backslash relative no traversal",
			input: `dir\sub\file.bin`,
			notes: "backslash-separated relative path without traversal accepted",
		},
	}

	for _, tc := range accepted {
		tc := tc
		t.Run("accept_"+tc.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateStoredRelativePath(tc.input); err != nil {
				t.Fatalf("ValidateStoredRelativePath(%q) should be accepted, got: %v; notes: %s",
					tc.input, err, tc.notes)
			}
		})
	}
}

// TestSafeJoinRejectsCrossplatformDangerousForms verifies SafeJoin refuses
// paths that are dangerous on any platform.
func TestSafeJoinRejectsCrossplatformDangerousForms(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	cases := []struct {
		name  string
		input string
		notes string
	}{
		{
			name:  "windows drive backslash",
			input: `C:\data\file.txt`,
			notes: "Windows drive path must be rejected by SafeJoin",
		},
		{
			name:  "windows drive forward slash",
			input: "D:/data/file.txt",
			notes: "Windows drive path with forward slash must be rejected",
		},
		{
			name:  "parent traversal",
			input: "../escape.txt",
			notes: "traversal must be rejected",
		},
		{
			name:  "deep traversal",
			input: "a/../../escape.txt",
			notes: "deep traversal must be rejected",
		},
		{
			name:  "unix absolute",
			input: "/etc/passwd",
			notes: "absolute path must be rejected",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := SafeJoin(root, tc.input); err == nil {
				t.Fatalf("SafeJoin(%q) should be rejected; notes: %s", tc.input, tc.notes)
			}
		})
	}
}

// TestMixedSeparatorPathsDoNotEscapeRoot confirms that mixed-separator
// inputs, which may look safe but behave differently across platforms,
// are handled deterministically by the pathsafe layer.
func TestMixedSeparatorPathsDoNotEscapeRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	// Paths with only backslashes and no traversal segments should be
	// accepted by ValidateStoredRelativePath (backslash is treated as
	// a separator character in the validator's splitPathSegments).
	safeBackslash := `subdir\file.bin`
	if err := ValidateStoredRelativePath(safeBackslash); err != nil {
		t.Fatalf("expected backslash-only relative path to be accepted: %v", err)
	}

	// The same path fed through SafeJoin must not escape root.
	joined, err := SafeJoin(root, safeBackslash)
	if err != nil {
		t.Fatalf("SafeJoin rejected safe backslash-only relative path: %v", err)
	}
	if !strings.HasPrefix(joined, root) {
		t.Fatalf("joined path escaped root: root=%q joined=%q", root, joined)
	}
}
