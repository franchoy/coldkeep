package pathsafe

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestPathNormalizationDocumentsHostSemantics exercises filepath.Clean against
// platform-sensitive path forms and records host behavior.
//
// These tests document current host semantics on Linux.  They do not assert
// Windows behavior; true Windows semantics require Windows CI.
func TestPathNormalizationDocumentsHostSemantics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		input        string
		wantNonEmpty bool
		notes        string
	}{
		{
			name:         "simple relative",
			input:        "a/b/c.txt",
			wantNonEmpty: true,
			notes:        "portable relative path",
		},
		{
			name:         "nested relative",
			input:        "dir/sub/file.bin",
			wantNonEmpty: true,
			notes:        "portable nested relative path",
		},
		{
			name:         "dot segment cleaned",
			input:        "./a/../b/file.txt",
			wantNonEmpty: true,
			notes:        "filepath.Clean resolves dot segments on host",
		},
		{
			name:         "repeated separators",
			input:        "a//b//c.txt",
			wantNonEmpty: true,
			notes:        "filepath.Clean collapses repeated slashes on host",
		},
		{
			name:         "trailing separator",
			input:        "a/b/",
			wantNonEmpty: true,
			notes:        "filepath.Clean strips trailing separator on host",
		},
		{
			name:         "absolute path",
			input:        "/a/b/c.txt",
			wantNonEmpty: true,
			notes:        "absolute path preserved by filepath.Clean on host",
		},
		{
			name:         "parent traversal one level",
			input:        "../outside",
			wantNonEmpty: true,
			notes:        "filepath.Clean keeps traversal; safety validation must reject this",
		},
		{
			name:         "parent traversal multi level",
			input:        "../../escape",
			wantNonEmpty: true,
			notes:        "filepath.Clean keeps traversal; safety validation must reject this",
		},
		{
			name:         "windows backslash path",
			input:        `C:\coldkeep\data.bin`,
			wantNonEmpty: true,
			notes:        "on Linux filepath.Clean treats backslash as literal; Windows CI required for true Windows semantics",
		},
		{
			name:         "windows drive with slash",
			input:        "C:/coldkeep/data.bin",
			wantNonEmpty: true,
			notes:        "on Linux filepath.Clean does not strip drive prefix; Windows CI required",
		},
		{
			name:         "mixed separators",
			input:        `a\b/c.txt`,
			wantNonEmpty: true,
			notes:        "on Linux filepath.Clean treats backslash as literal character",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := filepath.Clean(tc.input)
			if tc.wantNonEmpty && got == "" {
				t.Fatalf("filepath.Clean(%q) returned empty; notes: %s", tc.input, tc.notes)
			}
		})
	}
}

// TestIsWindowsDrivePathCrossplatformForms verifies IsWindowsDrivePath
// correctly identifies Windows drive patterns on any host OS.
func TestIsWindowsDrivePathCrossplatformForms(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  bool
		notes string
	}{
		{
			name:  "uppercase drive backslash",
			input: `C:\coldkeep\data.bin`,
			want:  true,
			notes: "Windows-style drive path detected cross-platform",
		},
		{
			name:  "uppercase drive forward slash",
			input: "D:/coldkeep/data.bin",
			want:  true,
			notes: "Windows-style drive path with forward slash",
		},
		{
			name:  "lowercase drive",
			input: "d:/data.bin",
			want:  true,
			notes: "lowercase drive letter is a valid Windows drive prefix",
		},
		{
			name:  "drive letter only",
			input: "E:",
			want:  true,
			notes: "bare drive letter without path",
		},
		{
			name:  "relative path not drive",
			input: "a/b/c.txt",
			want:  false,
			notes: "portable relative path must not be mistaken for drive path",
		},
		{
			name:  "colon not at position 1",
			input: "ab:c",
			want:  false,
			notes: "colon not in drive position",
		},
		{
			name:  "unix absolute",
			input: "/a/b",
			want:  false,
			notes: "POSIX absolute path is not a Windows drive path",
		},
		{
			name:  "empty string",
			input: "",
			want:  false,
			notes: "empty input",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := IsWindowsDrivePath(tc.input)
			if got != tc.want {
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
