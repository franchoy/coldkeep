package pathsafe

import (
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
