package pathsafe

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// ContainsNUL reports whether the input contains a NUL byte.
func ContainsNUL(p string) bool {
	return strings.ContainsRune(p, '\x00')
}

// IsWindowsDrivePath reports whether p starts with a Windows drive prefix.
// Examples: C:\\tmp\\a.txt, D:/tmp/a.txt, E:
func IsWindowsDrivePath(p string) bool {
	if len(p) < 2 {
		return false
	}
	c := p[0]
	isLetter := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
	if !isLetter || p[1] != ':' {
		return false
	}
	if len(p) == 2 {
		return true
	}
	return p[2] == '/' || p[2] == '\\'
}

func isUNCPath(p string) bool {
	return strings.HasPrefix(p, "\\\\") || strings.HasPrefix(p, "//")
}

func splitPathSegments(p string) []string {
	return strings.FieldsFunc(p, func(r rune) bool {
		return r == '/' || r == '\\'
	})
}

// ValidateStoredRelativePath validates a stored/snapshot-style relative path.
func ValidateStoredRelativePath(p string) error {
	if strings.TrimSpace(p) == "" {
		return fmt.Errorf("stored path is empty")
	}
	if ContainsNUL(p) {
		return fmt.Errorf("stored path contains NUL byte")
	}
	if filepath.IsAbs(p) || strings.HasPrefix(p, "/") || strings.HasPrefix(p, "\\") {
		return fmt.Errorf("stored path must be relative: %q", p)
	}
	if IsWindowsDrivePath(p) || isUNCPath(p) {
		return fmt.Errorf("stored path must not be a Windows drive or UNC path: %q", p)
	}

	for _, part := range splitPathSegments(p) {
		if part == ".." {
			return fmt.Errorf("stored path must not contain traversal segment: %q", p)
		}
	}

	normalized := strings.ReplaceAll(p, "\\", "/")
	clean := path.Clean(normalized)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("stored path escapes root after cleaning: %q", p)
	}
	if strings.HasPrefix(clean, "/") {
		return fmt.Errorf("stored path must remain relative after cleaning: %q", p)
	}

	return nil
}

// ValidateSafeFileName validates an opaque filename intended to be joined
// under a trusted directory.
func ValidateSafeFileName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("filename is empty")
	}
	if ContainsNUL(name) {
		return fmt.Errorf("filename contains NUL byte")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("filename must not be %q", name)
	}
	if strings.ContainsAny(name, `/\\`) {
		return fmt.Errorf("filename must not contain path separators: %q", name)
	}
	if filepath.IsAbs(name) || strings.HasPrefix(name, "/") || strings.HasPrefix(name, "\\") || IsWindowsDrivePath(name) || isUNCPath(name) {
		return fmt.Errorf("filename must not be absolute, drive, or UNC path: %q", name)
	}

	return nil
}

// SafeJoin validates rel and joins it under a trusted root with containment checks.
func SafeJoin(root string, rel string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", fmt.Errorf("root path is empty")
	}
	if err := ValidateStoredRelativePath(rel); err != nil {
		return "", err
	}

	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve root: %w", err)
	}

	normalizedRel := strings.ReplaceAll(rel, "\\", "/")
	joined := filepath.Join(rootAbs, filepath.FromSlash(normalizedRel))
	joinedAbs, err := filepath.Abs(joined)
	if err != nil {
		return "", fmt.Errorf("resolve joined path: %w", err)
	}

	relToRoot, err := filepath.Rel(rootAbs, joinedAbs)
	if err != nil {
		return "", fmt.Errorf("compare joined path with root: %w", err)
	}
	if relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("joined path escapes root: %q", rel)
	}

	return joinedAbs, nil
}
