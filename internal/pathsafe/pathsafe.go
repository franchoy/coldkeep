package pathsafe

import (
	"fmt"
	"os"
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
	if !hasWindowsDrivePrefix(p) {
		return false
	}
	if len(p) == 2 {
		return true
	}
	return p[2] == '/' || p[2] == '\\'
}

func hasWindowsDrivePrefix(p string) bool {
	if len(p) < 2 {
		return false
	}
	return isWindowsDriveLetter(p[0]) && p[1] == ':'
}

func isWindowsDriveLetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isUNCPath(p string) bool {
	return strings.HasPrefix(p, "\\\\") || strings.HasPrefix(p, "//")
}

func splitPathSegments(p string) []string {
	return strings.FieldsFunc(p, func(r rune) bool {
		return r == '/' || r == '\\'
	})
}

func validateNonEmptyTrimmed(value string, label string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is empty", label)
	}
	return nil
}

func validateNoNUL(value string, label string) error {
	if ContainsNUL(value) {
		return fmt.Errorf("%s contains NUL byte", label)
	}
	return nil
}

func validateNotAbsoluteOrRootedStoredPath(p string) error {
	if filepath.IsAbs(p) {
		return fmt.Errorf("stored path must be relative: %q", p)
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("stored path must be relative: %q", p)
	}
	if strings.HasPrefix(p, "\\") {
		return fmt.Errorf("stored path must be relative: %q", p)
	}
	return nil
}

func validateNoWindowsDriveOrUNCStoredPath(p string) error {
	if IsWindowsDrivePath(p) {
		return fmt.Errorf("stored path must not be a Windows drive or UNC path: %q", p)
	}
	if isUNCPath(p) {
		return fmt.Errorf("stored path must not be a Windows drive or UNC path: %q", p)
	}
	return nil
}

func validateNoTraversalSegmentsStoredPath(p string) error {
	for _, part := range splitPathSegments(p) {
		if part == ".." {
			return fmt.Errorf("stored path must not contain traversal segment: %q", p)
		}
	}
	return nil
}

func validateCleanRelativeStoredPath(p string) error {
	normalized := strings.ReplaceAll(p, "\\", "/")
	clean := path.Clean(normalized)
	if clean == "." {
		return fmt.Errorf("stored path escapes root after cleaning: %q", p)
	}
	if clean == ".." {
		return fmt.Errorf("stored path escapes root after cleaning: %q", p)
	}
	if strings.HasPrefix(clean, "../") {
		return fmt.Errorf("stored path escapes root after cleaning: %q", p)
	}
	if strings.HasPrefix(clean, "/") {
		return fmt.Errorf("stored path must remain relative after cleaning: %q", p)
	}
	return nil
}

func validateFileNameNotDotSegments(name string) error {
	if name == "." {
		return fmt.Errorf("filename must not be %q", name)
	}
	if name == ".." {
		return fmt.Errorf("filename must not be %q", name)
	}
	return nil
}

func validateFileNameHasNoSeparators(name string) error {
	if strings.ContainsRune(name, '/') {
		return fmt.Errorf("filename must not contain path separators: %q", name)
	}
	if strings.ContainsRune(name, '\\') {
		return fmt.Errorf("filename must not contain path separators: %q", name)
	}
	return nil
}

func validateFileNameNotAbsoluteOrNetwork(name string) error {
	if filepath.IsAbs(name) {
		return fmt.Errorf("filename must not be absolute, drive, or UNC path: %q", name)
	}
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("filename must not be absolute, drive, or UNC path: %q", name)
	}
	if strings.HasPrefix(name, "\\") {
		return fmt.Errorf("filename must not be absolute, drive, or UNC path: %q", name)
	}
	if IsWindowsDrivePath(name) {
		return fmt.Errorf("filename must not be absolute, drive, or UNC path: %q", name)
	}
	if isUNCPath(name) {
		return fmt.Errorf("filename must not be absolute, drive, or UNC path: %q", name)
	}
	return nil
}

// ValidateStoredRelativePath validates a stored/snapshot-style relative path.
func ValidateStoredRelativePath(p string) error {
	if err := validateNonEmptyTrimmed(p, "stored path"); err != nil {
		return err
	}
	if err := validateNoNUL(p, "stored path"); err != nil {
		return err
	}
	if err := validateNotAbsoluteOrRootedStoredPath(p); err != nil {
		return err
	}
	if err := validateNoWindowsDriveOrUNCStoredPath(p); err != nil {
		return err
	}
	if err := validateNoTraversalSegmentsStoredPath(p); err != nil {
		return err
	}
	return validateCleanRelativeStoredPath(p)
}

// ValidateSafeFileName validates an opaque filename intended to be joined
// under a trusted directory.
func ValidateSafeFileName(name string) error {
	if err := validateNonEmptyTrimmed(name, "filename"); err != nil {
		return err
	}
	if err := validateNoNUL(name, "filename"); err != nil {
		return err
	}
	if err := validateFileNameNotDotSegments(name); err != nil {
		return err
	}
	if err := validateFileNameHasNoSeparators(name); err != nil {
		return err
	}
	return validateFileNameNotAbsoluteOrNetwork(name)
}

// SafeJoin validates rel and joins it under a trusted root with containment checks.
func SafeJoin(root string, rel string) (string, error) {
	if err := validateNonEmptyTrimmed(root, "root path"); err != nil {
		return "", err
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
	if err := ValidatePathHasNoSymlinkComponents(joinedAbs); err != nil {
		return "", err
	}
	if err := validateJoinedPathWithinRoot(rootAbs, joinedAbs, rel); err != nil {
		return "", err
	}

	return joinedAbs, nil
}

func validateJoinedPathWithinRoot(rootAbs string, joinedAbs string, rel string) error {
	relToRoot, err := filepath.Rel(rootAbs, joinedAbs)
	if err != nil {
		return fmt.Errorf("compare joined path with root: %w", err)
	}
	if relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(filepath.Separator)) {
		return fmt.Errorf("joined path escapes root: %q", rel)
	}
	return nil
}

// ValidatePathHasNoSymlinkComponents rejects paths that traverse existing symlink
// components. Missing suffix components are allowed.
func ValidatePathHasNoSymlinkComponents(p string) error {
	if err := validateNonEmptyTrimmed(p, "path"); err != nil {
		return err
	}

	start, segments, err := symlinkCheckStartAndSegments(p)
	if err != nil {
		return err
	}

	return validateSegmentsHaveNoSymlinks(start, segments)
}

func symlinkCheckStartAndSegments(p string) (string, []string, error) {
	absPath, err := filepath.Abs(p)
	if err != nil {
		return "", nil, fmt.Errorf("resolve path: %w", err)
	}

	cleanPath := filepath.Clean(absPath)
	volume := filepath.VolumeName(cleanPath)
	relPath := strings.TrimPrefix(cleanPath, volume)
	relPath = strings.TrimPrefix(relPath, string(filepath.Separator))
	if relPath == "" {
		relPath = "."
	}

	start := volume
	if start == "" {
		start = string(filepath.Separator)
	}

	segments := strings.Split(relPath, string(filepath.Separator))
	return start, segments, nil
}

func validateSegmentsHaveNoSymlinks(start string, segments []string) error {
	current := start
	for _, segment := range segments {
		if segment == "" || segment == "." {
			continue
		}

		current = filepath.Join(current, segment)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			// Preserve existing restore error contracts for missing/inaccessible
			// path components; only explicit symlink traversal is rejected.
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component is a symlink: %q", current)
		}
	}

	return nil
}
