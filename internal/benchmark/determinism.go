package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// HashRestoredTree walks the directory tree rooted at root deterministically
// (sorted by relative path) and returns a map of relative path → SHA-256 hex
// digest of file contents. Empty directories are ignored. This is used to
// verify that store→restore produces identical user-visible output across
// independent benchmark runs.
func HashRestoredTree(root string) (map[string]string, error) {
	hashes := make(map[string]string)

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("compute relative path of %q: %w", path, err)
		}
		// Normalise to forward slashes so comparisons are platform-independent
		// even if the walk order differs by OS path separator.
		rel = filepath.ToSlash(rel)

		h, err := sha256File(path)
		if err != nil {
			return fmt.Errorf("hash file %q: %w", path, err)
		}
		hashes[rel] = h
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk restore tree %q: %w", root, err)
	}
	return hashes, nil
}

// EqualRestoredTreeHashes returns (true, "") when left and right contain
// identical relative-path→SHA-256 mappings. On mismatch it returns false and
// a human-readable description of the first difference found (sorted by path
// for deterministic output).
func EqualRestoredTreeHashes(left, right map[string]string) (bool, string) {
	if len(left) != len(right) {
		return false, fmt.Sprintf("file count mismatch: %d vs %d", len(left), len(right))
	}

	paths := make([]string, 0, len(left))
	for p := range left {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		lh := left[p]
		rh, ok := right[p]
		if !ok {
			return false, fmt.Sprintf("file %q present in first run but not second", p)
		}
		if lh != rh {
			return false, fmt.Sprintf("file %q: hash mismatch (%s != %s)", p, lh, rh)
		}
	}
	return true, ""
}

// sha256File returns the lowercase hex SHA-256 digest of the file at path.
func sha256File(path string) (string, error) {
	f, err := os.Open(path) // #nosec G304 — benchmark-only, path derived from filepath.Walk
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
