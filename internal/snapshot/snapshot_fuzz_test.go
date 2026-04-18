package snapshot

import (
	"database/sql"
	"path"
	"regexp"
	"strings"
	"testing"
)

// FuzzSnapshotQueryMatchCombinators exercises mixed query criteria to harden
// parser+matcher edge cases without changing command contracts.
func FuzzSnapshotQueryMatchCombinators(f *testing.F) {
	f.Add("docs/a.txt", "docs/", "docs/*.txt", "^docs/.+\\.txt$", int64(0), int64(4096))
	f.Add("./docs//nested/b.log", "docs/", "docs/*", "\\.log$", int64(10), int64(10))
	f.Add("img\\x.png", "img/", "img/*.png", "^img/", int64(1), int64(1<<20))

	f.Fuzz(func(t *testing.T, rawPath, rawPrefix, rawPattern, rawRegex string, minSize, maxSize int64) {
		normalizedPath, err := NormalizeSnapshotPath(rawPath)
		if err != nil {
			// Invalid path inputs are expected in fuzzing.
			return
		}

		q := &SnapshotQuery{}

		if trimmed := strings.TrimSpace(rawPrefix); trimmed != "" {
			if normalizedPrefix, err := NormalizeSnapshotPath(trimmed); err == nil && strings.HasSuffix(normalizedPrefix, "/") {
				q.Prefixes = []string{normalizedPrefix}
			}
		}

		if trimmed := strings.TrimSpace(rawPattern); trimmed != "" {
			if len(trimmed) > 256 {
				trimmed = trimmed[:256]
			}
			if _, err := path.Match(trimmed, ""); err == nil {
				q.Pattern = trimmed
			}
		}

		if trimmed := strings.TrimSpace(rawRegex); trimmed != "" {
			if len(trimmed) > 256 {
				trimmed = trimmed[:256]
			}
			if compiled, err := regexp.Compile(trimmed); err == nil {
				q.Regex = compiled
			}
		}

		if minSize < 0 {
			minSize = -minSize
		}
		if maxSize < 0 {
			maxSize = -maxSize
		}
		if minSize > maxSize {
			minSize, maxSize = maxSize, minSize
		}
		q.MinSize = &minSize
		q.MaxSize = &maxSize

		entry := SnapshotFileEntry{
			Path: normalizedPath,
			Size: sql.NullInt64{Int64: minSize, Valid: true},
		}

		// Match must be deterministic for identical input.
		first := q.Match(entry)
		second := q.Match(entry)
		if first != second {
			t.Fatalf("non-deterministic query match for path=%q", entry.Path)
		}
	})
}
