package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

// Snapshot represents an immutable point-in-time snapshot of a set of files.
// Snapshot rows are never modified after insert.
type Snapshot struct {
	ID        string
	CreatedAt time.Time
	Type      string // "full" | "partial"
	Label     sql.NullString
}

// SnapshotFile represents a single file entry within a snapshot.
// SnapshotFile rows are insert-only.
type SnapshotFile struct {
	ID            int64
	SnapshotID    string
	Path          string
	LogicalFileID int64
	Size          sql.NullInt64
	Mode          sql.NullInt64
	MTime         sql.NullTime
}

var multiSlash = regexp.MustCompile(`/{2,}`)

// NormalizeSnapshotPath normalizes a snapshot-relative path:
//   - rejects empty paths
//   - rejects leading/trailing whitespace
//   - converts Windows separators "\\" to "/"
//   - removes leading "./"
//   - collapses duplicate "/" separators into one
//   - rejects absolute paths (paths starting with "/")
func NormalizeSnapshotPath(path string) (string, error) {
	if path == "" || strings.TrimSpace(path) == "" {
		return "", errors.New("snapshot path cannot be empty")
	}

	if path != strings.TrimSpace(path) {
		return "", fmt.Errorf("snapshot path cannot have leading or trailing whitespace, got %q", path)
	}

	normalized := path

	// Normalize separators to ensure stable cross-platform snapshot paths.
	normalized = strings.ReplaceAll(normalized, "\\", "/")

	// Reject absolute paths – snapshots use relative paths.
	if strings.HasPrefix(normalized, "/") {
		return "", fmt.Errorf("snapshot path must be relative, got %q", normalized)
	}

	// Strip leading "./"
	for strings.HasPrefix(normalized, "./") {
		normalized = normalized[2:]
	}

	// Collapse consecutive slashes.
	normalized = multiSlash.ReplaceAllString(normalized, "/")

	// After stripping, path must not be empty.
	if normalized == "" {
		return "", errors.New("snapshot path cannot be empty after normalization")
	}

	return normalized, nil
}

// InsertSnapshot inserts an immutable snapshot row. id must be non-empty.
// snapshotType must be "full" or "partial".
func InsertSnapshot(ctx context.Context, db *sql.DB, s Snapshot) error {
	if s.ID == "" {
		return errors.New("snapshot id cannot be empty")
	}
	if s.Type != "full" && s.Type != "partial" {
		return fmt.Errorf("snapshot type must be 'full' or 'partial', got %q", s.Type)
	}
	if s.CreatedAt.IsZero() {
		return errors.New("snapshot created_at cannot be zero")
	}

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		s.ID,
		s.CreatedAt.UTC(),
		s.Type,
		s.Label,
	)
	if err != nil {
		return fmt.Errorf("insert snapshot id=%s: %w", s.ID, err)
	}

	log.Printf("snapshot: inserted id=%s type=%s", s.ID, s.Type)
	return nil
}

// InsertSnapshotFile inserts a snapshot_file row. The path is normalized before
// insert. The logical_file referenced by logicalFileID must exist.
func InsertSnapshotFile(ctx context.Context, db *sql.DB, sf SnapshotFile) (int64, error) {
	if sf.SnapshotID == "" {
		return 0, errors.New("snapshot_file snapshot_id cannot be empty")
	}

	normalizedPath, err := NormalizeSnapshotPath(sf.Path)
	if err != nil {
		return 0, fmt.Errorf("normalize snapshot_file path: %w", err)
	}

	if sf.LogicalFileID <= 0 {
		return 0, fmt.Errorf("snapshot_file logical_file_id must be positive, got %d", sf.LogicalFileID)
	}

	// Verify the logical_file exists before inserting.
	var exists int
	err = db.QueryRowContext(ctx, `SELECT 1 FROM logical_file WHERE id = $1`, sf.LogicalFileID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("snapshot_file references non-existent logical_file id=%d", sf.LogicalFileID)
		}
		return 0, fmt.Errorf("check logical_file existence id=%d: %w", sf.LogicalFileID, err)
	}

	var id int64
	err = db.QueryRowContext(
		ctx,
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id, size, mode, mtime)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		sf.SnapshotID,
		normalizedPath,
		sf.LogicalFileID,
		sf.Size,
		sf.Mode,
		sf.MTime,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert snapshot_file snapshot_id=%s path=%q: %w", sf.SnapshotID, normalizedPath, err)
	}

	log.Printf("snapshot: inserted snapshot_file id=%d snapshot_id=%s path=%q", id, sf.SnapshotID, normalizedPath)
	return id, nil
}
