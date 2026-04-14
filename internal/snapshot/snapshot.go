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
//   - trims whitespace
//   - rejects empty paths
//   - removes leading "./"
//   - collapses duplicate "/" separators into one
//   - rejects absolute paths (paths starting with "/")
func NormalizeSnapshotPath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", errors.New("snapshot path cannot be empty")
	}

	// Reject absolute paths – snapshots use relative paths.
	if strings.HasPrefix(trimmed, "/") {
		return "", fmt.Errorf("snapshot path must be relative, got %q", trimmed)
	}

	// Strip leading "./"
	for strings.HasPrefix(trimmed, "./") {
		trimmed = trimmed[2:]
	}

	// Collapse consecutive slashes.
	trimmed = multiSlash.ReplaceAllString(trimmed, "/")

	// After stripping, path must not be empty.
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return "", errors.New("snapshot path cannot be empty after normalization")
	}

	return trimmed, nil
}

// InsertSnapshot inserts an immutable snapshot row. id must be a UUID string.
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

	result, err := db.ExecContext(
		ctx,
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id, size, mode, mtime)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		sf.SnapshotID,
		normalizedPath,
		sf.LogicalFileID,
		sf.Size,
		sf.Mode,
		sf.MTime,
	)
	if err != nil {
		return 0, fmt.Errorf("insert snapshot_file snapshot_id=%s path=%q: %w", sf.SnapshotID, normalizedPath, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read inserted snapshot_file id: %w", err)
	}

	log.Printf("snapshot: inserted snapshot_file id=%d snapshot_id=%s path=%q", id, sf.SnapshotID, normalizedPath)
	return id, nil
}
