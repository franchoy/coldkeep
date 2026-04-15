package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
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

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

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
	return insertSnapshot(ctx, db, s)
}

func insertSnapshot(ctx context.Context, exec sqlExecutor, s Snapshot) error {
	if s.ID == "" {
		return errors.New("snapshot id cannot be empty")
	}
	if s.Type != "full" && s.Type != "partial" {
		return fmt.Errorf("snapshot type must be 'full' or 'partial', got %q", s.Type)
	}
	if s.CreatedAt.IsZero() {
		return errors.New("snapshot created_at cannot be zero")
	}

	_, err := exec.ExecContext(
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
	return insertSnapshotFile(ctx, db, sf)
}

func insertSnapshotFile(ctx context.Context, exec sqlExecutor, sf SnapshotFile) (int64, error) {
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
	err = exec.QueryRowContext(ctx, `SELECT 1 FROM logical_file WHERE id = $1`, sf.LogicalFileID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("snapshot_file references non-existent logical_file id=%d", sf.LogicalFileID)
		}
		return 0, fmt.Errorf("check logical_file existence id=%d: %w", sf.LogicalFileID, err)
	}

	var id int64
	err = exec.QueryRowContext(
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

func normalizeSourcePathForSnapshot(path string) (string, error) {
	normalized := strings.ReplaceAll(path, "\\", "/")
	for strings.HasPrefix(normalized, "/") {
		normalized = normalized[1:]
	}
	return NormalizeSnapshotPath(normalized)
}

// CreateSnapshot creates an atomic point-in-time snapshot from current physical_file rows.
// When paths is nil or empty, all physical_file rows are copied into the snapshot.
// When paths is non-empty, rows are filtered by exact paths and directory prefixes ending with '/'.
func CreateSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	snapshotType string,
	label *string,
	paths []string,
) error {
	if db == nil {
		return errors.New("snapshot db cannot be nil")
	}
	if snapshotID == "" {
		return errors.New("snapshot id cannot be empty")
	}
	if snapshotType != "full" && snapshotType != "partial" {
		return fmt.Errorf("snapshot type must be 'full' or 'partial', got %q", snapshotType)
	}

	hasPaths := len(paths) > 0
	if hasPaths && snapshotType != "partial" {
		return fmt.Errorf("snapshot type must be 'partial' when paths are provided, got %q", snapshotType)
	}
	if !hasPaths && snapshotType != "full" {
		return fmt.Errorf("snapshot type must be 'full' when no paths are provided, got %q", snapshotType)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin snapshot transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	s := Snapshot{
		ID:        snapshotID,
		CreatedAt: time.Now().UTC(),
		Type:      snapshotType,
	}
	if label != nil {
		s.Label = sql.NullString{String: *label, Valid: true}
	}

	if err := insertSnapshot(ctx, tx, s); err != nil {
		return err
	}

	var (
		exactFilters   []string
		dirPrefixes    []string
		exactFilterSet = make(map[string]struct{})
	)

	if hasPaths {
		seenInput := make(map[string]struct{})
		for _, rawPath := range paths {
			normalizedPath, normErr := NormalizeSnapshotPath(rawPath)
			if normErr != nil {
				return fmt.Errorf("normalize input path %q: %w", rawPath, normErr)
			}
			if _, exists := seenInput[normalizedPath]; exists {
				continue
			}
			seenInput[normalizedPath] = struct{}{}

			if strings.HasSuffix(normalizedPath, "/") {
				dirPrefixes = append(dirPrefixes, normalizedPath)
				continue
			}

			exactFilters = append(exactFilters, normalizedPath)
			exactFilterSet[normalizedPath] = struct{}{}
		}

		sort.Strings(exactFilters)
		sort.Strings(dirPrefixes)

		if len(exactFilters) == 0 && len(dirPrefixes) == 0 {
			return errors.New("partial snapshot requires at least one valid path filter")
		}
	}

	query := `
		SELECT pf.path, pf.logical_file_id, lf.total_size, pf.mode, pf.mtime
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
	`
	query += " ORDER BY pf.path, pf.logical_file_id"

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query snapshot source rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	seenSnapshotPaths := make(map[string]struct{})
	foundExact := make(map[string]struct{})
	insertedCount := 0

	for rows.Next() {
		var (
			path          string
			logicalFileID int64
			totalSize     int64
			mode          sql.NullInt64
			mtime         sql.NullTime
		)
		if err := rows.Scan(&path, &logicalFileID, &totalSize, &mode, &mtime); err != nil {
			return fmt.Errorf("scan snapshot source row: %w", err)
		}

		normalizedPath, err := normalizeSourcePathForSnapshot(path)
		if err != nil {
			return fmt.Errorf("normalize source physical_file path %q: %w", path, err)
		}

		if hasPaths {
			matched := false
			if _, isExact := exactFilterSet[normalizedPath]; isExact {
				foundExact[normalizedPath] = struct{}{}
				matched = true
			}
			if !matched {
				for _, prefix := range dirPrefixes {
					if strings.HasPrefix(normalizedPath, prefix) {
						matched = true
						break
					}
				}
			}
			if !matched {
				continue
			}
		}

		if _, duplicate := seenSnapshotPaths[normalizedPath]; duplicate {
			continue
		}
		seenSnapshotPaths[normalizedPath] = struct{}{}

		_, err = insertSnapshotFile(ctx, tx, SnapshotFile{
			SnapshotID:    snapshotID,
			Path:          normalizedPath,
			LogicalFileID: logicalFileID,
			Size:          sql.NullInt64{Int64: totalSize, Valid: true},
			Mode:          mode,
			MTime:         mtime,
		})
		if err != nil {
			return err
		}
		insertedCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshot source rows: %w", err)
	}

	for _, exactPath := range exactFilters {
		if _, ok := foundExact[exactPath]; !ok {
			return fmt.Errorf("path not found in current state: %s", exactPath)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit snapshot transaction: %w", err)
	}

	if insertedCount == 0 {
		log.Printf("snapshot: created id=%s type=%s files=0 (empty snapshot)", snapshotID, snapshotType)
		return nil
	}

	log.Printf("snapshot: created id=%s type=%s files=%d", snapshotID, snapshotType, insertedCount)
	return nil
}
