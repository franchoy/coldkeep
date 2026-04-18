package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/storage"
)

// Snapshot represents an immutable point-in-time snapshot of a set of files.
// Snapshot rows are never modified after insert.
type Snapshot struct {
	ID        string
	CreatedAt time.Time
	Type      string // "full" | "partial"
	Label     sql.NullString
	// ParentID optionally references a prior snapshot for lineage tracking.
	// It is stored in the DB but not yet surfaced to the CLI.
	ParentID sql.NullString
}

// DiffType represents the kind of change in a snapshot diff entry.
type DiffType string

const (
	DiffAdded    DiffType = "added"
	DiffRemoved  DiffType = "removed"
	DiffModified DiffType = "modified"
)

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

type RestoreSnapshotOptions struct {
	DestinationMode storage.RestoreDestinationMode
	Destination     string
	Overwrite       bool
	StrictMetadata  bool
	NoMetadata      bool
	StorageContext  *storage.StorageContext
	// Query is an optional filter applied on top of any path selections.
	// A nil Query matches all entries.
	Query *SnapshotQuery
}

type RestoreSnapshotResult struct {
	SnapshotID    string
	RestoredFiles int64
	// RequestedPaths counts raw CLI input paths before normalization/deduplication.
	RequestedPaths int64
	OutputPaths    []string
}

type SnapshotListFilter struct {
	Type  *string
	Label *string
	Since *time.Time
	Until *time.Time
	Limit int
}

type SnapshotFileEntry struct {
	Path          string
	LogicalFileID int64
	Size          sql.NullInt64
	Mode          sql.NullInt64
	MTime         sql.NullTime
}

type SnapshotStats struct {
	SnapshotCount     int64
	SnapshotFileCount int64
	TotalSizeBytes    int64
	SnapshotID        string
}

type SnapshotDiffEntry struct {
	Path            string        `json:"path"`
	Type            DiffType      `json:"type"` // DiffAdded | DiffRemoved | DiffModified
	BaseLogicalID   sql.NullInt64 `json:"base_logical_id"`
	TargetLogicalID sql.NullInt64 `json:"target_logical_id"`
}

type SnapshotDiffSummary struct {
	Added    int64 `json:"added"`
	Removed  int64 `json:"removed"`
	Modified int64 `json:"modified"`
}

type SnapshotDiffResult struct {
	BaseSnapshotID   string              `json:"base_snapshot_id"`
	TargetSnapshotID string              `json:"target_snapshot_id"`
	Entries          []SnapshotDiffEntry `json:"entries"`
	Summary          SnapshotDiffSummary `json:"summary"`
}

// SnapshotQuery defines optional filter criteria applied to snapshot file entries.
// All set criteria are ANDed together. A nil *SnapshotQuery matches all entries.
// Criteria are evaluated fast-to-slow: exact → prefix → pattern → regex → size → time.
type SnapshotQuery struct {
	// ExactPaths is a set of normalized paths that must match exactly.
	ExactPaths map[string]struct{}
	// Prefixes matches entries whose path has any of the given prefixes.
	Prefixes []string
	// Pattern is a path.Match glob applied to the normalized slash entry path.
	Pattern string
	// Regex is an optional compiled regular expression applied to the entry path.
	Regex *regexp.Regexp
	// MinSize filters out entries whose recorded size is below this threshold.
	// Entries with no recorded size pass this check.
	MinSize *int64
	// MaxSize filters out entries whose recorded size is above this threshold.
	// Entries with no recorded size pass this check.
	MaxSize *int64
	// ModifiedAfter filters out entries whose mtime is before this instant.
	// Entries with no recorded mtime pass this check.
	ModifiedAfter *time.Time
	// ModifiedBefore filters out entries whose mtime is after this instant.
	// Entries with no recorded mtime pass this check.
	ModifiedBefore *time.Time
}

// Match reports whether entry e satisfies all criteria in q.
// A nil query always returns true.
func (q *SnapshotQuery) Match(e SnapshotFileEntry) bool {
	if q == nil {
		return true
	}

	// 1. Exact path match.
	if len(q.ExactPaths) > 0 {
		if _, ok := q.ExactPaths[e.Path]; !ok {
			return false
		}
	}

	// 2. Prefix match.
	if len(q.Prefixes) > 0 {
		matched := false
		for _, p := range q.Prefixes {
			if strings.HasPrefix(e.Path, p) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// 3. Glob pattern match.
	if q.Pattern != "" {
		ok, _ := path.Match(q.Pattern, e.Path)
		if !ok {
			return false
		}
	}

	// 4. Regex match.
	if q.Regex != nil && !q.Regex.MatchString(e.Path) {
		return false
	}

	// 5. Size range. Entries with no recorded size pass both bounds.
	if q.MinSize != nil && e.Size.Valid && e.Size.Int64 < *q.MinSize {
		return false
	}
	if q.MaxSize != nil && e.Size.Valid && e.Size.Int64 > *q.MaxSize {
		return false
	}

	// 6. Time range. Entries with no recorded mtime pass both bounds.
	if q.ModifiedAfter != nil && e.MTime.Valid && e.MTime.Time.Before(*q.ModifiedAfter) {
		return false
	}
	if q.ModifiedBefore != nil && e.MTime.Valid && e.MTime.Time.After(*q.ModifiedBefore) {
		return false
	}

	return true
}

type snapshotRestoreRow struct {
	Path          string
	LogicalFileID int64
	Size          sql.NullInt64
	Mode          sql.NullInt64
	MTime         sql.NullTime
}

type snapshotRestorePlanItem struct {
	Path          string
	LogicalFileID int64
	Mode          sql.NullInt64
	MTime         sql.NullTime
	OutputPath    string
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
		`INSERT INTO snapshot (id, created_at, type, label, parent_id) VALUES ($1, $2, $3, $4, $5)`,
		s.ID,
		s.CreatedAt.UTC(),
		s.Type,
		s.Label,
		s.ParentID,
	)
	if err != nil {
		return fmt.Errorf("insert snapshot id=%s: %w", s.ID, err)
	}

	log.Printf("snapshot: inserted id=%s type=%s", s.ID, s.Type)
	return nil
}

// InsertSnapshotFile inserts a snapshot_file row. The path is normalized before
// insert. snapshot_path is upserted automatically. The logical_file referenced
// by logicalFileID must exist.
func InsertSnapshotFile(ctx context.Context, db *sql.DB, sf SnapshotFile) (int64, error) {
	return insertSnapshotFile(ctx, db, sf)
}

func insertSnapshotFile(ctx context.Context, exec pathResolverDB, sf SnapshotFile) (int64, error) {
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

	pathID, err := ResolveSnapshotPath(ctx, exec, normalizedPath)
	if err != nil {
		return 0, fmt.Errorf("resolve snapshot_path for %q: %w", normalizedPath, err)
	}

	return insertSnapshotFileByPathID(ctx, exec, sf, normalizedPath, pathID)
}

// insertSnapshotFileByPathID inserts a snapshot_file row using an already-resolved
// path_id. normalizedPath is used only for log/error messages.
func insertSnapshotFileByPathID(ctx context.Context, exec sqlExecutor, sf SnapshotFile, normalizedPath string, pathID int64) (int64, error) {
	var id int64
	err := exec.QueryRowContext(
		ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size, mode, mtime)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		sf.SnapshotID,
		pathID,
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

// insertSnapshotFileByPathIDNoReturning inserts a snapshot_file row using an
// already-resolved path_id without needing RETURNING support (SQLite-compatible).
func insertSnapshotFileByPathIDNoReturning(ctx context.Context, exec sqlExecutor, sf SnapshotFile, normalizedPath string, pathID int64) error {
	_, err := exec.ExecContext(
		ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size, mode, mtime)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		sf.SnapshotID,
		pathID,
		sf.LogicalFileID,
		sf.Size,
		sf.Mode,
		sf.MTime,
	)
	if err != nil {
		return fmt.Errorf("insert snapshot_file snapshot_id=%s path=%q: %w", sf.SnapshotID, normalizedPath, err)
	}

	log.Printf("snapshot: inserted snapshot_file snapshot_id=%s path=%q", sf.SnapshotID, normalizedPath)
	return nil
}

func ListSnapshots(ctx context.Context, db *sql.DB, filter SnapshotListFilter) ([]Snapshot, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}

	query := strings.Builder{}
	query.WriteString(`
		SELECT id, created_at, type, label
		FROM snapshot
		WHERE 1 = 1`)

	args := make([]any, 0, 5)
	argPos := 1
	appendArg := func(value any) string {
		args = append(args, value)
		placeholder := fmt.Sprintf("$%d", argPos)
		argPos++
		return placeholder
	}

	if filter.Type != nil && strings.TrimSpace(*filter.Type) != "" {
		query.WriteString(" AND type = " + appendArg(strings.TrimSpace(*filter.Type)))
	}
	if filter.Label != nil && strings.TrimSpace(*filter.Label) != "" {
		query.WriteString(" AND LOWER(label) LIKE LOWER(" + appendArg("%"+strings.TrimSpace(*filter.Label)+"%") + ")")
	}
	if filter.Since != nil {
		sinceUTC := filter.Since.UTC()
		query.WriteString(" AND created_at >= " + appendArg(sinceUTC))
	}
	if filter.Until != nil {
		untilUTC := filter.Until.UTC()
		query.WriteString(" AND created_at <= " + appendArg(untilUTC))
	}

	query.WriteString(" ORDER BY created_at DESC, id DESC")
	if filter.Limit > 0 {
		query.WriteString(" LIMIT " + appendArg(filter.Limit))
	}

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]Snapshot, 0)
	for rows.Next() {
		var item Snapshot
		if err := rows.Scan(&item.ID, &item.CreatedAt, &item.Type, &item.Label); err != nil {
			return nil, fmt.Errorf("scan snapshot list row: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot list rows: %w", err)
	}

	return result, nil
}

func GetSnapshot(ctx context.Context, db *sql.DB, snapshotID string) (*Snapshot, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}
	if strings.TrimSpace(snapshotID) == "" {
		return nil, errors.New("snapshot id cannot be empty")
	}

	var item Snapshot
	err := db.QueryRowContext(ctx, `SELECT id, created_at, type, label FROM snapshot WHERE id = $1`, strings.TrimSpace(snapshotID)).
		Scan(&item.ID, &item.CreatedAt, &item.Type, &item.Label)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		return nil, fmt.Errorf("get snapshot id=%s: %w", snapshotID, err)
	}
	return &item, nil
}

func ListSnapshotFiles(ctx context.Context, db *sql.DB, snapshotID string, limit int, query *SnapshotQuery) ([]SnapshotFileEntry, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}
	if strings.TrimSpace(snapshotID) == "" {
		return nil, errors.New("snapshot id cannot be empty")
	}
	if _, err := GetSnapshot(ctx, db, snapshotID); err != nil {
		return nil, err
	}

	sqlQuery := `
		SELECT sp.path, sf.logical_file_id, sf.size, sf.mode, sf.mtime
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sf.snapshot_id = $1
		ORDER BY sp.path, sf.logical_file_id`

	rows, err := db.QueryContext(ctx, sqlQuery, strings.TrimSpace(snapshotID))
	if err != nil {
		return nil, fmt.Errorf("list snapshot files snapshot_id=%s: %w", snapshotID, err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]SnapshotFileEntry, 0)
	for rows.Next() {
		var item SnapshotFileEntry
		if err := rows.Scan(&item.Path, &item.LogicalFileID, &item.Size, &item.Mode, &item.MTime); err != nil {
			return nil, fmt.Errorf("scan snapshot_file row: %w", err)
		}

		normalizedPath, err := NormalizeSnapshotPath(item.Path)
		if err != nil {
			return nil, fmt.Errorf("normalize snapshot_file path %q: %w", item.Path, err)
		}
		item.Path = normalizedPath

		if !query.Match(item) {
			continue
		}
		result = append(result, item)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot_file rows: %w", err)
	}

	return result, nil
}

func GetSnapshotStats(ctx context.Context, db *sql.DB, snapshotID string) (*SnapshotStats, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}

	stats := &SnapshotStats{SnapshotID: strings.TrimSpace(snapshotID)}
	if stats.SnapshotID == "" {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot`).Scan(&stats.SnapshotCount); err != nil {
			return nil, fmt.Errorf("count snapshots: %w", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM snapshot_file`).Scan(&stats.SnapshotFileCount, &stats.TotalSizeBytes); err != nil {
			return nil, fmt.Errorf("count snapshot files: %w", err)
		}
		return stats, nil
	}

	if _, err := GetSnapshot(ctx, db, stats.SnapshotID); err != nil {
		return nil, err
	}
	stats.SnapshotCount = 1
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM snapshot_file WHERE snapshot_id = $1`, stats.SnapshotID).Scan(&stats.SnapshotFileCount, &stats.TotalSizeBytes); err != nil {
		return nil, fmt.Errorf("count snapshot files snapshot_id=%s: %w", stats.SnapshotID, err)
	}
	return stats, nil
}

// DeleteSnapshot removes only snapshot metadata for snapshotID.
//
// This is intentionally a metadata-only lifecycle event: deleting a snapshot
// removes the snapshot row and its snapshot_file rows, but it does not delete
// logical content directly. The deletion may reduce logical-file reachability,
// which can make content eligible for a later GC pass under the normal
// reachability rules.
func DeleteSnapshot(ctx context.Context, db *sql.DB, snapshotID string) error {
	if db == nil {
		return errors.New("snapshot db cannot be nil")
	}
	if strings.TrimSpace(snapshotID) == "" {
		return errors.New("snapshot id cannot be empty")
	}
	snapshotID = strings.TrimSpace(snapshotID)

	if _, err := GetSnapshot(ctx, db, snapshotID); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin snapshot delete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot_file WHERE snapshot_id = $1`, snapshotID); err != nil {
		return fmt.Errorf("delete snapshot_file rows snapshot_id=%s: %w", snapshotID, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot WHERE id = $1`, snapshotID); err != nil {
		return fmt.Errorf("delete snapshot row id=%s: %w", snapshotID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit snapshot delete transaction: %w", err)
	}

	return nil
}

func loadSnapshotFilesByPath(ctx context.Context, db *sql.DB, snapshotID string) (map[string]SnapshotFileEntry, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT sp.path, sf.logical_file_id, sf.size, sf.mode, sf.mtime
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sf.snapshot_id = $1
	`, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("query snapshot_file rows snapshot_id=%s: %w", snapshotID, err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]SnapshotFileEntry)
	for rows.Next() {
		var entry SnapshotFileEntry
		if err := rows.Scan(&entry.Path, &entry.LogicalFileID, &entry.Size, &entry.Mode, &entry.MTime); err != nil {
			return nil, fmt.Errorf("scan snapshot_file row snapshot_id=%s: %w", snapshotID, err)
		}

		normalizedPath, err := NormalizeSnapshotPath(entry.Path)
		if err != nil {
			return nil, fmt.Errorf("normalize snapshot_file path %q for snapshot_id=%s: %w", entry.Path, snapshotID, err)
		}
		entry.Path = normalizedPath

		if existing, exists := result[normalizedPath]; exists && existing.LogicalFileID != entry.LogicalFileID {
			return nil, fmt.Errorf("duplicate normalized path %q in snapshot %s with conflicting logical IDs %d and %d", normalizedPath, snapshotID, existing.LogicalFileID, entry.LogicalFileID)
		}
		result[normalizedPath] = entry
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot_file rows snapshot_id=%s: %w", snapshotID, err)
	}

	return result, nil
}

// DiffSnapshots computes the diff between two snapshots identified by baseID and targetID.
// An optional query filters the diff entries after classification (added/removed/modified).
// The summary counts only the entries that pass the filter.
//
// Note: both snapshots are loaded fully into memory (O(N) memory, O(N log N) sort).
// This is acceptable for typical workloads in v1.x. A future streaming diff implementation
// may be introduced for very large snapshot sizes (e.g. millions of files).
func DiffSnapshots(ctx context.Context, db *sql.DB, baseID, targetID string, query *SnapshotQuery) (*SnapshotDiffResult, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}
	baseID = strings.TrimSpace(baseID)
	targetID = strings.TrimSpace(targetID)
	if baseID == "" {
		return nil, errors.New("base snapshot id cannot be empty")
	}
	if targetID == "" {
		return nil, errors.New("target snapshot id cannot be empty")
	}

	if _, err := GetSnapshot(ctx, db, baseID); err != nil {
		return nil, err
	}
	if _, err := GetSnapshot(ctx, db, targetID); err != nil {
		return nil, err
	}

	baseRows, err := loadSnapshotFilesByPath(ctx, db, baseID)
	if err != nil {
		return nil, err
	}
	targetRows, err := loadSnapshotFilesByPath(ctx, db, targetID)
	if err != nil {
		return nil, err
	}

	allPaths := make(map[string]struct{}, len(baseRows)+len(targetRows))
	for path := range baseRows {
		allPaths[path] = struct{}{}
	}
	for path := range targetRows {
		allPaths[path] = struct{}{}
	}

	paths := make([]string, 0, len(allPaths))
	for path := range allPaths {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	entries := make([]SnapshotDiffEntry, 0, len(paths))
	summary := SnapshotDiffSummary{}

	for _, path := range paths {
		baseEntry, baseExists := baseRows[path]
		targetEntry, targetExists := targetRows[path]

		entry := SnapshotDiffEntry{Path: path}
		if baseExists {
			entry.BaseLogicalID = sql.NullInt64{Int64: baseEntry.LogicalFileID, Valid: true}
		}
		if targetExists {
			entry.TargetLogicalID = sql.NullInt64{Int64: targetEntry.LogicalFileID, Valid: true}
		}

		// Classify the entry BEFORE applying the query filter.
		switch {
		case !baseExists && targetExists:
			entry.Type = DiffAdded
		case baseExists && !targetExists:
			entry.Type = DiffRemoved
		case baseExists && targetExists:
			if baseEntry.LogicalFileID == targetEntry.LogicalFileID {
				continue
			}
			entry.Type = DiffModified
		default:
			continue
		}

		// Apply query filter AFTER classification using the target-side metadata for
		// added/modified entries and base-side metadata for removed entries.
		if query != nil {
			fe := targetEntry
			if entry.Type == DiffRemoved {
				fe = baseEntry
			}
			if !query.Match(fe) {
				continue
			}
		}

		switch entry.Type {
		case DiffAdded:
			summary.Added++
		case DiffRemoved:
			summary.Removed++
		case DiffModified:
			summary.Modified++
		}
		entries = append(entries, entry)
	}

	return &SnapshotDiffResult{
		BaseSnapshotID:   baseID,
		TargetSnapshotID: targetID,
		Entries:          entries,
		Summary:          summary,
	}, nil
}

func normalizeSourcePathForSnapshot(path string) (string, error) {
	normalized := strings.ReplaceAll(path, "\\", "/")
	for strings.HasPrefix(normalized, "/") {
		normalized = normalized[1:]
	}
	return NormalizeSnapshotPath(normalized)
}

func normalizeSnapshotRestoreInputFilters(paths []string) (exactFilters []string, dirPrefixes []string, exactSet map[string]struct{}, err error) {
	seenInput := make(map[string]struct{})
	exactSet = make(map[string]struct{})

	for _, rawPath := range paths {
		normalizedPath, normErr := NormalizeSnapshotPath(rawPath)
		if normErr != nil {
			return nil, nil, nil, fmt.Errorf("normalize input path %q: %w", rawPath, normErr)
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
		exactSet[normalizedPath] = struct{}{}
	}

	sort.Strings(exactFilters)
	sort.Strings(dirPrefixes)
	return exactFilters, dirPrefixes, exactSet, nil
}

func resolveSnapshotRestoreSelection(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	requestedPaths []string,
	query *SnapshotQuery,
) ([]snapshotRestoreRow, []string, error) {
	var snapshotExists int
	if err := db.QueryRowContext(ctx, `SELECT 1 FROM snapshot WHERE id = $1`, snapshotID).Scan(&snapshotExists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		return nil, nil, fmt.Errorf("check snapshot existence id=%s: %w", snapshotID, err)
	}

	exactFilters, dirPrefixes, exactSet, err := normalizeSnapshotRestoreInputFilters(requestedPaths)
	if err != nil {
		return nil, nil, err
	}

	rows, err := db.QueryContext(ctx, `
		SELECT sp.path, sf.logical_file_id, sf.size, sf.mode, sf.mtime
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sf.snapshot_id = $1
		ORDER BY sp.path, sf.logical_file_id
	`, snapshotID)
	if err != nil {
		return nil, nil, fmt.Errorf("query snapshot rows for restore snapshot_id=%s: %w", snapshotID, err)
	}
	defer func() { _ = rows.Close() }()

	selected := make([]snapshotRestoreRow, 0)
	seenSelectedPaths := make(map[string]struct{})
	foundExact := make(map[string]struct{})

	hasFilters := len(exactFilters) > 0 || len(dirPrefixes) > 0

	for rows.Next() {
		var row snapshotRestoreRow
		if err := rows.Scan(&row.Path, &row.LogicalFileID, &row.Size, &row.Mode, &row.MTime); err != nil {
			return nil, nil, fmt.Errorf("scan snapshot restore row: %w", err)
		}

		normalizedPath, normErr := NormalizeSnapshotPath(row.Path)
		if normErr != nil {
			return nil, nil, fmt.Errorf("normalize snapshot_file path %q: %w", row.Path, normErr)
		}
		row.Path = normalizedPath

		if hasFilters {
			matched := false
			if _, isExact := exactSet[row.Path]; isExact {
				foundExact[row.Path] = struct{}{}
				matched = true
			}
			if !matched {
				for _, prefix := range dirPrefixes {
					// SAFETY INVARIANT: All dirPrefixes end with "/" (enforced by
					// normalizeSnapshotRestoreInputFilters). This ensures directory boundary
					// correctness: "docs/" matches "docs/file.txt" but NOT "docs_backup/file.txt".
					// HasPrefix is safe because the "/" separator is present in the prefix.
					if strings.HasPrefix(row.Path, prefix) {
						matched = true
						break
					}
				}
			}
			if !matched {
				continue
			}
		}

		// Apply SnapshotQuery as an additional in-memory filter on top of path selections.
		if query != nil {
			fe := SnapshotFileEntry(row)
			if !query.Match(fe) {
				continue
			}
		}

		if _, exists := seenSelectedPaths[row.Path]; exists {
			continue
		}
		seenSelectedPaths[row.Path] = struct{}{}
		selected = append(selected, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate snapshot restore rows: %w", err)
	}

	for _, exactPath := range exactFilters {
		if _, ok := foundExact[exactPath]; !ok {
			return nil, nil, fmt.Errorf("path not found in snapshot %s: %s", snapshotID, exactPath)
		}
	}

	outputExactFilters := make([]string, len(exactFilters))
	copy(outputExactFilters, exactFilters)
	return selected, outputExactFilters, nil
}

func planSnapshotRestoreOutputs(rows []snapshotRestoreRow, requestedPaths []string, opts RestoreSnapshotOptions) ([]snapshotRestorePlanItem, error) {
	mode := opts.DestinationMode
	if mode == "" {
		mode = storage.RestoreDestinationOriginal
	}

	if opts.StrictMetadata && opts.NoMetadata {
		return nil, errors.New("--strict and --no-metadata cannot be used together")
	}

	if mode == storage.RestoreDestinationOriginal && strings.TrimSpace(opts.Destination) != "" {
		return nil, errors.New("destination is only supported with prefix or override mode")
	}
	if (mode == storage.RestoreDestinationPrefix || mode == storage.RestoreDestinationOverride) && strings.TrimSpace(opts.Destination) == "" {
		return nil, fmt.Errorf("destination is required with mode %s", mode)
	}

	if mode == storage.RestoreDestinationOverride {
		if len(requestedPaths) != 1 || strings.HasSuffix(requestedPaths[0], "/") {
			return nil, errors.New("override mode is only allowed for single exact-path snapshot restore")
		}
		if len(rows) != 1 {
			return nil, errors.New("override mode requires exactly one matched snapshot file")
		}
	}

	plans := make([]snapshotRestorePlanItem, 0, len(rows))
	seenOutput := make(map[string]string)

	for _, row := range rows {
		var outputPath string
		switch mode {
		case storage.RestoreDestinationOriginal:
			// Snapshot path is already normalized and relative.
			outputPath = row.Path
		case storage.RestoreDestinationPrefix:
			prefix := strings.TrimSpace(opts.Destination)
			absPrefix, err := filepath.Abs(prefix)
			if err != nil {
				return nil, fmt.Errorf("resolve prefix destination: %w", err)
			}
			outputPath = filepath.Join(absPrefix, filepath.FromSlash(row.Path))
		case storage.RestoreDestinationOverride:
			overridePath := strings.TrimSpace(opts.Destination)
			absOverride, err := filepath.Abs(overridePath)
			if err != nil {
				return nil, fmt.Errorf("resolve override destination: %w", err)
			}
			outputPath = filepath.Clean(absOverride)
		default:
			return nil, fmt.Errorf("unsupported restore destination mode: %s", mode)
		}

		cleanOutputPath := filepath.Clean(outputPath)
		if firstPath, exists := seenOutput[cleanOutputPath]; exists {
			return nil, fmt.Errorf("restore output path collision: snapshot paths %q and %q map to %s", firstPath, row.Path, cleanOutputPath)
		}
		seenOutput[cleanOutputPath] = row.Path

		plans = append(plans, snapshotRestorePlanItem{
			Path:          row.Path,
			LogicalFileID: row.LogicalFileID,
			Mode:          row.Mode,
			MTime:         row.MTime,
			OutputPath:    cleanOutputPath,
		})
	}

	for _, plan := range plans {
		if !opts.Overwrite {
			if _, err := os.Stat(plan.OutputPath); err == nil {
				return nil, fmt.Errorf("output file already exists: %s (use --overwrite)", plan.OutputPath)
			} else if !os.IsNotExist(err) {
				return nil, fmt.Errorf("check output path %s: %w", plan.OutputPath, err)
			}
		}
	}

	return plans, nil
}

func applySnapshotMetadata(outputPath string, mode sql.NullInt64, mtime sql.NullTime, opts RestoreSnapshotOptions) error {
	if opts.NoMetadata {
		return nil
	}

	metadataErrs := make([]string, 0)

	if mode.Valid {
		if err := os.Chmod(outputPath, os.FileMode(mode.Int64)); err != nil {
			metadataErrs = append(metadataErrs, fmt.Sprintf("chmod: %v", err))
		}
	}

	if mtime.Valid {
		mt := mtime.Time
		if err := os.Chtimes(outputPath, mt, mt); err != nil {
			metadataErrs = append(metadataErrs, fmt.Sprintf("chtimes: %v", err))
		}
	}

	if len(metadataErrs) == 0 {
		return nil
	}

	metadataErr := fmt.Errorf("apply snapshot metadata for %q: %s", outputPath, strings.Join(metadataErrs, "; "))
	if opts.StrictMetadata {
		return metadataErr
	}
	log.Printf("snapshot: restore metadata warning path=%q error=%q", outputPath, metadataErr.Error())
	return nil
}

func executeSnapshotRestorePlan(ctx context.Context, plans []snapshotRestorePlanItem, opts RestoreSnapshotOptions) (*RestoreSnapshotResult, error) {
	if opts.StorageContext == nil {
		return nil, errors.New("storage context is required for snapshot restore")
	}

	result := &RestoreSnapshotResult{
		RestoredFiles: int64(0),
		OutputPaths:   make([]string, 0, len(plans)),
	}

	validatedDirs := make(map[string]struct{})
	for _, plan := range plans {
		dir := filepath.Dir(plan.OutputPath)
		if _, seen := validatedDirs[dir]; seen {
			continue
		}
		validatedDirs[dir] = struct{}{}

		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create destination directory %s: %w", dir, err)
		}
	}

	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		restoreResult, err := storage.RestoreFileWithStorageContextResultOptions(
			*opts.StorageContext,
			plan.LogicalFileID,
			plan.OutputPath,
			storage.RestoreOptions{Overwrite: opts.Overwrite, NoMetadata: true},
		)
		if err != nil {
			return nil, fmt.Errorf("restore snapshot path %q logical_file_id=%d: %w", plan.Path, plan.LogicalFileID, err)
		}

		if err := applySnapshotMetadata(restoreResult.OutputPath, plan.Mode, plan.MTime, opts); err != nil {
			return nil, err
		}

		result.RestoredFiles++
		result.OutputPaths = append(result.OutputPaths, restoreResult.OutputPath)
	}

	return result, nil
}

func RestoreSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	paths []string,
	opts RestoreSnapshotOptions,
) (*RestoreSnapshotResult, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}
	if strings.TrimSpace(snapshotID) == "" {
		return nil, errors.New("snapshot id cannot be empty")
	}
	if opts.StorageContext == nil {
		return nil, errors.New("storage context is required")
	}

	selected, normalizedExactPaths, err := resolveSnapshotRestoreSelection(ctx, db, snapshotID, paths, opts.Query)
	if err != nil {
		return nil, err
	}

	plans, err := planSnapshotRestoreOutputs(selected, normalizedExactPaths, opts)
	if err != nil {
		return nil, err
	}

	result, err := executeSnapshotRestorePlan(ctx, plans, opts)
	if err != nil {
		return nil, err
	}

	result.SnapshotID = snapshotID
	result.RequestedPaths = int64(len(paths))
	return result, nil
}

// CreateSnapshot creates an atomic point-in-time snapshot from current physical_file rows.
// When paths is nil or empty, all physical_file rows are copied into the snapshot.
// When paths is non-empty, rows are filtered by exact paths and directory prefixes ending with '/'.
//
// parentID is optional and stored internally for future lineage tracking; pass nil for all
// current callers. It is not yet surfaced to the CLI.
func CreateSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	snapshotType string,
	label *string,
	parentID *string,
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
	if parentID != nil && strings.TrimSpace(*parentID) != "" {
		s.ParentID = sql.NullString{String: strings.TrimSpace(*parentID), Valid: true}
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

	type pendingSnapshotFile struct {
		normalizedPath string
		logicalFileID  int64
		totalSize      int64
		mode           sql.NullInt64
		mtime          sql.NullTime
	}
	pending := make([]pendingSnapshotFile, 0, 128)

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
		pending = append(pending, pendingSnapshotFile{
			normalizedPath: normalizedPath,
			logicalFileID:  logicalFileID,
			totalSize:      totalSize,
			mode:           mode,
			mtime:          mtime,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshot source rows: %w", err)
	}

	for _, exactPath := range exactFilters {
		if _, ok := foundExact[exactPath]; !ok {
			return fmt.Errorf("path not found in current state: %s", exactPath)
		}
	}

	// Batch-resolve all unique snapshot paths to their snapshot_path IDs.
	allPaths := make([]string, 0, len(pending))
	for _, entry := range pending {
		allPaths = append(allPaths, entry.normalizedPath)
	}
	pathIDs, err := ResolveSnapshotPaths(ctx, tx, allPaths)
	if err != nil {
		return fmt.Errorf("resolve snapshot_path ids for snapshot %s: %w", snapshotID, err)
	}

	insertedCount := 0
	for _, entry := range pending {
		pathID, ok := pathIDs[entry.normalizedPath]
		if !ok {
			return fmt.Errorf("no path_id resolved for %q in snapshot %s", entry.normalizedPath, snapshotID)
		}
		err = insertSnapshotFileByPathIDNoReturning(ctx, tx, SnapshotFile{
			SnapshotID:    snapshotID,
			LogicalFileID: entry.logicalFileID,
			Size:          sql.NullInt64{Int64: entry.totalSize, Valid: true},
			Mode:          entry.mode,
			MTime:         entry.mtime,
		}, entry.normalizedPath, pathID)
		if err != nil {
			return err
		}
		insertedCount++
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
