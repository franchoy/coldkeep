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

	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/iodebug"
	"github.com/franchoy/coldkeep/internal/pathsafe"
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
	// It is stored in the DB and surfaced by snapshot CLI lineage views.
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

// snapshotFileDBRow models the persisted snapshot_file shape.
// This stays internal so higher layers can continue working with path strings.
type snapshotFileDBRow struct {
	SnapshotID    string
	PathID        int64
	LogicalFileID int64
	Size          sql.NullInt64
	Mode          sql.NullInt64
	MTime         sql.NullTime
}

func snapshotSourceQuery(dbconn *sql.DB) string {
	query := `
		SELECT pf.path, pf.logical_file_id, lf.total_size, pf.mode, pf.mtime
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.status = 'COMPLETED'
		ORDER BY pf.path, pf.logical_file_id
	`
	// On PostgreSQL, lock the rows that define the current-state snapshot view so
	// concurrent remove operations cannot delete logical_file/physical_file rows
	// between enumeration and snapshot_file insertion.
	return idb.QueryWithOptionalForUpdate(dbconn, query)
}

func newSnapshotFileDBRow(sf SnapshotFile, pathID int64) snapshotFileDBRow {
	return snapshotFileDBRow{
		SnapshotID:    sf.SnapshotID,
		PathID:        pathID,
		LogicalFileID: sf.LogicalFileID,
		Size:          sf.Size,
		Mode:          sf.Mode,
		MTime:         sf.MTime,
	}
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
	ParentSnapshotID  sql.NullString
	// LineageStatus describes why per-snapshot reuse/new metrics are present or absent.
	// Empty means not applicable (for global stats calls with no snapshot_id).
	LineageStatus   SnapshotLineageStatus
	ReusedFileCount sql.NullInt64
	NewFileCount    sql.NullInt64
	ReuseRatioPct   sql.NullFloat64
}

type SnapshotLineageStatus string

const (
	SnapshotLineageStatusNoParent      SnapshotLineageStatus = "no_parent"
	SnapshotLineageStatusParentMissing SnapshotLineageStatus = "parent_missing"
	SnapshotLineageStatusSkipped       SnapshotLineageStatus = "lineage_skipped"
	SnapshotLineageStatusComputed      SnapshotLineageStatus = "lineage_computed"
)

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

// SnapshotCreateOptions configures snapshot creation.
// ParentID is metadata-only lineage and does not alter snapshot contents.
type SnapshotCreateOptions struct {
	ID       string
	Type     string
	Label    *string
	ParentID *string
	Paths    []string
}

// CreateSnapshotResult reports the committed outcome of an atomic snapshot
// creation mutation.
type CreateSnapshotResult struct {
	SnapshotID    string
	Type          string
	PathsCount    int
	FilesInserted int
	Label         string
	ParentID      string
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
	TrustedRoot   string
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

	if err := pathsafe.ValidateStoredRelativePath(normalized); err != nil {
		return "", fmt.Errorf("snapshot path is invalid: %w", err)
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
	iodebug.IncSnapshotMetadataWrite()

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

	return insertSnapshotFileByPathID(ctx, exec, newSnapshotFileDBRow(sf, pathID), normalizedPath)
}

// insertSnapshotFileByPathID inserts a snapshot_file row using an already-resolved
// path_id. normalizedPath is used only for log/error messages.
func insertSnapshotFileByPathID(ctx context.Context, exec sqlExecutor, row snapshotFileDBRow, normalizedPath string) (int64, error) {
	var id int64
	err := exec.QueryRowContext(
		ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size, mode, mtime)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		row.SnapshotID,
		row.PathID,
		row.LogicalFileID,
		row.Size,
		row.Mode,
		row.MTime,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert snapshot_file snapshot_id=%s path=%q: %w", row.SnapshotID, normalizedPath, err)
	}
	iodebug.IncSnapshotMetadataWrite()

	log.Printf("snapshot: inserted snapshot_file id=%d snapshot_id=%s path=%q", id, row.SnapshotID, normalizedPath)
	return id, nil
}

func insertSnapshotFilesByPathIDNoReturningBatch(ctx context.Context, tx *sql.Tx, rows []snapshotFileDBRow, normalizedPaths []string) error {
	if len(rows) != len(normalizedPaths) {
		return fmt.Errorf("snapshot_file batch rows/paths mismatch: %d rows vs %d paths", len(rows), len(normalizedPaths))
	}
	if len(rows) == 0 {
		return nil
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size, mode, mtime)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
	)
	if err != nil {
		return fmt.Errorf("prepare snapshot_file batch insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for i, row := range rows {
		if _, err := stmt.ExecContext(
			ctx,
			row.SnapshotID,
			row.PathID,
			row.LogicalFileID,
			row.Size,
			row.Mode,
			row.MTime,
		); err != nil {
			return fmt.Errorf("insert snapshot_file snapshot_id=%s path=%q: %w", row.SnapshotID, normalizedPaths[i], err)
		}
		iodebug.IncSnapshotMetadataWrite()
		log.Printf("snapshot: inserted snapshot_file snapshot_id=%s path=%q", row.SnapshotID, normalizedPaths[i])
	}

	return nil
}

func ListSnapshots(ctx context.Context, db *sql.DB, filter SnapshotListFilter) ([]Snapshot, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}

	query := strings.Builder{}
	query.WriteString(`
		SELECT id, created_at, type, label, parent_id
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
		if err := rows.Scan(&item.ID, &item.CreatedAt, &item.Type, &item.Label, &item.ParentID); err != nil {
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
	trimmedID := strings.TrimSpace(snapshotID)
	if trimmedID == "" {
		return nil, errors.New("snapshot id cannot be empty")
	}

	var item Snapshot
	err := db.QueryRowContext(ctx, `SELECT id, created_at, type, label, parent_id FROM snapshot WHERE id = $1`, trimmedID).
		Scan(&item.ID, &item.CreatedAt, &item.Type, &item.Label, &item.ParentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("snapshot %q not found", trimmedID)
		}
		return nil, fmt.Errorf("get snapshot id=%s: %w", trimmedID, err)
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

	snapshotRow, err := GetSnapshot(ctx, db, stats.SnapshotID)
	if err != nil {
		return nil, err
	}
	stats.SnapshotCount = 1
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM snapshot_file WHERE snapshot_id = $1`, stats.SnapshotID).Scan(&stats.SnapshotFileCount, &stats.TotalSizeBytes); err != nil {
		return nil, fmt.Errorf("count snapshot files snapshot_id=%s: %w", stats.SnapshotID, err)
	}

	if snapshotRow.ParentID.Valid {
		if snapshotRow.Type != "full" {
			// Lineage reuse/new analysis is only meaningful for full-to-full comparisons.
			stats.LineageStatus = SnapshotLineageStatusSkipped
			return stats, nil
		}

		var parentType string
		err := db.QueryRowContext(ctx, `SELECT type FROM snapshot WHERE id = $1`, snapshotRow.ParentID.String).Scan(&parentType)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Parent lineage metadata is optional and non-authoritative for stats.
				// If parent no longer exists, skip lineage breakdown and return totals only.
				stats.LineageStatus = SnapshotLineageStatusParentMissing
				return stats, nil
			}
			return nil, fmt.Errorf("check parent snapshot existence snapshot_id=%s parent_id=%s: %w", stats.SnapshotID, snapshotRow.ParentID.String, err)
		}
		if parentType != "full" {
			// Guard against legacy/corrupt full->partial lineage metadata.
			// Parent lineage metadata is optional and non-authoritative for stats.
			// If parent is not full, skip lineage breakdown and return totals only.
			stats.LineageStatus = SnapshotLineageStatusSkipped
			return stats, nil
		}

		stats.ParentSnapshotID = snapshotRow.ParentID

		// Analysis-only lineage breakdown (never used for correctness decisions):
		// reused := count of child rows that match parent on (path_id, logical_file_id),
		// total := child snapshot_file count, new := total - reused.
		// Same path_id but different logical_file_id is treated as changed content,
		// so it is NOT reused and is counted as new.
		// This SQL strategy is intentionally simple and portable for PostgreSQL/SQLite.
		// It relies on existing snapshot_file indexes around snapshot_id/path_id and
		// avoids correctness coupling to lineage metadata.
		// Non-goals for this phase: chunk-level comparison, size-based diff heuristics,
		// or cross-snapshot optimization.
		var reusedCount int64
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM snapshot_file child
			JOIN snapshot_file parent
				ON parent.snapshot_id = $2
				AND parent.path_id = child.path_id
				AND parent.logical_file_id = child.logical_file_id
			WHERE child.snapshot_id = $1
		`, stats.SnapshotID, snapshotRow.ParentID.String).Scan(&reusedCount); err != nil {
			return nil, fmt.Errorf("count reused snapshot files snapshot_id=%s parent_id=%s: %w", stats.SnapshotID, snapshotRow.ParentID.String, err)
		}

		newCount := stats.SnapshotFileCount - reusedCount
		if newCount < 0 {
			newCount = 0
		}

		reuseRatioPct := 0.0
		if stats.SnapshotFileCount > 0 {
			reuseRatioPct = float64(reusedCount) * 100.0 / float64(stats.SnapshotFileCount)
		}

		stats.ReusedFileCount = sql.NullInt64{Int64: reusedCount, Valid: true}
		stats.NewFileCount = sql.NullInt64{Int64: newCount, Valid: true}
		stats.ReuseRatioPct = sql.NullFloat64{Float64: reuseRatioPct, Valid: true}
		stats.LineageStatus = SnapshotLineageStatusComputed
	} else {
		stats.LineageStatus = SnapshotLineageStatusNoParent
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
type DeleteSnapshotResult struct {
	SnapshotID string
	Deleted    bool
}

// DeleteSnapshotWithResult removes only snapshot metadata for snapshotID and
// returns commit-truthful deletion facts.
//
// Deleted is true only after the transaction commits and exactly one snapshot
// row is deleted.
func DeleteSnapshotWithResult(ctx context.Context, db *sql.DB, snapshotID string) (DeleteSnapshotResult, error) {
	if db == nil {
		return DeleteSnapshotResult{}, errors.New("snapshot db cannot be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	normalizedID, err := normalizeDeleteSnapshotID(snapshotID)
	if err != nil {
		return DeleteSnapshotResult{}, err
	}

	if _, err := GetSnapshot(ctx, db, normalizedID); err != nil {
		return DeleteSnapshotResult{}, err
	}

	tx, err := beginDeleteSnapshotTx(ctx, db)
	if err != nil {
		return DeleteSnapshotResult{}, fmt.Errorf("begin snapshot delete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	return deleteSnapshotWithResultInTx(ctx, tx, normalizedID)
}

func DeleteSnapshot(ctx context.Context, db *sql.DB, snapshotID string) error {
	_, err := DeleteSnapshotWithResult(ctx, db, snapshotID)
	return err
}

func normalizeDeleteSnapshotID(snapshotID string) (string, error) {
	normalizedID := strings.TrimSpace(snapshotID)
	if normalizedID == "" {
		return "", errors.New("snapshot id cannot be empty")
	}
	return normalizedID, nil
}

func beginDeleteSnapshotTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

func deleteSnapshotWithResultInTx(ctx context.Context, tx *sql.Tx, snapshotID string) (DeleteSnapshotResult, error) {
	if err := deleteSnapshotMembershipRows(ctx, tx, snapshotID); err != nil {
		return DeleteSnapshotResult{}, err
	}
	if err := deleteSnapshotRecord(ctx, tx, snapshotID); err != nil {
		return DeleteSnapshotResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeleteSnapshotResult{}, fmt.Errorf("commit snapshot delete transaction: %w", err)
	}
	return DeleteSnapshotResult{
		SnapshotID: snapshotID,
		Deleted:    true,
	}, nil
}

func deleteSnapshotMembershipRows(ctx context.Context, tx *sql.Tx, snapshotID string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot_file WHERE snapshot_id = $1`, snapshotID); err != nil {
		return fmt.Errorf("delete snapshot_file rows snapshot_id=%s: %w", snapshotID, err)
	}
	return nil
}

func deleteSnapshotRecord(ctx context.Context, tx *sql.Tx, snapshotID string) error {
	deleteResult, err := tx.ExecContext(ctx, `DELETE FROM snapshot WHERE id = $1`, snapshotID)
	if err != nil {
		return fmt.Errorf("delete snapshot row id=%s: %w", snapshotID, err)
	}
	rowsAffected, err := deleteResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("determine deleted snapshot row count id=%s: %w", snapshotID, err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("delete snapshot row id=%s affected %d rows", snapshotID, rowsAffected)
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

// DiffSnapshotsSummarySQL computes added/removed/modified counts using SQL-only
// path_id/logical_file_id comparisons.
//
// Classification rules:
// - added:    exists in target, missing in base (by path_id)
// - removed:  exists in base, missing in target (by path_id)
// - modified: exists in both by path_id, logical_file_id differs
//
// Correctness rule: diff identity is path identity + logical content identity.
// Size and timestamp metadata are intentionally ignored for change classification;
// logical_file_id is the content-addressed source of truth.
// Non-goals for v1.x diff classification: rename detection, move detection,
// and chunk-level diffing.
//
// This function is portable across PostgreSQL and SQLite and is intended for
// fast summary paths where full entry materialization is unnecessary.
//
// Performance contract (v1.x):
//   - joins/filtering are anchored on snapshot_id/path_id and rely on existing
//     snapshot_file indexes (including unique(snapshot_id, path_id))
//   - expected cost is linear in snapshot cardinality for cold-storage workloads
//   - do not introduce diff caching or precomputed delta state at this stage
func DiffSnapshotsSummarySQL(ctx context.Context, db *sql.DB, baseID, targetID string) (*SnapshotDiffSummary, error) {
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

	var targetParentID sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT parent_id FROM snapshot WHERE id = $1`, targetID).Scan(&targetParentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("snapshot %q not found", targetID)
		}
		return nil, fmt.Errorf("query target snapshot id=%s: %w", targetID, err)
	}

	var baseExists int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot WHERE id = $1`, baseID).Scan(&baseExists); err != nil {
		return nil, fmt.Errorf("check base snapshot id=%s: %w", baseID, err)
	}
	if baseExists == 0 {
		return nil, fmt.Errorf("snapshot %q not found", baseID)
	}

	// Common case optimization marker: direct parent-child diff.
	// We intentionally keep identical SQL summary semantics regardless of this
	// relationship and avoid introducing special delta logic in v1.x.
	_ = targetParentID.Valid && targetParentID.String == baseID

	summary := &SnapshotDiffSummary{}

	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM snapshot_file b
		LEFT JOIN snapshot_file a
			ON a.snapshot_id = $1
			AND a.path_id = b.path_id
		WHERE b.snapshot_id = $2
			AND a.path_id IS NULL
	`, baseID, targetID).Scan(&summary.Added); err != nil {
		return nil, fmt.Errorf("count added diff rows base=%s target=%s: %w", baseID, targetID, err)
	}

	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT path_id
			FROM snapshot_file
			WHERE snapshot_id = $1
		) a
		LEFT JOIN snapshot_file b
			ON b.path_id = a.path_id
			AND b.snapshot_id = $2
		WHERE b.path_id IS NULL
	`, baseID, targetID).Scan(&summary.Removed); err != nil {
		return nil, fmt.Errorf("count removed diff rows base=%s target=%s: %w", baseID, targetID, err)
	}

	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM snapshot_file a
		JOIN snapshot_file b
			ON a.path_id = b.path_id
		WHERE a.snapshot_id = $1
			AND b.snapshot_id = $2
			AND a.logical_file_id != b.logical_file_id
	`, baseID, targetID).Scan(&summary.Modified); err != nil {
		return nil, fmt.Errorf("count modified diff rows base=%s target=%s: %w", baseID, targetID, err)
	}

	return summary, nil
}

// DiffSnapshots computes the diff between two snapshots identified by baseID and targetID.
// An optional query filters the diff entries after classification (added/removed/modified).
// The summary counts only the entries that pass the filter.
// Change classification is based on normalized path identity plus logical_file_id,
// not on size/mode/mtime metadata.
// It intentionally does not attempt rename detection, move detection, or
// chunk-level diff analysis.
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
			return nil, nil, fmt.Errorf("snapshot %q not found", snapshotID)
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
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("resolve current working directory: %w", err)
	}
	cwdTrustedRoot, err := pathsafe.ValidateTrustedRootPath(cwd)
	if err != nil {
		return nil, fmt.Errorf("validate current working directory as trusted root: %w", err)
	}

	for _, row := range rows {
		if err := pathsafe.ValidateStoredRelativePath(row.Path); err != nil {
			return nil, fmt.Errorf("invalid snapshot restore path %q: %w", row.Path, err)
		}

		var outputPath string
		trustedRoot := ""
		switch mode {
		case storage.RestoreDestinationOriginal:
			// Snapshot path is already normalized and relative.
			outputPath = filepath.Clean(filepath.FromSlash(row.Path))
			trustedRoot = cwdTrustedRoot
		case storage.RestoreDestinationPrefix:
			prefix := strings.TrimSpace(opts.Destination)
			trustedRoot, err = pathsafe.ValidateTrustedRootPath(prefix)
			if err != nil {
				return nil, fmt.Errorf("resolve prefix destination: %w", err)
			}
			outputPath, err = pathsafe.SafeJoin(trustedRoot, row.Path)
			if err != nil {
				return nil, fmt.Errorf("resolve prefix destination: %w", err)
			}
		case storage.RestoreDestinationOverride:
			overridePath := strings.TrimSpace(opts.Destination)
			absOverride, err := filepath.Abs(overridePath)
			if err != nil {
				return nil, fmt.Errorf("resolve override destination: %w", err)
			}
			outputPath = filepath.Clean(absOverride)
			trustedRoot, err = pathsafe.NearestExistingAncestorDir(outputPath)
			if err != nil {
				return nil, fmt.Errorf("resolve override destination trusted root: %w", err)
			}
			if err := pathsafe.ValidateWritePathUnderTrustedRoot(trustedRoot, outputPath); err != nil {
				return nil, fmt.Errorf("resolve override destination: %w", err)
			}
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
			TrustedRoot:   trustedRoot,
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
			storage.RestoreOptions{
				Overwrite:   opts.Overwrite,
				TrustedRoot: plan.TrustedRoot,
				NoMetadata:  true,
			},
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

// CreateSnapshotWithOptions creates an atomic point-in-time snapshot from current physical_file rows.
// When opts.Paths is nil or empty, all physical_file rows are copied into the snapshot.
// When opts.Paths is non-empty, rows are filtered by exact paths and directory prefixes ending with '/'.
func CreateSnapshotWithOptionsResult(
	ctx context.Context,
	dbconn *sql.DB,
	opts SnapshotCreateOptions,
) (CreateSnapshotResult, error) {
	req, err := prepareSnapshotCreateOptionsResult(dbconn, opts)
	if err != nil {
		return CreateSnapshotResult{}, err
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return CreateSnapshotResult{}, fmt.Errorf("begin snapshot transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := createSnapshotWithOptionsInTx(ctx, tx, dbconn, req)
	if err != nil {
		return CreateSnapshotResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CreateSnapshotResult{}, fmt.Errorf("commit snapshot transaction: %w", err)
	}
	logSnapshotCreateResult(result)
	return result, nil
}

func createSnapshotWithOptionsInTx(
	ctx context.Context,
	tx *sql.Tx,
	dbconn *sql.DB,
	req snapshotCreateRequest,
) (CreateSnapshotResult, error) {
	snapshotRow, err := buildSnapshotCreateRow(ctx, tx, req)
	if err != nil {
		return CreateSnapshotResult{}, err
	}
	if err := insertSnapshot(ctx, tx, snapshotRow); err != nil {
		return CreateSnapshotResult{}, err
	}

	insertRows, insertPaths, err := buildSnapshotCreateInsertRows(ctx, tx, dbconn, req)
	if err != nil {
		return CreateSnapshotResult{}, err
	}
	if err := insertSnapshotFilesByPathIDNoReturningBatch(ctx, tx, insertRows, insertPaths); err != nil {
		return CreateSnapshotResult{}, err
	}
	return buildSnapshotCreateResult(req, len(insertRows)), nil
}

type snapshotCreateRequest struct {
	snapshotID   string
	snapshotType string
	label        *string
	parentID     *string
	paths        []string
	hasPaths     bool
}

type snapshotCreateFilters struct {
	exactFilters []string
	dirPrefixes  []string
	exactSet     map[string]struct{}
}

type pendingSnapshotFile struct {
	normalizedPath string
	logicalFileID  int64
	totalSize      int64
	mode           sql.NullInt64
	mtime          sql.NullTime
}

func prepareSnapshotCreateOptionsResult(dbconn *sql.DB, opts SnapshotCreateOptions) (snapshotCreateRequest, error) {
	req := snapshotCreateRequest{
		snapshotID:   opts.ID,
		snapshotType: opts.Type,
		label:        opts.Label,
		parentID:     opts.ParentID,
		paths:        opts.Paths,
		hasPaths:     len(opts.Paths) > 0,
	}
	if dbconn == nil {
		return snapshotCreateRequest{}, errors.New("snapshot db cannot be nil")
	}
	if req.snapshotID == "" {
		return snapshotCreateRequest{}, errors.New("snapshot id cannot be empty")
	}
	if err := validateSnapshotCreateTypeRequest(req); err != nil {
		return snapshotCreateRequest{}, err
	}
	return req, nil
}

func validateSnapshotCreateTypeRequest(req snapshotCreateRequest) error {
	if req.snapshotType != "full" && req.snapshotType != "partial" {
		return fmt.Errorf("snapshot type must be 'full' or 'partial', got %q", req.snapshotType)
	}
	if req.hasPaths && req.snapshotType != "partial" {
		return fmt.Errorf("snapshot type must be 'partial' when paths are provided, got %q", req.snapshotType)
	}
	if !req.hasPaths && req.snapshotType != "full" {
		return fmt.Errorf("snapshot type must be 'full' when no paths are provided, got %q", req.snapshotType)
	}
	return nil
}

func buildSnapshotCreateRow(ctx context.Context, tx *sql.Tx, req snapshotCreateRequest) (Snapshot, error) {
	s := Snapshot{
		ID:        req.snapshotID,
		CreatedAt: time.Now().UTC(),
		Type:      req.snapshotType,
	}
	if req.label != nil {
		s.Label = sql.NullString{String: *req.label, Valid: true}
	}
	if err := setSnapshotCreateParent(ctx, tx, req, &s); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}

func setSnapshotCreateParent(ctx context.Context, tx *sql.Tx, req snapshotCreateRequest, s *Snapshot) error {
	trimmedParentID, ok := snapshotCreateParentID(req.parentID)
	if !ok {
		return nil
	}
	if trimmedParentID == req.snapshotID {
		return fmt.Errorf("parent snapshot %q cannot reference itself", trimmedParentID)
	}
	if err := validateSnapshotCreateParentType(req.snapshotType); err != nil {
		return err
	}
	if err := validateSnapshotCreateParentAncestry(ctx, tx, trimmedParentID, req.snapshotID); err != nil {
		return err
	}
	parentType, err := lookupSnapshotCreateParentType(ctx, tx, trimmedParentID)
	if err != nil {
		return err
	}
	if parentType != "full" {
		return fmt.Errorf("parent snapshot %q is partial; --from is currently supported only for full snapshots", trimmedParentID)
	}
	s.ParentID = sql.NullString{String: trimmedParentID, Valid: true}
	return nil
}

func snapshotCreateParentID(parentID *string) (string, bool) {
	if parentID == nil {
		return "", false
	}
	trimmed := strings.TrimSpace(*parentID)
	if trimmed == "" {
		return "", false
	}
	return trimmed, true
}

func validateSnapshotCreateParentType(snapshotType string) error {
	if snapshotType != "full" {
		return errors.New("--from is currently supported only for full snapshots")
	}
	return nil
}

func validateSnapshotCreateParentAncestry(
	ctx context.Context,
	tx *sql.Tx,
	parentID string,
	snapshotID string,
) error {
	hasCycle, err := snapshotAncestorCycleExists(ctx, tx, parentID, snapshotID, 100)
	if err != nil {
		return fmt.Errorf("validate snapshot parent ancestry for %q: %w", parentID, err)
	}
	if hasCycle {
		return fmt.Errorf("parent snapshot %q has a cyclic ancestry; cannot create snapshot with cyclic lineage", parentID)
	}
	return nil
}

func lookupSnapshotCreateParentType(ctx context.Context, tx *sql.Tx, parentID string) (string, error) {
	var parentType string
	err := tx.QueryRowContext(ctx, `SELECT type FROM snapshot WHERE id = $1`, parentID).Scan(&parentType)
	if err == nil {
		return parentType, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("parent snapshot %q not found", parentID)
	}
	return "", fmt.Errorf("validate parent snapshot %q: %w", parentID, err)
}

func buildSnapshotCreateInsertRows(
	ctx context.Context,
	tx *sql.Tx,
	dbconn *sql.DB,
	req snapshotCreateRequest,
) ([]snapshotFileDBRow, []string, error) {
	filters, err := buildSnapshotCreateFilters(req.paths, req.hasPaths)
	if err != nil {
		return nil, nil, err
	}
	pending, err := collectPendingSnapshotCreateFiles(ctx, tx, dbconn, filters)
	if err != nil {
		return nil, nil, err
	}
	return resolveSnapshotCreateInsertRows(ctx, tx, req.snapshotID, pending)
}

func buildSnapshotCreateFilters(paths []string, hasPaths bool) (snapshotCreateFilters, error) {
	filters := snapshotCreateFilters{exactSet: make(map[string]struct{})}
	if !hasPaths {
		return filters, nil
	}

	seenInput := make(map[string]struct{})
	for _, rawPath := range paths {
		normalizedPath, err := NormalizeSnapshotPath(rawPath)
		if err != nil {
			return snapshotCreateFilters{}, fmt.Errorf("normalize input path %q: %w", rawPath, err)
		}
		if _, exists := seenInput[normalizedPath]; exists {
			continue
		}
		seenInput[normalizedPath] = struct{}{}
		if strings.HasSuffix(normalizedPath, "/") {
			filters.dirPrefixes = append(filters.dirPrefixes, normalizedPath)
			continue
		}
		filters.exactFilters = append(filters.exactFilters, normalizedPath)
		filters.exactSet[normalizedPath] = struct{}{}
	}

	sort.Strings(filters.exactFilters)
	sort.Strings(filters.dirPrefixes)
	if len(filters.exactFilters) == 0 && len(filters.dirPrefixes) == 0 {
		return snapshotCreateFilters{}, errors.New("partial snapshot requires at least one valid path filter")
	}
	return filters, nil
}

func collectPendingSnapshotCreateFiles(
	ctx context.Context,
	tx *sql.Tx,
	dbconn *sql.DB,
	filters snapshotCreateFilters,
) ([]pendingSnapshotFile, error) {
	rows, err := tx.QueryContext(ctx, snapshotSourceQuery(dbconn))
	if err != nil {
		return nil, fmt.Errorf("query snapshot source rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	foundExact := make(map[string]struct{})
	seenPaths := make(map[string]struct{})
	pending := make([]pendingSnapshotFile, 0, 128)
	for rows.Next() {
		entry, matched, err := scanPendingSnapshotCreateFile(rows, filters, foundExact)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		if _, duplicate := seenPaths[entry.normalizedPath]; duplicate {
			continue
		}
		seenPaths[entry.normalizedPath] = struct{}{}
		pending = append(pending, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot source rows: %w", err)
	}
	if err := validateSnapshotCreateExactMatches(filters.exactFilters, foundExact); err != nil {
		return nil, err
	}
	return pending, nil
}

func scanPendingSnapshotCreateFile(
	rows *sql.Rows,
	filters snapshotCreateFilters,
	foundExact map[string]struct{},
) (pendingSnapshotFile, bool, error) {
	var (
		path          string
		logicalFileID int64
		totalSize     int64
		mode          sql.NullInt64
		mtime         sql.NullTime
	)
	if err := rows.Scan(&path, &logicalFileID, &totalSize, &mode, &mtime); err != nil {
		return pendingSnapshotFile{}, false, fmt.Errorf("scan snapshot source row: %w", err)
	}

	normalizedPath, err := normalizeSourcePathForSnapshot(path)
	if err != nil {
		return pendingSnapshotFile{}, false, fmt.Errorf("normalize source physical_file path %q: %w", path, err)
	}
	matched := snapshotCreatePathMatches(normalizedPath, filters, foundExact)
	return pendingSnapshotFile{
		normalizedPath: normalizedPath,
		logicalFileID:  logicalFileID,
		totalSize:      totalSize,
		mode:           mode,
		mtime:          mtime,
	}, matched, nil
}

func snapshotCreatePathMatches(
	normalizedPath string,
	filters snapshotCreateFilters,
	foundExact map[string]struct{},
) bool {
	if len(filters.exactFilters) == 0 && len(filters.dirPrefixes) == 0 {
		return true
	}
	if _, isExact := filters.exactSet[normalizedPath]; isExact {
		foundExact[normalizedPath] = struct{}{}
		return true
	}
	for _, prefix := range filters.dirPrefixes {
		if strings.HasPrefix(normalizedPath, prefix) {
			return true
		}
	}
	return false
}

func validateSnapshotCreateExactMatches(exactFilters []string, foundExact map[string]struct{}) error {
	for _, exactPath := range exactFilters {
		if _, ok := foundExact[exactPath]; !ok {
			return fmt.Errorf("path not found in current state: %s", exactPath)
		}
	}
	return nil
}

func resolveSnapshotCreateInsertRows(
	ctx context.Context,
	tx *sql.Tx,
	snapshotID string,
	pending []pendingSnapshotFile,
) ([]snapshotFileDBRow, []string, error) {
	allPaths := make([]string, 0, len(pending))
	for _, entry := range pending {
		allPaths = append(allPaths, entry.normalizedPath)
	}
	pathIDs, err := ResolveSnapshotPaths(ctx, tx, allPaths)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve snapshot_path ids for snapshot %s: %w", snapshotID, err)
	}

	insertRows := make([]snapshotFileDBRow, 0, len(pending))
	insertPaths := make([]string, 0, len(pending))
	for _, entry := range pending {
		pathID, ok := pathIDs[entry.normalizedPath]
		if !ok {
			return nil, nil, fmt.Errorf("no path_id resolved for %q in snapshot %s", entry.normalizedPath, snapshotID)
		}
		insertRows = append(insertRows, snapshotFileDBRow{
			SnapshotID:    snapshotID,
			PathID:        pathID,
			LogicalFileID: entry.logicalFileID,
			Size:          sql.NullInt64{Int64: entry.totalSize, Valid: true},
			Mode:          entry.mode,
			MTime:         entry.mtime,
		})
		insertPaths = append(insertPaths, entry.normalizedPath)
	}
	return insertRows, insertPaths, nil
}

func buildSnapshotCreateResult(req snapshotCreateRequest, insertedCount int) CreateSnapshotResult {
	result := CreateSnapshotResult{
		SnapshotID:    req.snapshotID,
		Type:          req.snapshotType,
		PathsCount:    len(req.paths),
		FilesInserted: insertedCount,
	}
	if req.label != nil {
		result.Label = strings.TrimSpace(*req.label)
	}
	if req.parentID != nil {
		result.ParentID = strings.TrimSpace(*req.parentID)
	}
	return result
}

func logSnapshotCreateResult(result CreateSnapshotResult) {
	if result.FilesInserted == 0 {
		log.Printf("snapshot: created id=%s type=%s files=0 (empty snapshot)", result.SnapshotID, result.Type)
		return
	}
	log.Printf("snapshot: created id=%s type=%s files=%d", result.SnapshotID, result.Type, result.FilesInserted)
}

func CreateSnapshotWithOptions(
	ctx context.Context,
	dbconn *sql.DB,
	opts SnapshotCreateOptions,
) error {
	_, err := CreateSnapshotWithOptionsResult(ctx, dbconn, opts)
	return err
}

// CreateSnapshot is a compatibility wrapper for callers that still use positional arguments.
// snapshotAncestorCycleExists traverses the parent_id chain starting from
// startID and returns true if a cycle is detected (a node repeats) or if
// targetID appears in the chain. maxDepth bounds the traversal; exceeding it
// is treated as a cycle (fail-closed).
func snapshotAncestorCycleExists(ctx context.Context, tx *sql.Tx, startID string, targetID string, maxDepth int) (bool, error) {
	seen := make(map[string]struct{})
	current := startID
	for depth := 0; depth < maxDepth; depth++ {
		if _, ok := seen[current]; ok {
			return true, nil
		}
		if current == targetID {
			return true, nil
		}
		seen[current] = struct{}{}
		var parentID sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT parent_id FROM snapshot WHERE id = $1`, current).Scan(&parentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, fmt.Errorf("traverse snapshot ancestry: %w", err)
		}
		if !parentID.Valid {
			return false, nil
		}
		current = parentID.String
	}
	return true, nil // exceeded maxDepth — fail-closed
}

func CreateSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	snapshotType string,
	label *string,
	parentID *string,
	paths []string,
) error {
	return CreateSnapshotWithOptions(ctx, db, SnapshotCreateOptions{
		ID:       snapshotID,
		Type:     snapshotType,
		Label:    label,
		ParentID: parentID,
		Paths:    paths,
	})
}
