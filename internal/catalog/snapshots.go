package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// FindSnapshot implements SnapshotCatalog.
// Returns (nil, nil) when no row exists with the given ID.
func (s *Service) FindSnapshot(ctx context.Context, id string) (ref *SnapshotRef, err error) {
	defer func() {
		err = translateServiceError("find snapshot", "snapshot lookup failed", err)
	}()

	const q = `
SELECT s.id, s.type, COALESCE(s.label, ''), COALESCE(s.parent_id, ''), s.created_at,
       (SELECT COUNT(*) FROM snapshot_file sf WHERE sf.snapshot_id = s.id) AS file_count
FROM snapshot s
WHERE s.id = $1`

	row := s.db.QueryRowContext(ctx, q, id)
	ref = &SnapshotRef{}
	var createdAt string
	err = row.Scan(&ref.ID, &ref.Type, &ref.Label, &ref.ParentID, &createdAt, &ref.FileCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("catalog: find snapshot %q: %w", id, err)
	}
	ref.CreatedAt = parseTimestamp(createdAt)
	return ref, nil
}

// ListSnapshots implements SnapshotCatalog.
// Returns snapshots matching the filter, ordered newest first.
func (s *Service) ListSnapshots(ctx context.Context, filter SnapshotFilter) (refs []SnapshotRef, err error) {
	defer func() {
		err = translateServiceError("list snapshots", "snapshot list failed", err)
	}()

	args := snapshotListArgs(filter)
	if filter.Limit > 0 {
		return s.listSnapshotsWithLimit(ctx, args, filter.Limit)
	}
	return s.listSnapshotsWithoutLimit(ctx, args)
}

type snapshotListQueryArgs struct {
	snapType     string
	labelPattern string
	hasSince     int
	since        time.Time
	hasUntil     int
	until        time.Time
}

func snapshotListArgs(filter SnapshotFilter) snapshotListQueryArgs {
	args := snapshotListQueryArgs{
		snapType:     filter.Type,
		labelPattern: labelPattern(filter.LabelSubstring),
	}
	if filter.Since != nil {
		args.hasSince = 1
		args.since = filter.Since.UTC()
	}
	if filter.Until != nil {
		args.hasUntil = 1
		args.until = filter.Until.UTC()
	}
	return args
}

func labelPattern(labelSubstring string) string {
	if labelSubstring == "" {
		return ""
	}
	return "%" + labelSubstring + "%"
}

func (s *Service) listSnapshotsWithoutLimit(ctx context.Context, args snapshotListQueryArgs) ([]SnapshotRef, error) {
	values := snapshotListQueryValues(args)
	rows, err := s.db.QueryContext(ctx, `
SELECT s.id, s.type, COALESCE(s.label, ''), COALESCE(s.parent_id, ''), s.created_at,
       (SELECT COUNT(*) FROM snapshot_file sf WHERE sf.snapshot_id = s.id) AS file_count
FROM snapshot s
WHERE ($1 = '' OR s.type = $1)
  AND ($2 = '' OR LOWER(s.label) LIKE LOWER($2))
  AND ($3 = 0 OR s.created_at >= $4)
  AND ($5 = 0 OR s.created_at <= $6)
ORDER BY s.created_at DESC, s.id DESC`, values...)
	if err != nil {
		return nil, fmt.Errorf("catalog: list snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanSnapshotRows(rows)
}

func (s *Service) listSnapshotsWithLimit(ctx context.Context, args snapshotListQueryArgs, limit int) ([]SnapshotRef, error) {
	values := append(snapshotListQueryValues(args), limit)
	rows, err := s.db.QueryContext(ctx, `
SELECT s.id, s.type, COALESCE(s.label, ''), COALESCE(s.parent_id, ''), s.created_at,
       (SELECT COUNT(*) FROM snapshot_file sf WHERE sf.snapshot_id = s.id) AS file_count
FROM snapshot s
WHERE ($1 = '' OR s.type = $1)
  AND ($2 = '' OR LOWER(s.label) LIKE LOWER($2))
  AND ($3 = 0 OR s.created_at >= $4)
  AND ($5 = 0 OR s.created_at <= $6)
ORDER BY s.created_at DESC, s.id DESC
LIMIT $7`, values...)
	if err != nil {
		return nil, fmt.Errorf("catalog: list snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanSnapshotRows(rows)
}

func snapshotListQueryValues(args snapshotListQueryArgs) []any {
	return []any{
		args.snapType,
		args.labelPattern,
		args.hasSince,
		args.since,
		args.hasUntil,
		args.until,
	}
}

func scanSnapshotRows(rows *sql.Rows) ([]SnapshotRef, error) {
	var refs []SnapshotRef
	for rows.Next() {
		ref, err := scanSnapshotRow(rows)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate snapshot rows: %w", err)
	}
	return refs, nil
}

func scanSnapshotRow(rows *sql.Rows) (SnapshotRef, error) {
	var ref SnapshotRef
	var createdAt string
	if err := rows.Scan(&ref.ID, &ref.Type, &ref.Label, &ref.ParentID, &createdAt, &ref.FileCount); err != nil {
		return SnapshotRef{}, fmt.Errorf("catalog: scan snapshot row: %w", err)
	}
	ref.CreatedAt = parseTimestamp(createdAt)
	return ref, nil
}

// parseTimestamp parses a DB timestamp string into time.Time.
// It tries RFC3339Nano, RFC3339, and the SQLite DATETIME default format.
// Returns zero time.Time on parse failure rather than propagating an error,
// because a non-null timestamp that fails to parse is a data quality issue,
// not a catalog contract violation.
func parseTimestamp(s string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
