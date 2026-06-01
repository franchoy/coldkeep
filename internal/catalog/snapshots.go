package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// FindSnapshot implements SnapshotCatalog.
// Returns (nil, nil) when no row exists with the given ID.
func (s *Service) FindSnapshot(ctx context.Context, id string) (*SnapshotRef, error) {
	const q = `
SELECT id, type, COALESCE(label, ''), COALESCE(parent_id, ''), created_at
FROM snapshot
WHERE id = $1`

	row := s.db.QueryRowContext(ctx, q, id)
	ref := &SnapshotRef{}
	var createdAt string
	err := row.Scan(&ref.ID, &ref.Type, &ref.Label, &ref.ParentID, &createdAt)
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
func (s *Service) ListSnapshots(ctx context.Context, filter SnapshotFilter) ([]SnapshotRef, error) {
	var (
		args  []any
		where []string
		n     = 1
	)

	placeholder := func() string {
		p := fmt.Sprintf("$%d", n)
		n++
		return p
	}

	if filter.Type != "" {
		where = append(where, "type = "+placeholder())
		args = append(args, filter.Type)
	}
	if filter.LabelSubstring != "" {
		where = append(where, "label LIKE "+placeholder())
		args = append(args, "%"+filter.LabelSubstring+"%")
	}
	if filter.Since != nil {
		where = append(where, "created_at >= "+placeholder())
		args = append(args, filter.Since.UTC().Format(time.RFC3339Nano))
	}
	if filter.Until != nil {
		where = append(where, "created_at <= "+placeholder())
		args = append(args, filter.Until.UTC().Format(time.RFC3339Nano))
	}

	q := "SELECT id, type, COALESCE(label, ''), COALESCE(parent_id, ''), created_at FROM snapshot"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at DESC"
	if filter.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("catalog: list snapshots: %w", err)
	}
	defer rows.Close()

	var refs []SnapshotRef
	for rows.Next() {
		var ref SnapshotRef
		var createdAt string
		if err := rows.Scan(&ref.ID, &ref.Type, &ref.Label, &ref.ParentID, &createdAt); err != nil {
			return nil, fmt.Errorf("catalog: scan snapshot row: %w", err)
		}
		ref.CreatedAt = parseTimestamp(createdAt)
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate snapshot rows: %w", err)
	}
	return refs, nil
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
