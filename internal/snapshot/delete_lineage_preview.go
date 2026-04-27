package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// DeleteLineagePreview summarizes snapshot-delete dry-run lineage impact.
type DeleteLineagePreview struct {
	SnapshotID       string
	ParentID         sql.NullString
	ParentMissing    bool
	ChildSnapshotIDs []string
	TotalFiles       int64
	UniqueFiles      int64
	SharedFiles      int64
}

// LoadDeleteLineagePreview loads lineage and file sharing preview data for snapshot dry-run delete.
func LoadDeleteLineagePreview(ctx context.Context, dbconn *sql.DB, snapshotID string) (*DeleteLineagePreview, error) {
	if dbconn == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	trimmedID := strings.TrimSpace(snapshotID)
	if trimmedID == "" {
		return nil, errors.New("snapshot id cannot be empty")
	}

	preview := &DeleteLineagePreview{}
	if err := dbconn.QueryRowContext(ctx, `SELECT id, parent_id FROM snapshot WHERE id = $1`, trimmedID).Scan(&preview.SnapshotID, &preview.ParentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("snapshot %q not found", trimmedID)
		}
		return nil, fmt.Errorf("load snapshot delete preview snapshot_id=%s: %w", trimmedID, err)
	}
	if preview.ParentID.Valid {
		var parentExists bool
		if err := dbconn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM snapshot WHERE id = $1)`, preview.ParentID.String).Scan(&parentExists); err != nil {
			return nil, fmt.Errorf("load snapshot delete preview parent existence snapshot_id=%s parent_id=%s: %w", trimmedID, preview.ParentID.String, err)
		}
		preview.ParentMissing = !parentExists
	}

	rows, err := dbconn.QueryContext(ctx, `SELECT id FROM snapshot WHERE parent_id = $1 ORDER BY id`, trimmedID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot delete preview children snapshot_id=%s: %w", trimmedID, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var childID string
		if err := rows.Scan(&childID); err != nil {
			return nil, fmt.Errorf("scan snapshot delete preview child row snapshot_id=%s: %w", trimmedID, err)
		}
		preview.ChildSnapshotIDs = append(preview.ChildSnapshotIDs, childID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot delete preview child rows snapshot_id=%s: %w", trimmedID, err)
	}

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = $1`, trimmedID).Scan(&preview.TotalFiles); err != nil {
		return nil, fmt.Errorf("load snapshot delete preview count total files snapshot_id=%s: %w", trimmedID, err)
	}

	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM snapshot_file sf
		WHERE sf.snapshot_id = $1
		AND NOT EXISTS (
			SELECT 1
			FROM snapshot_file sf2
			WHERE sf2.path_id = sf.path_id
			  AND sf2.logical_file_id = sf.logical_file_id
			  AND sf2.snapshot_id != sf.snapshot_id
		)
	`, trimmedID).Scan(&preview.UniqueFiles); err != nil {
		return nil, fmt.Errorf("load snapshot delete preview count unique files snapshot_id=%s: %w", trimmedID, err)
	}

	preview.SharedFiles = preview.TotalFiles - preview.UniqueFiles
	return preview, nil
}
