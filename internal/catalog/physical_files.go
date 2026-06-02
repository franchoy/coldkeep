package catalog

import (
	"context"
	"database/sql"
	"fmt"
)

// FindPhysicalFilesForLogicalFile implements PhysicalFileCatalog.
// Returns the physical files mapping to the given logical file ID, ordered by
// path. Returns an empty slice (not nil error) when none exist.
func (s *Service) FindPhysicalFilesForLogicalFile(ctx context.Context, logicalFileID int64) ([]PhysicalFileRef, error) {
	const q = `
SELECT path, logical_file_id, mode, mtime, is_metadata_complete
FROM physical_file
WHERE logical_file_id = $1
ORDER BY path`

	rows, err := s.db.QueryContext(ctx, q, logicalFileID)
	if err != nil {
		return nil, fmt.Errorf("catalog: find physical files for logical file %d: %w", logicalFileID, err)
	}
	defer func() { _ = rows.Close() }()

	var refs []PhysicalFileRef
	for rows.Next() {
		var (
			ref                PhysicalFileRef
			mode               sql.NullInt64
			mtime              sql.NullTime
			isMetadataComplete bool
		)
		if err := rows.Scan(&ref.Path, &ref.LogicalFileID, &mode, &mtime, &isMetadataComplete); err != nil {
			return nil, fmt.Errorf("catalog: scan physical file row: %w", err)
		}
		if mode.Valid {
			ref.Mode = int(mode.Int64)
		}
		if mtime.Valid {
			t := mtime.Time.UTC()
			ref.MTime = &t
		}
		ref.IsMetadataComplete = isMetadataComplete
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate physical file rows: %w", err)
	}
	return refs, nil
}
