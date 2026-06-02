package catalog

import (
	"context"
	"database/sql"
	"fmt"
)

// FindLogicalFile implements LogicalFileCatalog.
// Returns (nil, nil) when no row exists with the given ID.
func (s *Service) FindLogicalFile(ctx context.Context, id int64) (*LogicalFileRef, error) {
	const q = `
SELECT id, original_name, total_size, file_hash, ref_count, status
FROM logical_file
WHERE id = $1`

	row := s.db.QueryRowContext(ctx, q, id)
	ref := &LogicalFileRef{}
	err := row.Scan(&ref.ID, &ref.OriginalName, &ref.TotalSize, &ref.FileHash, &ref.RefCount, &ref.Status)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("catalog: find logical file %d: %w", id, err)
	}
	return ref, nil
}
