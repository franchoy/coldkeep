package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
)

// LogicalFileInfo is lightweight metadata used by batch planning.
type LogicalFileInfo struct {
	FileID       int64
	OriginalName string
	Status       string
	ChunkerVersion chunk.Version
}

// GetLogicalFileInfoWithDB returns logical file metadata for a given ID.
func GetLogicalFileInfoWithDB(dbconn *sql.DB, fileID int64) (LogicalFileInfo, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	var info LogicalFileInfo
	if err := dbconn.QueryRowContext(
		ctx,
		`SELECT id, original_name, status, chunker_version FROM logical_file WHERE id = $1`,
		fileID,
	).Scan(&info.FileID, &info.OriginalName, &info.Status, &info.ChunkerVersion); err != nil {
		if err == sql.ErrNoRows {
			return LogicalFileInfo{}, err
		}
		return LogicalFileInfo{}, fmt.Errorf("query logical_file info for %d: %w", fileID, err)
	}

	return info, nil
}
