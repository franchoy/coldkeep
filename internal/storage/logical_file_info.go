package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
)

// LogicalFileInfo is lightweight metadata used by batch planning.
type LogicalFileInfo struct {
	FileID         int64
	OriginalName   string
	Status         string
	ChunkerVersion chunk.Version
}

// LogicalFileInspectInfo includes inspect-focused metadata for one logical file.
type LogicalFileInspectInfo struct {
	FileID            int64
	OriginalName      string
	Status            string
	ChunkerVersion    chunk.Version
	ChunkCount        int64
	AvgChunkSizeBytes float64
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

	if strings.TrimSpace(string(info.ChunkerVersion)) == "" {
		return LogicalFileInfo{}, fmt.Errorf("logical_file %d has empty chunker_version (repository corruption or incomplete migration)", fileID)
	}

	return info, nil
}

// GetLogicalFileInspectInfoWithDB returns inspect-focused metadata for a given file ID.
func GetLogicalFileInspectInfoWithDB(dbconn *sql.DB, fileID int64) (LogicalFileInspectInfo, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	var info LogicalFileInspectInfo
	if err := dbconn.QueryRowContext(
		ctx,
		`SELECT
			lf.id,
			lf.original_name,
			lf.status,
			lf.chunker_version,
			COALESCE(COUNT(fc.chunk_id), 0) AS chunk_count,
			COALESCE(AVG(c.size), 0) AS avg_chunk_size_bytes
		 FROM logical_file lf
		 LEFT JOIN file_chunk fc ON fc.logical_file_id = lf.id
		 LEFT JOIN chunk c ON c.id = fc.chunk_id
		 WHERE lf.id = $1
		 GROUP BY lf.id, lf.original_name, lf.status, lf.chunker_version`,
		fileID,
	).Scan(
		&info.FileID,
		&info.OriginalName,
		&info.Status,
		&info.ChunkerVersion,
		&info.ChunkCount,
		&info.AvgChunkSizeBytes,
	); err != nil {
		if err == sql.ErrNoRows {
			return LogicalFileInspectInfo{}, err
		}
		return LogicalFileInspectInfo{}, fmt.Errorf("query logical_file inspect info for %d: %w", fileID, err)
	}

	if strings.TrimSpace(string(info.ChunkerVersion)) == "" {
		return LogicalFileInspectInfo{}, fmt.Errorf("logical_file %d has empty chunker_version (repository corruption or incomplete migration)", fileID)
	}

	return info, nil
}
