package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

// RemoveFileResult contains structured metadata about a remove operation.
// Remove is a state-changing path: it deletes logical-file mappings and
// decrements chunk live_ref_count values.
type RemoveFileResult struct {
	FileID          int64 `json:"file_id"`
	RemovedMappings int   `json:"removed_mappings"`
}

func RemoveFile(fileID int64) error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if _, err := RemoveFileWithDBResult(dbconn, fileID); err != nil {
		return err
	}
	return nil
}

func RemoveFileWithDB(dbconn *sql.DB, fileID int64) error {
	_, err := RemoveFileWithDBResult(dbconn, fileID)
	return err
}

func RemoveFileWithDBResult(dbconn *sql.DB, fileID int64) (result RemoveFileResult, err error) {
	result.FileID = fileID
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return RemoveFileResult{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Read status atomically and lock the row when supported to prevent races with in-flight stores.
	statusQuery := db.QueryWithOptionalForUpdate(dbconn, "SELECT status FROM logical_file WHERE id = $1")

	var fileStatus string
	err = tx.QueryRowContext(
		ctx,
		statusQuery,
		fileID,
	).Scan(&fileStatus)
	if err == sql.ErrNoRows {
		return RemoveFileResult{}, fmt.Errorf("file ID %d not found", fileID)
	}
	if err != nil {
		return RemoveFileResult{}, err
	}

	if fileStatus == filestate.LogicalFileProcessing {
		return RemoveFileResult{}, fmt.Errorf("file ID %d is still PROCESSING and cannot be removed", fileID)
	}

	// Get chunk IDs
	rows, err := tx.QueryContext(ctx, `
		SELECT chunk_id
		FROM file_chunk
		WHERE logical_file_id = $1
	`, fileID)
	if err != nil {
		_ = tx.Rollback()
		return RemoveFileResult{}, err
	}
	defer func() { _ = rows.Close() }()

	var chunkIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = tx.Rollback()
			return RemoveFileResult{}, err
		}
		chunkIDs = append(chunkIDs, id)
	}
	result.RemovedMappings = len(chunkIDs)

	// Decrement live_ref_count
	for _, chunkID := range chunkIDs {
		var refCount int64
		err := tx.QueryRowContext(ctx, `
			UPDATE chunk
			SET live_ref_count = live_ref_count - 1
			WHERE id = $1
			AND live_ref_count > 0
			RETURNING live_ref_count
		`, chunkID).Scan(&refCount)

		if err == sql.ErrNoRows {
			_ = tx.Rollback()
			return RemoveFileResult{}, fmt.Errorf("invalid live_ref_count transition for chunk %d", chunkID)
		}
		if err != nil {
			_ = tx.Rollback()
			return RemoveFileResult{}, err
		}
	}

	// Remove mappings
	_, err = tx.ExecContext(ctx, `DELETE FROM file_chunk WHERE logical_file_id = $1`, fileID)
	if err != nil {
		_ = tx.Rollback()
		return RemoveFileResult{}, err
	}

	// Remove logical file
	_, err = tx.ExecContext(ctx, `DELETE FROM logical_file WHERE id = $1`, fileID)
	if err != nil {
		_ = tx.Rollback()
		return RemoveFileResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return RemoveFileResult{}, err
	}

	return result, nil
}
