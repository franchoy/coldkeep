package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

// RemoveFileResult contains structured metadata about a remove operation.
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

	// Lock the row and read status atomically to prevent races with in-flight stores.
	var fileStatus string
	err = tx.QueryRowContext(
		ctx,
		"SELECT status FROM logical_file WHERE id = $1 FOR UPDATE",
		fileID,
	).Scan(&fileStatus)
	if err != nil && strings.Contains(err.Error(), `near "FOR": syntax error`) {
		// SQLite does not support FOR UPDATE; retry without explicit row lock.
		err = tx.QueryRowContext(
			ctx,
			"SELECT status FROM logical_file WHERE id = $1",
			fileID,
		).Scan(&fileStatus)
	}
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

	// Decrement ref_count
	for _, chunkID := range chunkIDs {
		var refCount int64
		err := tx.QueryRowContext(ctx, `
			UPDATE chunk
			SET ref_count = ref_count - 1
			WHERE id = $1
			AND ref_count > 0
			RETURNING ref_count
		`, chunkID).Scan(&refCount)

		if err == sql.ErrNoRows {
			_ = tx.Rollback()
			return RemoveFileResult{}, fmt.Errorf("invalid ref_count transition for chunk %d", chunkID)
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
