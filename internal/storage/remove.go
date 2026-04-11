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

// RemovePhysicalFileResult contains structured metadata about unlinking
// a current-state physical_file mapping by path.
type RemovePhysicalFileResult struct {
	StoredPath        string `json:"stored_path"`
	LogicalFileID     int64  `json:"logical_file_id"`
	RemainingRefCount int64  `json:"remaining_ref_count"`
	Removed           bool   `json:"removed"`
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

func RemoveFileByStoredPathWithStorageContext(sgctx StorageContext, storedPath string) error {
	_, err := RemoveFileByStoredPathWithStorageContextResult(sgctx, storedPath)
	return err
}

func RemoveFileByStoredPathWithStorageContextResult(sgctx StorageContext, storedPath string) (RemovePhysicalFileResult, error) {
	if sgctx.DB == nil {
		return RemovePhysicalFileResult{}, fmt.Errorf("db connection is nil")
	}

	normalizedPath, err := normalizePhysicalFilePath(storedPath)
	if err != nil {
		return RemovePhysicalFileResult{}, err
	}

	return removePhysicalFileByPathWithDBResult(sgctx.DB, normalizedPath)
}

func removePhysicalFileByPathWithDBResult(dbconn *sql.DB, storedPath string) (result RemovePhysicalFileResult, err error) {
	result.StoredPath = storedPath

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return RemovePhysicalFileResult{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err := removePhysicalFileByPathTx(ctx, dbconn, tx, &result); err != nil {
		return RemovePhysicalFileResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return RemovePhysicalFileResult{}, err
	}

	result.Removed = true
	return result, nil
}

func removePhysicalFileByPathTx(ctx context.Context, dbconn *sql.DB, tx *sql.Tx, result *RemovePhysicalFileResult) error {
	if result == nil {
		return fmt.Errorf("remove result container is nil")
	}

	selectQuery := db.QueryWithOptionalForUpdate(dbconn, `SELECT logical_file_id FROM physical_file WHERE path = $1`)
	if err := tx.QueryRowContext(ctx, selectQuery, result.StoredPath).Scan(&result.LogicalFileID); err != nil {
		if err == sql.ErrNoRows {
			// Path was never stored. This is the "not found" case: the path has no mapping in physical_file.
			return fmt.Errorf("physical_file[%q]: not found (never stored)", result.StoredPath)
		}
		return err
	}

	deleteRes, err := tx.ExecContext(ctx, `DELETE FROM physical_file WHERE path = $1`, result.StoredPath)
	if err != nil {
		return err
	}
	rowsDeleted, err := deleteRes.RowsAffected()
	if err != nil {
		return err
	}
	// Concurrent remove race: the path existed when we SELECT-locked it above, but another
	// transaction deleted it before our DELETE executed. Treat as clean "already removed" (race)
	// rather than a conflict. This distinguishes from "not found" (never existed initially).
	if rowsDeleted == 0 {
		return fmt.Errorf("physical_file[%q]: already removed (concurrent race)", result.StoredPath)
	}
	if rowsDeleted != 1 {
		return fmt.Errorf("physical_file[%q]: unlink conflict (deleted=%d)", result.StoredPath, rowsDeleted)
	}

	if err := tx.QueryRowContext(
		ctx,
		`UPDATE logical_file
		 SET ref_count = ref_count - 1
		 WHERE id = $1 AND ref_count > 0
		 RETURNING ref_count`,
		result.LogicalFileID,
	).Scan(&result.RemainingRefCount); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("invalid logical_file.ref_count transition id=%d", result.LogicalFileID)
		}
		return err
	}

	var actualMappings int64
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`,
		result.LogicalFileID,
	).Scan(&actualMappings); err != nil {
		return err
	}
	if actualMappings != result.RemainingRefCount {
		return fmt.Errorf(
			"logical_file.ref_count invariant mismatch id=%d expected=%d actual=%d",
			result.LogicalFileID,
			result.RemainingRefCount,
			actualMappings,
		)
	}

	return nil
}

// removeAllPhysicalMappingsForLogicalFileTx removes all physical_file rows
// associated with a logical_file, maintaining ref_count invariants.
// This ensures that by the time logical_file is deleted, no physical_file
// rows reference it, maintaining referential integrity across the two removal
// semantics (remove-by-ID and remove-by-stored-path are now symmetric).
//
// Concurrency safety note:
// This function takes a transaction snapshot of all paths for the logical_file,
// then iterates to delete each one. A concurrent INSERT could add a new mapping
// after the SELECT but before we're done deleting.
// This is safe by design because:
//  1. Each per-path removal runs removePhysicalFileByPathTx, which verifies the
//     invariant: logical_file.ref_count == COUNT(physical_file rows for that logical)
//  2. If a new row was somehow added and missed, the concurrent storage operation
//     would increment ref_count, and our final invariant check would catch the mismatch
//  3. Transaction isolation prevents our snapshot from being corrupted by concurrent writes
//
// Result: Invariant-driven safety net ensures correctness even in edge cases.
func removeAllPhysicalMappingsForLogicalFileTx(ctx context.Context, tx *sql.Tx, logicalFileID int64) error {
	// Find all physical_file rows for this logical_file within the transaction snapshot.
	rows, err := tx.QueryContext(
		ctx,
		`SELECT path FROM physical_file WHERE logical_file_id = $1`,
		logicalFileID,
	)
	if err != nil {
		return fmt.Errorf("failed to query physical_file rows for logical_file_id %d: %w", logicalFileID, err)
	}
	defer func() { _ = rows.Close() }()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return fmt.Errorf("failed to scan physical_file path: %w", err)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Remove each physical_file mapping via the standard remove-by-path transaction.
	// This is an O(N) cascade where N = number of physical_file rows for this logical_file.
	//
	// Design rationale (v1.2):
	// - Correctness priority: Each iteration maintains the invariant
	//   logical_file.ref_count == COUNT(physical_file rows) before proceeding
	// - N is typically small (<10 paths per logical file in practice)
	// - Individual transactions provide strong isolation guarantees
	// - Batch delete + single invariant check would be faster but more fragile
	//
	// Future optimization (v1.4+):
	// Could batch delete all paths and validate count once instead of per-path,
	// but current per-path approach is safer for the initial correctness release.
	for _, path := range paths {
		result := &RemovePhysicalFileResult{StoredPath: path}
		if err := removePhysicalFileByPathTx(ctx, nil, tx, result); err != nil {
			return fmt.Errorf("failed to remove physical mapping %q for logical_file_id %d: %w", path, logicalFileID, err)
		}
	}

	return nil
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

	// PHASE 1: Remove all physical_file mappings for this logical_file.
	// This ensures that remove-by-ID and remove-by-stored-path are semantically consistent:
	// both remove physical mappings, maintain ref_count invariants throughout, and leave
	// no orphan physical_file rows pointing to a deleted logical_file.
	if err := removeAllPhysicalMappingsForLogicalFileTx(ctx, tx, fileID); err != nil {
		return RemoveFileResult{}, err
	}

	// PHASE 2: Remove logical_file metadata (file_chunk and logical_file records).
	// At this point, no physical_file rows reference this logical_file, so deletion is safe.

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
