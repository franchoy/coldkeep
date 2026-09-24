package maintenance

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
)

// RepairChunkLiveRefCountsResult captures the outcome of recomputing
// chunk.live_ref_count from completed, current-rooted recipe occurrences.
//
// Source of truth: file_chunk rows whose owning completed logical file has at
// least one current physical mapping. Snapshot membership does not contribute.
// This command is explicit and state-changing; verify/doctor remain detect-first.
type RepairChunkLiveRefCountsResult struct {
	ScannedChunks int64 `json:"scanned_chunks"`
	UpdatedChunks int64 `json:"updated_chunks"`
}

func RepairChunkLiveRefCountsResultWithDB(dbconn *sql.DB) (result RepairChunkLiveRefCountsResult, err error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return RepairChunkLiveRefCountsResultWithDBContext(ctx, dbconn)
}

func RepairChunkLiveRefCountsResultWithDBContext(ctx context.Context, dbconn *sql.DB) (result RepairChunkLiveRefCountsResult, err error) {
	if dbconn == nil {
		return RepairChunkLiveRefCountsResult{}, fmt.Errorf("db connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RepairChunkLiveRefCountsResult{}, err
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return RepairChunkLiveRefCountsResult{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk`).Scan(&result.ScannedChunks); err != nil {
		return RepairChunkLiveRefCountsResult{}, fmt.Errorf("count chunk rows for repair: %w", err)
	}

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk c
		WHERE c.live_ref_count <> (
			SELECT COUNT(*)
			FROM file_chunk fc
			JOIN logical_file lf ON lf.id = fc.logical_file_id
			WHERE fc.chunk_id = c.id
			AND lf.status = 'COMPLETED'
			AND EXISTS (
				SELECT 1 FROM physical_file pf
				WHERE pf.logical_file_id = lf.id
			)
		)
	`).Scan(&result.UpdatedChunks); err != nil {
		return RepairChunkLiveRefCountsResult{}, fmt.Errorf("count chunk rows needing live_ref_count repair: %w", err)
	}

	if result.UpdatedChunks > 0 {
		mutationResult, err := tx.ExecContext(ctx, `
			UPDATE chunk
			SET live_ref_count = (
				SELECT COUNT(*)
				FROM file_chunk fc
				JOIN logical_file lf ON lf.id = fc.logical_file_id
				WHERE fc.chunk_id = chunk.id
				AND lf.status = 'COMPLETED'
				AND EXISTS (
					SELECT 1 FROM physical_file pf
					WHERE pf.logical_file_id = lf.id
				)
			)
			WHERE live_ref_count <> (
				SELECT COUNT(*)
				FROM file_chunk fc
				JOIN logical_file lf ON lf.id = fc.logical_file_id
				WHERE fc.chunk_id = chunk.id
				AND lf.status = 'COMPLETED'
				AND EXISTS (
					SELECT 1 FROM physical_file pf
					WHERE pf.logical_file_id = lf.id
				)
			)
		`)
		if err != nil {
			return RepairChunkLiveRefCountsResult{}, fmt.Errorf("update chunk.live_ref_count from current-rooted recipe rows: %w", err)
		}
		if err := db.RequireRowsAffected(mutationResult, "repair chunk live refcounts", result.UpdatedChunks); err != nil {
			return RepairChunkLiveRefCountsResult{}, fmt.Errorf("update chunk.live_ref_count from current-rooted recipe rows: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return RepairChunkLiveRefCountsResult{}, err
	}

	return result, nil
}

func RepairChunkLiveRefCountsResultRun() (RepairChunkLiveRefCountsResult, error) {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return RepairChunkLiveRefCountsResult{}, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	return RepairChunkLiveRefCountsResultWithDB(dbconn)
}
