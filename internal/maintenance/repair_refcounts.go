package maintenance

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
)

// RepairLogicalRefCountsResult captures the outcome of recomputing
// logical_file.ref_count from physical_file rows.
//
// Source of truth: physical_file rows.
// This command is explicit and state-changing; doctor/verify remain detect-only.
type RepairLogicalRefCountsResult struct {
	ScannedLogicalFiles    int64 `json:"scanned_logical_files"`
	UpdatedLogicalFiles    int64 `json:"updated_logical_files"`
	OrphanPhysicalFileRows int64 `json:"orphan_physical_file_rows"`
}

func RepairLogicalRefCountsResultWithDB(dbconn *sql.DB) (result RepairLogicalRefCountsResult, err error) {
	if dbconn == nil {
		return RepairLogicalRefCountsResult{}, fmt.Errorf("db connection is nil")
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return RepairLogicalRefCountsResult{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM physical_file pf
		LEFT JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.id IS NULL
	`).Scan(&result.OrphanPhysicalFileRows); err != nil {
		return RepairLogicalRefCountsResult{}, fmt.Errorf("query orphan physical_file rows before repair: %w", err)
	}
	if result.OrphanPhysicalFileRows > 0 {
		return RepairLogicalRefCountsResult{}, invariants.New(
			invariants.CodeRepairRefusedOrphanRows,
			fmt.Sprintf(
				"ref_count repair refused: orphan physical_file rows=%d",
				result.OrphanPhysicalFileRows,
			),
			nil,
		)
	}

	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM logical_file`).Scan(&result.ScannedLogicalFiles); err != nil {
		return RepairLogicalRefCountsResult{}, fmt.Errorf("count logical_file rows for repair: %w", err)
	}

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM logical_file lf
		WHERE lf.ref_count <> (
			SELECT COUNT(*)
			FROM physical_file pf
			WHERE pf.logical_file_id = lf.id
		)
	`).Scan(&result.UpdatedLogicalFiles); err != nil {
		return RepairLogicalRefCountsResult{}, fmt.Errorf("count logical_file rows needing ref_count repair: %w", err)
	}

	if result.UpdatedLogicalFiles > 0 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE logical_file
			SET ref_count = (
				SELECT COUNT(*)
				FROM physical_file pf
				WHERE pf.logical_file_id = logical_file.id
			)
			WHERE ref_count <> (
				SELECT COUNT(*)
				FROM physical_file pf
				WHERE pf.logical_file_id = logical_file.id
			)
		`); err != nil {
			return RepairLogicalRefCountsResult{}, fmt.Errorf("update logical_file.ref_count from physical_file rows: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return RepairLogicalRefCountsResult{}, err
	}

	return result, nil
}

func RepairLogicalRefCountsResultRun() (RepairLogicalRefCountsResult, error) {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return RepairLogicalRefCountsResult{}, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	return RepairLogicalRefCountsResultWithDB(dbconn)
}
