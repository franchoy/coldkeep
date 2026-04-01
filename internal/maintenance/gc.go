package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
)

var gcAdvisoryUnlock = func(ctx context.Context, dbconn *sql.DB) error {
	_, err := dbconn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", gcAdvisoryLockID)
	return err
}

// GCResult contains structured metadata about a GC run.
type GCResult struct {
	DryRun             bool     `json:"dry_run"`
	AffectedContainers int      `json:"affected_containers"`
	ContainerFilenames []string `json:"container_filenames"`
}

func RunGC(dryRun bool) error {
	_, err := RunGCWithContainersDirResult(dryRun, container.ContainersDir)
	return err
}

func RunGCWithContainersDir(dryRun bool, containersDir string) error {
	_, err := RunGCWithContainersDirResult(dryRun, containersDir)
	return err
}

func RunGCWithContainersDirResult(dryRun bool, containersDir string) (result GCResult, err error) {
	result.DryRun = dryRun

	dbconn, err := db.ConnectDB()
	if err != nil {
		return GCResult{}, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	// Attempt to acquire advisory lock to ensure only one GC runs at a time
	var locked bool

	err = dbconn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", gcAdvisoryLockID).Scan(&locked)
	if err != nil {
		return GCResult{}, fmt.Errorf("failed to attempt advisory lock: %w", err)
	}

	if !locked {
		return GCResult{}, fmt.Errorf("GC already running (advisory lock held)")
	}

	defer func() {
		cleanupCtx, cleanupCancel := db.NewOperationContext(context.Background())
		defer cleanupCancel()
		unlockErr := gcAdvisoryUnlock(cleanupCtx, dbconn)
		if unlockErr != nil {
			log.Printf("warning: failed to release advisory lock: %v\n", unlockErr)
		}
	}()

	rows, err := dbconn.QueryContext(ctx, `
		SELECT id, filename
		FROM container WHERE quarantine = FALSE AND sealed = TRUE 
	`)
	if err != nil {
		return GCResult{}, err
	}
	defer func() { _ = rows.Close() }()

	var affectedContainers int

	for rows.Next() {
		var containerID int64
		var filename string

		if err := rows.Scan(&containerID, &filename); err != nil {
			return GCResult{}, err
		}

		if err := ctx.Err(); err != nil {
			return GCResult{}, err
		}

		tx, err := dbconn.BeginTx(ctx, nil)
		if err != nil {
			return GCResult{}, err
		}

		var stillEmpty bool
		if dryRun {
			// Dry-run keeps a non-blocking check and reports what would be deleted.
			err = tx.QueryRowContext(ctx, `
				SELECT 
					COALESCE(sealed, false) AND NOT EXISTS (
						SELECT 1
						FROM blocks b
						JOIN chunk ch ON ch.id = b.chunk_id
						WHERE b.container_id = $1
						AND (ch.live_ref_count > 0 OR ch.pin_count > 0)
					)
				FROM container where id = $1
			`, containerID).Scan(&stillEmpty)
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, err
			}
		} else {
			// Lock the container row first so status/metadata used for deletion is stable.
			var isSealed bool
			var isQuarantined bool
			err = tx.QueryRowContext(ctx, `
				SELECT COALESCE(sealed, false), COALESCE(quarantine, false)
				FROM container
				WHERE id = $1
				FOR UPDATE
			`, containerID).Scan(&isSealed, &isQuarantined)
			if err == sql.ErrNoRows {
				_ = tx.Rollback()
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, err
			}
			if !isSealed || isQuarantined {
				_ = tx.Rollback()
				continue
			}

			// Lock all chunk rows referenced by this container, then evaluate emptiness.
			err = tx.QueryRowContext(ctx, `
				WITH locked_chunks AS (
					SELECT ch.live_ref_count, ch.pin_count
					FROM blocks b
					JOIN chunk ch ON ch.id = b.chunk_id
					WHERE b.container_id = $1
					FOR UPDATE
				)
				SELECT NOT EXISTS (
					SELECT 1 FROM locked_chunks WHERE live_ref_count > 0 OR pin_count > 0
				)
			`, containerID).Scan(&stillEmpty)
			if err != nil {
				_ = tx.Rollback()
				return GCResult{}, err
			}
		}

		if !stillEmpty {
			_ = tx.Rollback()
			continue
		}

		// If dry-run, rollback transaction and skip file deletion
		// dry-run is just simulation and count
		if dryRun {
			_ = tx.Rollback()
			affectedContainers++
			result.ContainerFilenames = append(result.ContainerFilenames, filename)
			continue
		}

		// Delete location records and then chunk rows linked to this container.
		_, err = tx.ExecContext(ctx, `
			WITH deleted_blocks AS (
				DELETE FROM blocks
				WHERE container_id = $1
				RETURNING chunk_id
			)
			DELETE FROM chunk c
			USING deleted_blocks db
			WHERE c.id = db.chunk_id
			AND c.live_ref_count = 0
			AND c.pin_count = 0
		`, containerID)
		if err != nil {
			_ = tx.Rollback()
			return GCResult{}, err
		}

		// Delete container row
		_, err = tx.ExecContext(ctx, `DELETE FROM container WHERE id = $1`, containerID)
		if err != nil {
			_ = tx.Rollback()
			return GCResult{}, err
		}

		if err := tx.Commit(); err != nil {
			return GCResult{}, err
		}

		// After commit, delete file from disk
		containerPath := filepath.Join(containersDir, filename)

		if err := os.Remove(containerPath); err != nil {
			log.Println("warning: failed to delete container file:", err)
		}

		affectedContainers++
		result.ContainerFilenames = append(result.ContainerFilenames, filename)
	}

	if err := rows.Err(); err != nil {
		return GCResult{}, err
	}

	result.AffectedContainers = affectedContainers
	return result, nil
}
