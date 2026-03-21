package maintenance

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
)

func RunGC(dryRun bool) error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	// Attempt to acquire advisory lock to ensure only one GC runs at a time
	var locked bool

	err = dbconn.QueryRow("SELECT pg_try_advisory_lock($1)", gcAdvisoryLockID).Scan(&locked)
	if err != nil {
		return fmt.Errorf("failed to attempt advisory lock: %w", err)
	}

	if !locked {
		return fmt.Errorf("GC already running (advisory lock held)")
	}

	defer func() {
		_, err = dbconn.Exec("SELECT pg_advisory_unlock($1)", gcAdvisoryLockID)
		if err != nil {
			log.Printf("warning: failed to release advisory lock: %v\n", err)
		}
	}()

	rows, err := dbconn.Query(`
		SELECT id, filename
		FROM container WHERE quarantine = FALSE AND sealed = TRUE 
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var affectedContainers int

	for rows.Next() {
		var containerID int64
		var filename string

		if err := rows.Scan(&containerID, &filename); err != nil {
			return err
		}

		tx, err := dbconn.Begin()
		if err != nil {
			return err
		}

		// Re-check inside transaction
		var stillEmpty bool
		err = tx.QueryRow(`
			SELECT 
				COALESCE(sealed, false) AND NOT EXISTS (
					SELECT 1 FROM chunk
					WHERE container_id = $1
					AND ref_count > 0
				)
			FROM container where id = $1
		`, containerID).Scan(&stillEmpty)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		if !stillEmpty {
			_ = tx.Rollback()
			continue
		}

		// If dry-run, rollback transaction and skip file deletion
		// dry-run is just simulation and count
		if dryRun {
			fmt.Println("[DRY-RUN] Would delete container:", filename)
			_ = tx.Rollback()
			affectedContainers++
			continue
		}

		// Delete chunks
		_, err = tx.Exec(`DELETE FROM chunk WHERE container_id = $1 AND status = 'COMPLETED'`, containerID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		// Delete container row
		_, err = tx.Exec(`DELETE FROM container WHERE id = $1`, containerID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		// After commit, delete file from disk
		containerPath := filepath.Join(container.ContainersDir, filename)

		if err := os.Remove(containerPath); err != nil {
			log.Println("warning: failed to delete container file:", err)
		}

		affectedContainers++
		fmt.Println("Deleted container:", filename)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if affectedContainers == 0 {
		fmt.Println("GC completed. No containers eligible for deletion.")
		return nil
	}

	if dryRun {
		fmt.Printf("GC dry-run completed. Containers eligible for deletion: %d\n", affectedContainers)
	} else {
		fmt.Printf("GC completed. Containers deleted: %d\n", affectedContainers)
	}
	return nil
}
