package maintenance

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils"
)

func RunGC() error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer dbconn.Close()

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
		_, _ = dbconn.Exec("SELECT pg_advisory_unlock($1)", gcAdvisoryLockID)
	}()

	rows, err := dbconn.Query(`
		SELECT id, filename, compression_algorithm
		FROM container WHERE quarantine = FALSE 
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var deletedContainers int

	for rows.Next() {
		var containerID int64
		var filename string
		var algo string

		if err := rows.Scan(&containerID, &filename, &algo); err != nil {
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
				sealed AND NOT EXISTS (
					SELECT 1 FROM chunk
					WHERE container_id = $1
					AND ref_count > 0
				)
			FROM container where quarantine = FALSE
			and id = $1
		`, containerID).Scan(&stillEmpty)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		if !stillEmpty {
			_ = tx.Rollback()
			continue
		}

		// Delete chunks
		_, err = tx.Exec(`DELETE FROM chunk WHERE container_id = $1 and status = 'COMPLETED'`, containerID)
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
		if algo != "" && algo != string(utils.CompressionNone) {
			containerPath += "." + algo
		}

		if err := os.Remove(containerPath); err != nil {
			log.Println("warning: failed to delete container file:", err)
		}

		deletedContainers++
		fmt.Println("Deleted container:", filename)
	}

	fmt.Printf("GC completed. Containers deleted: %d\n", deletedContainers)
	return nil
}
