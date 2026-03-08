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
	db, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT id, filename, compression_algorithm
		FROM container
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

		tx, err := db.Begin()
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
			WHERE id = $1
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
		_, err = tx.Exec(`DELETE FROM chunk WHERE container_id = $1`, containerID)
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
		containerPath := filepath.Join(container.StorageDir, filename)
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