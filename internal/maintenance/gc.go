package maintenance

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
)

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

	// Attempt to acquire advisory lock to ensure only one GC runs at a time
	var locked bool

	err = dbconn.QueryRow("SELECT pg_try_advisory_lock($1)", gcAdvisoryLockID).Scan(&locked)
	if err != nil {
		return GCResult{}, fmt.Errorf("failed to attempt advisory lock: %w", err)
	}

	if !locked {
		return GCResult{}, fmt.Errorf("GC already running (advisory lock held)")
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

		tx, err := dbconn.Begin()
		if err != nil {
			return GCResult{}, err
		}

		// Re-check inside transaction
		var stillEmpty bool
		err = tx.QueryRow(`
			SELECT 
				COALESCE(sealed, false) AND NOT EXISTS (
					SELECT 1
					FROM blocks b
					JOIN chunk ch ON ch.id = b.chunk_id
					WHERE b.container_id = $1
					AND ch.ref_count > 0
				)
			FROM container where id = $1
		`, containerID).Scan(&stillEmpty)
		if err != nil {
			_ = tx.Rollback()
			return GCResult{}, err
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
		_, err = tx.Exec(`
			WITH deleted_blocks AS (
				DELETE FROM blocks
				WHERE container_id = $1
				RETURNING chunk_id
			)
			DELETE FROM chunk c
			USING deleted_blocks db
			WHERE c.id = db.chunk_id
			AND c.ref_count = 0
		`, containerID)
		if err != nil {
			_ = tx.Rollback()
			return GCResult{}, err
		}

		// Delete container row
		_, err = tx.Exec(`DELETE FROM container WHERE id = $1`, containerID)
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
