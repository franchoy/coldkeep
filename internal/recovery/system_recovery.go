package recovery

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
)

func SystemRecovery() error {

	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer dbconn.Close()

	err = abortProcessingLogicalFiles(dbconn)
	if err != nil {
		return err
	}
	err = abortProcessingChunks(dbconn)
	if err != nil {
		return err
	}
	err = quarantineMissingContainers(dbconn)
	if err != nil {
		return err
	}
	err = quarantineOrphanContainers(dbconn)
	if err != nil {
		return err
	}

	return nil
}

func abortProcessingLogicalFiles(dbconn *sql.DB) error {

	_, err := dbconn.Exec(`UPDATE logical_file SET status = 'ABORTED' WHERE status = 'PROCESSING' AND updated_at < NOW() - INTERVAL '10 minutes'`)
	if err != nil {
		return fmt.Errorf("query update logical_file to ABORTED: %w", err)
	}
	return nil
}

func abortProcessingChunks(dbconn *sql.DB) error {

	_, err := dbconn.Exec(`UPDATE chunk SET status = 'ABORTED' WHERE status = 'PROCESSING' AND updated_at < NOW() - INTERVAL '10 minutes'`)
	if err != nil {
		return fmt.Errorf("query update chunk to ABORTED: %w", err)
	}
	return nil
}

func quarantineMissingContainers(dbconn *sql.DB) error {

	rows, err := dbconn.Query(`SELECT id, filename FROM container WHERE quarantine = FALSE`)
	if err != nil {
		return fmt.Errorf("query retrieve container list: %w", err)
	}
	defer rows.Close()

	for rows.Next() {

		var id int64
		var filename string

		if err := rows.Scan(&id, &filename); err != nil {
			return err
		}

		path := filepath.Join(container.ContainersDir, filename)

		_, err := os.Stat(path)

		if os.IsNotExist(err) {

			_, err := dbconn.Exec(`UPDATE container SET quarantine = TRUE WHERE id = $1`, id)
			if err != nil {
				return fmt.Errorf("query update container to quarantine due to missing file: %w", err)
			}

		} else if err != nil {
			return fmt.Errorf("stat container file: %w", err)
		}
	}

	return rows.Err()
}

func quarantineOrphanContainers(dbconn *sql.DB) error {

	// recover files in container folder
	entries, err := os.ReadDir(container.ContainersDir)
	if err != nil {
		return fmt.Errorf("read containers dir: %w", err)
	}

	for _, file := range entries {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		// check if a container record exists for this filename
		var exists bool
		err := dbconn.QueryRow(`SELECT EXISTS(SELECT 1 FROM container WHERE filename = $1)`, name).Scan(&exists)
		if err != nil {
			return fmt.Errorf("query check container existence: %w", err)
		}
		if !exists {
			_, err := dbconn.Exec(`INSERT INTO container (filename, quarantine) VALUES ($1, TRUE) ON CONFLICT (filename) DO NOTHING`, name)
			if err != nil {
				return fmt.Errorf("insert orphan container record: %w", err)
			}
		}
	}

	return nil
}
