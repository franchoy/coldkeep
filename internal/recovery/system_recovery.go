package recovery

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"github.com/franchoy/coldkeep/internal/db"
	
)

func SystemRecovery() error {

	db, err := db.Connect()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	err = abortProcessingLogicalFiles(db)
	if err != nil {
		return err
	}
	err = abortProcessingChunks(db)
	if err != nil {
		return err
	}
	err = quarantineMissingContainers(db)
	if err != nil {
		return err
	}
	err = quarantineOrphanContainers(db)
	if err != nil {
		return err
	}

	return nil
}

func abortProcessingLogicalFiles(db *sql.DB) error {

	_, err := db.Exec(`UPDATE logical_file SET status = 'ABORTED' WHERE status = 'PROCESSING' AND updated_at < NOW() - INTERVAL '10 minutes'`)
	if err != nil {
		return fmt.Errorf("query update logical_file to ABORTED: %w", err)
	}
	return nil
}

func abortProcessingChunks(db *sql.DB) error {
	
	_, err := db.Exec(`UPDATE chunk SET status = 'ABORTED' WHERE status = 'PROCESSING' AND updated_at < NOW() - INTERVAL '10 minutes'`)	
	if err != nil {
		return fmt.Errorf("query update chunk to ABORTED: %w", err)
	}
	return nil
}	


func quarantineMissingContainers(db *sql.DB) error {

	rows, err := db.Query(`SELECT id, filename FROM container WHERE quarantine = FALSE`)
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

		path := filepath.Join(containersDir, filename)

		_, err := os.Stat(path)

		if os.IsNotExist(err) {

			_, err := db.Exec(`UPDATE container SET quarantine = TRUE WHERE id = $1`, id)
			if err != nil {
				return fmt.Errorf("query update container to quarantine due to missing file: %w", err)
			}

		} else if err != nil {
			return fmt.Errorf("stat container file: %w", err)
		}
	}

	return rows.Err()
}

func quarantineOrphanContainers(db *sql.DB) error {

	// recover files in container folder
	entries, err := os.ReadDir(containersDir)
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
		err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM container WHERE filename = $1)`, name).Scan(&exists)
		if err != nil {
			return fmt.Errorf("query check container existence: %w", err)
		}
		if !exists {
			_, err := db.Exec(`INSERT INTO container (filename, quarantine) VALUES ($1, TRUE) ON CONFLICT (filename) DO NOTHING`, name)
			if err != nil {
				return fmt.Errorf("insert orphan container record: %w", err)
			}
		}
	}

	return nil
}