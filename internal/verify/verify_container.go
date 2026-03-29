package verify

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

func checkContainersFileExistence(dbconn *sql.DB, containersDir string) error {
	// Check that all containers have their files present on disk
	// and that the file sizes match the DB records
	log.Printf("Checking container file existence and size consistency...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`select id, filename, current_size 
				from container 
				where quarantine = false and sealed = true`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query container files: %v", err)
		return fmt.Errorf("failed to query container files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id int
		var filename string
		var currentSize int64
		if err := rows.Scan(&id, &filename, &currentSize); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan container file: %w", err))
			continue
		}
		// Check if the file exists on disk and has the correct size
		if err := checkContainerFile(id, filename, currentSize, containersDir); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("container file check failed for container %d: %w", id, err))
		}
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkContainersFileExistence checks:", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkContainersFileExistence checks", errorCount)
	}
	log.Println(" SUCCESS ")

	return nil
}

func checkContainerFile(id int, filename string, currentSize int64, containersDir string) error {
	// Check if the file exists on disk and has the correct size

	fullPath := filepath.Join(containersDir, filename)

	info, err := os.Stat(fullPath)
	if err != nil {
		return err
	}

	actualSize := info.Size()

	// check if file size matches the DB record
	// Assumes verification runs against a consistent recovered state.
	// During in-flight writes or immediately after a crash (before recovery),
	// filesystem and DB metadata can temporarily diverge.
	if actualSize != currentSize {
		return fmt.Errorf("file size mismatch: expected %d, got %d", currentSize, actualSize)
	}

	return nil
}

func checkChunkContainerConsistency(dbconn *sql.DB) error {
	// Check that all chunks are correctly associated with their containers
	// if blocks.container_id exists for a chunk → chunk.status must be COMPLETED
	log.Printf("Checking chunk-container consistency...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT c.id
							FROM chunk c
							JOIN blocks b ON b.chunk_id = c.id
							WHERE c.status != 'COMPLETED';`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query chunk-container consistency: %v", err)
		return fmt.Errorf("failed to query chunk-container consistency: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan inconsistent chunk: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk with id %d has blocks.container_id but status is not COMPLETED", id))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkChunkContainerConsistency checks:", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkChunkContainerConsistency checks", errorCount)
	}

	log.Println(" SUCCESS ")
	return nil
}

func checkSealedContainersHash(dbconn *sql.DB, containersDir string) error {
	// Check that all sealed containers have a valid hash that matches the file content
	log.Printf("Checking container file hash consistency...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`select id, filename, container_hash
				from container
				where quarantine = false and sealed = true`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query container hashes: %v", err)
		return fmt.Errorf("failed to query container hashes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var totalRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE quarantine = false AND sealed = true`).Scan(&totalRows); err != nil {
		log.Printf("Failed to query total container count: %v", err)
		return fmt.Errorf("failed to query total container count: %w", err)
	}
	var containercount int
	for rows.Next() {
		containercount++
		log.Printf("Checking container %d / %d", containercount, totalRows)
		var id int
		var filename string
		var storedHash string
		if err := rows.Scan(&id, &filename, &storedHash); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan container hash: %w", err))
			continue
		}
		// Check if the file exists on disk and has the correct hash
		if err := container.CheckContainerHashFileInDir(id, filename, storedHash, containersDir); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("container hash check failed for container %d: %w", id, err))
		}
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkSealedContainersHash checks:", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkSealedContainersHash checks", errorCount)
	}
	log.Println(" SUCCESS ")

	return nil
}

func checkContainerCompleteness(dbconn *sql.DB) error {
	//sealed containers should not accept new chunks
	log.Println("Checking sealed containers for completeness (no new chunks should be added to sealed containers)...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT id
		FROM container
		WHERE sealed = TRUE
		AND EXISTS (
			SELECT 1
			FROM blocks b
			JOIN chunk ch ON ch.id = b.chunk_id
			WHERE b.container_id = container.id
			AND ch.status != 'COMPLETED'
		)`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query container completeness: %v", err)
		return fmt.Errorf("failed to query container completeness: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var containerID int
		if err := rows.Scan(&containerID); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan container info: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("Sealed container with incomplete chunks found: container ID %d has incomplete chunks", containerID))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkContainerCompleteness checks:", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkContainerCompleteness checks", errorCount)
	}
	log.Println(" SUCCESS ")
	return nil

}
