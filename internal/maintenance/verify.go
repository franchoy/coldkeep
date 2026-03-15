package maintenance

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils"
)

func RunVerify(VerifyLevel VerifyLevel) error {
	log.Printf("Starting verification with method: %s", VerifyLevelString(VerifyLevel))
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer dbconn.Close()

	switch VerifyLevel {
	case VerifyStandard:
		// standard verification
	case VerifyFull:
		// full verification
	case VerifyDeep:
		// deep verification
	default:
		return fmt.Errorf("invalid verification level: %s", VerifyLevelString(VerifyLevel))
	}

	//verifymethod
	//	standard
	//		reference count check
	//		orphan chunk check
	//		file chunk ordering check
	//	full
	//		standard checks +
	//			container file existence and size check
	//			chunk-container consistency check
	//			chunk offsets consistency check
	//			chunk offset validity check
	//			checkContainerCompleteness
	//	deep
	//		full checks +
	//			actual file integrity checks (e.g. read container files and verify chunk data against stored hashes)

	//standard checks ini

	var containerCount, chunkCount, fileCount int
	//list container counter to be checked
	err = dbconn.QueryRow("SELECT COUNT(*) FROM container").Scan(&containerCount)
	if err != nil {
		return fmt.Errorf("failed to query container count: %w", err)
	}
	//list chunk counter to be checked
	err = dbconn.QueryRow("SELECT COUNT(*) FROM chunk").Scan(&chunkCount)
	if err != nil {
		return fmt.Errorf("failed to query chunk count: %w", err)
	}
	//list file counter to be checked
	err = dbconn.QueryRow("SELECT COUNT(*) FROM logical_file").Scan(&fileCount)
	if err != nil {
		return fmt.Errorf("failed to query logical file count: %w", err)
	}

	log.Printf("Starting verification: %d containers, %d chunks, %d logical files to check", containerCount, chunkCount, fileCount)

	//check that all chunks have correct reference counts (chunk.ref_count should match the actual number of file_chunk references)
	if err = checkReferenceCounts(dbconn); err != nil {
		return err
	}

	//check that there are no orphan chunks (chunks with ref_count > 0 but no file_chunk references)
	if err = checkOrphanChunks(dbconn); err != nil {
		return err
	}

	//check that file_chunks for each file are ordered by chunk_offset without gaps
	if err = checkFileChunkOrdering(dbconn); err != nil {
		return err
	}

	//standard checks end

	//full checks ini
	if VerifyLevel == VerifyFull || VerifyLevel == VerifyDeep {
		//check that all containers have their files present on disk and that the file sizes match the DB records
		if err = checkContainersFileExistence(dbconn); err != nil {
			return err
		}

		//check that all chunks are correctly associated with their containers (if container_id != NULL → chunk.status must be COMPLETED)
		if err = checkChunkContainerConsistency(dbconn); err != nil {
			return err
		}

		//check that all chunks have location (container_id + chunk_offset) consistent with their status (if status = COMPLETED → container_id NOT NULL chunk_offset NOT NULL, if status != COMPLETED → container_id NULL chunk_offset NULL)
		if err = checkChunkOffsets(dbconn); err != nil {
			return err
		}

		//check that all chunks with status = COMPLETED have valid container_id and chunk_offset values and that the chunk_offset + size does not exceed the container's current_size
		if err = checkChunkOffsetValidity(dbconn); err != nil {
			return err
		}

		//check that sealed containers should not accept new chunks
		if err = checkContainerCompleteness(dbconn); err != nil {
			return err
		}
	}
	//full checks end

	//deep checks ini

	//deep checks end

	log.Printf("Verification completed successfully with method: %s", VerifyLevelString(VerifyLevel))
	return nil
}

func checkContainersFileExistence(dbconn *sql.DB) error {
	// Check that all containers have their files present on disk
	// and that the file sizes match the DB records
	log.Printf("Checking container file existence and size consistency...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`select id, filename, compression_algo, current_size 
				from container 
				where quarantine = false and sealed = true`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query container files: %v", err)
		return fmt.Errorf("failed to query container files: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var filename string
		var compressionalgo string
		var currentSize int64
		if err := rows.Scan(&id, &filename, &compressionalgo, &currentSize); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan container file: %w", err))
			continue
		}
		// Check if the file exists on disk and has the correct size
		if err := checkContainerFile(id, filename, compressionalgo, currentSize); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("container file check failed for container %d: %w", id, err))
		}
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkContainersFileExistence checks:", errorCount)
		if errorCount > maxErrorsToPrint {
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

func checkContainerFile(id int, filename string, compressionalgo string, currentSize int64) error {
	// Check if the file exists on disk and has the correct size
	if compressionalgo != "" && compressionalgo != string(utils.CompressionNone) {
		filename = filename + "." + compressionalgo
	}

	fullPath := filepath.Join(container.ContainersDir, filename)

	info, err := os.Stat(fullPath)
	if err != nil {
		return err
	}

	// check if file exists
	if !info.Mode().IsRegular() {
		return fmt.Errorf("file does not exist or is not a regular file: %s", fullPath)
	}

	// check if file size matches the DB record
	if info.Size() != currentSize {
		return fmt.Errorf("file size mismatch: expected %d, got %d", currentSize, info.Size())
	}

	return nil
}

func checkChunkContainerConsistency(dbconn *sql.DB) error {
	// Check that all chunks are correctly associated with their containers
	// if container_id != NULL → chunk.status must be COMPLETED
	log.Printf("Checking chunk-container consistency...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT id 
							FROM chunk 
							WHERE container_id IS NOT NULL 
							AND status != 'COMPLETED';`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query chunk-container consistency: %v", err)
		return fmt.Errorf("failed to query chunk-container consistency: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan inconsistent chunk: %w", err))
			continue
		}
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("chunk with id %d has container_id but status is not COMPLETED", id))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkChunkContainerConsistency checks:", errorCount)
		if errorCount > maxErrorsToPrint {
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

func checkChunkOffsets(dbconn *sql.DB) error {
	// Check that all chunks have location (container_id + chunk_offset) consistent with their status
	// if status = COMPLETED → container_id NOT NULL chunk_offset NOT NULL

	log.Printf("Checking chunk offsets consistency with status...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT id, container_id, chunk_offset, size, status 
							FROM chunk 
							WHERE status = 'COMPLETED' 
							AND (container_id IS NULL OR chunk_offset IS NULL);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query completed chunks: %v", err)
		return fmt.Errorf("failed to query completed chunks: %w", err)
	}
	defer rows.Close()

	type chunkInfo struct {
		id          int
		containerID int
		chunkOffset int64
		size        int64
		status      string
	}

	for rows.Next() {
		var c chunkInfo
		if err := rows.Scan(&c.id, &c.containerID, &c.chunkOffset, &c.size, &c.status); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan completed chunk: %w", err))
			continue
		}
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("chunk ID %d has status COMPLETED but missing location info: container_id=%v chunk_offset=%v", c.id, c.containerID, c.chunkOffset))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	rows.Close()

	// if status != COMPLETED → container_id NULL chunk_offset NULL
	rows, err = dbconn.Query(`SELECT id, container_id, chunk_offset, size, status 
							FROM chunk 
							WHERE status != 'COMPLETED' 
							AND (container_id IS NOT NULL OR chunk_offset IS NOT NULL);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query non-completed chunks: %v", err)
		return fmt.Errorf("failed to query non-completed chunks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var c chunkInfo
		if err := rows.Scan(&c.id, &c.containerID, &c.chunkOffset, &c.size, &c.status); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan non-completed chunk: %w", err))
			continue
		}
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("chunk ID %d has status %s but has location info: container_id=%v chunk_offset=%v", c.id, c.status, c.containerID, c.chunkOffset))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkChunkOffsets checks : ", errorCount)
		if errorCount > maxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkChunkOffsets checks", errorCount)
	}

	log.Println(" SUCCESS ")
	return nil
}

func checkChunkOffsetValidity(dbconn *sql.DB) error {
	// Check that all chunks with status = COMPLETED have valid container_id and chunk_offset values
	// and that the chunk_offset + size does not exceed the container's current_size
	log.Printf("Checking chunk offset validity for completed chunks...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT c.id, c.container_id, c.chunk_offset, c.size, cont.current_size 
		FROM chunk c 
		JOIN container cont ON c.container_id = cont.id 
		WHERE c.status = 'COMPLETED';`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query completed chunks for offset validity: %v", err)
		return fmt.Errorf("failed to query completed chunks for offset validity: %w", err)
	}
	defer rows.Close()

	type chunkInfo struct {
		id            int
		containerID   int
		chunkOffset   int64
		size          int64
		containerSize int64
	}

	for rows.Next() {
		var c chunkInfo
		if err := rows.Scan(&c.id, &c.containerID, &c.chunkOffset, &c.size, &c.containerSize); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan completed chunk for offset validity: %w", err))
			continue
		}

		if c.chunkOffset < 0 || c.size <= 0 || c.chunkOffset > c.containerSize-c.size {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("chunk ID %d in container %d has invalid location: chunk_offset=%d size=%d container_size=%d", c.id, c.containerID, c.chunkOffset, c.size, c.containerSize))
		}
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkChunkOffsetValidity checks :", errorCount)
		if errorCount > maxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkChunkOffsetValidity checks", errorCount)
	}
	log.Println(" SUCCESS ")
	return nil
}

func checkReferenceCounts(dbconn *sql.DB) error {
	// Check that all chunks have correct reference counts (chunk.ref_count should match the actual number of file_chunk references)
	log.Printf("Checking chunk reference counts consistency...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`
			SELECT chunk.id,
				chunk.ref_count,
				COUNT(file_chunk.chunk_id) AS actual
			FROM chunk
			LEFT JOIN file_chunk
			ON chunk.id = file_chunk.chunk_id
			GROUP BY chunk.id
			HAVING chunk.ref_count != COUNT(file_chunk.chunk_id)
			`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query chunk reference counts: %v", err)
		return fmt.Errorf("failed to query chunk reference counts: %w", err)
	}
	defer rows.Close()

	type chunkRefCount struct {
		id       int
		refCount int
		actual   int
	}

	for rows.Next() {
		var c chunkRefCount
		if err := rows.Scan(&c.id, &c.refCount, &c.actual); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan inconsistent chunk reference count: %w", err))
			continue
		}
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("Inconsistent chunk reference count: chunk ID %d has ref_count=%d but actual references=%d", c.id, c.refCount, c.actual))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkReferenceCounts checks : ", errorCount)
		if errorCount > maxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkReferenceCounts checks", errorCount)
	}
	log.Println(" SUCCESS ")
	return nil
}

func checkOrphanChunks(dbconn *sql.DB) error {
	// Check that there are no orphan chunks (chunks with ref_count > 0 but no file_chunk references)
	log.Printf("Checking for orphan chunks with ref_count > 0 but no file_chunk references...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT chunk.id 
							FROM chunk 
							LEFT JOIN file_chunk ON chunk.id = file_chunk.chunk_id 
							WHERE file_chunk.chunk_id IS NULL AND chunk.ref_count > 0;`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query orphan chunks: %v", err)
		return fmt.Errorf("failed to query orphan chunks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan orphan chunk: %w", err))
			continue
		}
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("orphan chunk found: chunk ID %d has ref_count > 0 but no file_chunk references", id))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkOrphanChunks checks:", errorCount)
		if errorCount > maxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkOrphanChunks checks", errorCount)
	}
	log.Println(" SUCCESS ")
	return nil
}

func checkFileChunkOrdering(dbconn *sql.DB) error {
	// Check that file_chunks for each file are ordered by chunk_offset without gaps
	log.Printf("Checking file chunk ordering and gaps...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT id
							FROM logical_file lf
							WHERE NOT EXISTS (
								SELECT 1
								FROM file_chunk fc
								WHERE fc.logical_file_id = lf.id
							);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query file chunk ordering: %v", err)
		return fmt.Errorf("failed to query file chunk ordering: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var logicalFileID int
		if err := rows.Scan(&logicalFileID); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan file chunk info: %w", err))
			continue
		}
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("File with no chunks found: logical file ID %d has no chunks", logicalFileID))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d lerrors in checkFileChunkOrdering checks :", errorCount)
		if errorCount > maxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in checkFileChunkOrdering checks", errorCount)
	}
	log.Println(" SUCCESS ")
	return nil
}

func appendToErrorList(errorList []error, err error) []error {
	if len(errorList) < maxErrorsToPrint {
		return append(errorList, err)
	}
	return errorList
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
			FROM chunk
			WHERE chunk.container_id = container.id
			AND chunk.status != 'COMPLETED'
		)`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query container completeness: %v", err)
		return fmt.Errorf("failed to query container completeness: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var containerID int
		if err := rows.Scan(&containerID); err != nil {
			errorCount++
			errorList = appendToErrorList(errorList, fmt.Errorf("failed to scan container info: %w", err))
			continue
		}
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("Sealed container with incomplete chunks found: container ID %d has incomplete chunks", containerID))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = appendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkContainerCompleteness checks:", errorCount)
		if errorCount > maxErrorsToPrint {
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
