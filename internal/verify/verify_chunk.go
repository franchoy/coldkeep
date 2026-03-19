package verify

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/utils_print"
)

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
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan inconsistent chunk reference count: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("Inconsistent chunk reference count: chunk ID %d has ref_count=%d but actual references=%d", c.id, c.refCount, c.actual))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkReferenceCounts checks : ", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
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
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan orphan chunk: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("orphan chunk found: chunk ID %d has ref_count > 0 but no file_chunk references", id))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkOrphanChunks checks:", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
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

func checkChunkOffsets(dbconn *sql.DB) error {
	// Check that all chunks have location (container_id + chunk_offset) consistent with their status
	// if status = COMPLETED → container_id NOT NULL chunk_offset NOT NULL

	log.Printf("Checking chunk offsets consistency with status...")
	var errorList []error
	var errorCount int
	rows1, err := dbconn.Query(`SELECT id, container_id, chunk_offset, size, status 
							FROM chunk 
							WHERE status = 'COMPLETED' 
							AND (container_id IS NULL OR chunk_offset IS NULL);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query completed chunks: %v", err)
		return fmt.Errorf("failed to query completed chunks: %w", err)
	}
	defer rows1.Close()

	type chunkInfo struct {
		id          int
		containerID int
		chunkOffset int64
		size        int64
		status      string
	}

	for rows1.Next() {
		var c chunkInfo
		if err := rows1.Scan(&c.id, &c.containerID, &c.chunkOffset, &c.size, &c.status); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan completed chunk: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk ID %d has status COMPLETED but missing location info: container_id=%v chunk_offset=%v", c.id, c.containerID, c.chunkOffset))
	}

	if err := rows1.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	rows1.Close()

	// if status != COMPLETED → container_id NULL chunk_offset NULL
	rows2, err := dbconn.Query(`SELECT id, container_id, chunk_offset, size, status 
							FROM chunk 
							WHERE status != 'COMPLETED' 
							AND (container_id IS NOT NULL OR chunk_offset IS NOT NULL);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query non-completed chunks: %v", err)
		return fmt.Errorf("failed to query non-completed chunks: %w", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var c chunkInfo
		if err := rows2.Scan(&c.id, &c.containerID, &c.chunkOffset, &c.size, &c.status); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan non-completed chunk: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk ID %d has status %s but has location info: container_id=%v chunk_offset=%v", c.id, c.status, c.containerID, c.chunkOffset))
	}

	if err := rows2.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkChunkOffsets checks : ", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
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
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan completed chunk for offset validity: %w", err))
			continue
		}

		if c.chunkOffset < 0 || c.size <= 0 || c.chunkOffset > c.containerSize-c.size {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk ID %d in container %d has invalid location: chunk_offset=%d size=%d container_size=%d", c.id, c.containerID, c.chunkOffset, c.size, c.containerSize))
		}
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkChunkOffsetValidity checks :", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
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
