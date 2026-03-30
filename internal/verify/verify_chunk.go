package verify

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/container"
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
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = rows.Close() }()

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
	// Check that all chunks have location (container_id + block_offset in blocks) consistent with their status
	// if status = COMPLETED → blocks row exists with container_id and block_offset

	log.Printf("Checking chunk offsets consistency with status...")
	var errorList []error
	var errorCount int
	rows1, err := dbconn.Query(`SELECT c.id, b.container_id, b.block_offset, c.status
							FROM chunk c
							LEFT JOIN blocks b ON b.chunk_id = c.id
							WHERE c.status = 'COMPLETED'
							AND (b.container_id IS NULL OR b.block_offset IS NULL);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query completed chunks: %v", err)
		return fmt.Errorf("failed to query completed chunks: %w", err)
	}
	defer func() { _ = rows1.Close() }()

	type chunkInfo struct {
		id               int
		blockContainerID sql.NullInt64
		blockOffset      sql.NullInt64
		status           string
	}

	for rows1.Next() {
		var c chunkInfo
		if err := rows1.Scan(&c.id, &c.blockContainerID, &c.blockOffset, &c.status); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan completed chunk: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk ID %d has status COMPLETED but missing location info in blocks: container_id=%v block_offset=%v", c.id, c.blockContainerID, c.blockOffset))
	}

	if err := rows1.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	_ = rows1.Close()

	// if status != COMPLETED → no row should exist in blocks
	rows2, err := dbconn.Query(`SELECT c.id, b.container_id, b.block_offset, c.status
							FROM chunk c
							LEFT JOIN blocks b ON b.chunk_id = c.id
							WHERE c.status != 'COMPLETED'
							AND (b.container_id IS NOT NULL OR b.block_offset IS NOT NULL);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query non-completed chunks: %v", err)
		return fmt.Errorf("failed to query non-completed chunks: %w", err)
	}
	defer func() { _ = rows2.Close() }()

	for rows2.Next() {
		var c chunkInfo
		if err := rows2.Scan(&c.id, &c.blockContainerID, &c.blockOffset, &c.status); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan non-completed chunk: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk ID %d has status %s but has location info in blocks: container_id=%v block_offset=%v", c.id, c.status, c.blockContainerID, c.blockOffset))
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
	// Check that all chunks with status = COMPLETED have valid blocks.container_id and blocks.block_offset values
	// and that the block_offset + stored_size does not exceed the container's current_size
	log.Printf("Checking chunk offset validity for completed chunks...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT c.id, b.container_id, b.block_offset, b.stored_size, cont.current_size
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container cont ON b.container_id = cont.id
		WHERE c.status = 'COMPLETED'
		ORDER BY b.container_id, b.block_offset;`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query completed chunks for offset validity: %v", err)
		return fmt.Errorf("failed to query completed chunks for offset validity: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type chunkInfo struct {
		id               int
		blockContainerID int
		blockOffset      int64
		storedSize       int64
		containerSize    int64
	}

	lastContainerID := -1
	expectedOffset := int64(container.ContainerHdrLen)

	for rows.Next() {
		var c chunkInfo
		if err := rows.Scan(&c.id, &c.blockContainerID, &c.blockOffset, &c.storedSize, &c.containerSize); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan completed chunk for offset validity: %w", err))
			continue
		}

		if c.blockContainerID != lastContainerID {
			lastContainerID = c.blockContainerID
			expectedOffset = int64(container.ContainerHdrLen)
		}

		if c.blockOffset != expectedOffset {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("non-contiguous offsets in container %d: expected %d got %d for chunk ID %d", c.blockContainerID, expectedOffset, c.blockOffset, c.id))
		}

		if c.blockOffset < 0 || c.storedSize <= 0 || c.blockOffset > c.containerSize-c.storedSize {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("block for chunk ID %d in container %d has invalid location: block_offset=%d stored_size=%d container_size=%d", c.id, c.blockContainerID, c.blockOffset, c.storedSize, c.containerSize))
		}

		if c.blockOffset >= 0 && c.storedSize > 0 {
			expectedOffset = c.blockOffset + c.storedSize
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
