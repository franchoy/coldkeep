package verify

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/utils_print"
)

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
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan file chunk info: %w", err))
			continue
		}
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("File with no chunks found: logical file ID %d has no chunks", logicalFileID))
	}

	if err := rows.Err(); err != nil {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed: %w", err))
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in checkFileChunkOrdering checks :", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
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

func VerifyFileStandard(dbconn *sql.DB, fileId int) error {

	if fileId <= 0 {
		return fmt.Errorf("invalid file ID: %d", fileId)
	}

	//ensure file id exists
	var id int
	var status string
	err := dbconn.QueryRow(`SELECT id, 
							status 
							from logical_file 
							where id = ?`, fileId).Scan(&id, &status)
	if err == sql.ErrNoRows {
		return fmt.Errorf("logical file %d not found", fileId)
	}
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}

	if status != "COMPLETED" {
		return fmt.Errorf("logical file %d has invalid status: expected COMPLETED but got %s", fileId, status)
	}

	//ensure file_chunks exists for the file
	filechunkrows, err := dbconn.Query(`SELECT chunk_id, chunk_order FROM file_chunk WHERE logical_file_id = ? order by chunk_order asc`, fileId)
	if err != nil {
		return fmt.Errorf("failed to query file chunks: %w", err)
	}
	defer filechunkrows.Close()

	if err == sql.ErrNoRows {
		return fmt.Errorf("logical file %d has no chunks", fileId)
	}

	var chunkIdList []int
	var previousChunkOrder int = 0
	for filechunkrows.Next() {
		var chunkId int
		var chunkOrder int
		if err := filechunkrows.Scan(&chunkId, &chunkOrder); err != nil {
			return fmt.Errorf("failed to scan file chunk info: %w", err)
		}
		chunkIdList = append(chunkIdList, chunkId)
		//if there is any missing chunk_order, it means the file chunk ordering is wrong
		if chunkOrder != previousChunkOrder {
			return fmt.Errorf("file chunk ordering error: expected chunk_order %d but got %d for chunk ID %d", previousChunkOrder, chunkOrder, chunkId)
		}
		previousChunkOrder++
	}

	if err := filechunkrows.Err(); err != nil {
		return fmt.Errorf("row iteration failed: %w", err)
	}

	//ensure all chunks exists with status COMPLETED and have valid container_id and chunk_offset
	chunkrows, err := dbconn.Query(`SELECT
									c.id,
									c.container_id,
									c.chunk_offset,
									c.size,
									c.status
								FROM chunk c
								JOIN file_chunk fc ON fc.chunk_id = c.id
								WHERE fc.logical_file_id = ?
								ORDER BY fc.chunk_order ASC`, fileId)
	if err != nil {
		return fmt.Errorf("failed to query chunks: %w", err)
	}
	defer chunkrows.Close()

	var chunkCount int
	for chunkrows.Next() {
		var id int
		var containerId sql.NullInt64
		var chunkOffset sql.NullInt64
		var size int64
		var status string
		if err := chunkrows.Scan(&id, &containerId, &chunkOffset, &size, &status); err != nil {
			return fmt.Errorf("failed to scan chunk info: %w", err)
		}
		if status != "COMPLETED" {
			return fmt.Errorf("chunk with ID %d has invalid status: expected COMPLETED but got %s", id, status)
		}
		if !containerId.Valid {
			return fmt.Errorf("chunk with ID %d has invalid location info: container_id is NULL", id)
		}
		if !chunkOffset.Valid {
			return fmt.Errorf("chunk with ID %d has invalid location info: chunk_offset is NULL", id)
		}
		chunkCount++
	}
	if err := chunkrows.Err(); err != nil {
		return fmt.Errorf("row iteration failed: %w", err)
	}

	if chunkCount != len(chunkIdList) {
		return fmt.Errorf("chunk count mismatch: expected %d but got %d", len(chunkIdList), chunkCount)
	}

	return nil
}

func VerifyFileFull(dbconn *sql.DB, fileId int) error {
	if err := VerifyFileStandard(dbconn, fileId); err != nil {
		return fmt.Errorf("standard verification failed: %w", err)
	}

	return nil
}

func VerifyFileDeep(dbconn *sql.DB, fileId int) error {
	if err := VerifyFileFull(dbconn, fileId); err != nil {
		return fmt.Errorf("full verification failed: %w", err)
	}

	return nil
}
