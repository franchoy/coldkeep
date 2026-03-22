package verify

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

func checkFileChunkOrdering(dbconn *sql.DB) error {
	// Check that file_chunks for each file are ordered by chunk_offset without gaps
	log.Printf("Checking file chunk ordering and gaps...")
	var errorList []error
	var errorCount int
	rows, err := dbconn.Query(`SELECT id
							FROM logical_file lf
							WHERE lf.total_size > 0
							  AND NOT EXISTS (
								SELECT 1
								FROM file_chunk fc
								WHERE fc.logical_file_id = lf.id
							);`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query file chunk ordering: %v", err)
		return fmt.Errorf("failed to query file chunk ordering: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	log.Printf("starting standard file verification for logical file with ID %d...", fileId)
	if fileId <= 0 {
		return fmt.Errorf("invalid file ID: %d", fileId)
	}

	//ensure file id exists
	var id int
	var status string
	var totalSize int64
	err := dbconn.QueryRow(`SELECT id, 
							status,
							total_size
							from logical_file 
							where id = $1`, fileId).Scan(&id, &status, &totalSize)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}

	if status != "COMPLETED" {
		return fmt.Errorf("logical file %d has invalid status: expected COMPLETED but got %s", fileId, status)
	}
	var hasChunks bool = false
	//ensure file_chunks exists for the file
	filechunkrows, err := dbconn.Query(`SELECT chunk_id, chunk_order FROM file_chunk WHERE logical_file_id = $1 order by chunk_order asc`, fileId)
	if err != nil {
		return fmt.Errorf("failed to query file chunks: %w", err)
	}
	defer func() { _ = filechunkrows.Close() }()

	var chunkIdList []int
	var previousChunkOrder int = 0
	for filechunkrows.Next() {
		hasChunks = true

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

	if !hasChunks {
		if totalSize == 0 {
			log.Printf("logical file %d is zero-byte with no chunks; accepted", fileId)
			return nil
		}
		return fmt.Errorf("logical file %d has no chunks", fileId)
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
								WHERE fc.logical_file_id = $1
								ORDER BY fc.chunk_order ASC`, fileId)
	if err != nil {
		return fmt.Errorf("failed to query chunks: %w", err)
	}
	defer func() { _ = chunkrows.Close() }()

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

	log.Printf("standard file verification for logical file with ID %d completed successfully", fileId)

	return nil
}

func verifyFileContainersAndOffsets(db *sql.DB, fileID int) error {
	rows, err := db.Query(`
		SELECT
			c.id,
			c.chunk_offset,
			c.size,
			ctr.id,
			ctr.filename,
			ctr.current_size,
			ctr.sealed,
			ctr.quarantine,
			ctr.container_hash 
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN container ctr ON ctr.id = c.container_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order
	`, fileID)
	if err != nil {
		return fmt.Errorf("query file containers and offsets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	const chunkRecordHeaderSize = container.ChunkRecordHeaderSize

	type containerInfo struct {
		path         string
		physicalSize int64
		currentSize  int64
	}

	containerInfoByID := map[int64]containerInfo{}

	for rows.Next() {
		var chunkID int
		var chunkOffset int64
		var chunkSize int64
		var containerID int64
		var filename string
		var currentSize int64
		var sealed bool
		var quarantine bool
		var containerHash string

		if err := rows.Scan(
			&chunkID,
			&chunkOffset,
			&chunkSize,
			&containerID,
			&filename,
			&currentSize,
			&sealed,
			&quarantine,
			&containerHash,
		); err != nil {
			return fmt.Errorf("scan file containers and offsets: %w", err)
		}

		if !sealed {
			return fmt.Errorf("container %d is not sealed", containerID)
		}
		if quarantine {
			return fmt.Errorf("container %d is quarantined", containerID)
		}
		if containerHash == "" {
			return fmt.Errorf("container %d missing hash", containerID)
		}

		info, ok := containerInfoByID[containerID]
		if !ok {
			containerFilename := filename

			fullPath := filepath.Join(container.ContainersDir, containerFilename)
			fileInfo, err := os.Stat(fullPath)
			if err != nil {
				return fmt.Errorf("missing container file: %s: %w", fullPath, err)
			}

			physicalSize := fileInfo.Size()

			info = containerInfo{
				path:         fullPath,
				physicalSize: physicalSize,
				currentSize:  currentSize,
			}
			containerInfoByID[containerID] = info

			// Assumes verification runs against a consistent recovered state.
			// During in-flight writes or immediately after a crash (before recovery),
			// filesystem and DB metadata can temporarily diverge.
			if physicalSize != currentSize {
				return fmt.Errorf("container %d size mismatch: expected %d got %d", containerID, currentSize, physicalSize)
			}
		}

		recordEnd := chunkOffset + chunkRecordHeaderSize + chunkSize
		if recordEnd > info.currentSize {
			return fmt.Errorf("chunk %d exceeds container %d bounds in metadata", chunkID, containerID)
		}
		if recordEnd > info.physicalSize {
			return fmt.Errorf("chunk %d exceeds container %d physical file size", chunkID, containerID)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate file containers and offsets: %w", err)
	}

	return nil
}

func VerifyFileFull(dbconn *sql.DB, fileId int) error {
	if err := VerifyFileStandard(dbconn, fileId); err != nil {
		return fmt.Errorf("standard verification failed: %w", err)
	}

	log.Printf("starting Full file verification for logical file with ID %d...", fileId)
	if err := verifyFileContainersAndOffsets(dbconn, fileId); err != nil {
		return fmt.Errorf("container and offset verification failed: %w", err)
	}
	log.Printf("Full file verification for logical file with ID %d completed successfully", fileId)

	return nil
}

func verifyFileChunkHashes(db *sql.DB, fileID int) error {
	rows, err := db.Query(`
		SELECT
			c.id,
			c.chunk_offset,
			c.size,
			c.chunk_hash,
			ctr.filename,
			ctr.max_size
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN container ctr ON ctr.id = c.container_id
		WHERE fc.logical_file_id = $1
		ORDER BY ctr.id, c.chunk_offset
	`, fileID)
	if err != nil {
		return fmt.Errorf("query file chunk hashes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var currentContainer *container.FileContainer
	var currentFilename string

	for rows.Next() {
		var chunkID int
		var chunkOffset int64
		var expectedSize int64
		var expectedChunkHash string
		var filename string
		var maxSize int64

		if err := rows.Scan(
			&chunkID,
			&chunkOffset,
			&expectedSize,
			&expectedChunkHash,
			&filename,
			&maxSize,
		); err != nil {
			return fmt.Errorf("scan file chunk hashes: %w", err)
		}

		if currentFilename != filename {
			if currentContainer != nil {
				if err := currentContainer.Close(); err != nil {
					return fmt.Errorf("close container %q: %w", currentFilename, err)
				}
			}

			fullPath := filepath.Join(container.ContainersDir, filename)
			currentContainer, err = container.OpenExistingContainer(true, fullPath, maxSize)
			if err != nil {
				return fmt.Errorf("open container %q: %w", fullPath, err)
			}
			currentFilename = filename
		}

		chunkData, err := container.ReadChunkDataAt(currentContainer, chunkOffset, expectedSize)
		if err != nil {
			return fmt.Errorf("read chunk %d data: %w", chunkID, err)
		}

		sum := sha256.Sum256(chunkData)
		computedHash := hex.EncodeToString(sum[:])
		if computedHash != expectedChunkHash {
			return fmt.Errorf("chunk %d corrupted: expected %s got %s", chunkID, expectedChunkHash, computedHash)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate file chunk hashes: %w", err)
	}

	if currentContainer != nil {
		if err := currentContainer.Close(); err != nil {
			return fmt.Errorf("close container %q: %w", currentFilename, err)
		}
	}

	return nil
}

func VerifyFileDeep(dbconn *sql.DB, fileId int) error {
	if err := VerifyFileFull(dbconn, fileId); err != nil {
		return fmt.Errorf("full verification failed: %w", err)
	}

	log.Printf("starting deep file verification for logical file with ID %d...", fileId)
	if err := verifyFileChunkHashes(dbconn, fileId); err != nil {
		return fmt.Errorf("chunk hash verification failed: %w", err)
	}
	log.Printf("Deep file verification for logical file with ID %d completed successfully", fileId)

	return nil
}
