package verify

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/utils_compression"
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
	err := dbconn.QueryRow(`SELECT id, 
							status 
							from logical_file 
							where id = ?`, fileId).Scan(&id, &status)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}

	if status != "COMPLETED" {
		return fmt.Errorf("logical file %d has invalid status: expected COMPLETED but got %s", fileId, status)
	}
	var hasChunks bool = false
	//ensure file_chunks exists for the file
	filechunkrows, err := dbconn.Query(`SELECT chunk_id, chunk_order FROM file_chunk WHERE logical_file_id = ? order by chunk_order asc`, fileId)
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
								WHERE fc.logical_file_id = ?
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
			ctr.container_hash,
			ctr.compression_algorithm
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN container ctr ON ctr.id = c.container_id
		WHERE fc.logical_file_id = ?
		ORDER BY fc.chunk_order
	`, fileID)
	if err != nil {
		return fmt.Errorf("query file containers and offsets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	const chunkRecordHeaderSize = int64(32 + 4)

	type containerInfo struct {
		path         string
		physicalSize int64
		currentSize  int64
		algo         utils_compression.CompressionType
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
		var compressionAlgorithm string

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
			&compressionAlgorithm,
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
			algo := utils_compression.CompressionType(compressionAlgorithm)
			containerFilename := filename
			if algo != utils_compression.CompressionNone {
				containerFilename = filename + "." + compressionAlgorithm
			}

			fullPath := filepath.Join(container.ContainersDir, containerFilename)
			stat, err := os.Stat(fullPath)
			if err != nil {
				return fmt.Errorf("missing container file: %s: %w", fullPath, err)
			}

			info = containerInfo{
				path:         fullPath,
				physicalSize: stat.Size(),
				currentSize:  currentSize,
				algo:         algo,
			}
			containerInfoByID[containerID] = info

			if algo == utils_compression.CompressionNone && stat.Size() != currentSize {
				return fmt.Errorf("container %d size mismatch: expected %d got %d", containerID, currentSize, stat.Size())
			}
		}

		recordEnd := chunkOffset + chunkRecordHeaderSize + chunkSize
		if recordEnd > info.currentSize {
			return fmt.Errorf("chunk %d exceeds container %d bounds in metadata", chunkID, containerID)
		}
		if info.algo == utils_compression.CompressionNone && recordEnd > info.physicalSize {
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
			ctr.compression_algorithm
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN container ctr ON ctr.id = c.container_id
		WHERE fc.logical_file_id = ?
		ORDER BY ctr.id, c.chunk_offset
	`, fileID)
	if err != nil {
		return fmt.Errorf("query file chunk hashes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var chunkID int
		var chunkOffset int64
		var expectedSize int64
		var expectedChunkHash string
		var filename string
		var compressionAlgorithm string

		if err := rows.Scan(
			&chunkID,
			&chunkOffset,
			&expectedSize,
			&expectedChunkHash,
			&filename,
			&compressionAlgorithm,
		); err != nil {
			return fmt.Errorf("scan file chunk hashes: %w", err)
		}

		containerFilename := filename
		algo := utils_compression.CompressionType(compressionAlgorithm)
		if algo != utils_compression.CompressionNone {
			containerFilename = filename + "." + compressionAlgorithm
		}

		r, err := utils_compression.OpenDecompressionReader(filepath.Join(container.ContainersDir, containerFilename), algo)
		if err != nil {
			return fmt.Errorf("open container for chunk %d: %w", chunkID, err)
		}

		if _, err := io.CopyN(io.Discard, r, chunkOffset); err != nil {
			_ = r.Close()
			return fmt.Errorf("seek chunk %d within container stream: %w", chunkID, err)
		}

		headerHash := make([]byte, sha256.Size)
		if _, err := io.ReadFull(r, headerHash); err != nil {
			_ = r.Close()
			return fmt.Errorf("read chunk %d header hash: %w", chunkID, err)
		}

		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, sizeBuf); err != nil {
			_ = r.Close()
			return fmt.Errorf("read chunk %d header size: %w", chunkID, err)
		}

		recordSize := int64(binary.LittleEndian.Uint32(sizeBuf))
		if recordSize != expectedSize {
			_ = r.Close()
			return fmt.Errorf("chunk %d size mismatch: expected %d got %d", chunkID, expectedSize, recordSize)
		}

		chunkData := make([]byte, recordSize)
		if _, err := io.ReadFull(r, chunkData); err != nil {
			_ = r.Close()
			return fmt.Errorf("read chunk %d data: %w", chunkID, err)
		}

		if err := r.Close(); err != nil {
			return fmt.Errorf("close container reader for chunk %d: %w", chunkID, err)
		}

		sum := sha256.Sum256(chunkData)
		computedHash := hex.EncodeToString(sum[:])
		if computedHash != expectedChunkHash {
			return fmt.Errorf("chunk %d corrupted: expected %s got %s", chunkID, expectedChunkHash, computedHash)
		}
		if hex.EncodeToString(headerHash) != expectedChunkHash {
			return fmt.Errorf("chunk %d record header hash mismatch", chunkID)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate file chunk hashes: %w", err)
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
