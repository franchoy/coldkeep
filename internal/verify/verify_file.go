package verify

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

func checkFileChunkOrdering(dbconn *sql.DB) error {
	// Check that all files have properly ordered chunks with no gaps (file_chunk.chunk_order should be sequential starting from 0 for each logical file)
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
	return VerifyFileStandardWithContainersDir(dbconn, fileId, container.ContainersDir)
}

func VerifyFileStandardWithContainersDir(dbconn *sql.DB, fileId int, containersDir string) error {
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

	//ensure all chunks have status COMPLETED and have associated blocks
	chunkrows, err := dbconn.Query(`SELECT
									c.id,
									c.status,
									b.id
								FROM chunk c
								JOIN file_chunk fc ON fc.chunk_id = c.id
								JOIN blocks b ON b.chunk_id = c.id
								WHERE fc.logical_file_id = $1
								ORDER BY fc.chunk_order ASC`, fileId)
	if err != nil {
		return fmt.Errorf("failed to query chunks: %w", err)
	}
	defer func() { _ = chunkrows.Close() }()

	var chunkCount int
	for chunkrows.Next() {
		var chunkid int
		var chunkStatus string
		var blockId sql.NullInt64
		if err := chunkrows.Scan(&chunkid, &chunkStatus, &blockId); err != nil {
			return fmt.Errorf("failed to scan chunk info: %w", err)
		}
		if chunkStatus != "COMPLETED" {
			return fmt.Errorf("chunk with ID %d has invalid status: expected COMPLETED but got %s", chunkid, chunkStatus)
		}
		if !blockId.Valid {
			return fmt.Errorf("chunk %d has no associated block", chunkid)
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

func verifyFileContainersAndOffsets(db *sql.DB, fileID int, containersDir string) error {
	rows, err := db.Query(`
		SELECT
			c.id,
			b.block_offset,
			b.stored_size,
			ctr.id,
			ctr.filename,
			ctr.current_size,
			ctr.sealed,
			ctr.quarantine,
			ctr.container_hash 
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order
	`, fileID)
	if err != nil {
		return fmt.Errorf("query file containers and offsets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type containerInfo struct {
		path         string
		physicalSize int64
		currentSize  int64
	}

	containerInfoByID := map[int64]containerInfo{}

	for rows.Next() {
		var chunkID int
		var blockOffset int64
		var storedSize int64
		var containerID int64
		var filename string
		var currentSize int64
		var quarantine bool
		var containerHash sql.NullString

		if err := rows.Scan(
			&chunkID,
			&blockOffset,
			&storedSize,
			&containerID,
			&filename,
			&currentSize,
			new(bool),
			&quarantine,
			&containerHash,
		); err != nil {
			return fmt.Errorf("scan file containers and offsets: %w", err)
		}

		if quarantine {
			return fmt.Errorf("container %d is quarantined", containerID)
		}

		info, ok := containerInfoByID[containerID]
		if !ok {
			containerFilename := filename

			fullPath := filepath.Join(containersDir, containerFilename)
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

		recordEnd := blockOffset + storedSize
		if recordEnd > info.currentSize {
			return fmt.Errorf("block for chunk %d exceeds container %d bounds in metadata", chunkID, containerID)
		}
		if recordEnd > info.physicalSize {
			return fmt.Errorf("block for chunk %d exceeds container %d physical file size", chunkID, containerID)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate file containers and offsets: %w", err)
	}

	return nil
}

func VerifyFileFull(dbconn *sql.DB, fileId int) error {
	return VerifyFileFullWithContainersDir(dbconn, fileId, container.ContainersDir)
}

func VerifyFileFullWithContainersDir(dbconn *sql.DB, fileId int, containersDir string) error {
	if err := VerifyFileStandardWithContainersDir(dbconn, fileId, containersDir); err != nil {
		return fmt.Errorf("standard verification failed: %w", err)
	}

	log.Printf("starting Full file verification for logical file with ID %d...", fileId)
	if err := verifyFileContainersAndOffsets(dbconn, fileId, containersDir); err != nil {
		return fmt.Errorf("container and offset verification failed: %w", err)
	}
	log.Printf("Full file verification for logical file with ID %d completed successfully", fileId)

	return nil
}

func verifyFileChunkHashes(db *sql.DB, fileID int, containersDir string) error {
	rows, err := db.Query(`
			SELECT
			c.id,
			b.block_offset,
			b.stored_size,
			b.plaintext_size,
			c.chunk_hash,
			b.codec,
			b.format_version,
			b.nonce,
			b.container_id,
			ctr.filename,
			ctr.max_size
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order
	`, fileID)
	if err != nil {
		return fmt.Errorf("query file chunk hashes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var currentContainer *container.FileContainer
	var currentFilename string

	for rows.Next() {
		var chunkID int
		var blockOffset int64
		var storedSize int64
		var plaintextSize int64
		var expectedChunkHash string
		var codec string
		var formatVersion int
		var nonce []byte
		var containerID int64
		var filename string
		var maxSize int64

		if err := rows.Scan(
			&chunkID,
			&blockOffset,
			&storedSize,
			&plaintextSize,
			&expectedChunkHash,
			&codec,
			&formatVersion,
			&nonce,
			&containerID,
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

			fullPath := filepath.Join(containersDir, filename)
			currentContainer, err = container.OpenExistingContainer(true, fullPath, maxSize)
			if err != nil {
				return fmt.Errorf("open container %q: %w", fullPath, err)
			}
			currentFilename = filename
		}

		payload, err := container.ReadPayloadAt(currentContainer, blockOffset, storedSize)
		if err != nil {
			return fmt.Errorf("read block payload for chunk %d: %w", chunkID, err)
		}

		transformer, err := blocks.GetBlockTransformer(blocks.Codec(codec))
		if err != nil {
			return fmt.Errorf("get transformer for codec %q: %w", codec, err)
		}

		plaintext, err := transformer.Decode(context.Background(), blocks.DecodeInput{
			ChunkHash: expectedChunkHash,
			Descriptor: blocks.Descriptor{
				ChunkID:       int64(chunkID),
				Codec:         blocks.Codec(codec),
				FormatVersion: formatVersion,
				PlaintextSize: plaintextSize,
				StoredSize:    storedSize,
				Nonce:         nonce,
				ContainerID:   containerID,
				BlockOffset:   blockOffset,
			},
			Payload: payload,
		})
		if err != nil {
			return fmt.Errorf("decode chunk %d data: %w", chunkID, err)
		}

		sum := sha256.Sum256(plaintext)
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
	return VerifyFileDeepWithContainersDir(dbconn, fileId, container.ContainersDir)
}

func VerifyFileDeepWithContainersDir(dbconn *sql.DB, fileId int, containersDir string) error {
	if err := VerifyFileFullWithContainersDir(dbconn, fileId, containersDir); err != nil {
		return fmt.Errorf("full verification failed: %w", err)
	}

	log.Printf("starting deep file verification for logical file with ID %d...", fileId)
	if err := verifyFileChunkHashes(dbconn, fileId, containersDir); err != nil {
		return fmt.Errorf("chunk hash verification failed: %w", err)
	}
	log.Printf("Deep file verification for logical file with ID %d completed successfully", fileId)

	return nil
}
