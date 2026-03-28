package verify

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

func printCounters(dbconn *sql.DB) error {
	var containerCount, chunkCount, fileCount int
	//list container counter to be checked
	err := dbconn.QueryRow("SELECT COUNT(*) FROM container").Scan(&containerCount)
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

	return nil
}

func VerifySystemStandard(dbconn *sql.DB) error {
	//	standard
	//		reference count check
	//		orphan chunk check
	//		file chunk ordering check
	log.Printf("Starting standard system verification...")

	var err error

	//print counters to be checked
	if err := printCounters(dbconn); err != nil {
		return err
	}

	//check that all chunks have correct reference counts (chunk.ref_count should match the actual number of file_chunk references)
	if err = checkReferenceCounts(dbconn); err != nil {
		return err
	}

	//check that there are no orphan chunks (chunks with ref_count > 0 but no file_chunk references)
	if err = checkOrphanChunks(dbconn); err != nil {
		return err
	}

	//check that all files have properly ordered chunks with no gaps (file_chunk.chunk_order should be sequential starting from 0 for each logical file)
	if err = checkFileChunkOrdering(dbconn); err != nil {
		return err
	}

	log.Printf("Standard system verification completed successfully.")

	return nil
}

func VerifySystemFull(dbconn *sql.DB) error {
	//	full
	//		standard checks +
	//			container file existence and size check
	//			container hash check
	//			chunk-container consistency check
	//			chunk offsets consistency check
	//			chunk offset validity check
	//			checkContainerCompleteness
	log.Printf("Starting Full system verification...")

	var err error

	//first verify standard checks
	if err = VerifySystemStandard(dbconn); err != nil {
		return err
	}

	//check that all containers have their files present on disk and that the file sizes match the DB records
	if err = checkContainersFileExistence(dbconn); err != nil {
		return err
	}

	//check that all sealed containers have a valid hash that matches the file content
	if err = checkSealedContainersHash(dbconn); err != nil {
		return err
	}

	//check that all chunks are correctly associated with their containers (if blocks.container_id exists → chunk.status must be COMPLETED)
	if err = checkChunkContainerConsistency(dbconn); err != nil {
		return err
	}

	//check that all chunks have location (blocks.container_id + blocks.block_offset) consistent with their status
	//if status = COMPLETED → blocks row with container_id and block_offset must exist
	if err = checkChunkOffsets(dbconn); err != nil {
		return err
	}

	//check that all chunks with status = COMPLETED have valid blocks.container_id and blocks.block_offset values and that block_offset + size does not exceed the container current_size
	if err = checkChunkOffsetValidity(dbconn); err != nil {
		return err
	}

	//check that sealed containers should not accept new chunks
	if err = checkContainerCompleteness(dbconn); err != nil {
		return err
	}

	log.Printf("Full system verification completed successfully.")

	return nil
}

func VerifySystemDeep(dbconn *sql.DB) error {
	//	deep
	//		standard checks +
	//		full checks +
	//			actual file integrity checks (e.g. read container files and verify chunk data against stored hashes)
	log.Printf("Starting Deep system verification...")

	var err error

	//first verify full checks
	if err = VerifySystemFull(dbconn); err != nil {
		return err
	}

	//real deep verification
	//for each container:
	//open container file
	//fetch chunks ordered by offset
	//read container sequentially
	//verify each chunk
	log.Println("Starting deep verification of container files...")
	var errorList []error
	var errorCount int
	//retrieve sealer container count
	containerCount := 0
	containerCountErr := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE sealed=true`).Scan(&containerCount)
	if containerCountErr != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query sealed container count: %v", containerCountErr)
		return fmt.Errorf("failed to query sealed container count: %w", containerCountErr)
	}

	processedContainers := 0

	containers, err := dbconn.Query(`SELECT id, filename, current_size, max_size FROM container WHERE sealed=true`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query sealed containers: %v", err)
		return fmt.Errorf("failed to query sealed containers: %w", err)
	}
	defer func() { _ = containers.Close() }()

	for containers.Next() {
		processedContainers++
		var containerID int
		var filename string
		var currentSize int64
		var maxSize int64
		if err := containers.Scan(&containerID, &filename, &currentSize, &maxSize); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan container info: %w", err))
			continue
		}
		log.Printf("Verifying container %d/%d: %s", processedContainers, containerCount, filename)

		//construct full path
		fullPath := filepath.Join(container.ContainersDir, filename)

		fileSize := currentSize

		//fetch chunks ordered by offset
		chunks, err := dbconn.Query(`SELECT 
									b.block_offset,
									b.stored_size,
									b.plaintext_size,
									c.chunk_hash,
									b.codec,
									b.format_version,
									b.nonce
								FROM blocks b
								JOIN chunk c ON c.id = b.chunk_id
								WHERE b.container_id = $1
								ORDER BY b.block_offset`, containerID)
		if err != nil {
			log.Printf("Failed to query chunks for container %d: %v", containerID, err)
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to query chunks for container %d: %w", containerID, err))
			continue
		}

		filecontainer, err := container.OpenExistingContainer(true, fullPath, maxSize)
		if err != nil {
			log.Printf("Failed to open container %s: %v", fullPath, err)
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to open container %s: %w", fullPath, err))
			_ = chunks.Close()
			continue
		}

		hasChunks := false

		for chunks.Next() {
			hasChunks = true
			var blockOffset int64
			var storedSize int64
			var plaintextSize int64
			var chunkHash string
			var codec string
			var formatVersion int
			var nonce []byte
			if err := chunks.Scan(&blockOffset, &storedSize, &plaintextSize, &chunkHash, &codec, &formatVersion, &nonce); err != nil {
				log.Printf("Failed to scan chunk info for container %d: %v", containerID, err)
				errorCount++
				errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan chunk info for container %d: %w", containerID, err))
				continue
			}

			if blockOffset < 0 || storedSize < 0 {
				log.Printf("Invalid block offset or size for container %d at offset %d: block size %d", containerID, blockOffset, storedSize)
				errorCount++
				errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("invalid block offset or size for container %d at offset %d: block size %d", containerID, blockOffset, storedSize))
				continue
			}

			if blockOffset+storedSize > fileSize {
				log.Printf("Block exceeds file size for container %d at offset %d: block size %d, file size %d", containerID, blockOffset, storedSize, fileSize)
				errorCount++
				errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("block exceeds file size for container %d at offset %d: block size %d, file size %d", containerID, blockOffset, storedSize, fileSize))
				continue
			}

			payload, err := container.ReadPayloadAt(filecontainer, blockOffset, storedSize)
			if err != nil {
				_ = filecontainer.Close()
				_ = chunks.Close()
				return fmt.Errorf("read block payload for container %q: %w", filename, err)
			}
			transformer, err := blocks.GetBlockTransformer(blocks.Codec(codec))
			if err != nil {
				_ = filecontainer.Close()
				_ = chunks.Close()
				return fmt.Errorf("get transformer for codec %q: %w", codec, err)
			}

			plaintext, err := transformer.Decode(context.Background(), blocks.DecodeInput{
				ChunkHash: chunkHash,
				Descriptor: blocks.Descriptor{
					Codec:         blocks.Codec(codec),
					FormatVersion: formatVersion,
					PlaintextSize: plaintextSize,
					StoredSize:    storedSize,
					Nonce:         nonce,
					ContainerID:   int64(containerID),
					BlockOffset:   blockOffset,
				},
				Payload: payload,
			})
			if err != nil {
				_ = filecontainer.Close()
				_ = chunks.Close()
				return fmt.Errorf("decode block payload for container %q: %w", filename, err)
			}

			if int64(len(plaintext)) != plaintextSize {
				return fmt.Errorf(
					"plaintext size mismatch at offset %d: expected %d got %d",
					blockOffset,
					plaintextSize,
					len(plaintext),
				)
			}

			// Validate hashes (DB hash and on-disk record hash)
			sum := sha256.Sum256(plaintext)
			sumHex := hex.EncodeToString(sum[:])

			if sumHex != chunkHash {
				_ = filecontainer.Close()
				_ = chunks.Close()
				return fmt.Errorf("block hash mismatch at offset %d (db=%s computed=%s) for container %q", blockOffset, chunkHash, sumHex, filename)
			}

		}

		if err := filecontainer.Close(); err != nil {
			_ = chunks.Close()
			return fmt.Errorf("close container %q: %w", filename, err)
		}

		if !hasChunks {
			log.Printf("WARNING: container %d has no chunks", containerID)
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("container %d has no chunks", containerID))
			_ = chunks.Close()
			continue
		}

		if err := chunks.Err(); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("row iteration failed for chunks of container %d: %w", containerID, err))
			_ = chunks.Close()
			continue
		}

		_ = chunks.Close()
	}
	if err := containers.Err(); err != nil {
		return fmt.Errorf("row iteration failed for containers: %w", err)
	}

	if len(errorList) > 0 {
		log.Println(" ERROR ")
		log.Printf("Found %d errors in deep verification of container files:", errorCount)
		if errorCount > utils_print.MaxErrorsToPrint {
			log.Printf("showing only first %d:", len(errorList))
		}
		for _, err := range errorList {
			log.Printf(" - %v", err)
		}
		return fmt.Errorf("found %d errors in deep verification of container files", errorCount)
	}

	log.Println("Deep verification completed successfully.")
	return nil
}
