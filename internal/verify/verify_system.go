package verify

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/utils_compression"
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

	//check that file_chunks for each file are ordered by chunk_offset without gaps
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
	if err = checkContainerHash(dbconn); err != nil {
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

	containers, err := dbconn.Query(`SELECT id, filename, compression_algorithm  FROM container WHERE sealed=true`)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query sealed containers: %v", err)
		return fmt.Errorf("failed to query sealed containers: %w", err)
	}
	defer func() { _ = containers.Close() }()

	maxChunkSize := chunk.MaxChunkSize

	for containers.Next() {
		processedContainers++
		var containerID int
		var filename string
		var compressionAlgo string
		if err := containers.Scan(&containerID, &filename, &compressionAlgo); err != nil {
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan container info: %w", err))
			continue
		}
		log.Printf("Verifying container %d/%d: %s", processedContainers, containerCount, filename)

		//construct full path with compression extension if needed
		fullPath := filepath.Join(container.ContainersDir, filename)
		algo := utils_compression.CompressionType(compressionAlgo)
		if algo != utils_compression.CompressionNone {
			fullPath = fullPath + "." + compressionAlgo
		}

		//get file size for validation
		info, err := os.Stat(fullPath)
		if err != nil {
			errorCount++
			log.Printf("Failed to stat container file %s: %v", fullPath, err)
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to stat container file %s: %w", fullPath, err))
			continue
		}
		fileSize := info.Size()

		//fetch chunks ordered by offset
		chunks, err := dbconn.Query(`SELECT chunk_offset, size, chunk_hash
								FROM chunk
								WHERE container_id = $1
								AND status = 'COMPLETED'
								ORDER BY chunk_offset`, containerID)
		if err != nil {
			log.Printf("Failed to query chunks for container %d: %v", containerID, err)
			errorCount++
			errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to query chunks for container %d: %w", containerID, err))
			continue
		}

		hasChunks := false

		for chunks.Next() {
			hasChunks = true
			var chunkOffset int64
			var chunkSize int64
			var chunkHash string
			if err := chunks.Scan(&chunkOffset, &chunkSize, &chunkHash); err != nil {
				log.Printf("Failed to scan chunk info for container %d: %v", containerID, err)
				errorCount++
				errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("failed to scan chunk info for container %d: %w", containerID, err))
				continue
			}

			if chunkOffset < 0 || chunkSize < 0 {
				log.Printf("Invalid chunk offset or size for container %d at offset %d: chunk size %d", containerID, chunkOffset, chunkSize)
				errorCount++
				errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("invalid chunk offset or size for container %d at offset %d: chunk size %d", containerID, chunkOffset, chunkSize))
				continue
			}

			if chunkSize > int64(maxChunkSize) {
				log.Printf("Chunk size %d exceeds maximum allowed size %d for chunk in container %d", chunkSize, maxChunkSize, containerID)
				errorCount++
				errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk size %d exceeds maximum allowed size %d for chunk in container %d", chunkSize, maxChunkSize, containerID))
				continue
			}

			if chunkOffset+chunkSize > fileSize {
				log.Printf("Chunk exceeds file size for container %d at offset %d: chunk size %d, file size %d", containerID, chunkOffset, chunkSize, fileSize)
				errorCount++
				errorList = utils_print.AppendToErrorList(errorList, fmt.Errorf("chunk exceeds file size for container %d at offset %d: chunk size %d, file size %d", containerID, chunkOffset, chunkSize, fileSize))
				continue
			}

			algo := utils_compression.CompressionType(compressionAlgo)

			containerPath := filepath.Join(container.ContainersDir, filename)

			// Open as plain file (seek) or as decompressed stream (skip bytes)
			var r io.ReadCloser
			var f *os.File

			if algo == utils_compression.CompressionNone {
				f, err = os.Open(containerPath)
				if err != nil {
					return fmt.Errorf("open container %q: %w", filename, err)
				}
				// Seek to record start
				if _, err := f.Seek(chunkOffset, io.SeekStart); err != nil {
					_ = f.Close()
					return fmt.Errorf("seek container %q to offset %d: %w", filename, chunkOffset, err)
				}
				// Use file as reader; close via f.Close() below
				r = f
			} else {
				r, err = utils_compression.OpenDecompressionReader(containerPath, algo)
				if err != nil {
					return fmt.Errorf("open compressed container %q: %w", filename, err)
				}
				// Skip to record offset inside the *uncompressed* stream
				if _, err := io.CopyN(io.Discard, r, chunkOffset); err != nil {
					_ = r.Close()
					return fmt.Errorf("skip to chunk offset in decompressed stream for container %q: %w", filename, err)
				}
			}

			// Read record header
			headerHash := make([]byte, 32)
			if _, err := io.ReadFull(r, headerHash); err != nil {
				_ = r.Close()
				return fmt.Errorf("read chunk header hash for container %q: %w", filename, err)
			}

			sizeBuf := make([]byte, 4)
			if _, err := io.ReadFull(r, sizeBuf); err != nil {
				_ = r.Close()
				return fmt.Errorf("read chunk header size for container %q: %w", filename, err)
			}
			recordSize := int64(binary.LittleEndian.Uint32(sizeBuf))

			if recordSize != chunkSize {
				_ = r.Close()
				return fmt.Errorf("chunk size mismatch at offset %d (db=%d record=%d) for container %q", chunkOffset, chunkSize, recordSize, filename)
			}

			// Read chunk data
			chunkData := make([]byte, recordSize)
			if _, err := io.ReadFull(r, chunkData); err != nil {
				_ = r.Close()
				return fmt.Errorf("read chunk data for container %q: %w", filename, err)
			}

			// Close container reader
			if err := r.Close(); err != nil {
				return fmt.Errorf("close container reader for container %q: %w", filename, err)
			}

			// Validate hashes (DB hash and on-disk record hash)
			sum := sha256.Sum256(chunkData)
			sumHex := hex.EncodeToString(sum[:])

			if sumHex != chunkHash {
				return fmt.Errorf("chunk hash mismatch at offset %d (db=%s computed=%s) for container %q", chunkOffset, chunkHash, sumHex, filename)
			}
			if !bytes.Equal(sum[:], headerHash) {
				return fmt.Errorf("chunk record header hash mismatch at offset %d for container %q", chunkOffset, filename)
			}

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
