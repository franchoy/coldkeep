package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

type deepVerifyContainer struct {
	ID          int
	Filename    string
	CurrentSize int64
	MaxSize     int64
}

func loadDeepVerifyContainers(ctx context.Context, dbconn *sql.DB) ([]deepVerifyContainer, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT ctr.id, ctr.filename, ctr.current_size, ctr.max_size
		FROM container ctr
		WHERE ctr.quarantine = FALSE
		AND EXISTS (
			SELECT 1
			FROM storage_blocks sb
			WHERE sb.container_id = ctr.id
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query deep-verify containers: %w", err)
	}

	closeWithError := func(prior error) error {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf("failed to close deep-verify container rows: %w", closeErr)
			if prior != nil {
				return errors.Join(prior, closeErr)
			}
			return closeErr
		}
		return prior
	}

	containers := make([]deepVerifyContainer, 0)
	for rows.Next() {
		var container deepVerifyContainer
		if err := rows.Scan(&container.ID, &container.Filename, &container.CurrentSize, &container.MaxSize); err != nil {
			return nil, closeWithError(fmt.Errorf("failed to scan container info: %w", err))
		}
		containers = append(containers, container)
	}
	if err := rows.Err(); err != nil {
		return nil, closeWithError(fmt.Errorf("row iteration failed for containers: %w", err))
	}
	if err := closeWithError(nil); err != nil {
		return nil, err
	}

	return containers, nil
}

func printCounters(dbconn *sql.DB) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	var containerCount, chunkCount, fileCount int
	//list container counter to be checked
	err := dbconn.QueryRowContext(ctx, "SELECT COUNT(*) FROM container").Scan(&containerCount)
	if err != nil {
		return fmt.Errorf("failed to query container count: %w", err)
	}
	//list chunk counter to be checked
	err = dbconn.QueryRowContext(ctx, "SELECT COUNT(*) FROM chunk").Scan(&chunkCount)
	if err != nil {
		return fmt.Errorf("failed to query chunk count: %w", err)
	}
	//list file counter to be checked
	err = dbconn.QueryRowContext(ctx, "SELECT COUNT(*) FROM logical_file").Scan(&fileCount)
	if err != nil {
		return fmt.Errorf("failed to query logical file count: %w", err)
	}

	log.Printf("Starting verification: %d containers, %d chunks, %d logical files to check", containerCount, chunkCount, fileCount)

	return nil
}

// runPhysicalIntegrityChecks confirms that stored chunk data is self-consistent
// at the storage layer: rows exist, location metadata is valid, reference counts
// are coherent, and version metadata is present.
//
// These checks are entirely read-side: no chunker algorithm is invoked. The
// chunker_version column is treated as an opaque label — only its presence is
// verified, never its value compared against the current active chunker.
func runPhysicalIntegrityChecks(dbconn *sql.DB) error {
	// Chunk rows exist and have valid location mappings.
	if err := checkCompletedChunkBlockCardinality(dbconn); err != nil {
		return err
	}

	// Chunk reference counts match actual file_chunk rows.
	if err := checkReferenceCounts(dbconn); err != nil {
		return err
	}

	// No orphan chunks (positive live_ref_count but zero file_chunk rows).
	if err := checkOrphanChunks(dbconn); err != nil {
		return err
	}

	// Restore pins may only be placed on COMPLETED chunks.
	if err := checkPinnedChunkStatus(dbconn); err != nil {
		return err
	}

	// chunker_version is non-empty on every logical_file and chunk row.
	// This confirms the metadata is present for read-side flows; it does NOT
	// compare versions against the current active chunker.
	if _, err := CheckChunkerVersionMetadataIntegrity(dbconn); err != nil {
		return err
	}

	return nil
}

// runLogicalReconstructionChecks confirms that the logical recipe stored in the
// database is coherent: file_chunk ordering is sane, snapshot→logical_file
// references are valid, and the physical_file graph has no drift.
//
// These checks operate entirely on persisted structure and never re-run the
// chunker algorithm. A file stored under v1-simple-rolling and one stored under
// a future v2 algorithm are treated identically — only the recipe is checked.
func runLogicalReconstructionChecks(dbconn *sql.DB) error {
	// Snapshots reference logical_file rows that exist and are reachable.
	// Runs before fine-grained ordering so snapshot-graph errors surface first.
	if _, err := CheckSnapshotReachabilityIntegrity(dbconn); err != nil {
		return err
	}

	// physical_file rows point to existing logical_file rows; ref_count matches
	// the actual number of physical_file mappings; no negative ref_counts.
	if _, err := CheckPhysicalFileGraphIntegrity(dbconn); err != nil {
		return err
	}

	// file_chunk.chunk_order is gapless and starts at 0 for every logical file.
	if err := checkFileChunkOrdering(dbconn); err != nil {
		return err
	}

	return nil
}

func VerifySystemStandardWithContainersDir(dbconn *sql.DB, containersDir string) error {
	// standard
	//   Physical integrity:  chunk rows exist, location metadata valid,
	//                        reference counts coherent, version metadata present.
	//   Logical reconstruction: file_chunk ordering, snapshot reachability,
	//                           physical_file graph coherence.
	//
	// Neither category re-runs the chunker algorithm.
	log.Printf("Starting standard system verification...")

	if err := printCounters(dbconn); err != nil {
		return err
	}

	// Phase 5 layered verification entrypoint.
	if err := VerifyRepository(dbconn, containersDir); err != nil {
		return err
	}

	log.Printf("Standard system verification completed successfully.")

	return nil
}

func VerifySystemFastWithContainersDir(dbconn *sql.DB, containersDir string) error {
	log.Printf("Starting fast system verification...")

	if err := printCounters(dbconn); err != nil {
		return err
	}

	if err := VerifyRepositoryFast(dbconn, containersDir); err != nil {
		return err
	}

	log.Printf("Fast system verification completed successfully.")

	return nil
}

func VerifySystemFullWithContainersDir(dbconn *sql.DB, containersDir string) error {
	// full = standard checks + extended physical storage checks.
	//
	// Extended physical integrity (no chunker algorithm involved):
	//   - Container files exist on disk and sizes match DB records.
	//   - Sealed container hashes match file content.
	//   - Chunk ↔ container associations are consistent.
	//   - Block offsets are coherent with chunk status.
	//   - Block offset arithmetic is within container bounds.
	//   - Sealed containers accept no further writes.
	log.Printf("Starting Full system verification...")

	var err error

	// Standard checks first (physical + logical reconstruction).
	if err = VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		return err
	}

	// --- Extended physical integrity ---

	// Container files exist on disk; filesystem sizes match DB current_size.
	if err = checkContainersFileExistence(dbconn, containersDir); err != nil {
		return err
	}

	// Sealed containers: stored hash matches actual file content.
	if err = checkSealedContainersHash(dbconn, containersDir); err != nil {
		return err
	}

	// Legacy-path consistency check: blocks.container_id present ↔
	// chunk.status = COMPLETED. Packed-path presence/structure is validated via
	// chunk_block_refs/storage_blocks checks in repository verification.
	if err = checkChunkContainerConsistency(dbconn); err != nil {
		return err
	}

	// Chunk location metadata consistency across migration states:
	// legacy blocks row (container_id/block_offset), packed refs
	// (chunk_block_refs->storage_blocks), or migration companion state (both).
	if err = checkChunkOffsets(dbconn); err != nil {
		return err
	}

	// Container-bound checks for persisted byte ranges. This applies to legacy
	// blocks offsets and packed storage_blocks offsets through their respective
	// verification paths.
	if err = checkChunkOffsetValidity(dbconn); err != nil {
		return err
	}

	// Sealed containers must not accept new chunks.
	if err = checkContainerCompleteness(dbconn); err != nil {
		return err
	}

	log.Printf("Full system verification completed successfully.")

	return nil
}

func VerifySystemDeepWithContainersDir(dbconn *sql.DB, containersDir string) error {
	// deep = full checks + byte-level physical integrity.
	//
	// For every container with packed block data: open the file and verify each
	// storage_blocks entry in container_offset order. Verification is stage-aware
	// (physical/compressed/logical) using per-block metadata and hashes; it does
	// not re-chunk data and does not infer state from legacy blocks comments.
	log.Printf("Starting Deep system verification...")

	var err error

	//first verify full checks
	if err = VerifySystemFullWithContainersDir(dbconn, containersDir); err != nil {
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
	appendDeepError := func(err error) {
		errorCount++
		errorList = utils_print.AppendToErrorList(errorList, err)
	}
	reader := FilesystemContainerReader{ContainersDir: containersDir}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	containers, err := loadDeepVerifyContainers(ctx, dbconn)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query deep-verify containers: %v", err)
		return err
	}

	containerCount := len(containers)
	processedContainers := 0
	for _, containerInfo := range containers {
		processedContainers++
		containerID := containerInfo.ID
		filename := containerInfo.Filename
		currentSize := containerInfo.CurrentSize
		maxSize := containerInfo.MaxSize
		log.Printf("Verifying container %d/%d: %s", processedContainers, containerCount, filename)

		fileSize := currentSize

		processContainerErr := func() (retErr error) {
			// Fetch packed storage_blocks ordered by container_offset.
			chunks, err := dbconn.QueryContext(ctx, `SELECT 
									sb.id,
									sb.container_offset,
									sb.stored_size,
									sb.plaintext_size,
									sb.compressed_size,
									sb.block_hash,
									sb.compressed_hash,
									sb.physical_hash,
									sb.codec,
									sb.format_version,
									sb.compression_codec,
									sb.compression_level
								FROM storage_blocks sb
								WHERE sb.container_id = $1
								ORDER BY sb.container_offset`, containerID)
			if err != nil {
				return fmt.Errorf("failed to query chunks for container %d: %w", containerID, err)
			}
			defer func() { _ = chunks.Close() }()

			hasChunks := false
			expectedOffset := int64(container.ContainerHdrLen)

			for chunks.Next() {
				hasChunks = true
				var blockID int64
				var blockOffset int64
				var storedSize int64
				var plaintextSize int64
				var compressedSize sql.NullInt64
				var blockHash []byte
				var compressedHash []byte
				var physicalHash []byte
				var codec string
				var formatVersion int
				var compressionCodec sql.NullString
				var compressionLevel sql.NullInt64
				if err := chunks.Scan(
					&blockID,
					&blockOffset,
					&storedSize,
					&plaintextSize,
					&compressedSize,
					&blockHash,
					&compressedHash,
					&physicalHash,
					&codec,
					&formatVersion,
					&compressionCodec,
					&compressionLevel,
				); err != nil {
					log.Printf("Failed to scan chunk info for container %d: %v", containerID, err)
					appendDeepError(fmt.Errorf("failed to scan chunk info for container %d: %w", containerID, err))
					continue
				}

				if blockOffset < 0 || storedSize <= 0 {
					log.Printf("Invalid block offset or size for container %d at offset %d: block size %d", containerID, blockOffset, storedSize)
					appendDeepError(fmt.Errorf("invalid block offset or size for container %d at offset %d: block size %d", containerID, blockOffset, storedSize))
					continue
				}

				if blockOffset != expectedOffset {
					log.Printf("Non-contiguous block offsets for container %d: expected %d got %d", containerID, expectedOffset, blockOffset)
					appendDeepError(fmt.Errorf("non-contiguous block offsets for container %d: expected %d got %d", containerID, expectedOffset, blockOffset))
				}

				nextExpectedOffset := blockOffset + storedSize
				if nextExpectedOffset > fileSize {
					log.Printf("Block exceeds file size for container %d at offset %d: block size %d, file size %d", containerID, blockOffset, storedSize, fileSize)
					appendDeepError(fmt.Errorf("block exceeds file size for container %d at offset %d: block size %d, file size %d", containerID, blockOffset, storedSize, fileSize))
					expectedOffset = nextExpectedOffset
					continue
				}

				var compressedSizePtr *int64
				if compressedSize.Valid {
					value := compressedSize.Int64
					compressedSizePtr = &value
				}

				var compressionLevelPtr *int
				if compressionLevel.Valid {
					value := int(compressionLevel.Int64)
					compressionLevelPtr = &value
				}

				compressionCodecValue := ""
				if compressionCodec.Valid {
					compressionCodecValue = compressionCodec.String
				}

				_, err = VerifyStoredBlock(ctx, BlockStorageMetadata{
					BlockID:          blockID,
					ContainerID:      int64(containerID),
					ContainerOffset:  blockOffset,
					ContainerName:    filename,
					ContainerMaxSize: maxSize,
					FormatVersion:    int64(formatVersion),
					Codec:            codec,
					PlaintextSize:    plaintextSize,
					CompressedSize:   compressedSizePtr,
					StoredSize:       storedSize,
					CompressionCodec: compressionCodecValue,
					CompressionLevel: compressionLevelPtr,
					LogicalHash:      blockHash,
					CompressedHash:   compressedHash,
					PhysicalHash:     physicalHash,
				}, reader)
				if err != nil {
					appendDeepError(fmt.Errorf("verify block payload for container %q at offset %d: %w", filename, blockOffset, err))
					expectedOffset = nextExpectedOffset
					continue
				}

				expectedOffset = nextExpectedOffset
			}

			// defensive — should not happen unless DB changes mid-run
			if !hasChunks {
				return fmt.Errorf("database invariant violation: container %d claimed to have storage blocks but returned none", containerID)
			}

			if err := chunks.Err(); err != nil {
				appendDeepError(fmt.Errorf("row iteration failed for chunks of container %d: %w", containerID, err))
			}

			if expectedOffset < fileSize {
				appendDeepError(fmt.Errorf(
					"trailing unaccounted bytes in container %d (%s): expected end at %d, file size is %d",
					containerID,
					filename,
					expectedOffset,
					fileSize,
				))
			}

			return nil
		}()
		if processContainerErr != nil {
			appendDeepError(fmt.Errorf("container %d (%s) deep verification failed: %w", containerID, filename, processContainerErr))
			continue
		}
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
