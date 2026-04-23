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
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

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

	// --- Physical integrity ---
	if err := runPhysicalIntegrityChecks(dbconn); err != nil {
		return err
	}

	// --- Logical reconstruction ---
	if err := runLogicalReconstructionChecks(dbconn); err != nil {
		return err
	}

	log.Printf("Standard system verification completed successfully.")

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

	// blocks.container_id present ↔ chunk.status = COMPLETED.
	if err = checkChunkContainerConsistency(dbconn); err != nil {
		return err
	}

	// COMPLETED chunks have a blocks row with valid container_id and block_offset.
	if err = checkChunkOffsets(dbconn); err != nil {
		return err
	}

	// block_offset + stored_size does not exceed container current_size.
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
	// For every container: open the file, read each block in offset order,
	// decode with the stored codec, and SHA-256 the plaintext against the
	// stored chunk_hash. No chunker algorithm is invoked at any point — the
	// hash comparison is against the value written at ingest time.
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
	transformerCache := make(map[blocks.Codec]blocks.Transformer)

	// Count all non-quarantined containers that currently hold completed chunks.
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	containerCount := 0
	containerCountErr := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM container ctr
		WHERE ctr.quarantine = FALSE
		AND EXISTS (
			SELECT 1
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE b.container_id = ctr.id
			AND c.status = $1
		)
	`, filestate.ChunkCompleted).Scan(&containerCount)
	if containerCountErr != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query deep-verify container count: %v", containerCountErr)
		return fmt.Errorf("failed to query deep-verify container count: %w", containerCountErr)
	}

	processedContainers := 0

	containers, err := dbconn.QueryContext(ctx, `
		SELECT ctr.id, ctr.filename, ctr.current_size, ctr.max_size
		FROM container ctr
		WHERE ctr.quarantine = FALSE
		AND EXISTS (
			SELECT 1
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE b.container_id = ctr.id
			AND c.status = $1
		)
	`, filestate.ChunkCompleted)
	if err != nil {
		log.Println(" ERROR ")
		log.Printf("Failed to query deep-verify containers: %v", err)
		return fmt.Errorf("failed to query deep-verify containers: %w", err)
	}
	defer func() { _ = containers.Close() }()

	for containers.Next() {
		processedContainers++
		var containerID int
		var filename string
		var currentSize int64
		var maxSize int64
		if err := containers.Scan(&containerID, &filename, &currentSize, &maxSize); err != nil {
			appendDeepError(fmt.Errorf("failed to scan container info: %w", err))
			continue
		}
		log.Printf("Verifying container %d/%d: %s", processedContainers, containerCount, filename)

		//construct full path
		fullPath := filepath.Join(containersDir, filename)

		fileSize := currentSize

		processContainerErr := func() (retErr error) {
			//fetch chunks ordered by offset
			chunks, err := dbconn.QueryContext(ctx, `SELECT 
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
								AND c.status = $2
								ORDER BY b.block_offset`, containerID, filestate.ChunkCompleted)
			if err != nil {
				return fmt.Errorf("failed to query chunks for container %d: %w", containerID, err)
			}
			defer func() { _ = chunks.Close() }()

			// Open in read-only mode for verification safety.
			filecontainer, err := container.OpenReadOnlyContainer(fullPath, maxSize)
			if err != nil {
				return fmt.Errorf("failed to open container %s: %w", fullPath, err)
			}
			defer func() {
				if closeErr := filecontainer.Close(); closeErr != nil && retErr == nil {
					retErr = fmt.Errorf("close container %q: %w", filename, closeErr)
				}
			}()

			hasChunks := false
			expectedOffset := int64(container.ContainerHdrLen)

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

				payload, err := container.ReadPayloadAt(filecontainer, blockOffset, storedSize)
				if err != nil {
					appendDeepError(fmt.Errorf("read block payload for container %q at offset %d: %w", filename, blockOffset, err))
					expectedOffset = nextExpectedOffset
					continue
				}
				codecType := blocks.Codec(codec)
				transformer, ok := transformerCache[codecType]
				if !ok {
					transformer, err = blocks.GetBlockTransformer(codecType)
					if err != nil {
						appendDeepError(fmt.Errorf("get transformer for codec %q in container %q: %w", codec, filename, err))
						expectedOffset = nextExpectedOffset
						continue
					}
					transformerCache[codecType] = transformer
				}

				plaintext, err := transformer.Decode(ctx, blocks.DecodeInput{
					ChunkHash: chunkHash,
					Descriptor: blocks.Descriptor{
						Codec:         codecType,
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
					appendDeepError(fmt.Errorf("decode block payload for container %q at offset %d: %w", filename, blockOffset, err))
					expectedOffset = nextExpectedOffset
					continue
				}

				if int64(len(plaintext)) != plaintextSize {
					appendDeepError(fmt.Errorf(
						"plaintext size mismatch in container %q at offset %d: expected %d got %d",
						filename,
						blockOffset,
						plaintextSize,
						len(plaintext),
					))
					expectedOffset = nextExpectedOffset
					continue
				}

				// Validate hashes (DB hash and on-disk record hash)
				sum := sha256.Sum256(plaintext)
				sumHex := hex.EncodeToString(sum[:])

				if sumHex != chunkHash {
					appendDeepError(fmt.Errorf("block hash mismatch at offset %d (db=%s computed=%s) for container %q", blockOffset, chunkHash, sumHex, filename))
					expectedOffset = nextExpectedOffset
					continue
				}

				expectedOffset = nextExpectedOffset
			}

			// defensive — should not happen unless DB changes mid-run
			if !hasChunks {
				return fmt.Errorf("database invariant violation: container %d claimed to have completed chunks but returned none", containerID)
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
	if err := containers.Err(); err != nil {
		appendDeepError(fmt.Errorf("row iteration failed for containers: %w", err))
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
