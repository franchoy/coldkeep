package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

func RestoreFile(id int64, outputPath string) error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RestoreFileWithDB(dbconn, id, outputPath); err != nil {
		return err
	}
	return nil
}

func RestoreFileWithDB(dbconn *sql.DB, fileID int64, outputPath string) error {
	return restoreFileWithDBAndDir(dbconn, fileID, outputPath, container.ContainersDir)
}

func RestoreFileWithStorageContext(sgctx StorageContext, fileID int64, outputPath string) error {
	return restoreFileWithDBAndDir(sgctx.DB, fileID, outputPath, sgctx.EffectiveContainerDir())
}

func restoreFileWithDBAndDir(dbconn *sql.DB, fileID int64, outputPath string, containersDir string) error {
	start := time.Now()
	// ------------------------------------------------------------
	// Fetch logical file metadata
	// ------------------------------------------------------------

	ctx := context.Background()

	var expectedFileHash string
	var originalName string

	err := dbconn.QueryRow(
		"SELECT original_name, file_hash FROM logical_file WHERE status = 'COMPLETED' AND id = $1",
		fileID,
	).Scan(&originalName, &expectedFileHash)

	if err == sql.ErrNoRows {
		return fmt.Errorf("logical file %d not found", fileID)
	}
	if err != nil {
		return fmt.Errorf("query logical_file: %w", err)
	}
	// ----------------------------------------------------------------------------------------------
	// Fetch chunk metadata for the logical file from chunk and blocks tables, ordered by chunk_order
	// Only restore from sealed containers to ensure data integrity
	// Only process chunks with COMPLETED status for consistency
	// ----------------------------------------------------------------------------------------------
	rows, err := dbconn.Query(`
					SELECT
				fc.chunk_order,
				b.block_offset,
				b.plaintext_size,
				b.stored_size,
				c.chunk_hash,
				c.size,
				b.codec,
				b.format_version,
				b.nonce,
				b.container_id,
				ctr.filename,
				c.status,
				ctr.max_size,
				c.id
			FROM file_chunk fc
			JOIN chunk c ON c.id = fc.chunk_id
			JOIN blocks b ON b.chunk_id = c.id
			JOIN container ctr ON ctr.id = b.container_id
			WHERE fc.logical_file_id = $1 AND ctr.sealed = TRUE AND c.status = 'COMPLETED'
			ORDER BY fc.chunk_order ASC
	`, fileID)

	if err != nil {
		return fmt.Errorf("query file chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// ------------------------------------------------------------
	// Prepare output file
	// ------------------------------------------------------------
	if st, err := os.Stat(outputPath); err == nil && st.IsDir() {
		outputPath = filepath.Join(outputPath, originalName)
	} else if strings.HasSuffix(outputPath, string(os.PathSeparator)) {
		// if user passed a non-existing dir with trailing slash
		if err := os.MkdirAll(outputPath, 0755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
		outputPath = filepath.Join(outputPath, originalName)
	}

	// Create parent directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("create parent directories for %s: %w", outputPath, err)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output file %s: %w", outputPath, err)
	}
	defer func() {
		if err := outFile.Close(); err != nil {
			fmt.Printf("warning: close output file: %v\n", err)
		}
	}()

	hasher := sha256.New()

	var filecontainer *container.FileContainer
	var containerfilename string

	// Cache transformers by codec to avoid repeated allocations
	transformerCache := make(map[blocks.Codec]blocks.Transformer)

	// Ensure container is closed on early error
	defer func() {
		if filecontainer != nil {
			_ = filecontainer.Close()
		}
	}()

	// ------------------------------------------------------------
	// Restore chunk by chunk
	// ------------------------------------------------------------
	var expectedOrder int64 = 0
	for rows.Next() {

		var chunkOrder int64
		var blockOffset int64
		var plaintextSize int64
		var storedSize int64
		var expectedChunkHash string
		var chunksize int64
		var blocksCodec string
		var blocksFormatVersion int
		var blocksNonce []byte
		var blocksContainerID int64
		var filename string
		var chunkStatus string
		var maxSize int64
		var chunkID int64

		if err := rows.Scan(&chunkOrder, &blockOffset, &plaintextSize, &storedSize, &expectedChunkHash, &chunksize, &blocksCodec, &blocksFormatVersion, &blocksNonce, &blocksContainerID, &filename, &chunkStatus, &maxSize, &chunkID); err != nil {
			return fmt.Errorf("scan chunk row: %w", err)
		}

		if chunkStatus != "COMPLETED" {
			return fmt.Errorf("chunk %d in file %d is not completed (status: %s)", chunkOrder, fileID, chunkStatus)
		}

		// Validate monotonically contiguous chunk sequence
		if chunkOrder != expectedOrder {
			return fmt.Errorf(
				"chunk order discontinuity for file %d: expected order %d got %d (possible missing chunk, broken file graph, or unsealed container reference)",
				fileID, expectedOrder, chunkOrder,
			)
		}
		expectedOrder++

		if containerfilename != filename {
			// Close previous container before opening new one
			if filecontainer != nil {
				if err := filecontainer.Close(); err != nil {
					return fmt.Errorf("close container %q: %w", containerfilename, err)
				}
				filecontainer = nil
			}

			containerPath := filepath.Join(containersDir, filename)
			filecontainer, err = container.OpenExistingContainer(false, containerPath, maxSize)
			if err != nil {
				return fmt.Errorf("open sealed container %q at %s: %w", filename, containerPath, err)
			}
			containerfilename = filename
		}

		// Read block payload
		payload, err := container.ReadPayloadAt(filecontainer, blockOffset, storedSize)
		if err != nil {
			return fmt.Errorf("read payload from container=%s offset=%d size=%d: %w", filename, blockOffset, storedSize, err)
		}

		// Use cached transformer to avoid repeated allocations
		codec := blocks.Codec(blocksCodec)
		transformer, ok := transformerCache[codec]
		if !ok {
			var err error
			transformer, err = blocks.GetBlockTransformer(codec)
			if err != nil {
				return fmt.Errorf("get block transformer for codec %s: %w", blocksCodec, err)
			}
			transformerCache[codec] = transformer
		}

		plaintext, err := transformer.Decode(ctx, blocks.DecodeInput{
			ChunkHash: expectedChunkHash,
			Descriptor: blocks.Descriptor{
				ChunkID:       chunkID,
				Codec:         codec,
				FormatVersion: blocksFormatVersion,
				PlaintextSize: plaintextSize,
				StoredSize:    storedSize,
				Nonce:         blocksNonce,
				ContainerID:   blocksContainerID,
				BlockOffset:   blockOffset,
			},
			Payload: payload,
		})
		if err != nil {
			return fmt.Errorf("decode block from chunk=%d container=%s codec=%s: %w", chunkOrder, filename, blocksCodec, err)
		}

		// Validate plaintext size
		if int64(len(plaintext)) != plaintextSize {
			return fmt.Errorf("plaintext size mismatch: expected %d got %d", plaintextSize, len(plaintext))
		}

		// Validate hashes (DB hash and on-disk record hash)
		sum := sha256.Sum256(plaintext)
		gotHash := hex.EncodeToString(sum[:])
		if gotHash != expectedChunkHash {
			return fmt.Errorf("restored chunk hash mismatch: expected %s got %s", expectedChunkHash, gotHash)
		}

		// Write to output
		if _, err := outFile.Write(plaintext); err != nil {
			return fmt.Errorf("write chunk %d to output file: %w", chunkOrder, err)
		}

		// Update file hash
		if _, err := hasher.Write(plaintext); err != nil {
			return fmt.Errorf("hash restored data: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate chunk rows: %w", err)
	}

	if expectedOrder == 0 {
		// valid ONLY if expectedFileHash == sha256("")
		const emptyFileSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		if expectedFileHash != emptyFileSHA256 {
			return fmt.Errorf("no chunks found for file %d but expected hash is not empty", fileID)
		}
		// else: empty file, no chunks, valid
	}

	// Fsync ensures data is written to disk before returning
	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("fsync output file: %w", err)
	}

	// ------------------------------------------------------------
	// Final Integrity Validation
	// ------------------------------------------------------------
	restoredHash := hex.EncodeToString(hasher.Sum(nil))

	if restoredHash != expectedFileHash {
		return fmt.Errorf(
			"restore integrity check failed: expected %s got %s",
			expectedFileHash,
			restoredHash,
		)
	}
	fmt.Printf("File %s restored successfully\n", originalName)
	fmt.Printf("  Output: %s\n", outputPath)
	fmt.Printf("  SHA256: %s\n", restoredHash)
	utils_print.PrintDuration(start)

	return nil
}
