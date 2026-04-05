package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

// RestoreFileResult contains structured metadata about a restore operation.
type RestoreFileResult struct {
	FileID       int64  `json:"file_id"`
	OriginalName string `json:"original_name"`
	OutputPath   string `json:"output_path"`
	RestoredHash string `json:"restored_hash"`
}

type restoreChunkRow struct {
	chunkOrder          int64
	blockOffset         int64
	plaintextSize       int64
	storedSize          int64
	expectedChunkHash   string
	chunkSize           int64
	blocksCodec         string
	blocksFormatVersion int
	blocksNonce         []byte
	blocksContainerID   int64
	filename            string
	chunkStatus         string
	maxSize             int64
	chunkID             int64
}

func pinLogicalFileRestoreChunks(dbconn *sql.DB, fileID int64) (string, string, []restoreChunkRow, []int64, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return pinLogicalFileRestoreChunksWithContext(ctx, dbconn, fileID)
}

func pinLogicalFileRestoreChunksWithContext(ctx context.Context, dbconn *sql.DB, fileID int64) (string, string, []restoreChunkRow, []int64, error) {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return "", "", nil, nil, err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	var expectedFileHash string
	var originalName string
	err = tx.QueryRowContext(
		ctx,
		"SELECT original_name, file_hash FROM logical_file WHERE status = $1 AND id = $2",
		filestate.LogicalFileCompleted,
		fileID,
	).Scan(&originalName, &expectedFileHash)
	if err == sql.ErrNoRows {
		return "", "", nil, nil, fmt.Errorf("logical file %d not found", fileID)
	}
	if err != nil {
		return "", "", nil, nil, fmt.Errorf("query logical_file: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
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
		LEFT JOIN container ctr ON ctr.id = b.container_id
		WHERE fc.logical_file_id = $1 AND c.status = $2
		ORDER BY fc.chunk_order ASC
	`, fileID, filestate.ChunkCompleted)
	if err != nil {
		return "", "", nil, nil, fmt.Errorf("query file chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	chunkRows := make([]restoreChunkRow, 0)
	pinnedChunkIDs := make([]int64, 0)
	for rows.Next() {
		var row restoreChunkRow
		if err := rows.Scan(
			&row.chunkOrder,
			&row.blockOffset,
			&row.plaintextSize,
			&row.storedSize,
			&row.expectedChunkHash,
			&row.chunkSize,
			&row.blocksCodec,
			&row.blocksFormatVersion,
			&row.blocksNonce,
			&row.blocksContainerID,
			&row.filename,
			&row.chunkStatus,
			&row.maxSize,
			&row.chunkID,
		); err != nil {
			return "", "", nil, nil, fmt.Errorf("scan chunk row: %w", err)
		}
		// If the container is missing (quarantined), filename will be NULL
		// Allow the chunk row, but mark filename as empty string
		if row.filename == "" {
			row.filename = ""
		}
		chunkRows = append(chunkRows, row)
		pinnedChunkIDs = append(pinnedChunkIDs, row.chunkID)
	}
	if err := rows.Err(); err != nil {
		return "", "", nil, nil, fmt.Errorf("iterate chunk rows: %w", err)
	}

	for _, chunkID := range pinnedChunkIDs {
		result, execErr := tx.ExecContext(
			ctx,
			`UPDATE chunk SET pin_count = pin_count + 1 WHERE id = $1`,
			chunkID,
		)
		if execErr != nil {
			return "", "", nil, nil, fmt.Errorf("pin chunk %d for restore: %w", chunkID, execErr)
		}
		rowsAffected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return "", "", nil, nil, fmt.Errorf("rows affected when pinning chunk %d: %w", chunkID, rowsErr)
		}
		if rowsAffected != 1 {
			return "", "", nil, nil, fmt.Errorf("chunk %d disappeared while pinning restore", chunkID)
		}
	}

	if err := tx.Commit(); err != nil {
		return "", "", nil, nil, err
	}
	tx = nil

	return originalName, expectedFileHash, chunkRows, pinnedChunkIDs, nil
}

func unpinRestoreChunks(dbconn *sql.DB, chunkIDs []int64) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return unpinRestoreChunksWithContext(ctx, dbconn, chunkIDs)
}

func unpinRestoreChunksWithContext(ctx context.Context, dbconn *sql.DB, chunkIDs []int64) error {
	if len(chunkIDs) == 0 {
		return nil
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	for _, chunkID := range chunkIDs {
		result, execErr := tx.ExecContext(
			ctx,
			`UPDATE chunk SET pin_count = pin_count - 1 WHERE id = $1 AND pin_count > 0`,
			chunkID,
		)
		if execErr != nil {
			return fmt.Errorf("unpin chunk %d after restore: %w", chunkID, execErr)
		}
		rowsAffected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("rows affected when unpinning chunk %d: %w", chunkID, rowsErr)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("invalid pin_count transition while unpinning chunk %d", chunkID)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil

	return nil
}

func RestoreFile(id int64, outputPath string) error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if _, err := RestoreFileWithDBResult(dbconn, id, outputPath); err != nil {
		return err
	}
	return nil
}

func RestoreFileWithDB(dbconn *sql.DB, fileID int64, outputPath string) error {
	_, err := RestoreFileWithDBResult(dbconn, fileID, outputPath)
	return err
}

func RestoreFileWithDBResult(dbconn *sql.DB, fileID int64, outputPath string) (RestoreFileResult, error) {
	return restoreFileWithDBAndDir(dbconn, fileID, outputPath, container.ContainersDir)
}

func RestoreFileWithStorageContext(sgctx StorageContext, fileID int64, outputPath string) error {
	_, err := RestoreFileWithStorageContextResult(sgctx, fileID, outputPath)
	return err
}

func RestoreFileWithStorageContextResult(sgctx StorageContext, fileID int64, outputPath string) (RestoreFileResult, error) {
	return restoreFileWithDBAndDir(sgctx.DB, fileID, outputPath, sgctx.EffectiveContainerDir())
}

func restoreFileWithDBAndDir(dbconn *sql.DB, fileID int64, outputPath string, containersDir string) (result RestoreFileResult, err error) {
	result.FileID = fileID
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	originalName, expectedFileHash, chunkRows, pinnedChunkIDs, err := pinLogicalFileRestoreChunksWithContext(ctx, dbconn, fileID)
	if err != nil {
		return RestoreFileResult{}, err
	}
	defer func() {
		cleanupCtx, cleanupCancel := db.NewOperationContext(context.Background())
		defer cleanupCancel()
		if unpinErr := unpinRestoreChunksWithContext(cleanupCtx, dbconn, pinnedChunkIDs); unpinErr != nil {
			log.Printf("event=restore_cleanup action=unpin_chunks file_id=%d error=%v", fileID, unpinErr)
			if err == nil {
				err = unpinErr
			}
		}
	}()

	result.OriginalName = originalName

	// ------------------------------------------------------------
	// Prepare output file
	// ------------------------------------------------------------
	if st, err := os.Stat(outputPath); err == nil && st.IsDir() {
		outputPath = filepath.Join(outputPath, originalName)
	} else if strings.HasSuffix(outputPath, string(os.PathSeparator)) {
		// if user passed a non-existing dir with trailing slash
		if err := os.MkdirAll(outputPath, 0755); err != nil {
			return RestoreFileResult{}, fmt.Errorf("create output directory: %w", err)
		}
		outputPath = filepath.Join(outputPath, originalName)
	}
	result.OutputPath = outputPath

	// Create parent directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return RestoreFileResult{}, fmt.Errorf("create parent directories for %s: %w", outputPath, err)
	}

	outFile, err := os.CreateTemp(filepath.Dir(outputPath), ".coldkeep-restore-*")
	if err != nil {
		return RestoreFileResult{}, fmt.Errorf("create temporary output file for %s: %w", outputPath, err)
	}
	tempOutputPath := outFile.Name()
	cleanupTemp := true
	defer func() {
		if outFile != nil {
			_ = outFile.Close()
		}
		if cleanupTemp {
			_ = os.Remove(tempOutputPath)
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
	validChunks := 0
	var firstRestoreError error
	for _, chunkRow := range chunkRows {
		if err := ctx.Err(); err != nil {
			return RestoreFileResult{}, err
		}

		if chunkRow.chunkStatus != filestate.ChunkCompleted {
			continue // skip incomplete chunks (should not happen)
		}

		// If the container is missing (quarantined), skip this chunk but continue restoring others
		if chunkRow.filename == "" {
			log.Printf("event=restore_skip_chunk action=missing_container file_id=%d chunk_id=%d", fileID, chunkRow.chunkID)
			continue
		}

		// Validate monotonically contiguous chunk sequence
		if chunkRow.chunkOrder != expectedOrder {
			log.Printf("event=restore_skip_chunk action=order_discontinuity file_id=%d chunk_order=%d expected=%d", fileID, chunkRow.chunkOrder, expectedOrder)
			continue
		}
		expectedOrder++

		if containerfilename != chunkRow.filename {
			// Close previous container before opening new one
			if filecontainer != nil {
				if err := filecontainer.Close(); err != nil {
					return RestoreFileResult{}, fmt.Errorf("close container %q: %w", containerfilename, err)
				}
				filecontainer = nil
			}

			containerPath := filepath.Join(containersDir, chunkRow.filename)
			filecontainer, err = container.OpenReadOnlyContainer(containerPath, chunkRow.maxSize)
			if err != nil {
				log.Printf("event=restore_skip_chunk action=container_open_failed file_id=%d chunk_id=%d container=%s err=%v", fileID, chunkRow.chunkID, chunkRow.filename, err)
				continue
			}
			containerfilename = chunkRow.filename
		}

		// Read block payload
		payload, err := container.ReadPayloadAt(filecontainer, chunkRow.blockOffset, chunkRow.storedSize)
		if err != nil {
			log.Printf("event=restore_skip_chunk action=read_payload_failed file_id=%d chunk_id=%d container=%s err=%v", fileID, chunkRow.chunkID, chunkRow.filename, err)
			continue
		}

		// Use cached transformer to avoid repeated allocations
		codec := blocks.Codec(chunkRow.blocksCodec)
		transformer, ok := transformerCache[codec]
		if !ok {
			var err error
			transformer, err = blocks.GetBlockTransformer(codec)
			if err != nil {
				log.Printf("event=restore_skip_chunk action=transformer_failed file_id=%d chunk_id=%d codec=%s err=%v", fileID, chunkRow.chunkID, chunkRow.blocksCodec, err)
				if firstRestoreError == nil {
					firstRestoreError = err
				}
				continue
			}
			transformerCache[codec] = transformer
		}

		plaintext, err := transformer.Decode(ctx, blocks.DecodeInput{
			ChunkHash: chunkRow.expectedChunkHash,
			Descriptor: blocks.Descriptor{
				ChunkID:       chunkRow.chunkID,
				Codec:         codec,
				FormatVersion: chunkRow.blocksFormatVersion,
				PlaintextSize: chunkRow.plaintextSize,
				StoredSize:    chunkRow.storedSize,
				Nonce:         chunkRow.blocksNonce,
				ContainerID:   chunkRow.blocksContainerID,
				BlockOffset:   chunkRow.blockOffset,
			},
			Payload: payload,
		})
		if err != nil {
			log.Printf("event=restore_skip_chunk action=decode_failed file_id=%d chunk_id=%d codec=%s err=%v", fileID, chunkRow.chunkID, chunkRow.blocksCodec, err)
			if firstRestoreError == nil {
				firstRestoreError = err
			}
			continue
		}

		// Validate plaintext size
		if int64(len(plaintext)) != chunkRow.plaintextSize {
			sizeErr := fmt.Errorf("plaintext size mismatch for chunk %d: expected %d got %d", chunkRow.chunkID, chunkRow.plaintextSize, len(plaintext))
			log.Printf("event=restore_skip_chunk action=plaintext_size_mismatch file_id=%d chunk_id=%d expected=%d got=%d", fileID, chunkRow.chunkID, chunkRow.plaintextSize, len(plaintext))
			if firstRestoreError == nil {
				firstRestoreError = sizeErr
			}
			continue
		}

		// Validate hashes (DB hash and on-disk record hash)
		sum := sha256.Sum256(plaintext)
		gotHash := hex.EncodeToString(sum[:])
		if gotHash != chunkRow.expectedChunkHash {
			hashErr := fmt.Errorf("restored chunk hash mismatch: expected %s got %s", chunkRow.expectedChunkHash, gotHash)
			log.Printf("event=restore_skip_chunk action=hash_mismatch file_id=%d chunk_id=%d expected=%s got=%s", fileID, chunkRow.chunkID, chunkRow.expectedChunkHash, gotHash)
			if firstRestoreError == nil {
				firstRestoreError = hashErr
			}
			continue
		}

		// Write to output
		if _, err := outFile.Write(plaintext); err != nil {
			log.Printf("event=restore_skip_chunk action=write_failed file_id=%d chunk_id=%d err=%v", fileID, chunkRow.chunkID, err)
			continue
		}

		// Update file hash
		if _, err := hasher.Write(plaintext); err != nil {
			log.Printf("event=restore_skip_chunk action=hash_failed file_id=%d chunk_id=%d err=%v", fileID, chunkRow.chunkID, err)
			continue
		}
		validChunks++
	}

	if validChunks == 0 {
		if firstRestoreError != nil {
			return RestoreFileResult{}, firstRestoreError
		}
		return RestoreFileResult{}, fmt.Errorf("no restorable chunks found for file %d (all referenced containers missing or quarantined)", fileID)
	}

	// Compute the final hash before fsync/close/rename
	restoredHash := hex.EncodeToString(hasher.Sum(nil))
	result.RestoredHash = restoredHash
	if restoredHash != expectedFileHash {
		log.Printf("event=restore_partial_warning file_id=%d expected_hash=%s restored_hash=%s", fileID, expectedFileHash, restoredHash)
	}

	if expectedOrder == 0 {
		// valid ONLY if expectedFileHash == sha256("")
		const emptyFileSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		if expectedFileHash != emptyFileSHA256 {
			return RestoreFileResult{}, fmt.Errorf("no chunks found for file %d but expected hash is not empty", fileID)
		}
		// else: empty file, no chunks, valid
	}

	// Compute the final hash before fsync/close/rename
	restoredHash = hex.EncodeToString(hasher.Sum(nil))
	result.RestoredHash = restoredHash
	if restoredHash != expectedFileHash {
		log.Printf("event=restore_partial_warning file_id=%d expected_hash=%s restored_hash=%s", fileID, expectedFileHash, restoredHash)
	}

	// Fsync ensures data is written to disk before returning
	if err := outFile.Sync(); err != nil {
		return RestoreFileResult{}, fmt.Errorf("fsync output file: %w", err)
	}

	// Close temp file before rename
	if err := outFile.Close(); err != nil {
		return RestoreFileResult{}, fmt.Errorf("close temporary output file: %w", err)
	}
	outFile = nil

	// TEST HOOK: simulate failure after temp file is written but before rename
	if TestRestoreFailBeforeRenameHook != nil {
		if hookErr := TestRestoreFailBeforeRenameHook(tempOutputPath, outputPath); hookErr != nil {
			return RestoreFileResult{}, fmt.Errorf("test hook restore failure: %w", hookErr)
		}
	}

	if err := os.Rename(tempOutputPath, outputPath); err != nil {
		return RestoreFileResult{}, fmt.Errorf("atomically replace output file %s: %w", outputPath, err)
	}
	// Flush directory metadata so the rename is durable across crashes on stricter filesystems.
	dir, err := os.Open(filepath.Dir(outputPath))
	if err != nil {
		return RestoreFileResult{}, fmt.Errorf("open output directory for fsync: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return RestoreFileResult{}, fmt.Errorf("fsync output directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return RestoreFileResult{}, fmt.Errorf("close output directory after fsync: %w", err)
	}
	cleanupTemp = false

	// Set result hash
	result.RestoredHash = restoredHash
	return result, nil
}

// testRestoreFailBeforeRenameHook is a test-only hook for simulating restore failures after temp file is written but before rename.
// It should only be set in tests.
var TestRestoreFailBeforeRenameHook func(tempOutputPath, outputPath string) error
