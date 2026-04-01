package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

type payloadStatefulWriter interface {
	AppendPayload(tx db.DBTX, payload []byte) (container.LocalPlacement, error)
	FinalizeContainer() error
}

type optionalActiveContainerSyncer interface {
	SyncActiveContainer() error
}

// optionalAppendRollbacker is implemented by writers that can truncate a physical
// container file back to its pre-append offset when the enclosing DB transaction
// is rolled back or fails to commit.
type optionalAppendRollbacker interface {
	RollbackLastAppend() error
}

// optionalAppendCommitAcknowledger is implemented by writers that maintain rollback
// state after a physical append. Calling AcknowledgeAppendCommitted after a
// successful tx.Commit() closes the commit path of the state machine, ensuring
// that already-committed bytes can never be accidentally truncated by a subsequent
// RollbackLastAppend call.
type optionalAppendCommitAcknowledger interface {
	AcknowledgeAppendCommitted()
}

type optionalFailedActiveContainerRetirer interface {
	RetireActiveContainer() error
}

type optionalWriterDBBinder interface {
	BindDB(dbconn *sql.DB)
}

// rollbackWriterLastAppend calls RollbackLastAppend on the writer if it supports it.
// Safe to call unconditionally on any error path; if no physical write is pending the
// implementation returns immediately without truncating.
func rollbackWriterLastAppend(writer payloadStatefulWriter) {
	if rb, ok := writer.(optionalAppendRollbacker); ok {
		_ = rb.RollbackLastAppend()
	}
}

// acknowledgeWriterAppendCommitted calls AcknowledgeAppendCommitted on the writer
// if it supports it. Must be called immediately after every successful tx.Commit()
// that followed an AppendPayload call, completing the commit path of the writer
// state machine so pending rollback bookkeeping is cleared.
func acknowledgeWriterAppendCommitted(writer payloadStatefulWriter) {
	if ack, ok := writer.(optionalAppendCommitAcknowledger); ok {
		ack.AcknowledgeAppendCommitted()
	}
}

func retireWriterActiveContainer(writer payloadStatefulWriter) error {
	if retirer, ok := writer.(optionalFailedActiveContainerRetirer); ok {
		return retirer.RetireActiveContainer()
	}
	return nil
}

func bindWriterDB(writer payloadStatefulWriter, dbconn *sql.DB) {
	if binder, ok := writer.(optionalWriterDBBinder); ok {
		binder.BindDB(dbconn)
	}
}

func quarantineContainerNow(dbconn *sql.DB, containerID int64) error {
	return container.QuarantineContainer(dbconn, containerID)
}

type optionalContainerSealer interface {
	SealContainer(tx db.DBTX, containerID int64, filename string, containersDir string) error
}

// optionalPreFinalizationMarker is implemented by writers that can commit a
// durable "sealing in progress" marker to the DB before the physical container
// file is closed. Calling this before FinalizeContainer allows startup recovery
// to detect and repair containers that were physically finalized but not yet
// logically sealed due to a crash or a failed DB transaction.
type optionalPreFinalizationMarker interface {
	MarkSealingForContainer(containerID int64) error
}

func markContainerSealingInTx(tx *sql.Tx, containerID int64) error {
	if tx == nil || containerID <= 0 {
		return nil
	}
	if _, err := tx.Exec(`UPDATE container SET sealing = TRUE WHERE id = $1`, containerID); err != nil {
		return fmt.Errorf("mark container %d sealing in tx: %w", containerID, err)
	}
	return nil
}

// StoreFileResult contains structured metadata about a store operation.
type StoreFileResult struct {
	FileID        int64  `json:"file_id"`
	FileHash      string `json:"file_hash"`
	Path          string `json:"path"`
	AlreadyStored bool   `json:"already_stored"`
}

func sealContainerWithWriter(tx db.DBTX, writer payloadStatefulWriter, containerID int64, filename string, containersDir string) error {
	if sealer, ok := writer.(optionalContainerSealer); ok {
		return sealer.SealContainer(tx, containerID, filename, containersDir)
	}
	return container.SealContainerInDir(tx, containerID, filename, containersDir)
}

func syncActiveContainerWithWriter(writer payloadStatefulWriter) error {
	if syncer, ok := writer.(optionalActiveContainerSyncer); ok {
		return syncer.SyncActiveContainer()
	}
	return nil
}

func newWriterFromPrototype(prototype container.ContainerWriter) (container.ContainerWriter, error) {
	switch w := prototype.(type) {
	case *container.LocalWriter:
		// Clone LocalWriter per worker for thread safety; propagate DB connection
		// so the per-worker writer can commit sealing markers independently.
		return container.NewLocalWriterWithDirAndDB(w.Dir(), w.MaxSize(), w.DB()), nil
	case *container.SimulatedWriter:
		// Do NOT clone SimulatedWriter; return the original for shared, realistic container packing.
		// Concurrency contract: SimulatedWriter is internally synchronized (mutex-protected).
		return w, nil
	default:
		return nil, fmt.Errorf("unsupported writer type for cloning: %T", prototype)
	}
}

type reusableLogicalFileGraphSummary struct {
	totalSize         int64
	chunkRefs         int64
	brokenChunkOrders int64
	invalidChunks     int64
	missingBlocks     int64
	invalidContainers int64
}

type reuseSemanticValidationMode string

const (
	reuseSemanticValidationOff        reuseSemanticValidationMode = "off"
	reuseSemanticValidationSuspicious reuseSemanticValidationMode = "suspicious"
	reuseSemanticValidationAlways     reuseSemanticValidationMode = "always"
)

type semanticReuseSuspicionSummary struct {
	fileRetryCount       int64
	chunkRetryRefs       int64
	mutableContainerRefs int64
}

type reusableCompletedChunkSummary struct {
	blockRows                int64
	existingContainerRows    int64
	quarantinedContainerRows int64
}

func validateReusableCompletedChunkWithContext(ctx context.Context, dbconn *sql.DB, chunkID int64) error {
	var summary reusableCompletedChunkSummary
	err := dbconn.QueryRowContext(ctx, `
		SELECT
			COUNT(b.id) AS block_rows,
			COUNT(ctr.id) AS existing_container_rows,
			COALESCE(SUM(CASE WHEN ctr.quarantine THEN 1 ELSE 0 END), 0) AS quarantined_container_rows
		FROM blocks b
		LEFT JOIN container ctr ON ctr.id = b.container_id
		WHERE b.chunk_id = $1
	`, chunkID).Scan(
		&summary.blockRows,
		&summary.existingContainerRows,
		&summary.quarantinedContainerRows,
	)
	if err != nil {
		return fmt.Errorf("query reusable completed chunk %d: %w", chunkID, err)
	}

	if summary.blockRows != 1 {
		return fmt.Errorf("chunk %d has invalid block metadata rows: expected 1 got %d", chunkID, summary.blockRows)
	}
	if summary.existingContainerRows != 1 {
		return fmt.Errorf("chunk %d has missing container metadata", chunkID)
	}
	if summary.quarantinedContainerRows != 0 {
		return fmt.Errorf("chunk %d references quarantined container", chunkID)
	}

	return nil
}

func markChunkForRebuildWithContext(ctx context.Context, dbconn *sql.DB, chunkID int64) error {
	result, err := dbconn.ExecContext(ctx,
		`UPDATE chunk SET status = $1 WHERE id = $2 AND status = $3`,
		filestate.ChunkAborted,
		chunkID,
		filestate.ChunkCompleted,
	)
	if err != nil {
		return fmt.Errorf("mark chunk %d for rebuild: %w", chunkID, err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("rows affected while marking chunk %d for rebuild: %w", chunkID, err)
	}
	if _, err := dbconn.ExecContext(ctx, `DELETE FROM blocks WHERE chunk_id = $1`, chunkID); err != nil {
		return fmt.Errorf("delete stale blocks while marking chunk %d for rebuild: %w", chunkID, err)
	}

	return nil
}

func validateReusableLogicalFileGraphWithContext(ctx context.Context, dbconn *sql.DB, fileID int64, containersDir string) error {
	var summary reusableLogicalFileGraphSummary
	err := dbconn.QueryRowContext(ctx, `
		WITH target_file AS (
			SELECT id, total_size
			FROM logical_file
			WHERE id = $1
		),
		ordered_chunks AS (
			SELECT
				fc.chunk_id,
				fc.chunk_order,
				ROW_NUMBER() OVER (ORDER BY fc.chunk_order) - 1 AS expected_order
			FROM file_chunk fc
			WHERE fc.logical_file_id = $1
		),
		graph_summary AS (
			SELECT
				COUNT(*) AS chunk_refs,
				COALESCE(SUM(CASE WHEN oc.chunk_order <> oc.expected_order THEN 1 ELSE 0 END), 0) AS broken_chunk_orders,
				COALESCE(SUM(CASE WHEN c.id IS NULL OR c.status <> $2 THEN 1 ELSE 0 END), 0) AS invalid_chunks,
				COALESCE(SUM(CASE WHEN b.id IS NULL THEN 1 ELSE 0 END), 0) AS missing_blocks,
				COALESCE(SUM(CASE WHEN ctr.id IS NULL OR ctr.quarantine THEN 1 ELSE 0 END), 0) AS invalid_containers
			FROM ordered_chunks oc
			LEFT JOIN chunk c ON c.id = oc.chunk_id
			LEFT JOIN blocks b ON b.chunk_id = oc.chunk_id
			LEFT JOIN container ctr ON ctr.id = b.container_id
		)
		SELECT
			tf.total_size,
			gs.chunk_refs,
			gs.broken_chunk_orders,
			gs.invalid_chunks,
			gs.missing_blocks,
			gs.invalid_containers
		FROM target_file tf
		CROSS JOIN graph_summary gs
	`, fileID, filestate.ChunkCompleted).Scan(
		&summary.totalSize,
		&summary.chunkRefs,
		&summary.brokenChunkOrders,
		&summary.invalidChunks,
		&summary.missingBlocks,
		&summary.invalidContainers,
	)
	if err == sql.ErrNoRows {
		return fmt.Errorf("logical file %d does not exist", fileID)
	}
	if err != nil {
		return fmt.Errorf("query reusable logical file graph %d: %w", fileID, err)
	}

	if summary.totalSize == 0 {
		if summary.chunkRefs != 0 {
			return fmt.Errorf("zero-byte logical file %d unexpectedly has %d file_chunk rows", fileID, summary.chunkRefs)
		}
		return nil
	}

	if summary.chunkRefs == 0 {
		return fmt.Errorf("logical file %d has no file_chunk rows", fileID)
	}
	if summary.brokenChunkOrders > 0 {
		return fmt.Errorf("logical file %d has non-contiguous chunk ordering", fileID)
	}
	if summary.invalidChunks > 0 {
		return fmt.Errorf("logical file %d references missing or non-completed chunks", fileID)
	}
	if summary.missingBlocks > 0 {
		return fmt.Errorf("logical file %d references chunks without block metadata", fileID)
	}
	if summary.invalidContainers > 0 {
		return fmt.Errorf("logical file %d references missing or quarantined containers", fileID)
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT ctr.id, ctr.filename
		FROM file_chunk fc
		JOIN blocks b ON b.chunk_id = fc.chunk_id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE fc.logical_file_id = $1
	`, fileID)
	if err != nil {
		return fmt.Errorf("query reusable logical file containers %d: %w", fileID, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var containerID int64
		var filename string
		if err := rows.Scan(&containerID, &filename); err != nil {
			return fmt.Errorf("scan reusable logical file container for file %d: %w", fileID, err)
		}
		fullPath := filepath.Join(containersDir, filename)
		if _, err := os.Stat(fullPath); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("logical file %d references missing container file %d (%s)", fileID, containerID, fullPath)
			}
			return fmt.Errorf("stat container file for logical file %d container %d: %w", fileID, containerID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate reusable logical file containers %d: %w", fileID, err)
	}

	return nil
}

func loadReuseSemanticValidationModeFromEnv() reuseSemanticValidationMode {
	modeValue := strings.ToLower(strings.TrimSpace(os.Getenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION")))
	switch modeValue {
	case "", string(reuseSemanticValidationSuspicious):
		return reuseSemanticValidationSuspicious
	case string(reuseSemanticValidationOff):
		return reuseSemanticValidationOff
	case string(reuseSemanticValidationAlways):
		return reuseSemanticValidationAlways
	default:
		log.Printf("event=store_reuse_semantic_validation_invalid_mode value=%q fallback=%q", modeValue, reuseSemanticValidationSuspicious)
		return reuseSemanticValidationSuspicious
	}
}

func shouldRunSemanticReuseValidationWithContext(ctx context.Context, dbconn *sql.DB, fileID int64, mode reuseSemanticValidationMode) (bool, string, error) {
	switch mode {
	case reuseSemanticValidationOff:
		return false, "mode=off", nil
	case reuseSemanticValidationAlways:
		return true, "mode=always", nil
	}

	var summary semanticReuseSuspicionSummary
	err := dbconn.QueryRowContext(ctx, `
		SELECT
			lf.retry_count,
			COALESCE(COUNT(DISTINCT CASE WHEN c.retry_count > 0 THEN c.id END), 0) AS chunk_retry_refs,
			COALESCE(COUNT(DISTINCT CASE WHEN ctr.sealed = FALSE OR ctr.sealing = TRUE THEN ctr.id END), 0) AS mutable_container_refs
		FROM logical_file lf
		LEFT JOIN file_chunk fc ON fc.logical_file_id = lf.id
		LEFT JOIN chunk c ON c.id = fc.chunk_id
		LEFT JOIN blocks b ON b.chunk_id = c.id
		LEFT JOIN container ctr ON ctr.id = b.container_id
		WHERE lf.id = $1
		GROUP BY lf.retry_count
	`, fileID).Scan(
		&summary.fileRetryCount,
		&summary.chunkRetryRefs,
		&summary.mutableContainerRefs,
	)
	if err == sql.ErrNoRows {
		return false, "", fmt.Errorf("logical file %d does not exist", fileID)
	}
	if err != nil {
		return false, "", fmt.Errorf("query semantic reuse suspicion summary for logical file %d: %w", fileID, err)
	}

	reasons := make([]string, 0, 3)
	if summary.fileRetryCount > 0 {
		reasons = append(reasons, fmt.Sprintf("file_retry_count=%d", summary.fileRetryCount))
	}
	if summary.chunkRetryRefs > 0 {
		reasons = append(reasons, fmt.Sprintf("chunk_retry_refs=%d", summary.chunkRetryRefs))
	}
	if summary.mutableContainerRefs > 0 {
		reasons = append(reasons, fmt.Sprintf("mutable_container_refs=%d", summary.mutableContainerRefs))
	}

	if len(reasons) == 0 {
		return false, "mode=suspicious no_signals", nil
	}

	return true, "mode=suspicious " + strings.Join(reasons, ","), nil
}

func validateReusableLogicalFileSemanticsWithContext(ctx context.Context, dbconn *sql.DB, fileID int64, containersDir string) (err error) {
	_, expectedFileHash, chunkRows, pinnedChunkIDs, err := pinLogicalFileRestoreChunksWithContext(ctx, dbconn, fileID)
	if err != nil {
		return fmt.Errorf("pin reusable logical file %d for semantic validation: %w", fileID, err)
	}
	defer func() {
		if unpinErr := unpinRestoreChunksWithContext(ctx, dbconn, pinnedChunkIDs); unpinErr != nil {
			if err == nil {
				err = fmt.Errorf("unpin reusable logical file %d chunks after semantic validation: %w", fileID, unpinErr)
				return
			}
			log.Printf("event=store_reuse_semantic_validation_unpin_failed file_id=%d error=%v", fileID, unpinErr)
		}
	}()

	hasher := sha256.New()
	var filecontainer *container.FileContainer
	containerfilename := ""
	defer func() {
		if filecontainer != nil {
			_ = filecontainer.Close()
		}
	}()

	transformerCache := make(map[blocks.Codec]blocks.Transformer)
	var expectedOrder int64

	for _, chunkRow := range chunkRows {
		if chunkRow.chunkOrder != expectedOrder {
			return fmt.Errorf("semantic reuse chunk order discontinuity for file %d: expected %d got %d", fileID, expectedOrder, chunkRow.chunkOrder)
		}
		expectedOrder++

		if containerfilename != chunkRow.filename {
			if filecontainer != nil {
				if closeErr := filecontainer.Close(); closeErr != nil {
					return fmt.Errorf("close container %q during semantic validation: %w", containerfilename, closeErr)
				}
				filecontainer = nil
			}

			containerPath := filepath.Join(containersDir, chunkRow.filename)
			filecontainer, err = container.OpenReadOnlyContainer(containerPath, chunkRow.maxSize)
			if err != nil {
				return fmt.Errorf("open container %q during semantic validation: %w", chunkRow.filename, err)
			}
			containerfilename = chunkRow.filename
		}

		payload, err := container.ReadPayloadAt(filecontainer, chunkRow.blockOffset, chunkRow.storedSize)
		if err != nil {
			return fmt.Errorf("read payload for semantic validation from container=%s offset=%d size=%d: %w", chunkRow.filename, chunkRow.blockOffset, chunkRow.storedSize, err)
		}

		codec := blocks.Codec(chunkRow.blocksCodec)
		transformer, ok := transformerCache[codec]
		if !ok {
			transformer, err = blocks.GetBlockTransformer(codec)
			if err != nil {
				return fmt.Errorf("get block transformer for semantic validation codec %s: %w", chunkRow.blocksCodec, err)
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
			return fmt.Errorf("decode payload for semantic validation file=%d chunk_id=%d: %w", fileID, chunkRow.chunkID, err)
		}

		if int64(len(plaintext)) != chunkRow.plaintextSize {
			return fmt.Errorf("semantic reuse plaintext size mismatch for file %d chunk %d: expected %d got %d", fileID, chunkRow.chunkID, chunkRow.plaintextSize, len(plaintext))
		}

		sum := sha256.Sum256(plaintext)
		gotChunkHash := hex.EncodeToString(sum[:])
		if gotChunkHash != chunkRow.expectedChunkHash {
			return fmt.Errorf("semantic reuse chunk hash mismatch for file %d chunk %d: expected %s got %s", fileID, chunkRow.chunkID, chunkRow.expectedChunkHash, gotChunkHash)
		}

		if _, err := hasher.Write(plaintext); err != nil {
			return fmt.Errorf("update semantic reuse file hash for file %d chunk %d: %w", fileID, chunkRow.chunkID, err)
		}
	}

	if expectedOrder == 0 {
		const emptyFileSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		if expectedFileHash != emptyFileSHA256 {
			return fmt.Errorf("semantic reuse found no chunks for non-empty-hash file %d", fileID)
		}
		return nil
	}

	gotFileHash := hex.EncodeToString(hasher.Sum(nil))
	if gotFileHash != expectedFileHash {
		return fmt.Errorf("semantic reuse file hash mismatch for file %d: expected %s got %s", fileID, expectedFileHash, gotFileHash)
	}

	return nil
}

func validateReusableLogicalFileForStoreWithContext(ctx context.Context, dbconn *sql.DB, fileID int64, containersDir string) error {
	if err := validateReusableLogicalFileGraphWithContext(ctx, dbconn, fileID, containersDir); err != nil {
		return err
	}

	mode := loadReuseSemanticValidationModeFromEnv()
	runSemanticValidation, reason, err := shouldRunSemanticReuseValidationWithContext(ctx, dbconn, fileID, mode)
	if err != nil {
		return err
	}
	if !runSemanticValidation {
		return nil
	}

	if err := validateReusableLogicalFileSemanticsWithContext(ctx, dbconn, fileID, containersDir); err != nil {
		return fmt.Errorf("semantic reuse validation failed (%s): %w", reason, err)
	}

	log.Printf("event=store_reuse_semantic_validation_passed file_id=%d mode=%s reason=%s", fileID, mode, reason)
	return nil
}

func markLogicalFileForRebuildWithContext(ctx context.Context, dbconn *sql.DB, fileID int64) error {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for logical file %d rebuild: %w", fileID, err)
	}
	txclosed := false
	defer func() {
		if !txclosed {
			_ = tx.Rollback()
		}
	}()

	// Mark the file ABORTED only if it is still COMPLETED; bail out silently if
	// some other goroutine already transitioned it.
	result, err := tx.ExecContext(ctx,
		`UPDATE logical_file SET status = $1 WHERE id = $2 AND status = $3`,
		filestate.LogicalFileAborted,
		fileID,
		filestate.LogicalFileCompleted,
	)
	if err != nil {
		return fmt.Errorf("mark logical file %d for rebuild: %w", fileID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected while marking logical file %d for rebuild: %w", fileID, err)
	}
	if rowsAffected == 0 {
		// Another goroutine already transitioned this row; nothing left to do.
		txclosed = true
		return tx.Rollback()
	}

	// Collect chunk IDs referenced by stale file_chunk mappings so we can
	// decrement their live_ref_count before removing the mappings.
	rows, err := tx.QueryContext(ctx,
		`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1`,
		fileID,
	)
	if err != nil {
		return fmt.Errorf("query stale file_chunk rows for logical file %d: %w", fileID, err)
	}
	var chunkIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan stale chunk id for logical file %d: %w", fileID, err)
		}
		chunkIDs = append(chunkIDs, id)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close stale file_chunk cursor for logical file %d: %w", fileID, err)
	}

	// Decrement live_ref_count for every stale mapping.  We guard against
	// going below zero (which the DB schema CHECK constraint would reject anyway)
	// by only decrementing when live_ref_count > 0.
	for _, chunkID := range chunkIDs {
		var remaining int64
		err := tx.QueryRowContext(ctx,
			`UPDATE chunk
			 SET live_ref_count = live_ref_count - 1
			 WHERE id = $1 AND live_ref_count > 0
			 RETURNING live_ref_count`,
			chunkID,
		).Scan(&remaining)
		if err == sql.ErrNoRows {
			// live_ref_count was already 0; inconsistent but non-fatal here –
			// the mapping is being removed regardless.
			continue
		}
		if err != nil {
			return fmt.Errorf("decrement live_ref_count for chunk %d (logical file %d): %w", chunkID, fileID, err)
		}
	}

	// Remove all stale file_chunk mappings for this logical file.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM file_chunk WHERE logical_file_id = $1`,
		fileID,
	); err != nil {
		return fmt.Errorf("delete stale file_chunk rows for logical file %d: %w", fileID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit logical file %d rebuild reset: %w", fileID, err)
	}
	txclosed = true
	return nil
}

// -----------------------------------------------------------------------------
// CLAIM-BASED CONCURRENCY CONTROL FOR LOGICAL FILES AND CHUNKS
// -----------------------------------------------------------------------------

func claimLogicalFile(dbconn *sql.DB, fileinfo os.FileInfo, fileHash string) (fileID int64, filestatus string, err error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return claimLogicalFileWithContext(ctx, dbconn, fileinfo, fileHash)
}

func claimLogicalFileWithContext(ctx context.Context, dbconn *sql.DB, fileinfo os.FileInfo, fileHash string) (fileID int64, filestatus string, err error) {

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return 0, "", err
	}
	txclosed := false
	defer func() {
		if err != nil && !txclosed {
			_ = tx.Rollback()
		}
	}()

	// Insert logical file (concurrency-safe)
	// If another goroutine inserts the same hash at the same time, we won't error.

	insErr := tx.QueryRowContext(
		ctx,
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (file_hash, total_size) DO NOTHING
		RETURNING id`,
		fileinfo.Name(),
		fileinfo.Size(),
		fileHash,
		filestate.LogicalFileProcessing,
	).Scan(&fileID)

	switch insErr {
	case sql.ErrNoRows:
		// Conflict happened: someone else already stored this file hash
		var existingID int64
		if err := tx.QueryRowContext(
			ctx,
			`SELECT id, status FROM logical_file WHERE file_hash = $1 and total_size = $2`,
			fileHash,
			fileinfo.Size(),
		).Scan(&existingID, &filestatus); err != nil {
			return 0, "", err
		}

		switch filestatus {
		case filestate.LogicalFileCompleted:
			// File is marked COMPLETED. Before reusing it, ensure linked chunks are still COMPLETED.
			_ = tx.Rollback() // Don't hold locks while validating
			txclosed = true

			var inconsistentChunks int
			if err := dbconn.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM file_chunk fc
				JOIN chunk c ON c.id = fc.chunk_id
				WHERE fc.logical_file_id = $1
				AND c.status != $2
			`, existingID, filestate.ChunkCompleted).Scan(&inconsistentChunks); err != nil {
				return 0, "", err
			}

			if inconsistentChunks == 0 {
				return existingID, filestatus, nil
			}

			// Chunk graph is inconsistent; force a retry on the same logical_file row.
			filestatus = filestate.LogicalFileAborted
		case filestate.LogicalFileProcessing:
			// Another process is currently storing this file: we can wait and reuse it once done
			_ = tx.Rollback() // Don't hold locks while waiting
			txclosed = true
			finalStatus, waitErr := waitForLogicalFileTerminalStatus(ctx, dbconn, existingID)
			if waitErr != nil {
				return 0, "", waitErr
			}
			if finalStatus == filestate.LogicalFileCompleted {
				return existingID, finalStatus, nil
			}
			filestatus = finalStatus
		}

		// If we reach here, it means the previous attempt was aborted while we were waiting: we can try to store again
		if filestatus == filestate.LogicalFileAborted {
			for casAttempt := 0; casAttempt < 3; casAttempt++ {
				tx2, beginErr := dbconn.BeginTx(ctx, nil)
				if beginErr != nil {
					return 0, "", beginErr
				}
				casResult, execErr := tx2.ExecContext(
					ctx,
					`UPDATE logical_file
					 SET status = $1, retry_count = retry_count + 1
					 WHERE id = $2 AND status = $3`,
					filestate.LogicalFileProcessing,
					existingID,
					filestate.LogicalFileAborted,
				)
				if execErr != nil {
					_ = tx2.Rollback()
					return 0, "", execErr
				}
				rowsAffected, rowsErr := casResult.RowsAffected()
				if rowsErr != nil {
					_ = tx2.Rollback()
					return 0, "", rowsErr
				}
				if commitErr := tx2.Commit(); commitErr != nil {
					_ = tx2.Rollback()
					return 0, "", commitErr
				}

				if rowsAffected == 1 {
					fileID = existingID
					filestatus = filestate.LogicalFileProcessing
					break
				}

				var latestStatus string
				if statusErr := dbconn.QueryRowContext(ctx, `SELECT status FROM logical_file WHERE id = $1`, existingID).Scan(&latestStatus); statusErr != nil {
					return 0, "", statusErr
				}
				switch latestStatus {
				case filestate.LogicalFileCompleted:
					return existingID, latestStatus, nil
				case filestate.LogicalFileProcessing:
					finalStatus, waitErr := waitForLogicalFileTerminalStatus(ctx, dbconn, existingID)
					if waitErr != nil {
						return 0, "", waitErr
					}
					if finalStatus == filestate.LogicalFileCompleted {
						return existingID, finalStatus, nil
					}
				case filestate.LogicalFileAborted:
					// Contended retry claim: loop and attempt CAS again.
				default:
					return 0, "", fmt.Errorf("unexpected logical_file status during claim retry: %s", latestStatus)
				}
			}

			if filestatus != filestate.LogicalFileProcessing {
				return 0, "", fmt.Errorf("could not claim aborted logical file %d after contention", existingID)
			}
		}
	case nil:
		// We won: this file is new and we should store it
		filestatus = filestate.LogicalFileProcessing
	default:
		return 0, "", insErr
	}
	if !txclosed {
		if err := tx.Commit(); err != nil {
			return 0, "", err
		}
	}

	return fileID, filestatus, nil
}

func claimChunk(dbconn *sql.DB, chunkHash string, chunksize int64) (chunkID int64, chunkstatus string, isNew bool, err error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return claimChunkWithContext(ctx, dbconn, chunkHash, chunksize)
}

func prepareLogicalFileForStoreWithContext(ctx context.Context, dbconn *sql.DB, fileinfo os.FileInfo, fileHash string, containersDir string) (fileID int64, filestatus string, err error) {
	fileID, filestatus, err = claimLogicalFileWithContext(ctx, dbconn, fileinfo, fileHash)
	if err != nil {
		return 0, "", err
	}

	if filestatus != filestate.LogicalFileCompleted {
		return fileID, filestatus, nil
	}

	reuseErr := validateReusableLogicalFileForStoreWithContext(ctx, dbconn, fileID, containersDir)
	if reuseErr == nil {
		return fileID, filestatus, nil
	}

	log.Printf("event=store_reuse_validation_failed file_id=%d error=%v", fileID, reuseErr)
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		return 0, "", errors.Join(reuseErr, err)
	}

	fileID, filestatus, err = claimLogicalFileWithContext(ctx, dbconn, fileinfo, fileHash)
	if err != nil {
		return 0, "", err
	}
	if filestatus != filestate.LogicalFileCompleted {
		return fileID, filestatus, nil
	}

	reuseErr = validateReusableLogicalFileForStoreWithContext(ctx, dbconn, fileID, containersDir)
	if reuseErr != nil {
		return 0, "", fmt.Errorf("logical file %d remained non-reusable after retry: %w", fileID, reuseErr)
	}

	return fileID, filestatus, nil
}

func claimChunkWithContext(ctx context.Context, dbconn *sql.DB, chunkHash string, chunksize int64) (chunkID int64, chunkstatus string, isNew bool, err error) {

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return 0, "", false, err
	}
	txclosed := false
	defer func() {
		if err != nil && !txclosed {
			_ = tx.Rollback()
		}
	}()

	// Insert chunk (concurrency-safe)
	// If another goroutine inserts the same hash at the same time, we won't error.
	insErr := tx.QueryRowContext(
		ctx,
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
				VALUES ($1, $2, $3, 0)
				ON CONFLICT (chunk_hash, size) DO NOTHING
				RETURNING id`,
		chunkHash,
		chunksize,
		filestate.ChunkProcessing,
	).Scan(&chunkID)

	switch insErr {
	case nil:
		// We won: this chunk is new
		chunkstatus = filestate.ChunkProcessing
		isNew = true
	case sql.ErrNoRows:
		// Someone else inserted it first
		if err := tx.QueryRowContext(ctx, `SELECT id, status FROM chunk WHERE chunk_hash = $1 AND size = $2`, chunkHash, chunksize).Scan(&chunkID, &chunkstatus); err != nil {
			return 0, "", false, err
		}
		switch chunkstatus {
		case filestate.ChunkCompleted:
			// Chunk is marked COMPLETED; verify its block/container metadata before reuse.
			_ = tx.Rollback() // Don't hold locks while waiting
			txclosed = true

			reuseErr := validateReusableCompletedChunkWithContext(ctx, dbconn, chunkID)
			if reuseErr == nil {
				return chunkID, chunkstatus, false, nil
			}

			log.Printf("event=chunk_reuse_validation_failed chunk_id=%d error=%v", chunkID, reuseErr)
			if err := markChunkForRebuildWithContext(ctx, dbconn, chunkID); err != nil {
				return 0, "", false, errors.Join(reuseErr, err)
			}
			chunkstatus = filestate.ChunkAborted
		case filestate.ChunkProcessing:
			// Another process is currently storing this chunk: we can wait and reuse it once done
			_ = tx.Rollback() // Don't hold locks while waiting
			txclosed = true
			finalStatus, waitErr := waitForChunkTerminalStatus(ctx, dbconn, chunkID)
			if waitErr != nil {
				return 0, "", false, waitErr
			}
			if finalStatus == filestate.ChunkCompleted {
				reuseErr := validateReusableCompletedChunkWithContext(ctx, dbconn, chunkID)
				if reuseErr == nil {
					return chunkID, finalStatus, false, nil
				}

				log.Printf("event=chunk_reuse_validation_failed chunk_id=%d error=%v", chunkID, reuseErr)
				if err := markChunkForRebuildWithContext(ctx, dbconn, chunkID); err != nil {
					return 0, "", false, errors.Join(reuseErr, err)
				}
				chunkstatus = filestate.ChunkAborted
				break
			}
			chunkstatus = finalStatus
		}

		// If we reach here, it means the previous attempt was aborted while we were waiting: we can try to store again
		if chunkstatus == filestate.ChunkAborted {
			for casAttempt := 0; casAttempt < 3; casAttempt++ {
				tx2, beginErr := dbconn.BeginTx(ctx, nil)
				if beginErr != nil {
					return 0, "", false, beginErr
				}
				casResult, execErr := tx2.ExecContext(
					ctx,
					`UPDATE chunk
					 SET status = $1, retry_count = retry_count + 1
					 WHERE id = $2 AND status = $3`,
					filestate.ChunkProcessing,
					chunkID,
					filestate.ChunkAborted,
				)
				if execErr != nil {
					_ = tx2.Rollback()
					return 0, chunkstatus, false, execErr
				}
				rowsAffected, rowsErr := casResult.RowsAffected()
				if rowsErr != nil {
					_ = tx2.Rollback()
					return 0, chunkstatus, false, rowsErr
				}
				if commitErr := tx2.Commit(); commitErr != nil {
					_ = tx2.Rollback()
					return 0, chunkstatus, false, commitErr
				}

				if rowsAffected == 1 {
					chunkstatus = filestate.ChunkProcessing
					break
				}

				var latestStatus string
				if statusErr := dbconn.QueryRowContext(ctx, `SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&latestStatus); statusErr != nil {
					return 0, chunkstatus, false, statusErr
				}
				switch latestStatus {
				case filestate.ChunkCompleted:
					reuseErr := validateReusableCompletedChunkWithContext(ctx, dbconn, chunkID)
					if reuseErr == nil {
						return chunkID, latestStatus, false, nil
					}

					log.Printf("event=chunk_reuse_validation_failed chunk_id=%d error=%v", chunkID, reuseErr)
					if err := markChunkForRebuildWithContext(ctx, dbconn, chunkID); err != nil {
						return 0, "", false, errors.Join(reuseErr, err)
					}
					chunkstatus = filestate.ChunkAborted
					continue
				case filestate.ChunkProcessing:
					finalStatus, waitErr := waitForChunkTerminalStatus(ctx, dbconn, chunkID)
					if waitErr != nil {
						return 0, "", false, waitErr
					}
					if finalStatus == filestate.ChunkCompleted {
						reuseErr := validateReusableCompletedChunkWithContext(ctx, dbconn, chunkID)
						if reuseErr == nil {
							return chunkID, finalStatus, false, nil
						}

						log.Printf("event=chunk_reuse_validation_failed chunk_id=%d error=%v", chunkID, reuseErr)
						if err := markChunkForRebuildWithContext(ctx, dbconn, chunkID); err != nil {
							return 0, "", false, errors.Join(reuseErr, err)
						}
						chunkstatus = filestate.ChunkAborted
						continue
					}
				case filestate.ChunkAborted:
					// Contended retry claim: loop and attempt CAS again.
				default:
					return 0, chunkstatus, false, fmt.Errorf("unexpected chunk status during claim retry: %s", latestStatus)
				}
			}

			if chunkstatus != filestate.ChunkProcessing {
				return 0, chunkstatus, false, fmt.Errorf("could not claim aborted chunk %d after contention", chunkID)
			}
		}
	default:
		return 0, "", false, insErr
	}

	if !txclosed {
		if err := tx.Commit(); err != nil {
			return 0, chunkstatus, isNew, err
		}
	}
	return chunkID, chunkstatus, isNew, nil
}

func waitForLogicalFileTerminalStatus(ctx context.Context, dbconn *sql.DB, fileID int64) (string, error) {
	attempt := 0
	waitStart := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if time.Since(waitStart) >= maxClaimWaitDuration {
			return "", fmt.Errorf("timeout waiting for logical file %d to finish processing", fileID)
		}

		// Poll with bounded exponential backoff to reduce DB pressure under contention.
		if err := sleepWithContext(ctx, claimPollingBackoff(logicalFileWaitingtime, attempt)); err != nil {
			return "", err
		}
		attempt++

		var finalStatus string
		if err := dbconn.QueryRowContext(ctx, `SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&finalStatus); err != nil {
			return "", err
		}
		switch finalStatus {
		case filestate.LogicalFileCompleted, filestate.LogicalFileAborted:
			return finalStatus, nil
		}
	}
}

func waitForChunkTerminalStatus(ctx context.Context, dbconn *sql.DB, chunkID int64) (string, error) {
	attempt := 0
	waitStart := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if time.Since(waitStart) >= maxClaimWaitDuration {
			return "", fmt.Errorf("timeout waiting for chunk %d to finish processing", chunkID)
		}

		// Poll with bounded exponential backoff to reduce DB pressure under contention.
		if err := sleepWithContext(ctx, claimPollingBackoff(chunkWaitingtime, attempt)); err != nil {
			return "", err
		}
		attempt++

		var finalStatus string
		if err := dbconn.QueryRowContext(ctx, `SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&finalStatus); err != nil {
			return "", err
		}
		switch finalStatus {
		case filestate.ChunkCompleted, filestate.ChunkAborted:
			return finalStatus, nil
		}
	}
}

func linkFileChunk(tx *sql.Tx, fileID int64, chunkID int64, chunkOrder int, incrementRefCount bool) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return linkFileChunkWithContext(ctx, tx, fileID, chunkID, chunkOrder, incrementRefCount)
}

func linkFileChunkWithContext(ctx context.Context, tx *sql.Tx, fileID int64, chunkID int64, chunkOrder int, incrementRefCount bool) error {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (logical_file_id, chunk_order) DO NOTHING`,
		fileID,
		chunkID,
		chunkOrder,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		var existingChunkID int64
		err := tx.QueryRowContext(
			ctx,
			`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 AND chunk_order = $2`,
			fileID,
			chunkOrder,
		).Scan(&existingChunkID)
		if err == sql.ErrNoRows {
			return fmt.Errorf("suspicious file_chunk conflict for file_id=%d chunk_order=%d: insert reported conflict but mapping is missing", fileID, chunkOrder)
		}
		if err != nil {
			return err
		}
		if existingChunkID != chunkID {
			return fmt.Errorf("suspicious file_chunk conflict for file_id=%d chunk_order=%d: existing chunk_id=%d attempted chunk_id=%d", fileID, chunkOrder, existingChunkID, chunkID)
		}
		return nil
	}

	if rowsAffected > 0 && incrementRefCount {
		if _, err := tx.ExecContext(ctx, `UPDATE chunk SET live_ref_count = live_ref_count + 1 WHERE id = $1`, chunkID); err != nil {
			return err
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// HIGH-LEVEL FILE AND FOLDER STORAGE FUNCTIONS
// -----------------------------------------------------------------------------

func StoreFile(path string) error {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}

	return StoreFileWithCodec(path, codec)
}

func StoreFileWithCodecString(path string, codecName string) error {
	codec, err := blocks.ParseCodec(codecName)
	if err != nil {
		return err
	}

	return StoreFileWithCodec(path, codec)
}

func StoreFileWithCodec(path string, codec blocks.Codec) error {
	cgstx, err := LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load default storage context: %w", err)
	}
	defer func() { _ = cgstx.Close() }()

	if err := StoreFileWithStorageContextAndCodec(cgstx, path, codec); err != nil {
		return err
	}
	return nil
}

func StoreFileWithStorageContext(sgctx StorageContext, path string) (err error) {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}

	return StoreFileWithStorageContextAndCodec(sgctx, path, codec)
}

// StoreFileWithStorageContextResult stores one file and returns structured result metadata.
func StoreFileWithStorageContextResult(sgctx StorageContext, path string) (StoreFileResult, error) {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return StoreFileResult{}, err
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if sgctx.Writer != nil {
		_ = sgctx.Writer.FinalizeContainer()
	}
	return result, err
}

func StoreFileWithStorageContextAndCodec(sgctx StorageContext, path string, codec blocks.Codec) (err error) {
	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if sgctx.Writer != nil {
		_ = sgctx.Writer.FinalizeContainer()
	}
	return err
}

// StoreFileWithStorageContextAndCodecResult stores one file and returns
// metadata suitable for CLI text and JSON output.
func StoreFileWithStorageContextAndCodecResult(sgctx StorageContext, path string, codec blocks.Codec) (result StoreFileResult, err error) {
	result.Path = path
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	transformer, err := blocks.GetBlockTransformer(codec)
	if err != nil {
		if codec == blocks.CodecAESGCM {
			return StoreFileResult{}, fmt.Errorf("encryption key required for aes-gcm: %w", err)
		}
		return StoreFileResult{}, fmt.Errorf("initialize codec %s: %w", codec, err)
	}

	blockRepo := &blocks.Repository{
		DB: sgctx.DB,
	}

	file, err := os.Open(path)
	if err != nil {
		return StoreFileResult{}, err
	}
	defer func() { _ = file.Close() }()

	fileinfo, err := file.Stat()
	if err != nil {
		return StoreFileResult{}, err
	}

	// Compute full file hash
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return StoreFileResult{}, err
	}
	fileHash := hex.EncodeToString(hasher.Sum(nil))
	result.FileHash = fileHash

	if _, err := file.Seek(0, 0); err != nil {
		return StoreFileResult{}, err
	}

	// Try to claim logical file for this hash (concurrency-safe)
	fileID, filestatus, err := prepareLogicalFileForStoreWithContext(ctx, sgctx.DB, fileinfo, fileHash, sgctx.EffectiveContainerDir())
	if err != nil {
		return StoreFileResult{}, err
	}
	result.FileID = fileID

	if filestatus == filestate.LogicalFileCompleted {
		result.AlreadyStored = true
		return result, nil
	}

	completed := false
	defer func() {
		if !completed {
			cleanupCtx, cleanupCancel := db.NewOperationContext(context.Background())
			defer cleanupCancel()
			if _, execErr := sgctx.DB.ExecContext(
				cleanupCtx,
				`UPDATE logical_file SET status = $1 WHERE id = $2`,
				filestate.LogicalFileAborted,
				fileID,
			); execErr != nil {
				log.Printf("event=store_cleanup action=mark_aborted file_id=%d error=%v", fileID, execErr)
			}
		}
	}()

	// At this point, we have a logical_file row in "PROCESSING" status for this file hash, either created by us or by another process.
	chunks, err := chunk.ChunkFile(path)
	if err != nil {
		return StoreFileResult{}, err
	}

	chunkOrder := 0
	writer, ok := sgctx.Writer.(payloadStatefulWriter)
	if !ok {
		return StoreFileResult{}, fmt.Errorf("StoreFileWithStorageContextAndCodec requires writer with AppendPayload/FinalizeContainer, got %T", sgctx.Writer)
	}
	bindWriterDB(writer, sgctx.DB)
	// Writer finalization is owned by call boundaries (wrappers/CLI/context close),
	// not by this low-level result function.

	for _, chunkData := range chunks {
		if err := ctx.Err(); err != nil {
			return StoreFileResult{}, err
		}
		sum := sha256.Sum256(chunkData)
		chunkHash := hex.EncodeToString(sum[:])
		// Try to claim chunk for this hash (concurrency-safe)
		claimedChunkID, chunkStatus, _, err := claimChunkWithContext(ctx, sgctx.DB, chunkHash, int64(len(chunkData)))
		if err != nil {
			return StoreFileResult{}, err
		}

		if chunkStatus == filestate.ChunkCompleted {
			// Chunk already stored and ready: we can reuse it, just need to link it to the logical file
			tx, err := sgctx.DB.BeginTx(ctx, nil)
			if err != nil {
				return StoreFileResult{}, err
			}

			if err := linkFileChunkWithContext(ctx, tx, fileID, claimedChunkID, chunkOrder, true); err != nil {
				_ = tx.Rollback()
				return StoreFileResult{}, err
			}

			if err = tx.Commit(); err != nil {
				_ = tx.Rollback()
				return StoreFileResult{}, err
			}

			chunkOrder++
			continue // Move to next chunk
		}

		// At this point, we have a chunk row in "PROCESSING" status for this chunk hash, either created by us or by another process.

		for {
			if err := ctx.Err(); err != nil {
				return StoreFileResult{}, err
			}
			tx, err := sgctx.DB.BeginTx(ctx, nil)
			if err != nil {
				return StoreFileResult{}, err
			}

			// Append chunk data to container file
			placement, _, err := storeChunkAsPlainBlockWithWriter(
				ctx,
				tx,
				blockRepo,
				writer,
				claimedChunkID,
				chunkHash,
				chunkData,
				transformer,
			)

			if err != nil {
				_ = tx.Rollback()
				var brokenOpenErr *container.BrokenOpenContainerError
				if errors.As(err, &brokenOpenErr) {
					if quarantineErr := quarantineContainerNow(sgctx.DB, brokenOpenErr.ContainerID); quarantineErr != nil {
						return StoreFileResult{}, errors.Join(err, fmt.Errorf("quarantine broken open container %d after rollback: %w", brokenOpenErr.ContainerID, quarantineErr))
					}
					return StoreFileResult{}, err
				}
				if errors.Is(err, container.ErrContainerLockContention) {
					continue
				}
				if errors.Is(err, container.ErrContainerFull) {
					continue
				}
				existingBlock, getBlockErr := blockRepo.GetByChunkID(ctx, claimedChunkID)
				if getBlockErr == nil && existingBlock != nil {
					// Retry scenario: chunk row was set back to ABORTED/PROCESSING but
					// block metadata already exists for this chunk ID.
					tx2, err2 := sgctx.DB.BeginTx(ctx, nil)
					if err2 != nil {
						rollbackWriterLastAppend(writer)
						return StoreFileResult{}, err2
					}

					if _, err2 = tx2.ExecContext(ctx, `UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, claimedChunkID); err2 != nil {
						_ = tx2.Rollback()
						rollbackWriterLastAppend(writer)
						return StoreFileResult{}, err2
					}

					if err2 = linkFileChunkWithContext(ctx, tx2, fileID, claimedChunkID, chunkOrder, true); err2 != nil {
						_ = tx2.Rollback()
						rollbackWriterLastAppend(writer)
						return StoreFileResult{}, err2
					}

					if err2 = tx2.Commit(); err2 != nil {
						_ = tx2.Rollback()
						rollbackWriterLastAppend(writer)
						return StoreFileResult{}, err2
					}

					rollbackWriterLastAppend(writer)
					chunkOrder++
					break
				}
				if getBlockErr != nil && !errors.Is(getBlockErr, sql.ErrNoRows) {
					rollbackWriterLastAppend(writer)
					return StoreFileResult{}, getBlockErr
				}

				if _, err3 := sgctx.DB.ExecContext(
					ctx,
					`UPDATE chunk SET status = $1 WHERE id = $2`,
					filestate.ChunkAborted,
					claimedChunkID,
				); err3 != nil {
					rollbackWriterLastAppend(writer)
					return StoreFileResult{}, err3
				}
				rollbackWriterLastAppend(writer)
				return StoreFileResult{}, err
			}

			// Mark chunk as completed
			if _, err := tx.ExecContext(
				ctx,
				`UPDATE chunk SET status = $1 WHERE id = $2`,
				filestate.ChunkCompleted,
				claimedChunkID,
			); err != nil {
				_ = tx.Rollback()
				rollbackWriterLastAppend(writer)
				return StoreFileResult{}, err
			}

			if placement.Rotated {
				// Contract: LocalWriter only handles physical finalize/close on rotation.
				// Caller must persist final size and seal the previously active container row in DB.
				if err := container.UpdateContainerSize(tx, placement.PreviousID, placement.PreviousSize); err != nil {
					_ = tx.Rollback()
					rollbackWriterLastAppend(writer)
					return StoreFileResult{}, err
				}
				if err := sealContainerWithWriter(tx, writer, placement.PreviousID, placement.PreviousFilename, sgctx.EffectiveContainerDir()); err != nil {
					_ = tx.Rollback()
					rollbackWriterLastAppend(writer)
					retireErr := quarantineContainerNow(sgctx.DB, placement.PreviousID)
					if retireErr != nil {
						return StoreFileResult{}, errors.Join(err, fmt.Errorf("quarantine rotated container %d after seal failure: %w", placement.PreviousID, retireErr))
					}
					return StoreFileResult{}, err
				}
			}

			// Always persist size for the container that received this payload.
			if err := container.UpdateContainerSize(tx, placement.ContainerID, placement.NewContainerSize); err != nil {
				_ = tx.Rollback()
				rollbackWriterLastAppend(writer)
				return StoreFileResult{}, err
			}

			// Link file ↔ chunk
			if err := linkFileChunkWithContext(ctx, tx, fileID, claimedChunkID, chunkOrder, true); err != nil {
				_ = tx.Rollback()
				rollbackWriterLastAppend(writer)
				return StoreFileResult{}, err
			}
			if placement.Full {
				// Mark sealing in the current transaction, which already owns the row lock.
				if err := markContainerSealingInTx(tx, placement.ContainerID); err != nil {
					_ = tx.Rollback()
					rollbackWriterLastAppend(writer)
					return StoreFileResult{}, err
				}
				if err := writer.FinalizeContainer(); err != nil {
					_ = tx.Rollback()
					retireErr := retireWriterActiveContainer(writer)
					if retireErr != nil {
						return StoreFileResult{}, errors.Join(err, fmt.Errorf("retire active container after finalize failure: %w", retireErr))
					}
					return StoreFileResult{}, err
				}
				// Contract: FinalizeContainer only closes physical file handle; DB seal is required here.
				if err := sealContainerWithWriter(tx, writer, placement.ContainerID, placement.Filename, sgctx.EffectiveContainerDir()); err != nil {
					_ = tx.Rollback()
					rollbackWriterLastAppend(writer)
					retireErr := quarantineContainerNow(sgctx.DB, placement.ContainerID)
					if retireErr != nil {
						return StoreFileResult{}, errors.Join(err, fmt.Errorf("quarantine full container %d after seal failure: %w", placement.ContainerID, retireErr))
					}
					return StoreFileResult{}, err
				}
			}

			// Crash-durability barrier: flush payload bytes before publishing COMPLETED metadata in DB.
			if err := syncActiveContainerWithWriter(writer); err != nil {
				_ = tx.Rollback()
				retireErr := retireWriterActiveContainer(writer)
				if retireErr != nil {
					return StoreFileResult{}, errors.Join(err, fmt.Errorf("retire active container after sync failure: %w", retireErr))
				}
				return StoreFileResult{}, err
			}

			if err = tx.Commit(); err != nil {
				rollbackWriterLastAppend(writer)
				return StoreFileResult{}, err
			}
			acknowledgeWriterAppendCommitted(writer)

			chunkOrder++
			break
		}
	}

	// After all chunks are stored and linked, mark logical file as "COMPLETED"
	_, err = sgctx.DB.ExecContext(
		ctx,
		`UPDATE logical_file SET status = $1 WHERE id = $2`,
		filestate.LogicalFileCompleted,
		fileID,
	)
	if err != nil {
		return StoreFileResult{}, err
	}
	// Mark the operation as completed to avoid aborting it in the deferred function
	completed = true

	return result, nil
}

// -----------------------------------------------------------------------------
// STORE FOLDER FUNCTION (RECURSIVE)
// -----------------------------------------------------------------------------

func StoreFolder(root string) error {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}

	sgctx, err := LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load default storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	return StoreFolderWithStorageContextAndCodec(sgctx, root, codec)
}

func StoreFolderWithCodec(root string, codecName string) error {
	codec, err := blocks.ParseCodec(codecName)
	if err != nil {
		return err
	}

	sgctx, err := LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load default storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	return StoreFolderWithStorageContextAndCodec(sgctx, root, codec)
}

func StoreFolderWithStorageContext(sgctx StorageContext, root string) error {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}

	return StoreFolderWithStorageContextAndCodec(sgctx, root, codec)
}

func StoreFolderWithStorageContextAndCodec(sgctx StorageContext, root string, codec blocks.Codec) error {

	workerCount := runtime.NumCPU()
	if _, ok := sgctx.Writer.(*container.SimulatedWriter); ok {
		workerCount = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fileChan := make(chan string, 256)

	var wg sync.WaitGroup
	var firstErr error
	var firstErrMu sync.Mutex
	reportErr := func(err error) {
		if err == nil {
			return
		}
		firstErrMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		firstErrMu.Unlock()
	}
	getFirstErr := func() error {
		firstErrMu.Lock()
		defer firstErrMu.Unlock()
		return firstErr
	}

	// Workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			workerWriter, err := newWriterFromPrototype(sgctx.Writer)
			if err != nil {
				reportErr(err)
				return
			}

			workerCtx := sgctx
			workerCtx.Writer = workerWriter
			// workerWriter is reused across files by this worker, but each file store
			// still finalizes the active writer state by design (see StoreFile*Result).

			for {
				// Prefer cancellation over draining buffered work when shutting down.
				if ctx.Err() != nil {
					return
				}

				select {
				case <-ctx.Done():
					return
				case p, ok := <-fileChan:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
					if err := StoreFileWithStorageContextAndCodec(workerCtx, p, codec); err != nil {
						reportErr(err)
						return
					}
				}
			}
		}()
	}

	// Producer
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			reportErr(walkErr)
			return walkErr
		}

		if !d.IsDir() {
			select {
			case <-ctx.Done():
				if err := getFirstErr(); err != nil {
					return err
				}
				return context.Canceled
			case fileChan <- path:
			}
		}

		return nil
	})

	close(fileChan)
	wg.Wait()

	if err := getFirstErr(); err != nil {
		return err
	}
	if walkErr != nil {
		return walkErr
	}

	return nil
}

func sleepWithContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------

// StoreBlock is the new version of StoreChunk that takes care of block encoding and metadata management in the blocks table
func storeChunkAsPlainBlockWithWriter(
	ctx context.Context,
	tx *sql.Tx,
	repo *blocks.Repository,
	writer payloadStatefulWriter,
	chunkID int64,
	chunkHash string,
	chunk []byte,
	transformer blocks.Transformer,
) (placement container.LocalPlacement, desc *blocks.Descriptor, err error) {
	encoded, err := transformer.Encode(ctx, blocks.EncodeInput{
		ChunkID:   chunkID,
		ChunkHash: chunkHash,
		Plaintext: chunk,
	})
	if err != nil {
		return container.LocalPlacement{}, nil, err
	}

	placement, err = writer.AppendPayload(tx, encoded.Payload)
	if err != nil {
		return container.LocalPlacement{}, nil, err
	}

	encoded.Descriptor.ContainerID = placement.ContainerID
	encoded.Descriptor.BlockOffset = placement.Offset

	if err := repo.Insert(ctx, tx, &encoded.Descriptor); err != nil {
		return container.LocalPlacement{}, nil, err
	}

	return placement, &encoded.Descriptor, nil
}

// new store payload data into a container directly, without the chunk record wrapper
func StoreBlockPayload(c container.Container, payload []byte) (offset int64, newSize int64, err error) {
	offset, err = c.Append(payload)
	if err != nil {
		return 0, 0, err
	}

	return offset, offset + int64(len(payload)), nil
}

func SyncCloseAndSealContainer(tx db.DBTX, activecontainer container.ActiveContainer, containersDir string) error {
	// sync active container to disk
	if err := activecontainer.Container.Sync(); err != nil {
		return err
	}
	// close active container file
	if err := activecontainer.Container.Close(); err != nil {
		return err
	}
	// seal active container in DB
	if err := container.SealContainerInDir(tx, activecontainer.ID, activecontainer.Filename, containersDir); err != nil {
		return err
	}

	return nil
}
