package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/utils_hash"
)

type repairQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type completedRepairRecipe struct {
	logicalID   int64
	status      string
	retryCount  int64
	refCount    int64
	chunks      []completedRepairChunk
	fingerprint string
}

type completedRepairChunk struct {
	order    int
	id       int64
	hash     string
	size     int64
	status   string
	liveRefs int64
	pins     int64
	legacy   retiredLegacyPlacement
	packed   *retiredPackedPlacement
}

type retiredLegacyPlacement struct {
	id            int64
	codec         string
	formatVersion int
	plaintextSize int64
	storedSize    int64
	nonce         []byte
	containerID   int64
	offset        int64
}

type retiredPackedPlacement struct {
	blockID       int64
	offsetInBlock int64
	sizeInBlock   int64
}

type stagedRepairBlock struct {
	ordinal          int
	container        container.RepairStagingContainer
	formatVersion    int
	codec            string
	plaintextSize    int64
	compressionCodec string
	compressionLevel *int
	compressedSize   int64
	storedSize       int64
	blockHash        []byte
	compressionRatio float64
	payloadHash      string
	compressedHash   []byte
	physicalHash     []byte
	legacyNonce      []byte
	chunks           []stagedRepairChunk
}

type stagedRepairChunk struct {
	id     int64
	order  int
	offset int64
	size   int64
}

var completedRepairLocks sync.Map

func completedRepairLock(fileID int64) *sync.Mutex {
	value, _ := completedRepairLocks.LoadOrStore(fileID, &sync.Mutex{})
	return value.(*sync.Mutex)
}

// tryRepairCompletedLogicalFile intercepts only an invalid COMPLETED object
// with an intact recipe. Healthy reuse and all non-completed claims continue
// through the established Store path.
func tryRepairCompletedLogicalFile(
	ctx context.Context,
	sgctx StorageContext,
	runtime *storeFileRuntime,
	prepared preparedFile,
	normalizedPath string,
	replace bool,
) (bool, StoreFileResult, error) {
	dbconn := runtime.storeService.Repository().DB()
	var fileID int64
	var status string
	err := dbconn.QueryRowContext(ctx,
		`SELECT id, status FROM logical_file WHERE file_hash = $1 AND total_size = $2`,
		prepared.LogicalHash, prepared.SizeBytes,
	).Scan(&fileID, &status)
	if errors.Is(err, sql.ErrNoRows) || status != filestate.LogicalFileCompleted {
		return false, StoreFileResult{}, nil
	}
	if err != nil {
		return true, StoreFileResult{}, err
	}

	if err := validateReusableLogicalFileForStoreWithPolicy(ctx, dbconn, fileID, runtime.reuseValidation); err == nil {
		return false, StoreFileResult{}, nil
	}

	lock := completedRepairLock(fileID)
	lock.Lock()
	defer lock.Unlock()

	// Another Store may have repaired this candidate while this caller waited.
	if err := validateReusableLogicalFileForStoreWithPolicy(ctx, dbconn, fileID, runtime.reuseValidation); err == nil {
		return false, StoreFileResult{}, nil
	}

	recipe, err := loadCompletedRepairRecipe(ctx, dbconn, fileID, prepared)
	if err != nil {
		return true, StoreFileResult{}, fmt.Errorf("completed object %d is not safely repairable without changing its recipe: %w", fileID, err)
	}
	if len(recipe.chunks) == 0 {
		return true, StoreFileResult{}, fmt.Errorf("completed object %d has no repairable recipe", fileID)
	}

	state := storeInterleavingStateFromContext(ctx)
	storeOpID, storeCodec := "", string(runtime.codec)
	if state != nil {
		storeOpID = state.storeOpID
	}
	if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
		StoreOpID: storeOpID,
		ChunkID:   recipe.chunks[0].id,
		ChunkHash: recipe.chunks[0].hash,
		Codec:     storeCodec,
		Event:     storeInterleavingEventBeforeMarkChunkForRebuild,
	}); err != nil {
		return true, StoreFileResult{}, err
	}

	attemptID, err := createStoreRepairAttempt(ctx, dbconn, recipe, prepared)
	if err != nil {
		return true, StoreFileResult{}, err
	}
	staged, err := stageCompletedRepair(ctx, dbconn, attemptID, sgctx, runtime, recipe, prepared)
	if err != nil {
		_, _ = dbconn.ExecContext(context.Background(),
			`UPDATE store_repair_attempt SET status = 'ABORTED', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status <> 'PUBLISHED'`, attemptID)
		return true, StoreFileResult{}, err
	}
	result, err := dbconn.ExecContext(ctx,
		`UPDATE store_repair_attempt SET status = 'READY', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status = 'PREPARING'`, attemptID,
	)
	if err != nil {
		return true, StoreFileResult{}, fmt.Errorf("mark repair attempt ready: %w", err)
	}
	if err := db.RequireExactlyOneRow(result, "mark repair attempt ready"); err != nil {
		return true, StoreFileResult{}, err
	}
	if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
		StoreOpID: storeOpID,
		ChunkID:   recipe.chunks[0].id,
		ChunkHash: recipe.chunks[0].hash,
		Codec:     storeCodec,
		Event:     storeInterleavingEventBeforeRepairPublication,
	}); err != nil {
		_, _ = dbconn.ExecContext(context.Background(),
			`UPDATE store_repair_attempt SET status = 'ABORTED', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status <> 'PUBLISHED'`, attemptID)
		return true, StoreFileResult{}, err
	}

	if err := publishCompletedRepair(ctx, dbconn, attemptID, sgctx, recipe, prepared, normalizedPath, replace, staged); err != nil {
		_, _ = dbconn.ExecContext(context.Background(),
			`UPDATE store_repair_attempt SET status = 'ABORTED', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status <> 'PUBLISHED'`, attemptID)
		return true, StoreFileResult{}, err
	}
	return true, StoreFileResult{
		FileID: fileID, FileHash: prepared.LogicalHash, Path: normalizedPath, AlreadyStored: false,
	}, nil
}

func loadCompletedRepairRecipe(ctx context.Context, q repairQueryer, fileID int64, prepared preparedFile) (completedRepairRecipe, error) {
	var recipe completedRepairRecipe
	recipe.logicalID = fileID
	if err := q.QueryRowContext(ctx,
		`SELECT status, retry_count, ref_count FROM logical_file WHERE id = $1`, fileID,
	).Scan(&recipe.status, &recipe.retryCount, &recipe.refCount); err != nil {
		return recipe, err
	}
	if recipe.status != filestate.LogicalFileCompleted {
		return recipe, fmt.Errorf("logical status is %s, want COMPLETED", recipe.status)
	}

	rows, err := q.QueryContext(ctx, `
		SELECT fc.chunk_order, c.id, c.chunk_hash, c.size, c.status,
		       c.live_ref_count, c.pin_count,
		       b.id, b.codec, b.format_version, b.plaintext_size,
		       b.stored_size, b.nonce, b.container_id, b.block_offset
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN blocks b ON b.chunk_id = c.id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order`, fileID)
	if err != nil {
		return recipe, err
	}
	for rows.Next() {
		var item completedRepairChunk
		if err := rows.Scan(
			&item.order, &item.id, &item.hash, &item.size, &item.status,
			&item.liveRefs, &item.pins,
			&item.legacy.id, &item.legacy.codec, &item.legacy.formatVersion,
			&item.legacy.plaintextSize, &item.legacy.storedSize, &item.legacy.nonce,
			&item.legacy.containerID, &item.legacy.offset,
		); err != nil {
			return recipe, err
		}
		recipe.chunks = append(recipe.chunks, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return recipe, err
	}
	if err := rows.Close(); err != nil {
		return recipe, err
	}
	for index := range recipe.chunks {
		var packed retiredPackedPlacement
		err := q.QueryRowContext(ctx,
			`SELECT block_id, offset_in_block, size_in_block FROM chunk_block_refs WHERE chunk_id = $1`, recipe.chunks[index].id,
		).Scan(&packed.blockID, &packed.offsetInBlock, &packed.sizeInBlock)
		if err == nil {
			recipe.chunks[index].packed = &packed
		} else if !errors.Is(err, sql.ErrNoRows) {
			return recipe, err
		}
	}
	if len(recipe.chunks) != len(prepared.Chunks) {
		return recipe, fmt.Errorf("recipe length=%d source chunks=%d", len(recipe.chunks), len(prepared.Chunks))
	}
	for index := range recipe.chunks {
		item, source := recipe.chunks[index], prepared.Chunks[index]
		if item.order != index || source.Index != index || item.hash != source.Hash || item.size != int64(source.Size) {
			return recipe, fmt.Errorf("ordered recipe differs from source at index %d", index)
		}
		if item.status != filestate.ChunkCompleted {
			return recipe, fmt.Errorf("recipe chunk %d status=%s", item.id, item.status)
		}
	}
	fingerprint, err := completedRepairFingerprint(ctx, q, recipe)
	if err != nil {
		return recipe, err
	}
	recipe.fingerprint = fingerprint
	return recipe, nil
}

func completedRepairFingerprint(ctx context.Context, q repairQueryer, recipe completedRepairRecipe) (string, error) {
	parts := []string{fmt.Sprintf("logical|%d|%s|%d|%d", recipe.logicalID, recipe.status, recipe.retryCount, recipe.refCount)}
	for _, item := range recipe.chunks {
		parts = append(parts, fmt.Sprintf("chunk|%d|%d|%s|%d|%s|%d|%d", item.order, item.id, item.hash, item.size, item.status, item.liveRefs, item.pins))
		parts = append(parts, fmt.Sprintf("legacy|%d|%s|%d|%d|%d|%x|%d|%d", item.legacy.id, item.legacy.codec, item.legacy.formatVersion, item.legacy.plaintextSize, item.legacy.storedSize, item.legacy.nonce, item.legacy.containerID, item.legacy.offset))
		if item.packed != nil {
			parts = append(parts, fmt.Sprintf("packed|%d|%d|%d", item.packed.blockID, item.packed.offsetInBlock, item.packed.sizeInBlock))
		}
	}
	for _, query := range []string{
		`SELECT path || '|' || logical_file_id FROM physical_file WHERE logical_file_id = $1 ORDER BY path`,
		`SELECT snapshot_id || '|' || path_id || '|' || logical_file_id FROM snapshot_file WHERE logical_file_id = $1 ORDER BY snapshot_id, path_id`,
	} {
		rows, err := q.QueryContext(ctx, query, recipe.logicalID)
		if err != nil {
			return "", err
		}
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				_ = rows.Close()
				return "", err
			}
			parts = append(parts, value)
		}
		if err := rows.Close(); err != nil {
			return "", err
		}
	}
	sort.Strings(parts[1:])
	digest := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(digest[:]), nil
}

func createStoreRepairAttempt(ctx context.Context, dbconn *sql.DB, recipe completedRepairRecipe, prepared preparedFile) (int64, error) {
	var attemptID int64
	err := dbconn.QueryRowContext(ctx, `
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, $2, $3, $4, 'PREPARING')
		RETURNING id`,
		recipe.logicalID, prepared.LogicalHash, prepared.SizeBytes, recipe.fingerprint,
	).Scan(&attemptID)
	if err != nil {
		return 0, fmt.Errorf("create durable repair attempt: %w", err)
	}
	return attemptID, nil
}

func stageCompletedRepair(
	ctx context.Context,
	dbconn *sql.DB,
	attemptID int64,
	sgctx StorageContext,
	runtime *storeFileRuntime,
	recipe completedRepairRecipe,
	prepared preparedFile,
) ([]stagedRepairBlock, error) {
	maxSize := container.GetContainerMaxSize()
	target := packedBlockTargetSizeBytesFromEnv()
	if physicalTarget := maxSize - container.ContainerHdrLen - 4096; physicalTarget < target {
		target = physicalTarget
	}
	if target <= 0 {
		return nil, fmt.Errorf("container maximum %d cannot hold a staged packed block", maxSize)
	}

	builder := blocks.NewBlockBuilder(target)
	pending := make([]stagedRepairChunk, 0, 8)
	var staged []stagedRepairBlock
	flush := func() error {
		if builder.Empty() {
			return nil
		}
		enc, err := buildAndEncodePackedBlock(builder)
		if err != nil {
			return err
		}
		transformed, err := applyPackedBlockTransforms(ctx, runtime.transformer, runtime.compression, enc)
		if err != nil {
			return err
		}
		stagedContainer, err := container.CreateRepairStagingContainer(ctx, dbconn, attemptID, sgctx.EffectiveContainerDir(), transformed.storedPayload, maxSize)
		if err != nil {
			return err
		}
		block := stagedRepairBlock{
			ordinal: len(staged), container: stagedContainer, formatVersion: 1,
			codec: transformed.storageCodec, plaintextSize: int64(len(enc.plaintextEncoded)),
			compressionCodec: transformed.metadata.CompressionCodec,
			compressionLevel: transformed.compressionLvl, compressedSize: transformed.compressedSize,
			storedSize: int64(len(transformed.storedPayload)), blockHash: enc.blockHash,
			compressionRatio: transformed.metadata.CompressionRatio,
			payloadHash:      transformed.metadata.PayloadHash, compressedHash: transformed.compressedHash,
			physicalHash: transformed.physicalHash, legacyNonce: transformed.legacyNonce,
			chunks: append([]stagedRepairChunk(nil), pending...),
		}
		runningOffset := int64(0)
		for index := range block.chunks {
			block.chunks[index].offset = runningOffset
			runningOffset += block.chunks[index].size
		}
		if err := persistRepairStageMetadata(ctx, dbconn, attemptID, block); err != nil {
			return err
		}
		staged = append(staged, block)
		builder.Reset()
		pending = pending[:0]
		return nil
	}

	for index, source := range prepared.Chunks {
		if builder.ShouldFlushBeforeAdd(int64(source.Size)) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		item := recipe.chunks[index]
		hashBytes, err := hex.DecodeString(source.Hash)
		if err != nil {
			return nil, fmt.Errorf("decode chunk hash %s: %w", source.Hash, err)
		}
		if err := builder.Add(blocks.PendingChunk{ChunkID: item.id, Hash: hashBytes, Data: source.Data, Size: int64(source.Size)}); err != nil {
			return nil, err
		}
		pending = append(pending, stagedRepairChunk{id: item.id, order: index, size: int64(source.Size)})
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return staged, nil
}

func persistRepairStageMetadata(ctx context.Context, dbconn *sql.DB, attemptID int64, block stagedRepairBlock) error {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var compressionLevel any
	if block.compressionLevel != nil {
		compressionLevel = *block.compressionLevel
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO store_repair_block
		 (attempt_id, block_ordinal, format_version, codec, plaintext_size,
		  compression_codec, compression_level, compressed_size, stored_size,
		  container_id, container_offset, block_hash, compression_ratio,
		  payload_hash, compressed_hash, physical_hash, legacy_nonce)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		attemptID, block.ordinal, block.formatVersion, block.codec, block.plaintextSize,
		block.compressionCodec, compressionLevel, block.compressedSize, block.storedSize,
		block.container.ID, block.container.PayloadOffset, block.blockHash, block.compressionRatio,
		block.payloadHash, block.compressedHash, block.physicalHash, block.legacyNonce,
	); err != nil {
		return fmt.Errorf("persist staged repair block: %w", err)
	}

	for _, chunk := range block.chunks {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO store_repair_chunk
			 (attempt_id, chunk_id, chunk_order, block_ordinal, offset_in_block, size_in_block)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			attemptID, chunk.id, chunk.order, block.ordinal, chunk.offset, chunk.size,
		); err != nil {
			return fmt.Errorf("persist staged repair chunk: %w", err)
		}
	}
	return tx.Commit()
}

func publishCompletedRepair(
	ctx context.Context,
	dbconn *sql.DB,
	attemptID int64,
	sgctx StorageContext,
	original completedRepairRecipe,
	prepared preparedFile,
	normalizedPath string,
	replace bool,
	staged []stagedRepairBlock,
) error {
	for _, block := range staged {
		info, err := os.Stat(block.container.Path)
		if err != nil || info.Size() != block.container.PhysicalSize {
			return fmt.Errorf("staged repair container %d is not durable at publication: %w", block.container.ID, err)
		}
		hash, err := utils_hash.ComputeFileHashHex(block.container.Path)
		if err != nil || hash != block.container.PhysicalHash {
			return fmt.Errorf("staged repair container %d hash changed before publication", block.container.ID)
		}
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := db.AcquireRepositoryMutationLock(ctx, dbconn, tx); err != nil {
		return err
	}
	if err := validateRepairStageForPublication(ctx, dbconn, tx, attemptID, original, prepared, staged); err != nil {
		return err
	}
	var lockedID int64
	if err := tx.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(dbconn,
		`SELECT id FROM logical_file WHERE id = $1`), original.logicalID).Scan(&lockedID); err != nil {
		return fmt.Errorf("lock repair logical object: %w", err)
	}
	current, err := loadCompletedRepairRecipe(ctx, tx, original.logicalID, prepared)
	if err != nil {
		return fmt.Errorf("revalidate repair recipe at publication: %w", err)
	}
	if current.fingerprint != original.fingerprint {
		return fmt.Errorf("repair publication fingerprint changed: before=%s current=%s", original.fingerprint, current.fingerprint)
	}
	for _, item := range current.chunks {
		if item.pins != 0 {
			return fmt.Errorf("repair publication requires zero restore pins: chunk=%d pins=%d", item.id, item.pins)
		}
	}

	for _, item := range current.chunks {
		if item.packed != nil {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO retired_chunk_block_ref
				 (block_id, embedded_chunk_id, offset_in_block, size_in_block, repair_attempt_id)
				VALUES ($1,$2,$3,$4,$5)`,
				item.packed.blockID, item.id, item.packed.offsetInBlock, item.packed.sizeInBlock, attemptID,
			); err != nil {
				return fmt.Errorf("retire packed membership for chunk %d: %w", item.id, err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM chunk_block_refs WHERE chunk_id = $1`, item.id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM blocks WHERE chunk_id = $1`, item.id); err != nil {
				return err
			}
		} else {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO retired_legacy_block_extent
				 (container_id, block_offset, stored_size, plaintext_size, codec,
				  format_version, nonce, historical_block_id, historical_chunk_id, repair_attempt_id)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
				item.legacy.containerID, item.legacy.offset, item.legacy.storedSize,
				item.legacy.plaintextSize, item.legacy.codec, item.legacy.formatVersion,
				item.legacy.nonce, item.legacy.id, item.id, attemptID,
			); err != nil {
				return fmt.Errorf("retire legacy extent for chunk %d: %w", item.id, err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM blocks WHERE id = $1`, item.legacy.id); err != nil {
				return err
			}
		}
	}
	if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
		ChunkID:   current.chunks[0].id,
		ChunkHash: current.chunks[0].hash,
		Event:     storeInterleavingEventAfterRepairRetirement,
	}); err != nil {
		return err
	}

	for _, block := range staged {
		var compressionLevel any
		if block.compressionLevel != nil {
			compressionLevel = *block.compressionLevel
		}
		var blockID int64
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO storage_blocks
			 (format_version, codec, plaintext_size, stored_size, container_id,
			  container_offset, block_hash, compression_codec, compression_level,
			  compressed_size, compression_ratio, payload_hash, compressed_hash, physical_hash)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			RETURNING id`,
			block.formatVersion, block.codec, block.plaintextSize, block.storedSize,
			block.container.ID, block.container.PayloadOffset, block.blockHash,
			block.compressionCodec, compressionLevel, block.compressedSize,
			block.compressionRatio, block.payloadHash, block.compressedHash, block.physicalHash,
		).Scan(&blockID); err != nil {
			return fmt.Errorf("publish replacement storage block: %w", err)
		}
		prefix := block.plaintextSize
		for _, chunk := range block.chunks {
			prefix -= chunk.size
		}
		runningOffset := int64(0)
		for _, chunk := range block.chunks {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1,$2,$3,$4)`,
				chunk.id, blockID, runningOffset, chunk.size,
			); err != nil {
				return fmt.Errorf("publish replacement chunk membership: %w", err)
			}
			legacyCodec := "plain"
			legacyNonce := []byte{}
			legacyOffset := block.container.PayloadOffset + prefix + runningOffset
			legacyStoredSize := chunk.size
			if block.codec == string(blocks.CodecAESGCM) {
				legacyCodec = string(blocks.CodecAESGCM)
				legacyNonce = block.legacyNonce
				legacyOffset = block.container.PayloadOffset
				legacyStoredSize = block.storedSize
			}
			if err := insertLegacyCompanionBlockRowWithContext(ctx, tx, chunk.id, legacyCodec, legacyNonce,
				block.container.ID, legacyOffset, chunk.size, legacyStoredSize); err != nil {
				return fmt.Errorf("publish replacement companion: %w", err)
			}
			runningOffset += chunk.size
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE container SET quarantine = FALSE WHERE id = $1 AND sealed = TRUE`, block.container.ID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE store_repair_container SET status = 'PUBLISHED' WHERE attempt_id = $1 AND container_id = $2`, attemptID, block.container.ID,
		); err != nil {
			return err
		}
	}

	if _, err := ensurePhysicalFileForPathWithPolicyWithTx(ctx, dbconn, tx, normalizedPath,
		original.logicalID, prepared.PhysicalMetadata, replace, recipeLivenessAlreadyAccounted); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE store_repair_attempt SET status = 'PUBLISHED', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status = 'READY'`, attemptID,
	)
	if err != nil {
		return err
	}
	if err := db.RequireExactlyOneRow(result, "publish repair attempt"); err != nil {
		return err
	}
	return tx.Commit()
}

func validateRepairStageForPublication(
	ctx context.Context,
	dbconn *sql.DB,
	tx *sql.Tx,
	attemptID int64,
	original completedRepairRecipe,
	prepared preparedFile,
	staged []stagedRepairBlock,
) error {
	var logicalID, sourceSize int64
	var sourceHash, fingerprint, status string
	if err := tx.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(dbconn, `
		SELECT logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status
		FROM store_repair_attempt WHERE id = $1`), attemptID,
	).Scan(&logicalID, &sourceHash, &sourceSize, &fingerprint, &status); err != nil {
		return fmt.Errorf("lock repair attempt for publication: %w", err)
	}
	if logicalID != original.logicalID || sourceHash != prepared.LogicalHash || sourceSize != prepared.SizeBytes || fingerprint != original.fingerprint || status != "READY" {
		return fmt.Errorf("repair attempt changed before publication")
	}

	var containerCount, blockCount, chunkCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM store_repair_container WHERE attempt_id = $1`, attemptID).Scan(&containerCount); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM store_repair_block WHERE attempt_id = $1`, attemptID).Scan(&blockCount); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM store_repair_chunk WHERE attempt_id = $1`, attemptID).Scan(&chunkCount); err != nil {
		return err
	}
	if containerCount != len(staged) || blockCount != len(staged) || chunkCount != len(original.chunks) {
		return fmt.Errorf("repair stage cardinality changed before publication: containers=%d blocks=%d chunks=%d", containerCount, blockCount, chunkCount)
	}

	for _, block := range staged {
		var (
			physicalSize, currentSize                    int64
			physicalHash, containerHash, containerStatus string
			quarantine, sealed                           bool
		)
		if err := tx.QueryRowContext(ctx, `
			SELECT rc.physical_size, rc.physical_hash, rc.status,
			       c.quarantine, c.sealed, c.current_size, COALESCE(c.container_hash, '')
			FROM store_repair_container rc
			JOIN container c ON c.id = rc.container_id
			WHERE rc.attempt_id = $1 AND rc.container_id = $2`, attemptID, block.container.ID,
		).Scan(&physicalSize, &physicalHash, &containerStatus, &quarantine, &sealed, &currentSize, &containerHash); err != nil {
			return fmt.Errorf("validate staged repair container %d: %w", block.container.ID, err)
		}
		if physicalSize != block.container.PhysicalSize || currentSize != physicalSize ||
			physicalHash != block.container.PhysicalHash || containerHash != physicalHash ||
			containerStatus != "DURABLE" || !quarantine || !sealed {
			return fmt.Errorf("staged repair container %d metadata changed before publication", block.container.ID)
		}

		var (
			formatVersion                                                           int
			codec, compressionCodec, payloadHash                                    string
			plaintextSize, compressedSize, storedSize, containerID, containerOffset int64
			compressionLevel                                                        sql.NullInt64
			blockHash, compressedHash, physicalHashBytes, legacyNonce               []byte
			compressionRatio                                                        float64
		)
		if err := tx.QueryRowContext(ctx, `
			SELECT format_version, codec, plaintext_size, compression_codec,
			       compression_level, compressed_size, stored_size, container_id,
			       container_offset, block_hash, compression_ratio, payload_hash,
			       compressed_hash, physical_hash, legacy_nonce
			FROM store_repair_block
			WHERE attempt_id = $1 AND block_ordinal = $2`, attemptID, block.ordinal,
		).Scan(&formatVersion, &codec, &plaintextSize, &compressionCodec,
			&compressionLevel, &compressedSize, &storedSize, &containerID,
			&containerOffset, &blockHash, &compressionRatio, &payloadHash,
			&compressedHash, &physicalHashBytes, &legacyNonce); err != nil {
			return fmt.Errorf("validate staged repair block %d: %w", block.ordinal, err)
		}
		levelMatches := block.compressionLevel == nil && !compressionLevel.Valid
		if block.compressionLevel != nil && compressionLevel.Valid {
			levelMatches = int64(*block.compressionLevel) == compressionLevel.Int64
		}
		if formatVersion != block.formatVersion || codec != block.codec || plaintextSize != block.plaintextSize ||
			compressionCodec != block.compressionCodec || !levelMatches || compressedSize != block.compressedSize ||
			storedSize != block.storedSize || containerID != block.container.ID || containerOffset != block.container.PayloadOffset ||
			!bytes.Equal(blockHash, block.blockHash) || compressionRatio != block.compressionRatio || payloadHash != block.payloadHash ||
			!bytes.Equal(compressedHash, block.compressedHash) || !bytes.Equal(physicalHashBytes, block.physicalHash) ||
			!bytes.Equal(legacyNonce, block.legacyNonce) {
			return fmt.Errorf("staged repair block %d metadata changed before publication", block.ordinal)
		}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT chunk_id, chunk_order, block_ordinal, offset_in_block, size_in_block
		FROM store_repair_chunk WHERE attempt_id = $1 ORDER BY chunk_order`, attemptID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	seen := 0
	for rows.Next() {
		var chunkID, offset, size int64
		var order, blockOrdinal int
		if err := rows.Scan(&chunkID, &order, &blockOrdinal, &offset, &size); err != nil {
			return err
		}
		if order != seen || seen >= len(original.chunks) {
			return fmt.Errorf("staged repair chunk order changed before publication")
		}
		want := original.chunks[seen]
		if chunkID != want.id || size != want.size {
			return fmt.Errorf("staged repair chunk %d changed before publication", want.id)
		}
		found := false
		for _, block := range staged {
			if block.ordinal != blockOrdinal {
				continue
			}
			for _, chunk := range block.chunks {
				if chunk.id == chunkID && chunk.order == order && chunk.offset == offset && chunk.size == size {
					found = true
				}
			}
		}
		if !found {
			return fmt.Errorf("staged repair chunk %d placement changed before publication", chunkID)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seen != len(original.chunks) {
		return fmt.Errorf("staged repair chunk count changed before publication")
	}
	return nil
}
