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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
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
	order      int
	id         int64
	hash       string
	size       int64
	status     string
	retryCount int64
	liveRefs   int64
	pins       int64
	legacy     retiredLegacyPlacement
	packed     *retiredPackedPlacement
}

type completedRepairEntity struct {
	chunk            completedRepairChunk
	source           preparedChunk
	stagingOrdinal   int
	replacePlacement bool
	repairStatus     bool
}

type completedRepairPlan struct {
	entities         []completedRepairEntity
	staged           []completedRepairEntity
	repairedChunkIDs []int64
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

type completedRepairContentionError struct {
	chunkID   int64
	chunkHash string
	reason    string
}

func (err *completedRepairContentionError) Error() string {
	return fmt.Sprintf("completed repair contended on chunk %d (%s): %s", err.chunkID, err.chunkHash, err.reason)
}

type completedRepairContentionBudget struct {
	deadline    time.Time
	pollAttempt int
}

var errCompletedRepairNoEntity = errors.New("reuse failed without an identifiable repair entity")

var completedRepairLocks sync.Map

func completedRepairLock(fileID int64) *sync.Mutex {
	value, _ := completedRepairLocks.LoadOrStore(fileID, &sync.Mutex{})
	lock, ok := value.(*sync.Mutex)
	if !ok {
		panic("storage: completed repair lock has unexpected type")
	}
	return lock
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
	var contentionBudget completedRepairContentionBudget
	for {
		lock.Lock()
		handled, result, repairErr := tryRepairCompletedLogicalFileLocked(
			ctx, sgctx, runtime, prepared, normalizedPath, replace, fileID,
			!contentionBudget.deadline.IsZero(),
		)
		lock.Unlock()

		var contention *completedRepairContentionError
		if !errors.As(repairErr, &contention) {
			return handled, result, repairErr
		}
		if err := waitForCompletedRepairContention(ctx, dbconn, contention, &contentionBudget); err != nil {
			return true, StoreFileResult{}, err
		}
	}
}

func tryRepairCompletedLogicalFileLocked(
	ctx context.Context,
	sgctx StorageContext,
	runtime *storeFileRuntime,
	prepared preparedFile,
	normalizedPath string,
	replace bool,
	fileID int64,
	contentionObserved bool,
) (bool, StoreFileResult, error) {
	dbconn := runtime.storeService.Repository().DB()

	// Another Store may have repaired this candidate while this caller waited.
	if err := validateReusableLogicalFileForStoreWithPolicy(ctx, dbconn, fileID, runtime.reuseValidation); err == nil {
		tx, err := dbconn.BeginTx(ctx, nil)
		if err != nil {
			return true, StoreFileResult{}, err
		}
		if _, err := ensurePhysicalFileForPathWithPolicyWithTx(
			ctx, dbconn, tx, normalizedPath, fileID, prepared.PhysicalMetadata,
			replace, recipeLivenessActivateOnFirstMapping,
		); err != nil {
			_ = tx.Rollback()
			return true, StoreFileResult{}, err
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return true, StoreFileResult{}, err
		}
		return true, StoreFileResult{
			FileID: fileID, FileHash: prepared.LogicalHash, Path: normalizedPath, AlreadyStored: true,
		}, nil
	}

	recipe, err := loadCompletedRepairRecipe(ctx, dbconn, fileID, prepared)
	if err != nil {
		return true, StoreFileResult{}, fmt.Errorf("completed object %d is not safely repairable without changing its recipe: %w", fileID, err)
	}
	if len(recipe.chunks) == 0 {
		return true, StoreFileResult{}, fmt.Errorf("completed object %d has no repairable recipe", fileID)
	}
	plan, err := buildCompletedRepairPlan(ctx, dbconn, sgctx.EffectiveContainerDir(), recipe, prepared)
	if err != nil {
		if contentionObserved && errors.Is(err, errCompletedRepairNoEntity) {
			return true, StoreFileResult{}, &completedRepairContentionError{
				chunkID: recipe.chunks[0].id, chunkHash: recipe.chunks[0].hash,
				reason: "terminal competitor state requires serialized healthy revalidation",
			}
		}
		return true, StoreFileResult{}, fmt.Errorf("completed object %d repair plan is unsafe: %w", fileID, err)
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
	staged, err := stageCompletedRepair(ctx, dbconn, attemptID, sgctx, runtime, plan)
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

	if err := publishCompletedRepair(ctx, dbconn, attemptID, sgctx, recipe, plan, prepared, normalizedPath, replace, staged); err != nil {
		var contention *completedRepairContentionError
		if errors.As(err, &contention) {
			if cleanupErr := cleanupContendedStoreRepairAttempt(ctx, dbconn, attemptID, recipe.logicalID, sgctx.EffectiveContainerDir()); cleanupErr != nil {
				return true, StoreFileResult{}, errors.Join(err, fmt.Errorf("clean contended repair attempt %d: %w", attemptID, cleanupErr))
			}
			return true, StoreFileResult{}, err
		}
		_, _ = dbconn.ExecContext(context.Background(),
			`UPDATE store_repair_attempt SET status = 'ABORTED', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status <> 'PUBLISHED'`, attemptID)
		return true, StoreFileResult{}, err
	}
	return true, StoreFileResult{
		FileID: fileID, FileHash: prepared.LogicalHash, Path: normalizedPath, AlreadyStored: false,
	}, nil
}

func waitForCompletedRepairContention(
	ctx context.Context,
	dbconn *sql.DB,
	contention *completedRepairContentionError,
	budget *completedRepairContentionBudget,
) error {
	if contention == nil || contention.chunkID <= 0 {
		return fmt.Errorf("invalid completed repair contention identity")
	}
	if budget.deadline.IsZero() {
		budget.deadline = time.Now().Add(maxClaimWaitDuration)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(budget.deadline)
		if remaining <= 0 {
			return fmt.Errorf("timeout waiting for chunk %d to finish processing", contention.chunkID)
		}
		wait := claimPollingBackoff(chunkWaitingtime, budget.pollAttempt)
		budget.pollAttempt++
		if wait > remaining {
			wait = remaining
		}
		if err := sleepWithContext(ctx, wait); err != nil {
			return err
		}
		var status string
		if err := dbconn.QueryRowContext(ctx, `SELECT status FROM chunk WHERE id = $1`, contention.chunkID).Scan(&status); err != nil {
			return err
		}
		switch status {
		case filestate.ChunkCompleted, filestate.ChunkAborted:
			return nil
		case filestate.ChunkProcessing:
			continue
		default:
			return fmt.Errorf("unexpected chunk %d status during completed repair contention: %s", contention.chunkID, status)
		}
	}
}

type contendedRepairContainer struct {
	id       int64
	filename string
	path     string
}

func cleanupContendedStoreRepairAttempt(
	ctx context.Context,
	dbconn *sql.DB,
	attemptID int64,
	logicalID int64,
	containersDir string,
) error {
	var ownedLogicalID int64
	var status string
	if err := dbconn.QueryRowContext(ctx,
		`SELECT logical_file_id, status FROM store_repair_attempt WHERE id = $1`, attemptID,
	).Scan(&ownedLogicalID, &status); err != nil {
		return fmt.Errorf("load contended repair attempt: %w", err)
	}
	if ownedLogicalID != logicalID || (status != "PREPARING" && status != "READY") {
		return fmt.Errorf("contended repair attempt ownership changed: logical=%d status=%s", ownedLogicalID, status)
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT c.id, c.filename, c.quarantine,
		       (SELECT COUNT(*) FROM storage_blocks sb WHERE sb.container_id = c.id),
		       (SELECT COUNT(*) FROM blocks b WHERE b.container_id = c.id),
		       rc.status
		FROM store_repair_container rc
		JOIN container c ON c.id = rc.container_id
		WHERE rc.attempt_id = $1
		ORDER BY c.id`, attemptID)
	if err != nil {
		return fmt.Errorf("load contended repair containers: %w", err)
	}
	var candidates []contendedRepairContainer
	for rows.Next() {
		var candidate contendedRepairContainer
		var quarantine bool
		var activePacked, activeLegacy int64
		var containerStatus string
		if err := rows.Scan(&candidate.id, &candidate.filename, &quarantine, &activePacked, &activeLegacy, &containerStatus); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan contended repair container: %w", err)
		}
		if !quarantine || activePacked != 0 || activeLegacy != 0 ||
			(containerStatus != "ALLOCATED" && containerStatus != "DURABLE") {
			_ = rows.Close()
			return fmt.Errorf(
				"refuse contended repair cleanup for container %d: quarantine=%t packed_refs=%d legacy_refs=%d status=%s",
				candidate.id, quarantine, activePacked, activeLegacy, containerStatus,
			)
		}
		candidate.path, err = container.SafeContainerPath(containersDir, candidate.filename)
		if err != nil {
			_ = rows.Close()
			return fmt.Errorf("invalid contended repair container filename %q: %w", candidate.filename, err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate contended repair containers: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close contended repair containers: %w", err)
	}

	syncedDirs := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if err := os.Remove(candidate.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove contended repair container %d: %w", candidate.id, err)
		}
		syncedDirs[filepath.Dir(candidate.path)] = struct{}{}
	}
	for dir := range syncedDirs {
		if err := fsx.SyncDir(dir); err != nil {
			return fmt.Errorf("sync contended repair container directory: %w", err)
		}
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin contended repair cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := db.AcquireRepositoryMutationLock(ctx, dbconn, tx); err != nil {
		return fmt.Errorf("lock contended repair cleanup: %w", err)
	}
	var lockedLogicalID int64
	var lockedStatus string
	if err := tx.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(dbconn,
		`SELECT logical_file_id, status FROM store_repair_attempt WHERE id = $1`), attemptID,
	).Scan(&lockedLogicalID, &lockedStatus); err != nil {
		return fmt.Errorf("lock contended repair attempt: %w", err)
	}
	if lockedLogicalID != logicalID || (lockedStatus != "PREPARING" && lockedStatus != "READY") {
		return fmt.Errorf("contended repair attempt changed before cleanup: logical=%d status=%s", lockedLogicalID, lockedStatus)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM store_repair_chunk WHERE attempt_id = $1`, attemptID); err != nil {
		return fmt.Errorf("delete contended repair chunks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM store_repair_block WHERE attempt_id = $1`, attemptID); err != nil {
		return fmt.Errorf("delete contended repair blocks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM store_repair_container WHERE attempt_id = $1`, attemptID); err != nil {
		return fmt.Errorf("delete contended repair container ownership: %w", err)
	}
	for _, candidate := range candidates {
		result, err := tx.ExecContext(ctx, `DELETE FROM container WHERE id = $1 AND quarantine = TRUE`, candidate.id)
		if err != nil {
			return fmt.Errorf("delete contended repair container %d: %w", candidate.id, err)
		}
		if err := db.RequireExactlyOneRow(result, "delete contended repair container"); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx,
		`DELETE FROM store_repair_attempt WHERE id = $1 AND logical_file_id = $2 AND status IN ('PREPARING', 'READY')`,
		attemptID, logicalID,
	)
	if err != nil {
		return fmt.Errorf("delete contended repair attempt: %w", err)
	}
	if err := db.RequireExactlyOneRow(result, "delete contended repair attempt"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit contended repair cleanup: %w", err)
	}
	return nil
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
		       c.retry_count, c.live_ref_count, c.pin_count,
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
			&item.retryCount, &item.liveRefs, &item.pins,
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
		if item.status == filestate.ChunkProcessing {
			return recipe, &completedRepairContentionError{
				chunkID: item.id, chunkHash: item.hash,
				reason: "another Store owns the transient PROCESSING claim",
			}
		}
		if item.status != filestate.ChunkCompleted && item.status != filestate.ChunkAborted {
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

func buildCompletedRepairPlan(
	ctx context.Context,
	dbconn *sql.DB,
	containersDir string,
	recipe completedRepairRecipe,
	prepared preparedFile,
) (completedRepairPlan, error) {
	var plan completedRepairPlan
	byID := make(map[int64]int, len(recipe.chunks))
	for index, item := range recipe.chunks {
		if existingIndex, ok := byID[item.id]; ok {
			existing := plan.entities[existingIndex]
			if !sameCompletedRepairEntity(existing.chunk, item) ||
				existing.source.Hash != prepared.Chunks[index].Hash ||
				existing.source.Size != prepared.Chunks[index].Size {
				return plan, fmt.Errorf("repeated recipe chunk %d has inconsistent identity or placement", item.id)
			}
			continue
		}
		entity := completedRepairEntity{chunk: item, source: prepared.Chunks[index]}
		entity.repairStatus = item.status == filestate.ChunkAborted
		replacePlacement, err := completedRepairPlacementNeedsRepair(ctx, dbconn, containersDir, item, entity.source)
		if err != nil {
			return plan, err
		}
		entity.replacePlacement = replacePlacement
		byID[item.id] = len(plan.entities)
		plan.entities = append(plan.entities, entity)
	}

	for index := range plan.entities {
		entity := &plan.entities[index]
		if entity.replacePlacement {
			entity.stagingOrdinal = len(plan.staged)
			plan.staged = append(plan.staged, *entity)
		}
		if entity.replacePlacement || entity.repairStatus {
			plan.repairedChunkIDs = append(plan.repairedChunkIDs, entity.chunk.id)
		}
	}
	if len(plan.repairedChunkIDs) == 0 {
		return plan, errCompletedRepairNoEntity
	}
	return plan, nil
}

func sameCompletedRepairEntity(left, right completedRepairChunk) bool {
	if left.id != right.id || left.hash != right.hash || left.size != right.size ||
		left.status != right.status || left.retryCount != right.retryCount ||
		left.liveRefs != right.liveRefs || left.pins != right.pins ||
		left.legacy.id != right.legacy.id || left.legacy.codec != right.legacy.codec ||
		left.legacy.formatVersion != right.legacy.formatVersion ||
		left.legacy.plaintextSize != right.legacy.plaintextSize ||
		left.legacy.storedSize != right.legacy.storedSize ||
		!bytes.Equal(left.legacy.nonce, right.legacy.nonce) ||
		left.legacy.containerID != right.legacy.containerID || left.legacy.offset != right.legacy.offset {
		return false
	}
	if left.packed == nil || right.packed == nil {
		return left.packed == nil && right.packed == nil
	}
	return *left.packed == *right.packed
}

func completedRepairPlacementNeedsRepair(
	ctx context.Context,
	dbconn *sql.DB,
	containersDir string,
	item completedRepairChunk,
	source preparedChunk,
) (bool, error) {
	containerID := item.legacy.containerID
	offset := item.legacy.offset
	storedSize := item.legacy.storedSize
	if item.packed != nil {
		if err := dbconn.QueryRowContext(ctx, `
			SELECT sb.container_id, sb.container_offset, sb.stored_size
			FROM storage_blocks sb
			JOIN chunk_block_refs r ON r.block_id = sb.id
			WHERE r.chunk_id = $1`, item.id,
		).Scan(&containerID, &offset, &storedSize); err != nil {
			return false, fmt.Errorf("load packed placement for chunk %d: %w", item.id, err)
		}
	}
	var filename string
	var currentSize, maxSize int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT filename, current_size, max_size FROM container WHERE id = $1`, containerID,
	).Scan(&filename, &currentSize, &maxSize); err != nil {
		return false, fmt.Errorf("load container %d for chunk %d: %w", containerID, item.id, err)
	}
	if currentSize < int64(container.ContainerHdrLen) || storedSize <= 0 ||
		offset < int64(container.ContainerHdrLen) || offset > currentSize || storedSize > currentSize-offset {
		return false, fmt.Errorf("chunk %d has invalid authoritative placement bounds", item.id)
	}
	path, err := container.SafeContainerPath(containersDir, filename)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat repair placement for chunk %d: %w", item.id, err)
	}
	if info.Size() < offset+storedSize {
		return true, nil
	}

	var plaintext []byte
	if item.packed != nil {
		reader := NewStorageBlockReader(dbconn, containersDir)
		block, err := reader.ReadBlock(ctx, item.packed.blockID)
		if err != nil {
			return true, nil
		}
		plaintext, err = blocks.SliceChunkFromPayload(block.Payload, blocks.ChunkEntry{
			Offset: uint64(item.packed.offsetInBlock),
			Size:   uint64(item.packed.sizeInBlock),
		})
		if err != nil {
			return true, nil
		}
	} else {
		filecontainer, err := container.OpenReadOnlyContainer(path, maxSize)
		if err != nil {
			return true, nil
		}
		payload, readErr := container.ReadPayloadAt(filecontainer, offset, storedSize)
		closeErr := filecontainer.Close()
		if readErr != nil || closeErr != nil {
			return true, nil
		}
		transformer, err := blocks.GetBlockTransformer(blocks.Codec(item.legacy.codec))
		if err != nil {
			return false, fmt.Errorf("get repair validator for chunk %d codec %s: %w", item.id, item.legacy.codec, err)
		}
		plaintext, err = transformer.Decode(ctx, blocks.DecodeInput{
			ChunkHash: item.hash,
			Descriptor: blocks.Descriptor{
				ChunkID:       item.id,
				Codec:         blocks.Codec(item.legacy.codec),
				FormatVersion: item.legacy.formatVersion,
				PlaintextSize: item.legacy.plaintextSize,
				StoredSize:    item.legacy.storedSize,
				Nonce:         item.legacy.nonce,
				ContainerID:   item.legacy.containerID,
				BlockOffset:   item.legacy.offset,
			},
			Payload: payload,
		})
		if err != nil {
			return true, nil
		}
	}
	return !bytes.Equal(plaintext, source.Data), nil
}

func completedRepairFingerprint(ctx context.Context, q repairQueryer, recipe completedRepairRecipe) (string, error) {
	parts := []string{fmt.Sprintf("logical|%d|%s|%d|%d", recipe.logicalID, recipe.status, recipe.retryCount, recipe.refCount)}
	for _, item := range recipe.chunks {
		parts = append(parts, fmt.Sprintf("chunk|%d|%d|%s|%d|%s|%d|%d|%d", item.order, item.id, item.hash, item.size, item.status, item.retryCount, item.liveRefs, item.pins))
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
	plan completedRepairPlan,
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

	for _, entity := range plan.staged {
		source := entity.source
		if builder.ShouldFlushBeforeAdd(int64(source.Size)) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		hashBytes, err := hex.DecodeString(source.Hash)
		if err != nil {
			return nil, fmt.Errorf("decode chunk hash %s: %w", source.Hash, err)
		}
		if err := builder.Add(blocks.PendingChunk{ChunkID: entity.chunk.id, Hash: hashBytes, Data: source.Data, Size: int64(source.Size)}); err != nil {
			return nil, err
		}
		pending = append(pending, stagedRepairChunk{id: entity.chunk.id, order: entity.stagingOrdinal, size: int64(source.Size)})
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
	plan completedRepairPlan,
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
	if err := validateRepairStageForPublication(ctx, dbconn, tx, attemptID, original, plan, prepared, staged); err != nil {
		return err
	}
	var lockedID int64
	if err := tx.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(dbconn,
		`SELECT id FROM logical_file WHERE id = $1`), original.logicalID).Scan(&lockedID); err != nil {
		return fmt.Errorf("lock repair logical object: %w", err)
	}
	if err := lockCompletedRepairPublicationRows(ctx, dbconn, tx, original); err != nil {
		return err
	}
	current, err := loadCompletedRepairRecipe(ctx, tx, original.logicalID, prepared)
	if err != nil {
		return fmt.Errorf("revalidate repair recipe at publication: %w", err)
	}
	if current.fingerprint != original.fingerprint {
		if !completedRepairHasSupportedTerminalDisplacement(original, current) {
			return fmt.Errorf(
				"repair publication fingerprint changed without a supported Store chunk transition: before=%s current=%s",
				original.fingerprint, current.fingerprint,
			)
		}
		return &completedRepairContentionError{
			chunkID: current.chunks[0].id, chunkHash: current.chunks[0].hash,
			reason: fmt.Sprintf("repair publication fingerprint displaced: before=%s current=%s", original.fingerprint, current.fingerprint),
		}
	}
	currentByID := make(map[int64]completedRepairChunk, len(current.chunks))
	for _, item := range current.chunks {
		if _, exists := currentByID[item.id]; exists {
			continue
		}
		currentByID[item.id] = item
		if item.pins != 0 {
			return fmt.Errorf("repair publication requires zero restore pins: chunk=%d pins=%d", item.id, item.pins)
		}
	}

	for _, entity := range plan.staged {
		item, ok := currentByID[entity.chunk.id]
		if !ok {
			return fmt.Errorf("repair publication lost staged chunk %d", entity.chunk.id)
		}
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
	logicalResult, err := tx.ExecContext(ctx,
		`UPDATE logical_file SET retry_count = retry_count + 1 WHERE id = $1 AND status = $2`,
		original.logicalID, filestate.LogicalFileCompleted,
	)
	if err != nil {
		return fmt.Errorf("increment repaired logical file %d retry count: %w", original.logicalID, err)
	}
	if err := db.RequireExactlyOneRow(logicalResult, "increment repaired logical retry count"); err != nil {
		return err
	}
	for _, chunkID := range plan.repairedChunkIDs {
		validated, ok := currentByID[chunkID]
		if !ok {
			return fmt.Errorf("repair publication lost repaired chunk %d", chunkID)
		}
		chunkResult, err := tx.ExecContext(ctx, `
			UPDATE chunk
			SET status = $1, retry_count = retry_count + 1
			WHERE id = $2 AND status = $3 AND retry_count = $4`,
			filestate.ChunkCompleted, chunkID, validated.status, validated.retryCount,
		)
		if err != nil {
			return fmt.Errorf("publish repaired chunk %d status and retry count: %w", chunkID, err)
		}
		if err := db.RequireExactlyOneRow(chunkResult, "publish repaired chunk status and retry count"); err != nil {
			return err
		}
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

func completedRepairHasSupportedTerminalDisplacement(original, current completedRepairRecipe) bool {
	if original.logicalID != current.logicalID || len(original.chunks) != len(current.chunks) {
		return false
	}
	displaced := false
	for index := range original.chunks {
		before, after := original.chunks[index], current.chunks[index]
		if before.order != after.order || before.id != after.id || before.hash != after.hash || before.size != after.size {
			return false
		}
		if before.status == after.status && before.retryCount == after.retryCount {
			continue
		}
		if after.retryCount < before.retryCount ||
			(after.status != filestate.ChunkCompleted && after.status != filestate.ChunkAborted) {
			return false
		}
		switch {
		case before.status == filestate.ChunkCompleted && after.status == filestate.ChunkAborted:
		case after.retryCount > before.retryCount:
		default:
			return false
		}
		displaced = true
	}
	return displaced
}

func lockCompletedRepairPublicationRows(
	ctx context.Context,
	dbconn *sql.DB,
	tx *sql.Tx,
	original completedRepairRecipe,
) error {
	packedIDs := make(map[int64]struct{}, len(original.chunks))
	chunkIDs := make(map[int64]struct{}, len(original.chunks))
	legacyIDs := make(map[int64]struct{}, len(original.chunks))
	for _, item := range original.chunks {
		chunkIDs[item.id] = struct{}{}
		legacyIDs[item.legacy.id] = struct{}{}
		if item.packed != nil {
			packedIDs[item.packed.blockID] = struct{}{}
		}
	}

	for _, blockID := range sortedCompletedRepairLockIDs(packedIDs) {
		var lockedBlockID int64
		if err := tx.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(dbconn,
			`SELECT id FROM storage_blocks WHERE id = $1`), blockID,
		).Scan(&lockedBlockID); err != nil {
			return fmt.Errorf("lock repair packed block %d: %w", blockID, err)
		}
		rows, err := tx.QueryContext(ctx, db.QueryWithOptionalForUpdate(dbconn,
			`SELECT chunk_id FROM chunk_block_refs WHERE block_id = $1 ORDER BY chunk_id`), blockID)
		if err != nil {
			return fmt.Errorf("lock repair packed block %d membership: %w", blockID, err)
		}
		for rows.Next() {
			var memberChunkID int64
			if err := rows.Scan(&memberChunkID); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan repair packed block %d membership lock: %w", blockID, err)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate repair packed block %d membership locks: %w", blockID, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close repair packed block %d membership locks: %w", blockID, err)
		}
	}

	for _, chunkID := range sortedCompletedRepairLockIDs(chunkIDs) {
		var lockedChunkID int64
		if err := tx.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(dbconn,
			`SELECT id FROM chunk WHERE id = $1`), chunkID,
		).Scan(&lockedChunkID); err != nil {
			return fmt.Errorf("lock repair chunk %d: %w", chunkID, err)
		}
	}

	for _, blockID := range sortedCompletedRepairLockIDs(legacyIDs) {
		var lockedBlockID int64
		if err := tx.QueryRowContext(ctx, db.QueryWithOptionalForUpdate(dbconn,
			`SELECT id FROM blocks WHERE id = $1`), blockID,
		).Scan(&lockedBlockID); err != nil {
			return fmt.Errorf("lock repair legacy block %d: %w", blockID, err)
		}
	}
	return nil
}

func sortedCompletedRepairLockIDs(ids map[int64]struct{}) []int64 {
	sorted := make([]int64, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	return sorted
}

func validateRepairStageForPublication(
	ctx context.Context,
	dbconn *sql.DB,
	tx *sql.Tx,
	attemptID int64,
	original completedRepairRecipe,
	plan completedRepairPlan,
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
	if containerCount != len(staged) || blockCount != len(staged) || chunkCount != len(plan.staged) {
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
		if order != seen || seen >= len(plan.staged) {
			return fmt.Errorf("staged repair chunk order changed before publication")
		}
		want := plan.staged[seen]
		if chunkID != want.chunk.id || size != want.chunk.size {
			return fmt.Errorf("staged repair chunk %d changed before publication", want.chunk.id)
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
	if seen != len(plan.staged) {
		return fmt.Errorf("staged repair chunk count changed before publication")
	}
	return nil
}
