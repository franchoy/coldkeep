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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/execution"
	filestate "github.com/franchoy/coldkeep/internal/status"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

type payloadStatefulWriter interface {
	AppendPayload(tx db.DBTX, payload []byte) (container.LocalPlacement, error)
	FinalizeContainer() error
}

type storeInterleavingEvent string

var errSharedPackedBlockPartialRebuild = errors.New("partial rebuild of shared packed block refused")

const (
	storeInterleavingEventAfterChunkClaim            storeInterleavingEvent = "after_chunk_claim"
	storeInterleavingEventBeforePackedFlush          storeInterleavingEvent = "before_packed_flush"
	storeInterleavingEventAfterPackedMetadata        storeInterleavingEvent = "after_packed_metadata"
	storeInterleavingEventAfterChunkCompleted        storeInterleavingEvent = "after_chunk_completed"
	storeInterleavingEventAfterLegacyCompanionInsert storeInterleavingEvent = "after_legacy_companion_insert"
	storeInterleavingEventBeforePackedCommit         storeInterleavingEvent = "before_packed_commit"
	storeInterleavingEventAfterPackedCommit          storeInterleavingEvent = "after_packed_commit"
	storeInterleavingEventBeforeChunkRetryCAS        storeInterleavingEvent = "before_chunk_retry_cas"
	storeInterleavingEventBeforeMarkChunkForRebuild  storeInterleavingEvent = "before_mark_chunk_for_rebuild"
	storeInterleavingEventAfterMarkChunkForRebuild   storeInterleavingEvent = "after_mark_chunk_for_rebuild"
)

type storeInterleavingHookEvent struct {
	StoreOpID string
	ChunkID   int64
	ChunkHash string
	Codec     string
	Event     storeInterleavingEvent
	BlockID   int64
	TxAttempt int
}

type storeInterleavingHooks struct {
	onEvent func(context.Context, storeInterleavingHookEvent) error
}

type storeInterleavingState struct {
	hooks     *storeInterleavingHooks
	storeOpID string
	codec     string
	fileHash  string
}

type storeInterleavingContextKey string

const (
	storeInterleavingContextKeyState storeInterleavingContextKey = "store_interleaving_state"
)

func withStoreInterleavingState(ctx context.Context, state *storeInterleavingState) context.Context {
	if state == nil {
		return ctx
	}
	return context.WithValue(ctx, storeInterleavingContextKeyState, state)
}

func storeInterleavingStateFromContext(ctx context.Context) *storeInterleavingState {
	state, _ := ctx.Value(storeInterleavingContextKeyState).(*storeInterleavingState)
	return state
}

func fireStoreInterleavingHook(ctx context.Context, event storeInterleavingHookEvent) error {
	state := storeInterleavingStateFromContext(ctx)
	if state == nil || state.hooks == nil || state.hooks.onEvent == nil {
		return nil
	}
	return state.hooks.onEvent(ctx, event)
}

type TestStoreInterleavingEvent = storeInterleavingEvent
type TestStoreInterleavingHookEvent = storeInterleavingHookEvent

const (
	TestStoreInterleavingEventAfterChunkClaim            = storeInterleavingEventAfterChunkClaim
	TestStoreInterleavingEventBeforePackedFlush          = storeInterleavingEventBeforePackedFlush
	TestStoreInterleavingEventAfterPackedMetadata        = storeInterleavingEventAfterPackedMetadata
	TestStoreInterleavingEventAfterChunkCompleted        = storeInterleavingEventAfterChunkCompleted
	TestStoreInterleavingEventAfterLegacyCompanionInsert = storeInterleavingEventAfterLegacyCompanionInsert
	TestStoreInterleavingEventBeforePackedCommit         = storeInterleavingEventBeforePackedCommit
	TestStoreInterleavingEventAfterPackedCommit          = storeInterleavingEventAfterPackedCommit
	TestStoreInterleavingEventBeforeChunkRetryCAS        = storeInterleavingEventBeforeChunkRetryCAS
	TestStoreInterleavingEventBeforeMarkChunkForRebuild  = storeInterleavingEventBeforeMarkChunkForRebuild
	TestStoreInterleavingEventAfterMarkChunkForRebuild   = storeInterleavingEventAfterMarkChunkForRebuild
)

func InstallTestStoreInterleavingHooks(
	sgctx *StorageContext,
	onEvent func(context.Context, TestStoreInterleavingHookEvent) error,
) func() {
	if sgctx == nil {
		return func() {}
	}
	prevHooks := sgctx.interleavingHooks
	prevSeq := sgctx.interleavingSeq
	sgctx.interleavingHooks = &storeInterleavingHooks{onEvent: onEvent}
	if sgctx.interleavingSeq == nil {
		sgctx.interleavingSeq = &atomic.Uint64{}
	}
	return func() {
		sgctx.interleavingHooks = prevHooks
		sgctx.interleavingSeq = prevSeq
	}
}

const defaultPackedBlockTargetSizeBytes = blocks.DefaultPackedBlockTargetSizeBytes

func packedBlockTargetSizeBytesFromEnv() int64 {
	resolution := blocks.ResolvePackedBlockTarget()
	switch resolution.Warning {
	case blocks.PackedBlockTargetWarningInvalid:
		log.Printf("invalid packed block target size mb=%d; using default %d bytes", resolution.Megabytes, defaultPackedBlockTargetSizeBytes)
	case blocks.PackedBlockTargetWarningUnsupported:
		log.Printf("unsupported packed block target size mb=%d; v1.8 supports override values 1,2,3; using locked default %d bytes", resolution.Megabytes, defaultPackedBlockTargetSizeBytes)
	case blocks.PackedBlockTargetWarningOverflow:
		log.Printf("packed block target size mb=%d overflows int64 bytes; using default %d bytes", resolution.Megabytes, defaultPackedBlockTargetSizeBytes)
	}
	return resolution.Bytes
}

// preparedFile is the internal output of the CPU-side preparation phase.
// It captures deterministic, immutable metadata before any DB/container mutation.
type preparedFile struct {
	Path             string
	LogicalHash      string
	SizeBytes        int64
	ChunkerVersion   string
	PhysicalMetadata physicalFileMetadata
	Chunks           []preparedChunk
}

// hashFile computes the logical identity hash from raw file bytes.
// Keep this helper for parity tests that guard identity semantics.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// prepareFileForStoreWithContext performs single-pass file preparation and
// returns the immutable file-level metadata used by the commit phase.
// fileinfo is the os.FileInfo for path; if nil, PhysicalMetadata in the result will be zero-valued.
func prepareFileForStoreWithContext(
	ctx context.Context,
	path string,
	effectiveChunker chunk.Chunker,
	chunkerVersion string,
	fileinfo os.FileInfo,
) (preparedFile, error) {
	if err := ctx.Err(); err != nil {
		return preparedFile{}, err
	}

	chunkResults, err := effectiveChunker.ChunkFile(path)
	if err != nil {
		return preparedFile{}, fmt.Errorf("chunk file %s: %w", path, err)
	}

	prepared := make([]preparedChunk, 0, len(chunkResults))
	chunkHasher := sha256.New()
	fileHasher := sha256.New()
	hashBuf := make([]byte, 0, sha256.Size)
	totalSize := int64(0)

	for i, res := range chunkResults {
		if err := ctx.Err(); err != nil {
			return preparedFile{}, err
		}

		data := res.Data
		_, _ = fileHasher.Write(data)

		hash := res.Info.Hash
		if strings.TrimSpace(hash) == "" {
			chunkHasher.Reset()
			_, _ = chunkHasher.Write(data)
			hashBuf = chunkHasher.Sum(hashBuf[:0])
			hash = hex.EncodeToString(hashBuf)
		}

		prepared = append(prepared, preparedChunk{
			Index:          i,
			Offset:         res.Info.Offset,
			Size:           len(data),
			Hash:           hash,
			ChunkerVersion: chunkerVersion,
			Data:           data,
		})
		totalSize += int64(len(data))
	}

	// Preserve deterministic commit semantics even if preparation gains internal
	// parallelism in future phases.
	sort.Slice(prepared, func(i, j int) bool {
		return prepared[i].Index < prepared[j].Index
	})
	for i, ch := range prepared {
		if ch.Index != i {
			return preparedFile{}, fmt.Errorf("non-contiguous chunk index: got %d want %d", ch.Index, i)
		}
	}

	return preparedFile{
		Path:             path,
		LogicalHash:      hex.EncodeToString(fileHasher.Sum(nil)),
		SizeBytes:        totalSize,
		ChunkerVersion:   chunkerVersion,
		PhysicalMetadata: buildPhysicalFileMetadata(fileinfo),
		Chunks:           prepared,
	}, nil
}

// commitInfoForChunks captures immutable information needed for commit phase.
type commitInfoForChunks struct {
	fileID           int64
	fileHash         string
	normalizedPath   string
	physicalMetadata physicalFileMetadata
	reuseValidation  reusableValidationPolicy
	replace          bool
}

// commitPreparedChunksWithContext commits prepared chunks to storage sequentially.
// This phase handles all DB mutations and container writes in deterministic order.
// Receives fully prepared chunks (hashed, sized, ordered) and commits them safely.
func commitPreparedChunksWithContext(
	ctx context.Context,
	dbconn *sql.DB,
	writer payloadStatefulWriter,
	_ *blocks.Repository,
	transformer blocks.Transformer,
	compression storeRuntimeCompression,
	sgctx StorageContext,
	commitInfo commitInfoForChunks,
	preparedChunks []preparedChunk,
	activeVersionString string,
) (StoreFileResult, error) {
	result := StoreFileResult{
		FileID:        commitInfo.fileID,
		FileHash:      commitInfo.fileHash,
		Path:          commitInfo.normalizedPath,
		AlreadyStored: false,
	}

	if len(preparedChunks) == 0 {
		// Empty file: finalize and return
		if err := finalizeLogicalFileStorageWithContext(ctx, dbconn, commitInfo.fileID, 0); err != nil {
			return StoreFileResult{}, err
		}
		tx, err := dbconn.BeginTx(ctx, nil)
		if err != nil {
			return StoreFileResult{}, err
		}
		if _, err := ensurePhysicalFileForPathWithPolicyWithTx(ctx, dbconn, tx, commitInfo.normalizedPath, commitInfo.fileID, commitInfo.physicalMetadata, commitInfo.replace, recipeLivenessAlreadyAccounted); err != nil {
			_ = tx.Rollback()
			return StoreFileResult{}, err
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return StoreFileResult{}, err
		}
		return result, nil
	}

	// Step 8 invariant: the commit/pack/write path must consume an ordered chunk
	// stream. Worker completion order from any upstream parallel prep is not a
	// valid source of layout order.
	for i, ch := range preparedChunks {
		if ch.Index != i {
			return StoreFileResult{}, fmt.Errorf("prepared chunks out of order: got index %d at position %d", ch.Index, i)
		}
	}

	type pendingPackedChunk struct {
		chunkID  int64
		hash     string
		prepared preparedChunk
	}

	builder := blocks.NewBlockBuilder(packedBlockTargetSizeBytesFromEnv())
	pendingPacked := make([]pendingPackedChunk, 0, 8)
	pendingPackedHashes := make(map[string]struct{})
	interleavingState := storeInterleavingStateFromContext(ctx)
	storeOpID := ""
	storeCodec := ""
	if interleavingState != nil {
		storeOpID = interleavingState.storeOpID
		storeCodec = interleavingState.codec
	}

	flushPackedPending := func() error {
		if builder.Empty() {
			return nil
		}

		for {
			if err := ctx.Err(); err != nil {
				return err
			}

			tx, err := dbconn.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
				StoreOpID: storeOpID,
				Codec:     storeCodec,
				Event:     storeInterleavingEventBeforePackedFlush,
			}); err != nil {
				_ = tx.Rollback()
				return err
			}

			persisted, err := storePackedBlockWithWriter(ctx, tx, writer, transformer, compression, builder)
			if err != nil {
				_ = tx.Rollback()
				var brokenOpenErr *container.BrokenOpenContainerError
				if errors.As(err, &brokenOpenErr) {
					if quarantineErr := quarantineContainerNow(sgctx.DB, brokenOpenErr.ContainerID, sgctx.EffectiveContainerDir()); quarantineErr != nil {
						return errors.Join(err, fmt.Errorf("quarantine broken open container %d after rollback: %w", brokenOpenErr.ContainerID, quarantineErr))
					}
					return err
				}
				if errors.Is(err, container.ErrContainerLockContention) || errors.Is(err, container.ErrContainerFull) {
					continue
				}

				for _, pending := range pendingPacked {
					if _, err3 := dbconn.ExecContext(
						ctx,
						`UPDATE chunk SET status = $1 WHERE id = $2`,
						filestate.ChunkAborted,
						pending.chunkID,
					); err3 != nil {
						if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
							return errors.Join(err3, rbErr)
						}
						return err3
					}
				}
				if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
					return errors.Join(err, rbErr)
				}
				return err
			}

			for _, pending := range pendingPacked {
				segment, ok := persisted.Segments[pending.chunkID]
				if !ok {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(fmt.Errorf("missing packed segment metadata for chunk %d", pending.chunkID), rbErr)
					}
					return fmt.Errorf("missing packed segment metadata for chunk %d", pending.chunkID)
				}
				if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
					StoreOpID: storeOpID,
					ChunkID:   pending.chunkID,
					ChunkHash: pending.hash,
					Codec:     storeCodec,
					Event:     storeInterleavingEventAfterPackedMetadata,
					BlockID:   persisted.BlockID,
				}); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}

				result, err := tx.ExecContext(
					ctx,
					`UPDATE chunk SET status = $1 WHERE id = $2`,
					filestate.ChunkCompleted,
					pending.chunkID,
				)
				if err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}
				if err := db.RequireExactlyOneRow(result, "complete packed chunk"); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}
				if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
					StoreOpID: storeOpID,
					ChunkID:   pending.chunkID,
					ChunkHash: pending.hash,
					Codec:     storeCodec,
					Event:     storeInterleavingEventAfterChunkCompleted,
					BlockID:   persisted.BlockID,
				}); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}

				legacyCodec := "plain"
				legacyNonce := []byte{}
				legacyOffset := persisted.Placement.Offset + segment.Offset
				legacyStoredSize := segment.Size

				if persisted.StorageCodec == string(blocks.CodecAESGCM) {
					legacyCodec = string(blocks.CodecAESGCM)
					legacyNonce = append(legacyNonce, persisted.LegacyNonce...)
					// AES-GCM packed payload stores one encrypted block blob per append.
					// Companion rows should point at the block start for compatibility lookups.
					legacyOffset = persisted.Placement.Offset
					legacyStoredSize = persisted.StoredSize
				}

				if err := insertLegacyCompanionBlockRowWithContext(
					ctx,
					tx,
					pending.chunkID,
					legacyCodec,
					legacyNonce,
					persisted.Placement.ContainerID,
					legacyOffset,
					segment.Size,
					legacyStoredSize,
				); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}
				if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
					StoreOpID: storeOpID,
					ChunkID:   pending.chunkID,
					ChunkHash: pending.hash,
					Codec:     storeCodec,
					Event:     storeInterleavingEventAfterLegacyCompanionInsert,
					BlockID:   persisted.BlockID,
				}); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}

				if err := linkFileChunkWithContext(ctx, tx, commitInfo.fileID, pending.chunkID, pending.prepared.Index, true); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}
			}

			if persisted.Placement.Rotated {
				if err := container.UpdateContainerSize(tx, persisted.Placement.PreviousID, persisted.Placement.PreviousSize); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}
				if err := sealContainerWithWriter(tx, writer, persisted.Placement.PreviousID, persisted.Placement.PreviousFilename, sgctx.EffectiveContainerDir()); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					quarantineErr := quarantineContainerNow(sgctx.DB, persisted.Placement.PreviousID, sgctx.EffectiveContainerDir())
					if quarantineErr != nil {
						return errors.Join(err, fmt.Errorf("quarantine rotated container %d after seal failure: %w", persisted.Placement.PreviousID, quarantineErr))
					}
					return err
				}
			}

			if err := container.UpdateContainerSize(tx, persisted.Placement.ContainerID, persisted.Placement.NewContainerSize); err != nil {
				_ = tx.Rollback()
				if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
					return errors.Join(err, rbErr)
				}
				return err
			}

			if persisted.Placement.Full {
				if err := markContainerSealingInTx(tx, persisted.Placement.ContainerID); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					return err
				}
				if err := writer.FinalizeContainer(); err != nil {
					_ = tx.Rollback()
					quarantineErr := quarantineWriterActiveContainer(writer)
					if quarantineErr != nil {
						return errors.Join(err, fmt.Errorf("quarantine active container after finalize failure: %w", quarantineErr))
					}
					return err
				}
				if err := sealContainerWithWriter(tx, writer, persisted.Placement.ContainerID, persisted.Placement.Filename, sgctx.EffectiveContainerDir()); err != nil {
					_ = tx.Rollback()
					if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
						return errors.Join(err, rbErr)
					}
					quarantineErr := quarantineContainerNow(sgctx.DB, persisted.Placement.ContainerID, sgctx.EffectiveContainerDir())
					if quarantineErr != nil {
						return errors.Join(err, fmt.Errorf("quarantine full container %d after seal failure: %w", persisted.Placement.ContainerID, quarantineErr))
					}
					return err
				}
			}

			if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
				StoreOpID: storeOpID,
				Codec:     storeCodec,
				Event:     storeInterleavingEventBeforePackedCommit,
				BlockID:   persisted.BlockID,
			}); err != nil {
				_ = tx.Rollback()
				if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
					return errors.Join(err, rbErr)
				}
				return err
			}

			if err = tx.Commit(); err != nil {
				if rbErr := rollbackWriterLastAppendWithQuarantine(writer); rbErr != nil {
					return errors.Join(err, rbErr)
				}
				return err
			}
			if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
				StoreOpID: storeOpID,
				Codec:     storeCodec,
				Event:     storeInterleavingEventAfterPackedCommit,
				BlockID:   persisted.BlockID,
			}); err != nil {
				return err
			}
			acknowledgeWriterAppendCommitted(writer)

			builder.Reset()
			pendingPacked = pendingPacked[:0]
			for hash := range pendingPackedHashes {
				delete(pendingPackedHashes, hash)
			}
			return nil
		}
	}

	// Deterministic sequential commit of all prepared chunks
	for _, prepared := range preparedChunks {
		if err := ctx.Err(); err != nil {
			return StoreFileResult{}, err
		}

		// Prevent claim waits only when this prepared hash is already buffered in
		// pending packed chunks. Distinct hashes can continue batching.
		if _, pendingDup := pendingPackedHashes[prepared.Hash]; pendingDup {
			if err := flushPackedPending(); err != nil {
				return StoreFileResult{}, err
			}
		}

		// Use precomputed hash and data; no re-allocation or re-hashing in this loop.
		// Try to claim chunk for this hash (concurrency-safe)
		claimedChunkID, chunkStatus, isNewChunkClaim, err := claimChunkWithValidationPolicy(
			ctx,
			dbconn,
			prepared.Hash,
			int64(prepared.Size),
			activeVersionString,
			commitInfo.reuseValidation,
		)
		if err != nil {
			return StoreFileResult{}, err
		}
		if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
			StoreOpID: storeOpID,
			ChunkID:   claimedChunkID,
			ChunkHash: prepared.Hash,
			Codec:     storeCodec,
			Event:     storeInterleavingEventAfterChunkClaim,
		}); err != nil {
			return StoreFileResult{}, err
		}

		// Phase 4 Step 2 invariant: dedup decision happens before packing/writing.
		// If chunk is already COMPLETED, never write/pack again; only link reference.
		if chunkStatus == filestate.ChunkCompleted {
			if err := flushPackedPending(); err != nil {
				return StoreFileResult{}, err
			}

			// Chunk already stored and ready: just link it to the logical file
			tx, err := dbconn.BeginTx(ctx, nil)
			if err != nil {
				return StoreFileResult{}, err
			}

			// Preserve logical recipe order: file_chunk.chunk_order comes from prepared.Index.
			if err := linkFileChunkWithContext(ctx, tx, commitInfo.fileID, claimedChunkID, prepared.Index, true); err != nil {
				_ = tx.Rollback()
				return StoreFileResult{}, err
			}

			if err = tx.Commit(); err != nil {
				_ = tx.Rollback()
				return StoreFileResult{}, err
			}

			continue // Move to next chunk
		}

		if chunkStatus != filestate.ChunkProcessing {
			return StoreFileResult{}, fmt.Errorf("unexpected chunk status %q for chunk_id=%d before write boundary", chunkStatus, claimedChunkID)
		}

		if !isNewChunkClaim {
			// This is a reclaimed processing claim (e.g. previously aborted/corrupted chunk).
			// It is intentionally rewritten to repair state, not a dedup duplicate write.
			log.Printf("event=store_chunk_reclaim action=write_rebuild file_id=%d chunk_id=%d hash=%s", commitInfo.fileID, claimedChunkID, prepared.Hash)

			hasPackedRef, err := chunkHasPackedRefWithContext(ctx, dbconn, claimedChunkID)
			if err != nil {
				return StoreFileResult{}, err
			}
			hasLegacyBlock, err := chunkHasLegacyBlockWithContext(ctx, dbconn, claimedChunkID)
			if err != nil {
				return StoreFileResult{}, err
			}
			if hasPackedRef || hasLegacyBlock {
				tx, err := dbconn.BeginTx(ctx, nil)
				if err != nil {
					return StoreFileResult{}, err
				}

				result, err := tx.ExecContext(ctx, `UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, claimedChunkID)
				if err != nil {
					_ = tx.Rollback()
					return StoreFileResult{}, err
				}
				if err := db.RequireExactlyOneRow(result, "complete reclaimed chunk"); err != nil {
					_ = tx.Rollback()
					return StoreFileResult{}, err
				}

				if err := linkFileChunkWithContext(ctx, tx, commitInfo.fileID, claimedChunkID, prepared.Index, true); err != nil {
					_ = tx.Rollback()
					return StoreFileResult{}, err
				}

				if err := tx.Commit(); err != nil {
					_ = tx.Rollback()
					return StoreFileResult{}, err
				}
				continue
			}
		}

		if builder.ShouldFlushBeforeAdd(int64(prepared.Size)) {
			if err := flushPackedPending(); err != nil {
				return StoreFileResult{}, err
			}
		}

		if err := builder.Add(blocks.PendingChunk{
			ChunkID: claimedChunkID,
			Data:    prepared.Data,
			Size:    int64(prepared.Size),
		}); err != nil {
			return StoreFileResult{}, err
		}
		pendingPacked = append(pendingPacked, pendingPackedChunk{chunkID: claimedChunkID, hash: prepared.Hash, prepared: prepared})
		pendingPackedHashes[prepared.Hash] = struct{}{}
	}

	if err := flushPackedPending(); err != nil {
		return StoreFileResult{}, err
	}

	// Atomically verify all chunks are linked and mark logical file as COMPLETED.
	if err := finalizeLogicalFileStorageWithContext(ctx, dbconn, commitInfo.fileID, len(preparedChunks)); err != nil {
		return StoreFileResult{}, err
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return StoreFileResult{}, err
	}
	if _, err := ensurePhysicalFileForPathWithPolicyWithTx(ctx, dbconn, tx, commitInfo.normalizedPath, commitInfo.fileID, commitInfo.physicalMetadata, commitInfo.replace, recipeLivenessAlreadyAccounted); err != nil {
		_ = tx.Rollback()
		return StoreFileResult{}, err
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return StoreFileResult{}, err
	}

	return result, nil
}

// commitPreparedFileWithContext commits a fully prepared file recipe without
// re-reading or re-hashing file contents.
func commitPreparedFileWithContext(
	ctx context.Context,
	dbconn *sql.DB,
	writer payloadStatefulWriter,
	blockRepo *blocks.Repository,
	transformer blocks.Transformer,
	compression storeRuntimeCompression,
	sgctx StorageContext,
	prepared preparedFile,
	fileID int64,
	normalizedPath string,
	physicalMetadata physicalFileMetadata,
	reuseValidation reusableValidationPolicy,
	replace bool,
) (StoreFileResult, error) {
	commitInfo := commitInfoForChunks{
		fileID:           fileID,
		fileHash:         prepared.LogicalHash,
		normalizedPath:   normalizedPath,
		physicalMetadata: physicalMetadata,
		reuseValidation:  reuseValidation,
		replace:          replace,
	}

	return commitPreparedChunksWithContext(
		ctx,
		dbconn,
		writer,
		blockRepo,
		transformer,
		compression,
		sgctx,
		commitInfo,
		prepared.Chunks,
		prepared.ChunkerVersion,
	)
}

// Append lifecycle state machine (authoritative v1.0 contract):
//
// Trigger:
//   - Every successful AppendPayload(tx, payload) creates exactly one unresolved
//     append outcome that MUST be resolved before returning to the caller.
//
// Terminal resolution (exactly one path per successful append):
//   - Commit acknowledgment path:
//     If tx.Commit() succeeds, caller MUST invoke AcknowledgeAppendCommitted()
//     exactly once when supported.
//   - Rollback path:
//     If tx is rolled back, or any error happens before tx.Commit() succeeds,
//     caller MUST invoke RollbackLastAppend() when supported.
//
// Quarantine on failed cleanup:
//   - If RollbackLastAppend() fails, caller MUST trigger quarantine for the active
//     container before any further writes.
//   - If physical finalize/sync fails, caller MUST trigger quarantine for the active
//     container before any further writes.
//   - If DB seal/update fails after physical finalize, implementation MUST NOT
//     permit that container to be reused; quarantine (or equivalent
//     exclusion from writable selection) is required.
//
// Boundary invariants:
//   - No successful append may remain unresolved at function boundary.
//   - No successful append may execute both rollback path and commit acknowledgment path.
//   - No container that crosses a failed rollback/finalize/seal boundary may
//     re-enter the writable pool.

// optionalAppendRollbacker is implemented by writers that can truncate a physical
// container file back to its pre-append offset when the enclosing DB transaction
// is rolled back or fails to commit.
type optionalAppendRollbacker interface {
	RollbackLastAppend() error
}

// optionalAppendCommitAcknowledger is implemented by writers that maintain rollback
// state after an unresolved append. Calling AcknowledgeAppendCommitted after a
// successful tx.Commit() closes the commit acknowledgment path of the state
// machine, ensuring that already-committed bytes can never be accidentally
// truncated by a subsequent RollbackLastAppend call.
type optionalAppendCommitAcknowledger interface {
	AcknowledgeAppendCommitted()
}

type optionalFailedActiveContainerQuarantiner interface {
	QuarantineActiveContainer() error
}

type optionalWriterDBBinder interface {
	BindDB(dbconn *sql.DB)
}

// rollbackWriterLastAppend calls RollbackLastAppend on the writer if it supports it.
// Safe to call on any rollback path; if no unresolved append is pending the
// implementation returns immediately without truncating.
// Returns any error from the rollback operation; callers must handle rollback failures
// as they indicate physical/logical inconsistency and must trigger container
// quarantine.
func rollbackWriterLastAppend(writer payloadStatefulWriter) error {
	if rb, ok := writer.(optionalAppendRollbacker); ok {
		return rb.RollbackLastAppend()
	}
	return nil
}

// acknowledgeWriterAppendCommitted calls AcknowledgeAppendCommitted on the writer
// if it supports it. Must be called immediately after every successful tx.Commit()
// that followed an AppendPayload call, completing the commit acknowledgment path of the writer
// state machine so pending rollback bookkeeping is cleared before function return.
// This call is expected exactly once per successful append transaction.
func acknowledgeWriterAppendCommitted(writer payloadStatefulWriter) {
	if ack, ok := writer.(optionalAppendCommitAcknowledger); ok {
		ack.AcknowledgeAppendCommitted()
	}
}

func quarantineWriterActiveContainer(writer payloadStatefulWriter) error {
	if quarantiner, ok := writer.(optionalFailedActiveContainerQuarantiner); ok {
		return quarantiner.QuarantineActiveContainer()
	}
	return nil
}

// rollbackWriterLastAppendWithQuarantine attempts to rollback the writer's last append,
// and if that fails, executes the active-container quarantine path to
// prevent further use.
// Returns the rollback error, or if rollback fails, returns an error wrapping both
// the rollback failure and any quarantine error. This ensures that failed rollbacks
// (physical consistency violations) are treated as critical container failures.
func rollbackWriterLastAppendWithQuarantine(writer payloadStatefulWriter) error {
	rollbackErr := rollbackWriterLastAppend(writer)
	if rollbackErr == nil {
		return nil
	}

	// Rollback failed; this indicates physical/logical inconsistency.
	// Log loudly and attempt to quarantine the active container as a safeguard.
	log.Printf("event=store_rollback_failed error=%v", rollbackErr)

	quarantineErr := quarantineWriterActiveContainer(writer)
	if quarantineErr != nil {
		return errors.Join(
			fmt.Errorf("rollback failed (physical append may be truncated): %w", rollbackErr),
			fmt.Errorf("active container quarantine also failed: %w", quarantineErr),
		)
	}

	return fmt.Errorf("rollback failed; quarantined active container as precaution: %w", rollbackErr)
}

func bindWriterDB(writer payloadStatefulWriter, dbconn *sql.DB) {
	if binder, ok := writer.(optionalWriterDBBinder); ok {
		binder.BindDB(dbconn)
	}
}

func quarantineContainerNow(dbconn *sql.DB, containerID int64, containersDir string) error {
	return container.QuarantineContainerInDir(dbconn, containerID, containersDir)
}

type optionalContainerSealer interface {
	SealContainer(tx db.DBTX, containerID int64, filename string, containersDir string) error
}

func markContainerSealingInTx(tx *sql.Tx, containerID int64) error {
	if tx == nil || containerID <= 0 {
		return nil
	}
	result, err := tx.Exec(`UPDATE container SET sealing = TRUE WHERE id = $1`, containerID)
	if err != nil {
		return fmt.Errorf("mark container %d sealing in tx: %w", containerID, err)
	}
	if err := db.RequireExactlyOneRow(result, "mark container sealing"); err != nil {
		return fmt.Errorf("mark container %d sealing in tx: %w", containerID, err)
	}
	return nil
}

// StoreFileResult contains structured metadata about a store operation.
// Store is a state-changing path: it mutates logical-file, chunk, block, and
// container state as payload is committed.
type StoreFileResult struct {
	FileID        int64  `json:"file_id"`
	FileHash      string `json:"file_hash"`
	Path          string `json:"path"`
	AlreadyStored bool   `json:"already_stored"`
}

// storeFileRuntime captures immutable per-operation dependencies reused across
// many file stores (for example StoreFolder with many small files).
type storeFileRuntime struct {
	codec             blocks.Codec
	transformer       blocks.Transformer
	compression       storeRuntimeCompression
	blockRepo         *blocks.Repository
	storeService      *StoreService
	reuseValidation   reusableValidationPolicy
	interleavingHooks *storeInterleavingHooks
	interleavingSeq   *atomic.Uint64
}

// storeRuntimeCompression groups the optional compression transform and its codec name.
type storeRuntimeCompression struct {
	compressor storagecompression.Compressor
	codec      string
	level      *int
}

func buildStoreFileRuntime(sgctx StorageContext, codec blocks.Codec) (*storeFileRuntime, error) {
	transformer, err := blocks.GetBlockTransformer(codec)
	if err != nil {
		if codec == blocks.CodecAESGCM {
			return nil, fmt.Errorf("encryption key required for aes-gcm: %w", err)
		}
		return nil, fmt.Errorf("initialize codec %s: %w", codec, err)
	}

	reuseValidation := reusableValidationPolicy{
		scope:         reusableValidationFullRepository,
		containersDir: sgctx.EffectiveContainerDir(),
	}
	if sgctx.IsSimulated() {
		reuseValidation = reusableValidationPolicy{scope: reusableValidationSimulationGraphOnly}
	}

	tx, err := sgctx.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx for compression defaults: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	repoCodec, repoCodecErr := GetDefaultCompression(tx)
	if repoCodecErr != nil {
		return nil, fmt.Errorf("load repository default compression codec: %w", repoCodecErr)
	}

	compressionCodec := strings.TrimSpace(strings.ToLower(repoCodec))
	compressionLevel := defaultCompressionLevel

	// Optional environment overrides remain available for tests/operators.
	if rawCodec, ok := os.LookupEnv("COLDKEEP_COMPRESSION"); ok {
		if trimmedCodec := strings.TrimSpace(strings.ToLower(rawCodec)); trimmedCodec != "" {
			compressionCodec = trimmedCodec
		}
	}

	// compression_level is only meaningful when effective compression is zstd.
	if compressionCodec == storagecompression.CompressionZstd {
		if strings.TrimSpace(strings.ToLower(repoCodec)) == storagecompression.CompressionZstd {
			repoLevel, err := GetDefaultCompressionLevel(tx)
			if err != nil {
				return nil, fmt.Errorf("load repository default compression level for codec=%q: %w", compressionCodec, err)
			}
			compressionLevel = repoLevel
		}

		if rawLevel, ok := os.LookupEnv("COLDKEEP_COMPRESSION_LEVEL"); ok {
			trimmedLevel := strings.TrimSpace(rawLevel)
			if trimmedLevel != "" {
				parsedLevel, parseErr := strconv.Atoi(trimmedLevel)
				if parseErr != nil {
					return nil, fmt.Errorf("parse COLDKEEP_COMPRESSION_LEVEL=%q: %w", rawLevel, parseErr)
				}
				compressionLevel = parsedLevel
			}
		}

		if compressionLevel < minCompressionLevel || compressionLevel > maxCompressionLevel {
			return nil, fmt.Errorf("compression level %d out of repository range [%d, %d] for codec=%q", compressionLevel, minCompressionLevel, maxCompressionLevel, compressionCodec)
		}
	}

	compressionRuntime, err := loadCompressionRuntime(compressionCodec, compressionLevel)
	if err != nil {
		return nil, fmt.Errorf("initialize compression runtime codec=%q level=%d: %w", compressionCodec, compressionLevel, err)
	}

	return &storeFileRuntime{
		codec:             codec,
		transformer:       transformer,
		compression:       compressionRuntime,
		blockRepo:         &blocks.Repository{DB: sgctx.DB},
		storeService:      NewStoreService(NewRepository(sgctx.DB), sgctx.Chunker),
		reuseValidation:   reuseValidation,
		interleavingHooks: sgctx.interleavingHooks,
		interleavingSeq:   sgctx.interleavingSeq,
	}, nil
}

func loadCompressionRuntime(codec string, level int) (storeRuntimeCompression, error) {
	normalized := strings.TrimSpace(strings.ToLower(codec))
	if normalized == "" {
		normalized = storagecompression.CompressionNone
	}

	if normalized == storagecompression.CompressionZstd {
		compressor, err := storagecompression.NewZstdCompressor(level)
		if err != nil {
			return storeRuntimeCompression{}, err
		}
		levelCopy := level
		return storeRuntimeCompression{compressor: compressor, codec: normalized, level: &levelCopy}, nil
	}

	compressor, err := storagecompression.Lookup(normalized)
	if err != nil {
		return storeRuntimeCompression{}, err
	}
	return storeRuntimeCompression{compressor: compressor, codec: normalized, level: nil}, nil
}

func sealContainerWithWriter(tx db.DBTX, writer payloadStatefulWriter, containerID int64, filename string, containersDir string) error {
	if sealer, ok := writer.(optionalContainerSealer); ok {
		return sealer.SealContainer(tx, containerID, filename, containersDir)
	}
	return container.SealContainerInDir(tx, containerID, filename, containersDir)
}

func newWriterFromPrototype(prototype container.ContainerWriter) (container.ContainerWriter, error) {
	switch w := prototype.(type) {
	case *container.LocalWriter:
		// Clone LocalWriter per worker for isolation; propagate DB connection
		// so each worker-owned writer can commit sealing markers independently.
		return container.NewLocalWriterWithDirAndDB(w.Dir(), w.MaxSize(), w.DB()), nil
	case *container.SimulatedWriter:
		// Do NOT clone SimulatedWriter; return the original for shared, realistic container packing.
		// Concurrency contract: SimulatedWriter is internally synchronized (mutex-protected).
		return w, nil
	default:
		return nil, fmt.Errorf("unsupported writer type for cloning: %T", prototype)
	}
}

// preparedChunk holds precomputed chunk metadata before any DB mutations.
// This separation enables CPU-side optimization: prepare all chunks deterministically,
// then commit sequentially without re-hashing or re-allocating.
// Index preserves deterministic order across preparation and commit phases.
type preparedChunk struct {
	Index          int
	Offset         int64
	Size           int
	Hash           string
	ChunkerVersion string
	Data           []byte
}

// Test hook: lets tests inject per-index delay/behavior to emulate
// out-of-order worker completion. Keep nil in production.
var testPrepareChunksWorkerHook func(index int)

// prepareChunksWithContext materializes chunk metadata deterministically.
// It computes hashes for chunkers that didn't provide them.
func prepareChunksWithContext(ctx context.Context, results []chunk.Result, chunkerVersion string) ([]preparedChunk, error) {
	if len(results) == 0 {
		return []preparedChunk{}, nil
	}

	type preparedResult struct {
		idx int
		ch  preparedChunk
		err error
	}

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(results) {
		workerCount = len(results)
	}

	jobs := make(chan int)
	resultsCh := make(chan preparedResult, len(results))

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		chunkHasher := sha256.New()
		hashBuf := make([]byte, 0, sha256.Size)
		for i := range jobs {
			if err := ctx.Err(); err != nil {
				resultsCh <- preparedResult{err: err}
				continue
			}
			if testPrepareChunksWorkerHook != nil {
				testPrepareChunksWorkerHook(i)
			}

			res := results[i]
			data := res.Data
			hash := res.Info.Hash
			if strings.TrimSpace(hash) == "" {
				chunkHasher.Reset()
				_, _ = chunkHasher.Write(data)
				hashBuf = chunkHasher.Sum(hashBuf[:0])
				hash = hex.EncodeToString(hashBuf)
			}

			resultsCh <- preparedResult{
				idx: i,
				ch: preparedChunk{
					Index:          i,
					Offset:         res.Info.Offset,
					Size:           len(data),
					Hash:           hash,
					ChunkerVersion: chunkerVersion,
					Data:           data,
				},
			}
		}
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	go func() {
		defer close(jobs)
		for i := range results {
			jobs <- i
		}
	}()

	prepared := make([]preparedChunk, len(results))
	for i := 0; i < len(results); i++ {
		item := <-resultsCh
		if item.err != nil {
			wg.Wait()
			return nil, item.err
		}
		prepared[item.idx] = item.ch
	}
	wg.Wait()

	for i, ch := range prepared {
		if ch.Index != i {
			return nil, fmt.Errorf("non-contiguous chunk index: got %d want %d", ch.Index, i)
		}
	}

	return prepared, nil
}

type reusableLogicalFileGraphSummary struct {
	totalSize         int64
	chunkRefs         int64
	brokenChunkOrders int64
	invalidChunks     int64
	missingBlocks     int64
	invalidContainers int64
}

const (
	retryableTxAbortMaxAttempts = 4
	retryableTxAbortBaseBackoff = 10 * time.Millisecond
)

func isRetryableTxAbortError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "current transaction is aborted") || strings.Contains(msg, "25p02")
}

func runWithRetryableTxAbort(ctx context.Context, fn func(attempt int) error) error {
	var err error
	for attempt := 0; attempt < retryableTxAbortMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err = fn(attempt)
		if err == nil {
			return nil
		}
		if !isRetryableTxAbortError(err) || attempt == retryableTxAbortMaxAttempts-1 {
			return err
		}

		if sleepErr := sleepWithContext(ctx, retryableTxAbortBaseBackoff*time.Duration(attempt+1)); sleepErr != nil {
			return sleepErr
		}
	}

	return err
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
	packedRows               int64
	existingContainerRows    int64
	quarantinedContainerRows int64
}

type reusableValidationScope uint8

const (
	reusableValidationInvalid reusableValidationScope = iota
	reusableValidationFullRepository
	reusableValidationSimulationGraphOnly
)

type reusableValidationPolicy struct {
	scope         reusableValidationScope
	containersDir string
}

func (p reusableValidationPolicy) validate() error {
	switch p.scope {
	case reusableValidationFullRepository:
		if strings.TrimSpace(p.containersDir) == "" {
			return fmt.Errorf("full repository reuse validation container directory is required")
		}
		return nil
	case reusableValidationSimulationGraphOnly:
		return nil
	default:
		return fmt.Errorf("invalid reusable validation scope %d", p.scope)
	}
}

func cleanupLogicalFileChunkMappingsWithContext(ctx context.Context, tx *sql.Tx, fileID int64, markChunksSuspicious bool) error {
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
			continue
		}
		if err != nil {
			return fmt.Errorf("decrement live_ref_count for chunk %d (logical file %d): %w", chunkID, fileID, err)
		}
	}

	if markChunksSuspicious {
		seenChunkIDs := make(map[int64]struct{}, len(chunkIDs))
		for _, chunkID := range chunkIDs {
			if _, ok := seenChunkIDs[chunkID]; ok {
				continue
			}
			seenChunkIDs[chunkID] = struct{}{}
			if _, err := tx.ExecContext(ctx,
				`UPDATE chunk SET retry_count = retry_count + 1 WHERE id = $1`,
				chunkID,
			); err != nil {
				return fmt.Errorf("mark chunk %d suspicious during logical file %d rebuild: %w", chunkID, fileID, err)
			}
		}
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM file_chunk WHERE logical_file_id = $1`,
		fileID,
	); err != nil {
		return fmt.Errorf("delete stale file_chunk rows for logical file %d: %w", fileID, err)
	}

	return nil
}

func validateReusableCompletedChunkWithPolicy(ctx context.Context, dbconn *sql.DB, chunkID int64, policy reusableValidationPolicy) error {
	if err := policy.validate(); err != nil {
		return err
	}

	var summary reusableCompletedChunkSummary
	err := dbconn.QueryRowContext(ctx, `
		SELECT
			COUNT(b.id) AS block_rows,
			COUNT(r.chunk_id) AS packed_rows,
			COUNT(ctr.id) AS existing_container_rows,
			COALESCE(SUM(CASE WHEN ctr.quarantine THEN 1 ELSE 0 END), 0) AS quarantined_container_rows
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		LEFT JOIN container ctr ON ctr.id = COALESCE(sb.container_id, b.container_id)
		WHERE c.id = $1
	`, chunkID).Scan(
		&summary.blockRows,
		&summary.packedRows,
		&summary.existingContainerRows,
		&summary.quarantinedContainerRows,
	)
	if err != nil {
		return fmt.Errorf("query reusable completed chunk %d: %w", chunkID, err)
	}

	if summary.blockRows != 1 || (summary.packedRows != 0 && summary.packedRows != 1) {
		return fmt.Errorf("chunk %d has invalid physical metadata rows: blocks=%d packed=%d", chunkID, summary.blockRows, summary.packedRows)
	}
	if summary.packedRows == 1 {
		validCompanion, err := validateReusableChunkCompanionMappingWithContext(ctx, dbconn, chunkID)
		if err != nil {
			return fmt.Errorf("validate reusable completed chunk %d companion mapping: %w", chunkID, err)
		}
		if !validCompanion {
			return fmt.Errorf("chunk %d has invalid packed/legacy companion mapping", chunkID)
		}
	}
	if summary.existingContainerRows != 1 {
		return fmt.Errorf("chunk %d has missing container metadata", chunkID)
	}
	// Loss-minimizing: allow chunks referencing quarantined containers to pass validation.
	// if summary.quarantinedContainerRows != 0 {
	//     return fmt.Errorf("chunk %d references quarantined container", chunkID)
	// }
	var (
		containerID   int64
		filename      string
		blockOffset   int64
		storedSize    int64
		containerSize int64
		maxSize       int64
	)
	err = dbconn.QueryRowContext(ctx, `
		SELECT
			ctr.id,
			ctr.filename,
			COALESCE(sb.container_offset, b.block_offset),
			COALESCE(sb.stored_size, b.stored_size),
			ctr.current_size,
			ctr.max_size
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		LEFT JOIN container ctr ON ctr.id = COALESCE(sb.container_id, b.container_id)
		WHERE c.id = $1
	`, chunkID).Scan(
		&containerID,
		&filename,
		&blockOffset,
		&storedSize,
		&containerSize,
		&maxSize,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("chunk %d has missing container metadata", chunkID)
		}
		return fmt.Errorf("query reusable completed chunk placement %d: %w", chunkID, err)
	}

	if maxSize > 0 && containerSize > maxSize {
		return fmt.Errorf("chunk %d references container %d with invalid size metadata: current_size=%d max_size=%d", chunkID, containerID, containerSize, maxSize)
	}
	if storedSize <= 0 {
		return fmt.Errorf("chunk %d has invalid stored_size=%d", chunkID, storedSize)
	}
	if blockOffset < int64(container.ContainerHdrLen) {
		return fmt.Errorf("chunk %d has invalid block_offset=%d before container header", chunkID, blockOffset)
	}
	if blockOffset > containerSize-storedSize {
		return fmt.Errorf("chunk %d has out-of-bounds placement in container %d: block_offset=%d stored_size=%d container_size=%d", chunkID, containerID, blockOffset, storedSize, containerSize)
	}
	if policy.scope == reusableValidationSimulationGraphOnly {
		return nil
	}

	fullPath, err := container.SafeContainerPath(policy.containersDir, filename)
	if err != nil {
		return fmt.Errorf("invalid container filename %q: %w", filename, err)
	}
	info, statErr := os.Stat(fullPath)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return fmt.Errorf("chunk %d references missing container file %d (%s)", chunkID, containerID, fullPath)
		}
		return fmt.Errorf("stat container file for reusable chunk %d container %d: %w", chunkID, containerID, statErr)
	}
	if info.Size() < blockOffset+storedSize {
		return fmt.Errorf("chunk %d placement exceeds physical container file bounds for container %d: block_offset=%d stored_size=%d file_size=%d", chunkID, containerID, blockOffset, storedSize, info.Size())
	}

	return nil
}

func validateReusableChunkCompanionMappingWithContext(ctx context.Context, dbconn *sql.DB, chunkID int64) (bool, error) {
	var chunkSize int64
	var codec string
	var formatVersion int64
	var plaintextSize int64
	var storedSize int64
	var nonce []byte
	var legacyContainerID int64
	var legacyOffset int64
	var blockID int64
	var offsetInBlock int64
	var sizeInBlock int64
	var packedContainerID int64
	var packedContainerOffset int64
	var packedPlaintextSize int64
	var totalReferencedBytes int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			c.size,
			b.codec,
			b.format_version,
			b.plaintext_size,
			b.stored_size,
			b.nonce,
			b.container_id,
			b.block_offset,
			r.block_id,
			r.offset_in_block,
			r.size_in_block,
			sb.container_id,
			sb.container_offset,
			sb.plaintext_size,
			(
				SELECT COALESCE(SUM(size_in_block), 0)
				FROM chunk_block_refs
				WHERE block_id = r.block_id
			)
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE c.id = $1
	`, chunkID).Scan(
		&chunkSize,
		&codec,
		&formatVersion,
		&plaintextSize,
		&storedSize,
		&nonce,
		&legacyContainerID,
		&legacyOffset,
		&blockID,
		&offsetInBlock,
		&sizeInBlock,
		&packedContainerID,
		&packedContainerOffset,
		&packedPlaintextSize,
		&totalReferencedBytes,
	); err != nil {
		return false, err
	}

	_ = blockID
	if formatVersion != 1 {
		return false, nil
	}

	payloadPrefixBytes := packedPlaintextSize - totalReferencedBytes
	if payloadPrefixBytes < 0 {
		return false, nil
	}

	switch codec {
	case "plain":
		if plaintextSize != chunkSize || storedSize != chunkSize || sizeInBlock != chunkSize {
			return false, nil
		}
		expectedLegacyOffset := packedContainerOffset + payloadPrefixBytes + offsetInBlock
		return legacyContainerID == packedContainerID && legacyOffset == expectedLegacyOffset, nil
	case "aes-gcm":
		if plaintextSize != chunkSize || storedSize <= 0 || len(nonce) != 12 {
			return false, nil
		}
		return legacyContainerID == packedContainerID && legacyOffset == packedContainerOffset, nil
	default:
		return false, nil
	}
}

func lockAndValidateChunkRebuildCandidatesWithContext(
	ctx context.Context,
	dbconn *sql.DB,
	tx *sql.Tx,
	chunkID int64,
) ([]int64, error) {
	candidateQuery := db.QueryWithOptionalForUpdate(dbconn, `
		SELECT sb.id
		FROM storage_blocks sb
		JOIN chunk_block_refs target ON target.block_id = sb.id
		WHERE target.chunk_id = $1
		ORDER BY sb.id
	`)
	rows, err := tx.QueryContext(ctx, candidateQuery, chunkID)
	if err != nil {
		return nil, fmt.Errorf("query and lock storage_blocks for chunk %d rebuild cleanup: %w", chunkID, err)
	}
	candidateBlockIDs, err := scanLockedBlockIDs(rows, chunkID)
	if err != nil {
		return nil, err
	}

	memberQuery := db.QueryWithOptionalForUpdate(dbconn, `
		SELECT chunk_id
		FROM chunk_block_refs
		WHERE block_id = $1
		ORDER BY chunk_id
	`)
	for _, blockID := range candidateBlockIDs {
		memberRows, err := tx.QueryContext(ctx, memberQuery, blockID)
		if err != nil {
			return nil, fmt.Errorf("query and lock members for storage_block %d during chunk %d rebuild cleanup: %w", blockID, chunkID, err)
		}
		memberCount, err := countLockedBlockMembers(memberRows, blockID, chunkID)
		if err != nil {
			return nil, err
		}
		if memberCount > 1 {
			return nil, fmt.Errorf(
				"cannot rebuild chunk %d independently: packed block %d has %d active members: %w",
				chunkID,
				blockID,
				memberCount,
				errSharedPackedBlockPartialRebuild,
			)
		}
	}
	return candidateBlockIDs, nil
}

func scanLockedBlockIDs(rows *sql.Rows, chunkID int64) ([]int64, error) {
	var blockIDs []int64
	for rows.Next() {
		var blockID int64
		if err := rows.Scan(&blockID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan storage_block id for chunk %d rebuild cleanup: %w", chunkID, err)
		}
		blockIDs = append(blockIDs, blockID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate storage_block ids for chunk %d rebuild cleanup: %w", chunkID, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close storage_block ids rows for chunk %d rebuild cleanup: %w", chunkID, err)
	}
	return blockIDs, nil
}

func countLockedBlockMembers(rows *sql.Rows, blockID, chunkID int64) (int, error) {
	count := 0
	for rows.Next() {
		var memberChunkID int64
		if err := rows.Scan(&memberChunkID); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan member for storage_block %d during chunk %d rebuild cleanup: %w", blockID, chunkID, err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate members for storage_block %d during chunk %d rebuild cleanup: %w", blockID, chunkID, err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close members for storage_block %d during chunk %d rebuild cleanup: %w", blockID, chunkID, err)
	}
	return count, nil
}

func markChunkForRebuildWithContext(ctx context.Context, dbconn *sql.DB, chunkID int64) error {
	state := storeInterleavingStateFromContext(ctx)
	storeOpID := ""
	codec := ""
	chunkHash := ""
	if state != nil {
		storeOpID = state.storeOpID
		codec = state.codec
		chunkHash = state.fileHash
	}
	if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
		StoreOpID: storeOpID,
		ChunkID:   chunkID,
		ChunkHash: chunkHash,
		Codec:     codec,
		Event:     storeInterleavingEventBeforeMarkChunkForRebuild,
	}); err != nil {
		return err
	}
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction while marking chunk %d for rebuild: %w", chunkID, err)
	}
	defer func() { _ = tx.Rollback() }()

	candidateBlockIDs, err := lockAndValidateChunkRebuildCandidatesWithContext(ctx, dbconn, tx, chunkID)
	if err != nil {
		return err
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE chunk SET status = $1 WHERE id = $2 AND status = $3`,
		filestate.ChunkAborted,
		chunkID,
		filestate.ChunkCompleted,
	)
	if err != nil {
		return fmt.Errorf("mark chunk %d for rebuild: %w", chunkID, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected while marking chunk %d for rebuild: %w", chunkID, err)
	}
	if n == 0 {
		// Another worker already demoted this chunk; nothing to clean up here.
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM chunk_block_refs WHERE chunk_id = $1`, chunkID); err != nil {
		return fmt.Errorf("delete stale chunk_block_refs while marking chunk %d for rebuild: %w", chunkID, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM blocks WHERE chunk_id = $1`, chunkID); err != nil {
		return fmt.Errorf("delete stale blocks while marking chunk %d for rebuild: %w", chunkID, err)
	}
	for _, blockID := range candidateBlockIDs {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM storage_blocks
			 WHERE id = $1
			   AND NOT EXISTS (SELECT 1 FROM chunk_block_refs WHERE block_id = $1)`,
			blockID,
		); err != nil {
			return fmt.Errorf("delete orphan storage_block %d while marking chunk %d for rebuild: %w", blockID, chunkID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit while marking chunk %d for rebuild: %w", chunkID, err)
	}
	if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
		StoreOpID: storeOpID,
		ChunkID:   chunkID,
		ChunkHash: chunkHash,
		Codec:     codec,
		Event:     storeInterleavingEventAfterMarkChunkForRebuild,
	}); err != nil {
		return err
	}
	return nil
}

func validateReusableLogicalFileGraphWithContext(ctx context.Context, dbconn *sql.DB, fileID int64, containersDir string) error {
	return validateReusableLogicalFileGraphWithPolicy(ctx, dbconn, fileID, reusableValidationPolicy{
		scope:         reusableValidationFullRepository,
		containersDir: containersDir,
	})
}

func validateReusableLogicalFileGraphWithPolicy(ctx context.Context, dbconn *sql.DB, fileID int64, policy reusableValidationPolicy) error {
	if err := policy.validate(); err != nil {
		return err
	}

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
				COALESCE(SUM(CASE WHEN b.id IS NULL AND r.block_id IS NULL THEN 1 ELSE 0 END), 0) AS missing_blocks,
				COALESCE(SUM(CASE WHEN ctr.id IS NULL THEN 1 ELSE 0 END), 0) AS invalid_containers
			FROM ordered_chunks oc
			LEFT JOIN chunk c ON c.id = oc.chunk_id
			LEFT JOIN blocks b ON b.chunk_id = oc.chunk_id
			LEFT JOIN chunk_block_refs r ON r.chunk_id = oc.chunk_id
			LEFT JOIN storage_blocks sb ON sb.id = r.block_id
			LEFT JOIN container ctr ON ctr.id = COALESCE(b.container_id, sb.container_id)
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
		return fmt.Errorf("logical file %d references missing container metadata", fileID)
	}
	if policy.scope == reusableValidationSimulationGraphOnly {
		return validateReusableLogicalFileChunksWithPolicy(ctx, dbconn, fileID, policy)
	}
	// Only treat as invalid if the container is missing, not merely quarantined.
	// For v1.0, allow logical files referencing quarantined containers to be restored.
	// Optionally, log a warning or set a degraded status here.
	// log.Printf("warning: logical file %d references quarantined container(s)", fileID)
	// (If you want to distinguish missing vs quarantined, you can refine the SQL above.)

	rows, err := dbconn.QueryContext(ctx, `
		       SELECT DISTINCT ctr.id, ctr.filename
		       FROM file_chunk fc
		       JOIN chunk c ON c.id = fc.chunk_id
		       LEFT JOIN blocks b ON b.chunk_id = c.id
		       LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		       LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		       JOIN container ctr ON ctr.id = COALESCE(b.container_id, sb.container_id)
		       WHERE fc.logical_file_id = $1
	       `, fileID)
	if err != nil {
		return fmt.Errorf("query reusable logical file containers %d: %w", fileID, err)
	}
	defer func() { _ = rows.Close() }()

	foundAny := false
	missingCount := 0
	for rows.Next() {
		var containerID int64
		var filename string
		if err := rows.Scan(&containerID, &filename); err != nil {
			return fmt.Errorf("scan reusable logical file container for file %d: %w", fileID, err)
		}
		fullPath, err := container.SafeContainerPath(policy.containersDir, filename)
		if err != nil {
			return fmt.Errorf("invalid container filename %q: %w", filename, err)
		}
		if _, err := os.Stat(fullPath); err != nil {
			if os.IsNotExist(err) {
				log.Printf("warning: logical file %d references missing container file %d (%s)", fileID, containerID, fullPath)
				missingCount++
				continue
			}
			return fmt.Errorf("stat container file for logical file %d container %d: %w", fileID, containerID, err)
		}
		foundAny = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate reusable logical file containers %d: %w", fileID, err)
	}
	if !foundAny {
		return fmt.Errorf("logical file %d: all referenced containers are missing/quarantined", fileID)
	}
	if missingCount > 0 {
		log.Printf("warning: logical file %d: %d referenced containers are missing/quarantined", fileID, missingCount)
	}
	return nil
}

func validateReusableLogicalFileChunksWithPolicy(ctx context.Context, dbconn *sql.DB, fileID int64, policy reusableValidationPolicy) error {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT chunk_id
		FROM file_chunk
		WHERE logical_file_id = $1
		ORDER BY chunk_order
	`, fileID)
	if err != nil {
		return fmt.Errorf("query reusable logical file chunks %d: %w", fileID, err)
	}

	var chunkIDs []int64
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan reusable logical file chunk for file %d: %w", fileID, err)
		}
		chunkIDs = append(chunkIDs, chunkID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate reusable logical file chunks %d: %w", fileID, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close reusable logical file chunks %d: %w", fileID, err)
	}

	for _, chunkID := range chunkIDs {
		if err := validateReusableCompletedChunkWithPolicy(ctx, dbconn, chunkID, policy); err != nil {
			return fmt.Errorf("logical file %d has invalid reusable chunk %d: %w", fileID, chunkID, err)
		}
	}
	return nil
}

func loadReuseSemanticValidationModeFromEnv() (reuseSemanticValidationMode, error) {
	const envMode = "COLDKEEP_REUSE_SEMANTIC_VALIDATION"
	rawValue, isSet := os.LookupEnv(envMode)
	if !isSet {
		return reuseSemanticValidationSuspicious, nil
	}

	modeValue := strings.ToLower(strings.TrimSpace(rawValue))
	switch modeValue {
	case "":
		return "", fmt.Errorf("%s must not be empty", envMode)
	case string(reuseSemanticValidationSuspicious):
		return reuseSemanticValidationSuspicious, nil
	case string(reuseSemanticValidationOff):
		return reuseSemanticValidationOff, nil
	case string(reuseSemanticValidationAlways):
		return reuseSemanticValidationAlways, nil
	default:
		return "", fmt.Errorf("%s has invalid value %q (accepted: off, suspicious, always)", envMode, rawValue)
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

	restoreService := &RestoreService{
		ChunkResolver: NewDualCompatChunkResolver(dbconn),
		BlockReader:   NewStorageBlockReader(dbconn, containersDir),
	}

	transformerCache := make(map[blocks.Codec]blocks.Transformer)
	var expectedOrder int64

	for _, chunkRow := range chunkRows {
		if chunkRow.chunkOrder != expectedOrder {
			return fmt.Errorf("semantic reuse chunk order discontinuity for file %d: expected %d got %d", fileID, expectedOrder, chunkRow.chunkOrder)
		}
		expectedOrder++

		seg, segErr := restoreService.ResolveChunkLocation(ctx, chunkRow.chunkID)
		if segErr != nil {
			return fmt.Errorf("resolve chunk for semantic validation file=%d chunk_id=%d: %w", fileID, chunkRow.chunkID, segErr)
		}

		var plaintext []byte
		if seg != nil && seg.BlockID > 0 {
			plaintext, err = restoreService.ReadChunkFromBlock(ctx, seg.BlockID, seg.Offset, seg.Size)
			if err != nil {
				return fmt.Errorf("read packed chunk for semantic validation file=%d chunk_id=%d: %w", fileID, chunkRow.chunkID, err)
			}
		} else {
			if containerfilename != chunkRow.filename {
				if filecontainer != nil {
					if closeErr := filecontainer.Close(); closeErr != nil {
						return fmt.Errorf("close container %q during semantic validation: %w", containerfilename, closeErr)
					}
					filecontainer = nil
				}

				containerPath, err := container.SafeContainerPath(containersDir, chunkRow.filename)
				if err != nil {
					return fmt.Errorf("open container %q during semantic validation: %w", chunkRow.filename, err)
				}
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

			plaintext, err = transformer.Decode(ctx, blocks.DecodeInput{
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
	return validateReusableLogicalFileForStoreWithPolicy(ctx, dbconn, fileID, reusableValidationPolicy{
		scope:         reusableValidationFullRepository,
		containersDir: containersDir,
	})
}

func validateReusableLogicalFileForStoreWithPolicy(ctx context.Context, dbconn *sql.DB, fileID int64, policy reusableValidationPolicy) error {
	if err := validateReusableLogicalFileGraphWithPolicy(ctx, dbconn, fileID, policy); err != nil {
		return err
	}
	if policy.scope == reusableValidationSimulationGraphOnly {
		return nil
	}

	// Check if any referenced container is quarantined; if so, skip semantic validation.
	var quarantinedCount int
	err := dbconn.QueryRowContext(ctx, `
		       SELECT COUNT(DISTINCT ctr.id)
		       FROM file_chunk fc
		       JOIN blocks b ON b.chunk_id = fc.chunk_id
		       JOIN container ctr ON ctr.id = b.container_id
		       WHERE fc.logical_file_id = $1 AND ctr.quarantine = TRUE
	       `, fileID).Scan(&quarantinedCount)
	if err != nil {
		return fmt.Errorf("check quarantined containers for logical file %d: %w", fileID, err)
	}
	if quarantinedCount > 0 {
		// Loss-minimizing: skip semantic validation for logical files referencing quarantined containers.
		return nil
	}

	mode, err := loadReuseSemanticValidationModeFromEnv()
	if err != nil {
		return fmt.Errorf("parse COLDKEEP_REUSE_SEMANTIC_VALIDATION: %w", err)
	}
	runSemanticValidation, reason, err := shouldRunSemanticReuseValidationWithContext(ctx, dbconn, fileID, mode)
	if err != nil {
		return err
	}
	if !runSemanticValidation {
		return nil
	}

	if err := validateReusableLogicalFileSemanticsWithContext(ctx, dbconn, fileID, policy.containersDir); err != nil {
		return fmt.Errorf("semantic reuse validation failed (%s): %w", reason, err)
	}

	return nil
}

func markLogicalFileForRebuildWithPolicyWithContext(ctx context.Context, dbconn *sql.DB, fileID int64, markChunksSuspicious bool) error {
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
		var currentStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&currentStatus); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("logical file %d does not exist", fileID)
			}
			return fmt.Errorf("read logical file %d status during rebuild reset: %w", fileID, err)
		}
		if currentStatus != filestate.LogicalFileAborted {
			// Another goroutine already transitioned this row; nothing left to do.
			txclosed = true
			return tx.Rollback()
		}
	}

	if err := cleanupLogicalFileChunkMappingsWithContext(ctx, tx, fileID, markChunksSuspicious); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit logical file %d rebuild reset: %w", fileID, err)
	}
	txclosed = true
	return nil
}

func markLogicalFileForRebuildWithContext(ctx context.Context, dbconn *sql.DB, fileID int64) error {
	return markLogicalFileForRebuildWithPolicyWithContext(ctx, dbconn, fileID, false)
}

func markLogicalFileForReuseValidationFailureWithContext(ctx context.Context, dbconn *sql.DB, fileID int64) error {
	return markLogicalFileForRebuildWithPolicyWithContext(ctx, dbconn, fileID, true)
}

func assertLogicalFileVersionMatchesActive(insertedLogicalFileVersion string, activeVersion string) error {
	insertedTrimmed := strings.TrimSpace(insertedLogicalFileVersion)
	activeTrimmed := strings.TrimSpace(activeVersion)
	if insertedTrimmed != activeTrimmed {
		return fmt.Errorf("logical_file chunker_version mismatch: inserted=%q active=%q", insertedLogicalFileVersion, activeVersion)
	}
	return nil
}

func assertChunkVersionMatchesActive(insertedChunkVersion string, activeVersion string) error {
	insertedTrimmed := strings.TrimSpace(insertedChunkVersion)
	activeTrimmed := strings.TrimSpace(activeVersion)
	if insertedTrimmed != activeTrimmed {
		return fmt.Errorf("chunk chunker_version mismatch: inserted=%q active=%q", insertedChunkVersion, activeVersion)
	}
	return nil
}

// -----------------------------------------------------------------------------
// CLAIM-BASED CONCURRENCY CONTROL FOR LOGICAL FILES AND CHUNKS
// -----------------------------------------------------------------------------

func claimLogicalFileWithContext(ctx context.Context, dbconn *sql.DB, fileinfo os.FileInfo, fileHash string, activeVersion string, containersDir string) (fileID int64, filestatus string, err error) {
	return claimLogicalFileWithValidationPolicy(ctx, dbconn, fileinfo, fileHash, activeVersion, reusableValidationPolicy{
		scope:         reusableValidationFullRepository,
		containersDir: containersDir,
	})
}

func claimLogicalFileWithValidationPolicy(ctx context.Context, dbconn *sql.DB, fileinfo os.FileInfo, fileHash string, activeVersion string, policy reusableValidationPolicy) (fileID int64, filestatus string, err error) {
	if err := policy.validate(); err != nil {
		return 0, "", err
	}
	if strings.TrimSpace(activeVersion) == "" {
		return 0, "", fmt.Errorf("logical_file.chunker_version must not be empty")
	}

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

	var insertedLogicalFileVersion string
	insErr := tx.QueryRowContext(
		ctx,
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ($1, $2, $3, $4, 0, $5)
		ON CONFLICT (file_hash, total_size) DO NOTHING
		RETURNING id, chunker_version`,
		fileinfo.Name(),
		fileinfo.Size(),
		fileHash,
		filestate.LogicalFileProcessing,
		activeVersion,
	).Scan(&fileID, &insertedLogicalFileVersion)

	switch insErr {
	case sql.ErrNoRows:
		// Conflict happened: someone else already stored this file hash
		var existingID int64
		var existingChunkerVersion string
		if err := tx.QueryRowContext(
			ctx,
			`SELECT id, status, chunker_version FROM logical_file WHERE file_hash = $1 and total_size = $2`,
			fileHash,
			fileinfo.Size(),
		).Scan(&existingID, &filestatus, &existingChunkerVersion); err != nil {
			return 0, "", err
		}
		if strings.TrimSpace(existingChunkerVersion) == "" {
			return 0, "", fmt.Errorf("logical_file %d has empty chunker_version (repository corruption or incomplete migration)", existingID)
		}

		switch filestatus {
		case filestate.LogicalFileCompleted:
			// File is marked COMPLETED. Reuse is only safe if the full reusable graph
			// is still structurally valid. Do not use a partial chunk-status shortcut:
			// a corrupted completed file can have zero file_chunk rows and still look
			// "clean" under the old inconsistentChunks query.
			_ = tx.Rollback() // Don't hold locks while validating
			txclosed = true

			reuseErr := validateReusableLogicalFileGraphWithPolicy(ctx, dbconn, existingID, policy)
			if reuseErr == nil {
				return existingID, filestatus, nil
			}

			log.Printf("event=store_reuse_claim_graph_invalid file_id=%d error=%v", existingID, reuseErr)

			// Completed row exists but its reusable graph is broken; treat it like an
			// aborted prior attempt so the caller can rebuild on the same logical_file row.
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
			if !txclosed {
				_ = tx.Rollback()
				txclosed = true
			}
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

				if rowsAffected == 1 {
					if cleanupErr := cleanupLogicalFileChunkMappingsWithContext(ctx, tx2, existingID, false); cleanupErr != nil {
						_ = tx2.Rollback()
						return 0, "", cleanupErr
					}
					if commitErr := tx2.Commit(); commitErr != nil {
						_ = tx2.Rollback()
						return 0, "", commitErr
					}
					fileID = existingID
					filestatus = filestate.LogicalFileProcessing
					break
				}

				if rollbackErr := tx2.Rollback(); rollbackErr != nil {
					return 0, "", rollbackErr
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
		// Invariant: the logical file recipe owner version must match the
		// resolved chunker version chosen for this store operation.
		if err := assertLogicalFileVersionMatchesActive(insertedLogicalFileVersion, activeVersion); err != nil {
			return 0, "", err
		}
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

func claimChunk(dbconn *sql.DB, chunkHash string, chunksize int64, activeVersion string) (chunkID int64, chunkstatus string, isNew bool, err error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return claimChunkWithContext(ctx, dbconn, chunkHash, chunksize, activeVersion, container.ContainersDir)
}

func prepareLogicalFileForStoreWithValidationPolicy(ctx context.Context, dbconn *sql.DB, fileinfo os.FileInfo, fileHash string, activeVersion string, policy reusableValidationPolicy) (fileID int64, filestatus string, err error) {
	if err := policy.validate(); err != nil {
		return 0, "", err
	}

	// Reuse acceptance is intentionally two-phase:
	//  1) claim by content identity (file_hash + size), then
	//  2) validate graph/semantic replay safety for COMPLETED candidates.
	// If validation fails, we mark the candidate ABORTED, clean stale mappings,
	// and claim again so the caller rebuilds a fresh canonical recipe.
	//
	// This is deliberate safety behavior, not opportunistic best-effort reuse.
	fileID, filestatus, err = claimLogicalFileWithValidationPolicy(ctx, dbconn, fileinfo, fileHash, activeVersion, policy)
	if err != nil {
		return 0, "", err
	}

	if filestatus != filestate.LogicalFileCompleted {
		return fileID, filestatus, nil
	}

	reuseErr := validateReusableLogicalFileForStoreWithPolicy(ctx, dbconn, fileID, policy)
	if reuseErr == nil {
		return fileID, filestatus, nil
	}

	log.Printf("event=store_reuse_validation_failed file_id=%d error=%v", fileID, reuseErr)
	if err := markLogicalFileForReuseValidationFailureWithContext(ctx, dbconn, fileID); err != nil {
		return 0, "", errors.Join(reuseErr, err)
	}

	fileID, filestatus, err = claimLogicalFileWithValidationPolicy(ctx, dbconn, fileinfo, fileHash, activeVersion, policy)
	if err != nil {
		return 0, "", err
	}
	if filestatus != filestate.LogicalFileCompleted {
		return fileID, filestatus, nil
	}

	reuseErr = validateReusableLogicalFileForStoreWithPolicy(ctx, dbconn, fileID, policy)
	if reuseErr != nil {
		return 0, "", fmt.Errorf("logical file %d remained non-reusable after retry: %w", fileID, reuseErr)
	}

	return fileID, filestatus, nil
}

func claimChunkWithContext(ctx context.Context, dbconn *sql.DB, chunkHash string, chunksize int64, activeVersion string, containersDir string) (chunkID int64, chunkstatus string, isNew bool, err error) {
	return claimChunkWithValidationPolicy(ctx, dbconn, chunkHash, chunksize, activeVersion, reusableValidationPolicy{
		scope:         reusableValidationFullRepository,
		containersDir: containersDir,
	})
}

func claimChunkWithValidationPolicy(ctx context.Context, dbconn *sql.DB, chunkHash string, chunksize int64, activeVersion string, policy reusableValidationPolicy) (chunkID int64, chunkstatus string, isNew bool, err error) {
	if err := policy.validate(); err != nil {
		return 0, "", false, err
	}
	if strings.TrimSpace(activeVersion) == "" {
		return 0, "", false, fmt.Errorf("chunk.chunker_version must not be empty")
	}

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
	var insertedChunkVersion string
	insErr := tx.QueryRowContext(
		ctx,
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
				VALUES ($1, $2, $3, 0, $4)
				ON CONFLICT (chunk_hash, size) DO NOTHING
				RETURNING id, chunker_version`,
		chunkHash,
		chunksize,
		filestate.ChunkProcessing,
		activeVersion,
	).Scan(&chunkID, &insertedChunkVersion)

	switch insErr {
	case nil:
		// We won: this chunk is new
		if err := assertChunkVersionMatchesActive(insertedChunkVersion, activeVersion); err != nil {
			return 0, "", false, err
		}
		chunkstatus = filestate.ChunkProcessing
		isNew = true
	case sql.ErrNoRows:
		// Someone else inserted a content-identical chunk first.
		// Keep dedup identity keyed only by hash+size.
		// chunk.chunker_version is origin metadata for the existing chunk row,
		// not a compatibility gate for reuse by a later logical file recipe.
		var existingChunkerVersion string
		if err := tx.QueryRowContext(ctx, `SELECT id, status, chunker_version FROM chunk WHERE chunk_hash = $1 AND size = $2`, chunkHash, chunksize).Scan(&chunkID, &chunkstatus, &existingChunkerVersion); err != nil {
			return 0, "", false, err
		}
		if strings.TrimSpace(existingChunkerVersion) == "" {
			return 0, "", false, fmt.Errorf("chunk %d has empty chunker_version (repository corruption or incomplete migration)", chunkID)
		}
		switch chunkstatus {
		case filestate.ChunkCompleted:
			// Chunk is marked COMPLETED; verify its block/container metadata before reuse.
			_ = tx.Rollback() // Don't hold locks while waiting
			txclosed = true

			reuseErr := validateReusableCompletedChunkWithPolicy(ctx, dbconn, chunkID, policy)
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
				reuseErr := validateReusableCompletedChunkWithPolicy(ctx, dbconn, chunkID, policy)
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
				state := storeInterleavingStateFromContext(ctx)
				storeOpID := ""
				codec := ""
				if state != nil {
					storeOpID = state.storeOpID
					codec = state.codec
				}
				if err := fireStoreInterleavingHook(ctx, storeInterleavingHookEvent{
					StoreOpID: storeOpID,
					ChunkID:   chunkID,
					ChunkHash: chunkHash,
					Codec:     codec,
					Event:     storeInterleavingEventBeforeChunkRetryCAS,
					TxAttempt: casAttempt,
				}); err != nil {
					return 0, chunkstatus, false, err
				}
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
					reuseErr := validateReusableCompletedChunkWithPolicy(ctx, dbconn, chunkID, policy)
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
						reuseErr := validateReusableCompletedChunkWithPolicy(ctx, dbconn, chunkID, policy)
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
		result, err := tx.ExecContext(ctx, `UPDATE chunk SET live_ref_count = live_ref_count + 1 WHERE id = $1`, chunkID)
		if err != nil {
			return err
		}
		if err := db.RequireExactlyOneRow(result, "increment linked chunk live refcount"); err != nil {
			return err
		}
	}

	return nil
}

// finalizeLogicalFileStorageWithContext atomically verifies that all chunks are linked
// and marks the logical file as COMPLETED in a single transaction. This ensures that
// if verification fails, the file remains in PROCESSING state and no partial completion
// leaks out. If this transaction fails, corrective recovery/cleanup will mark the file ABORTED,
// maintaining semantic consistency: either all chunks are linked AND file is complete,
// or file is in PROCESSING/ABORTED (never a state where chunks exist but file isn't marked).
func finalizeLogicalFileStorageWithContext(ctx context.Context, dbconn *sql.DB, fileID int64, expectedChunkCount int) error {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Verify all chunks are linked (chunk_order must be 0..expectedChunkCount-1 contiguous).
	// If any are missing, this transaction fails and file stays PROCESSING for later recovery.
	var linkedCount int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`,
		fileID,
	).Scan(&linkedCount); err != nil {
		return fmt.Errorf("count file chunks: %w", err)
	}
	if linkedCount != expectedChunkCount {
		return fmt.Errorf("finalize: file %d has %d linked chunks, expected %d (incomplete store or race)", fileID, linkedCount, expectedChunkCount)
	}

	// Verify chunk_order contiguity (0, 1, 2,... n-1 with no gaps).
	var maxOrder int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(chunk_order), -1) FROM file_chunk WHERE logical_file_id = $1`,
		fileID,
	).Scan(&maxOrder); err != nil {
		return fmt.Errorf("check chunk_order max: %w", err)
	}
	if maxOrder != expectedChunkCount-1 {
		return fmt.Errorf("finalize: file %d chunk_order max is %d, expected %d (non-contiguous linking)", fileID, maxOrder, expectedChunkCount-1)
	}

	// All verification passed; mark file complete in the same transaction.
	result, err := tx.ExecContext(
		ctx,
		`UPDATE logical_file SET status = $1 WHERE id = $2`,
		filestate.LogicalFileCompleted,
		fileID,
	)
	if err != nil {
		return fmt.Errorf("update logical_file to COMPLETED: %w", err)
	}
	if err := db.RequireExactlyOneRow(result, "finalize logical file storage"); err != nil {
		return fmt.Errorf("update logical_file to COMPLETED: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize transaction: %w", err)
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
	return StoreFileWithStorageContextResultContext(context.Background(), sgctx, path)
}

// StoreFileWithStorageContextResultContext is the caller-context-aware form of
// StoreFileWithStorageContextResult. Ordinary store work is owned by ctx.
func StoreFileWithStorageContextResultContext(ctx context.Context, sgctx StorageContext, path string) (StoreFileResult, error) {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return StoreFileResult{}, err
	}

	return StoreFileWithStorageContextAndCodecResultContext(ctx, sgctx, path, codec)
}

func StoreFileWithStorageContextAndCodec(sgctx StorageContext, path string, codec blocks.Codec) (err error) {
	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	return err
}

// StoreFileWithStorageContextAndCodecResult stores one file and returns
// metadata suitable for CLI text and JSON output.
func StoreFileWithStorageContextAndCodecResult(sgctx StorageContext, path string, codec blocks.Codec) (result StoreFileResult, err error) {
	return StoreFileWithStorageContextAndCodecResultContext(context.Background(), sgctx, path, codec)
}

// StoreFileWithStorageContextAndCodecResultContext stores one file while
// preserving caller cancellation through all ordinary work.
func StoreFileWithStorageContextAndCodecResultContext(ctx context.Context, sgctx StorageContext, path string, codec blocks.Codec) (result StoreFileResult, err error) {
	return StoreFileWithStorageContextAndCodecResultWithPolicyContext(ctx, sgctx, path, codec, true)
}

// StoreFileWithStorageContextAndCodecResultWithPolicy stores one file and returns
// metadata suitable for CLI text and JSON output, applying the given path-conflict policy.
// When replace is false, existing path mapped to different logical content fails.
// When replace is true, existing path mapping is atomically retargeted.
func StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx StorageContext, path string, codec blocks.Codec, replace bool) (result StoreFileResult, err error) {
	return StoreFileWithStorageContextAndCodecResultWithPolicyContext(context.Background(), sgctx, path, codec, replace)
}

// StoreFileWithStorageContextAndCodecResultWithPolicyContext is the
// caller-context-aware form of StoreFileWithStorageContextAndCodecResultWithPolicy.
func StoreFileWithStorageContextAndCodecResultWithPolicyContext(ctx context.Context, sgctx StorageContext, path string, codec blocks.Codec, replace bool) (result StoreFileResult, err error) {
	defer func() {
		result, err = finalizeSingleFileStore(result, err, sgctx.Writer)
	}()

	runtime, err := buildStoreFileRuntime(sgctx, codec)
	if err != nil {
		return StoreFileResult{}, err
	}
	return storeFileWithStorageContextAndRuntimeResultWithPolicy(ctx, sgctx, path, replace, nil, runtime)
}

func finalizeSingleFileStore(result StoreFileResult, storeErr error, writer container.ContainerWriter) (StoreFileResult, error) {
	if writer == nil {
		return result, storeErr
	}
	finalizeErr := writer.FinalizeContainer()
	if finalizeErr == nil {
		return result, storeErr
	}
	if storeErr == nil {
		return result, finalizeErr
	}
	return result, errors.Join(storeErr, finalizeErr)
}

func storeFileWithStorageContextAndRuntimeResultWithPolicy(
	ctx context.Context,
	sgctx StorageContext,
	path string,
	replace bool,
	knownFileInfo os.FileInfo,
	runtime *storeFileRuntime,
) (result StoreFileResult, err error) {
	normalizedPath, err := normalizePhysicalFilePath(path)
	if err != nil {
		return StoreFileResult{}, err
	}
	result.Path = normalizedPath
	ctx, cancel := db.NewOperationContext(ctx)
	defer cancel()

	if runtime == nil {
		return StoreFileResult{}, fmt.Errorf("store runtime must not be nil")
	}

	fileinfo := knownFileInfo
	if fileinfo == nil {
		fileinfo, err = os.Stat(path)
		if err != nil {
			return StoreFileResult{}, err
		}
	}
	reuseValidation := runtime.reuseValidation

	// Current store flow pattern:
	//  1. resolve one active chunker for the whole operation
	//  2. capture one active version from that chunker
	//  3. claim/create the logical file recipe owner with that version
	//  4. chunk the file with the resolved chunker
	//  5. for each chunk: hash, reuse-or-create chunk row with activeVersion when new,
	//     then link ordered file_chunk membership
	//  6. finalize logical-file state and attach physical-file metadata
	// The logical_file claim happens before ChunkFile() so the store path preserves
	// its existing concurrency, duplicate-detection, and recovery semantics, but the
	// version source is the same resolved chunker used to produce the chunk list.
	storeService := runtime.storeService
	dbconn := storeService.Repository().DB()
	activeChunker, err := storeService.ResolveActiveChunker()
	if err != nil {
		return StoreFileResult{}, err
	}
	effectiveChunker := activeChunker.Chunker
	activeVersion := activeChunker.Version
	if strings.TrimSpace(string(activeVersion)) == "" {
		return StoreFileResult{}, fmt.Errorf("resolved active chunker version must not be empty")
	}
	activeVersionString := string(activeVersion)
	// Phase 5 single-pass prepare: chunk + hash + metadata materialization first,
	// then claim and commit sequentially.
	prepared, err := prepareFileForStoreWithContext(ctx, path, effectiveChunker, activeVersionString, fileinfo)
	if err != nil {
		return StoreFileResult{}, err
	}
	fileHash := prepared.LogicalHash
	if runtime.interleavingHooks != nil {
		if runtime.interleavingSeq == nil {
			runtime.interleavingSeq = &atomic.Uint64{}
		}
		ctx = withStoreInterleavingState(ctx, &storeInterleavingState{
			hooks:     runtime.interleavingHooks,
			storeOpID: fmt.Sprintf("store-op-%d", runtime.interleavingSeq.Add(1)),
			codec:     string(runtime.codec),
			fileHash:  fileHash,
		})
	}
	result.FileHash = fileHash

	// Try to claim logical file for this hash (concurrency-safe)
	fileID, filestatus, err := prepareLogicalFileForStoreWithValidationPolicy(ctx, dbconn, fileinfo, fileHash, activeVersionString, reuseValidation)
	if err != nil {
		return StoreFileResult{}, err
	}
	result.FileID = fileID

	if filestatus == filestate.LogicalFileCompleted {
		tx, err := dbconn.BeginTx(ctx, nil)
		if err != nil {
			return StoreFileResult{}, err
		}
		if _, err := ensurePhysicalFileForPathWithPolicyWithTx(ctx, dbconn, tx, normalizedPath, fileID, prepared.PhysicalMetadata, replace, recipeLivenessActivateOnFirstMapping); err != nil {
			_ = tx.Rollback()
			return StoreFileResult{}, err
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return StoreFileResult{}, err
		}
		result.AlreadyStored = true
		return result, nil
	}

	completed := false
	defer func() {
		if !completed {
			cleanupCtx, cleanupCancel := db.NewOperationContext(context.Background())
			defer cleanupCancel()
			if _, execErr := dbconn.ExecContext(
				cleanupCtx,
				`UPDATE logical_file SET status = $1 WHERE id = $2`,
				filestate.LogicalFileAborted,
				fileID,
			); execErr != nil {
				log.Printf("event=store_cleanup action=mark_aborted file_id=%d error=%v", fileID, execErr)
				err = errors.Join(err, fmt.Errorf("mark logical file %d aborted: %w", fileID, execErr))
				result = StoreFileResult{}
			}
		}
	}()

	writer, ok := sgctx.Writer.(payloadStatefulWriter)
	if !ok {
		return StoreFileResult{}, fmt.Errorf("StoreFileWithStorageContextAndCodec requires writer with AppendPayload/FinalizeContainer, got %T", sgctx.Writer)
	}
	bindWriterDB(writer, sgctx.DB)
	// Writer finalization is owned by call boundaries (wrappers/CLI/context close),
	// not by this low-level result function.

	commitResult, err := commitPreparedFileWithContext(
		ctx,
		dbconn,
		writer,
		runtime.blockRepo,
		runtime.transformer,
		runtime.compression,
		sgctx,
		prepared,
		fileID,
		normalizedPath,
		prepared.PhysicalMetadata,
		reuseValidation,
		replace,
	)
	if err != nil {
		return StoreFileResult{}, err
	}

	// Mark the operation as completed to avoid aborting it in the deferred function
	completed = true

	return commitResult, nil
}

// -----------------------------------------------------------------------------
// STORE FOLDER FUNCTION (RECURSIVE)
// -----------------------------------------------------------------------------

func StoreFolder(root string) error {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}
	opts, err := execution.FromEnv(execution.DefaultOptions())
	if err != nil {
		return err
	}

	sgctx, err := LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load default storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	return StoreFolderWithStorageContextAndCodecAndOptions(sgctx, root, codec, opts)
}

func StoreFolderWithCodec(root string, codecName string) error {
	codec, err := blocks.ParseCodec(codecName)
	if err != nil {
		return err
	}
	opts, err := execution.FromEnv(execution.DefaultOptions())
	if err != nil {
		return err
	}

	sgctx, err := LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load default storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	return StoreFolderWithStorageContextAndCodecAndOptions(sgctx, root, codec, opts)
}

func StoreFolderWithStorageContext(sgctx StorageContext, root string) error {
	opts, err := execution.FromEnv(execution.DefaultOptions())
	if err != nil {
		return err
	}
	return StoreFolderWithStorageContextAndOptions(sgctx, root, opts)
}

func StoreFolderWithStorageContextAndOptions(sgctx StorageContext, root string, opts execution.Options) error {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}
	return StoreFolderWithStorageContextAndCodecAndOptions(sgctx, root, codec, opts)
}

func StoreFolderWithStorageContextAndCodec(sgctx StorageContext, root string, codec blocks.Codec) error {
	opts, err := execution.FromEnv(execution.DefaultOptions())
	if err != nil {
		return err
	}
	return StoreFolderWithStorageContextAndCodecAndOptions(sgctx, root, codec, opts)
}

type FileJob struct {
	Index int
	Path  string
}

type WorkerStats struct {
	Files int
	Bytes int64
}

func determineStoreFolderWorkerCount(writer container.ContainerWriter, requested int) (int, error) {
	if writer == nil {
		return 0, fmt.Errorf("store folder requires non-nil writer")
	}

	switch writer.(type) {
	case *container.LocalWriter:
		// Option A: LocalWriter is cloned per worker in processFile, so concurrent
		// workers never mutate the same active-container/rollback state.
		return requested, nil
	case *container.SimulatedWriter:
		// Option B: keep one worker for simulated mode. SimulatedWriter has an
		// internal mutex, and single-worker mode preserves stable baseline behavior.
		return 1, nil
	default:
		return 0, fmt.Errorf("writer type %T does not support isolated concurrent workers", writer)
	}
}

func StoreFolderWithStorageContextAndCodecAndOptions(sgctx StorageContext, root string, codec blocks.Codec, opts execution.Options) error {
	_, err := StoreFolderWithStorageContextAndCodecAndOptionsWithStats(sgctx, root, codec, opts)
	return err
}

func StoreFolderWithStorageContextAndCodecAndOptionsWithStats(sgctx StorageContext, root string, codec blocks.Codec, opts execution.Options) (execution.ExecutionStats, error) {
	return StoreFolderWithStorageContextAndCodecAndOptionsWithStatsContext(context.Background(), sgctx, root, codec, opts)
}

func StoreFolderWithStorageContextAndCodecAndOptionsWithStatsContext(ctx context.Context, sgctx StorageContext, root string, codec blocks.Codec, opts execution.Options) (execution.ExecutionStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return execution.ExecutionStats{}, err
	}
	// Default to a single worker for deterministic append ordering and safer
	// container mutation semantics under mixed file sizes.
	err := opts.Validate()
	if err != nil {
		return execution.ExecutionStats{}, err
	}
	// Phase 2 guardrail: execution policy exists, but we intentionally do not
	// enable staged pipelines yet. Keep store-folder semantics equivalent to
	// the v1.6/v1.7 baseline while workers only control file-level fan-out.
	if opts.PipelineDepth != 1 {
		return execution.ExecutionStats{}, fmt.Errorf("pipeline depth must be 1 in v1.7 phase 2")
	}
	workerCount, err := determineStoreFolderWorkerCount(sgctx.Writer, opts.StoreFolderWorkers)
	if err != nil {
		return execution.ExecutionStats{}, err
	}

	runtime, err := buildStoreFileRuntime(sgctx, codec)
	if err != nil {
		return execution.ExecutionStats{}, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobCh := make(chan FileJob, 256)
	errCh := make(chan error, 1)
	statsCh := make(chan WorkerStats, workerCount)
	paths, err := discoverFiles(root)
	if err != nil {
		return execution.ExecutionStats{}, err
	}
	jobs := buildFileJobs(paths)

	var wg sync.WaitGroup
	processFile := func(workerCtx *StorageContext, job FileJob) (int64, error) {
		info, statErr := os.Stat(job.Path)
		if statErr != nil {
			return 0, statErr
		}
		// Per-worker isolation boundary:
		// - one writer instance is owned by one worker and reused across files
		// - retryable transaction-abort retries rebuild that worker writer before retry
		// - per-file execution still uses the worker-local storage context copy
		// - shared *sql.DB pool only (driver handles concurrent tx safety)
		// - no nested per-file parallelism in this phase; job execution stays
		//   single-threaded so ordering and writer/tx semantics remain explicit
		// StoreFileWithStorageContextAndCodecResult performs open/chunk/hash/claim/
		// append/metadata under its transaction-safe internal flow.
		err := runWithRetryableTxAbort(ctx, func(attempt int) error {
			if attempt > 0 {
				if workerCtx.Writer != nil {
					if err := workerCtx.Writer.FinalizeContainer(); err != nil {
						return fmt.Errorf("reset worker writer before retry: %w", err)
					}
				}

				workerWriter, err := newWriterFromPrototype(sgctx.Writer)
				if err != nil {
					return err
				}
				workerCtx.Writer = workerWriter
			}

			_, err := storeFileWithStorageContextAndRuntimeResultWithPolicy(ctx, *workerCtx, job.Path, true, info, runtime)
			return err
		})
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}

	worker := func(workerID int, jobs <-chan FileJob, errCh chan<- error, wg *sync.WaitGroup) {
		defer wg.Done()
		local := WorkerStats{}
		defer func() { statsCh <- local }()

		workerWriter, err := newWriterFromPrototype(sgctx.Writer)
		if err != nil {
			select {
			case errCh <- fmt.Errorf("worker %d: %w", workerID, err):
				cancel()
			default:
			}
			return
		}

		workerCtx := sgctx
		workerCtx.Writer = workerWriter
		defer func() {
			if workerCtx.Writer == nil {
				return
			}
			if finalizeErr := workerCtx.Writer.FinalizeContainer(); finalizeErr != nil {
				select {
				case errCh <- fmt.Errorf("worker %d: finalize worker writer: %w", workerID, finalizeErr):
					cancel()
				default:
				}
			}
		}()

		for job := range jobs {
			// Do not infer completion order from dispatch order. Determinism comes
			// from sorted job construction and persisted metadata such as chunk_order,
			// not from when workers happen to finish.
			if ctx.Err() != nil {
				return
			}
			bytesProcessed, err := processFile(&workerCtx, job)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("worker %d: %w", workerID, err):
					cancel()
				default:
				}
				return
			}
			local.Files++
			local.Bytes += bytesProcessed
		}
	}

	// Workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(i, jobCh, errCh, &wg)
	}

	// Controller: feed a deterministic plan and fail fast when any worker reports
	// an error. The first observed worker error is pushed back so the main return
	// path can report it after wg.Wait().
	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case jobCh <- job:
			case err := <-errCh:
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	aggregatedStats := execution.ExecutionStats{WorkersUsed: workerCount}
	for i := 0; i < workerCount; i++ {
		workerStats := <-statsCh
		aggregatedStats.TotalFilesProcessed += workerStats.Files
		aggregatedStats.TotalBytesProcessed += workerStats.Bytes
	}

	select {
	case err := <-errCh:
		return aggregatedStats, err
	default:
	}
	return aggregatedStats, nil
}

func discoverFiles(root string) ([]string, error) {
	paths := make([]string, 0)
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func buildFileJobs(paths []string) []FileJob {
	jobs := make([]FileJob, len(paths))
	for i, p := range paths {
		jobs[i] = FileJob{Index: i, Path: p}
	}
	return jobs
}

func insertLegacyCompanionBlockRowWithContext(
	ctx context.Context,
	tx *sql.Tx,
	chunkID int64,
	codec string,
	nonce []byte,
	containerID int64,
	containerOffset int64,
	plaintextSize int64,
	storedSize int64,
) error {
	if codec == "" {
		codec = "plain"
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, 1, $3, $4, $5, $6, $7)
		 ON CONFLICT (chunk_id) DO UPDATE SET
		   codec          = EXCLUDED.codec,
		   format_version = EXCLUDED.format_version,
		   plaintext_size = EXCLUDED.plaintext_size,
		   stored_size    = EXCLUDED.stored_size,
		   nonce          = EXCLUDED.nonce,
		   container_id   = EXCLUDED.container_id,
		   block_offset   = EXCLUDED.block_offset`,
		chunkID,
		codec,
		plaintextSize,
		storedSize,
		nonce,
		containerID,
		containerOffset,
	); err != nil {
		return err
	}
	return nil
}

func chunkHasPackedRefWithContext(ctx context.Context, dbconn *sql.DB, chunkID int64) (bool, error) {
	var marker int
	err := dbconn.QueryRowContext(ctx, `SELECT 1 FROM chunk_block_refs WHERE chunk_id = $1 LIMIT 1`, chunkID).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func chunkHasLegacyBlockWithContext(ctx context.Context, dbconn *sql.DB, chunkID int64) (bool, error) {
	var marker int
	err := dbconn.QueryRowContext(ctx, `SELECT 1 FROM blocks WHERE chunk_id = $1 LIMIT 1`, chunkID).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// Store a chunk payload as one encoded block and persist its metadata.
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

type packedBlockPersistResult struct {
	BlockID      int64
	BlockHash    []byte
	StorageCodec string
	LegacyNonce  []byte
	Placement    container.LocalPlacement
	StoredSize   int64
	Segments     map[int64]packedChunkSegment
}

type packedChunkSegment struct {
	Offset int64
	Size   int64
}

const packedStorageBlockCodecNone = "none"
const packedStorageBlockAESGCMNonceSize = 12

// ---------------------------------------------------------------------------
// Write pipeline — storage layer semantics
//
// Layer 1 (logical block): canonical encoded plaintext block bytes.
//   block_hash = sha256(logical block bytes) — computed in buildAndEncodePackedBlock.
//   This hash is the dedup key and the restore integrity anchor. It is ALWAYS
//   computed before any transform is applied and never changes.
//
// Layer 2 (compressed payload): post-compression, pre-encryption bytes.
//   Compression runs first (store-if-smaller). compressed_hash is computed
//   over this layer when present. Layer 2 does NOT include encryption output.
//
// Layer 3 (persisted payload): bytes actually appended to the container.
//   If codec=aes-gcm, Layer 3 = encrypt(Layer 2): nonce(12B) || ciphertext.
//   physical_hash is computed over this exact byte sequence.
// ---------------------------------------------------------------------------

// packedBlockEncoded holds the result of the build + encode + hash stage.
type packedBlockEncoded struct {
	encodedBlock     *blocks.EncodedBlock
	plaintextEncoded []byte
	blockHash        []byte
	metadata         blocks.TransformMetadata
}

// packedBlockTransformed holds the result of the transform stage.
type packedBlockTransformed struct {
	storedPayload  []byte
	storageCodec   string
	legacyNonce    []byte
	compressedSize int64
	compressionLvl *int
	compressedHash []byte // hash of post-compression, pre-encryption payload
	physicalHash   []byte // hash of the exact persisted payload bytes
	metadata       blocks.TransformMetadata
}

// buildAndEncodePackedBlock builds the block from the builder, serializes it to
// binary, and computes the mandatory logical hash over the plaintext bytes.
func buildAndEncodePackedBlock(builder *blocks.BlockBuilder) (packedBlockEncoded, error) {
	encodedBlock, _, err := builder.Build()
	if err != nil {
		return packedBlockEncoded{}, err
	}

	plaintextEncoded, err := blocks.EncodeBlock(encodedBlock)
	if err != nil {
		return packedBlockEncoded{}, err
	}

	blockHash := blocks.HashLogical(plaintextEncoded)
	metadata := blocks.TransformMetadata{
		PayloadHash:      hex.EncodeToString(blockHash),
		CompressionCodec: packedStorageBlockCodecNone,
		CompressionRatio: 1.0,
	}
	encodedBlock.Metadata = metadata

	return packedBlockEncoded{
		encodedBlock:     encodedBlock,
		plaintextEncoded: plaintextEncoded,
		blockHash:        blockHash,
		metadata:         metadata,
	}, nil
}

// applyPackedBlockTransforms runs the transformer over the plaintext encoded bytes
// and assembles the final stored payload (including nonce prefix for AES-GCM).
func applyPackedBlockTransforms(
	ctx context.Context,
	transformer blocks.Transformer,
	compression storeRuntimeCompression,
	enc packedBlockEncoded,
) (packedBlockTransformed, error) {
	// Stage 2a: Compress before encryption (if compression is active).
	// Store-if-smaller policy: if compressed payload is not smaller than plaintext,
	// store the block uncompressed. This handles expansion in tiny/random/already-compressed data.
	compressedPayload := enc.plaintextEncoded
	compressionCodec := packedStorageBlockCodecNone
	compressionLevel := compression.level
	if compression.compressor != nil {
		compressed, err := compression.compressor.Compress(enc.plaintextEncoded)
		if err != nil {
			return packedBlockTransformed{}, fmt.Errorf("compress block: %w", err)
		}
		// Apply store-if-smaller policy: only use compressed if it's actually smaller.
		if len(compressed) < len(enc.plaintextEncoded) {
			compressedPayload = compressed
			compressionCodec = compression.codec
		} else {
			// Compression expanded the data; store uncompressed instead.
			compressionCodec = packedStorageBlockCodecNone
			compressionLevel = nil
		}
		if compressionCodec == storagecompression.CompressionNone {
			compressionLevel = nil
		}
	}

	// Hash pre-encryption bytes at the transform boundary.
	// With compression_codec=none, this equals logical block hash.
	compressedHash := blocks.HashCompressed(compressedPayload)

	// Stage 2b: Apply encryption transformer (or identity for plain codec).
	transformed, err := transformer.Encode(ctx, blocks.EncodeInput{
		ChunkID:   0,
		ChunkHash: hex.EncodeToString(enc.blockHash),
		Plaintext: compressedPayload,
	})
	if err != nil {
		return packedBlockTransformed{}, err
	}

	storageCodec := packedStorageBlockCodecNone
	legacyNonce := []byte{}
	storedPayload := transformed.Payload

	switch transformed.Descriptor.Codec {
	case blocks.CodecPlain:
		// Keep legacy v1.8 metadata contract for plain payloads.
	case blocks.CodecAESGCM:
		if len(transformed.Descriptor.Nonce) != packedStorageBlockAESGCMNonceSize {
			return packedBlockTransformed{}, fmt.Errorf("packed block aes-gcm nonce size mismatch: got %d want %d", len(transformed.Descriptor.Nonce), packedStorageBlockAESGCMNonceSize)
		}
		// storage_blocks has no nonce column, so prefix nonce into stored payload.
		storedPayload = make([]byte, 0, len(transformed.Descriptor.Nonce)+len(transformed.Payload))
		storedPayload = append(storedPayload, transformed.Descriptor.Nonce...)
		storedPayload = append(storedPayload, transformed.Payload...)
		storageCodec = string(blocks.CodecAESGCM)
		legacyNonce = append(legacyNonce, transformed.Descriptor.Nonce...)
	default:
		return packedBlockTransformed{}, fmt.Errorf("unsupported packed block transform codec: %q", transformed.Descriptor.Codec)
	}

	metadata := enc.metadata
	metadata.CompressionCodec = compressionCodec
	if len(enc.plaintextEncoded) > 0 {
		metadata.CompressionRatio = float64(len(compressedPayload)) / float64(len(enc.plaintextEncoded))
	}

	// Compute physical hash over the exact persisted payload bytes (post-encryption).
	physicalHash := blocks.HashPhysical(storedPayload)

	return packedBlockTransformed{
		storedPayload:  storedPayload,
		storageCodec:   storageCodec,
		legacyNonce:    legacyNonce,
		compressedSize: int64(len(compressedPayload)),
		compressionLvl: compressionLevel,
		compressedHash: compressedHash,
		physicalHash:   physicalHash,
		metadata:       metadata,
	}, nil
}

// persistPackedBlockPayload appends the stored payload to the container and
// returns the placement (container ID + offset).
func persistPackedBlockPayload(
	tx *sql.Tx,
	writer payloadStatefulWriter,
	tr packedBlockTransformed,
) (container.LocalPlacement, error) {
	return writer.AppendPayload(tx, tr.storedPayload)
}

// persistPackedBlockMetadata inserts the storage_blocks row and all
// chunk_block_refs rows inside the already-open transaction.
func persistPackedBlockMetadata(
	ctx context.Context,
	tx *sql.Tx,
	enc packedBlockEncoded,
	tr packedBlockTransformed,
	placement container.LocalPlacement,
) (int64, map[int64]packedChunkSegment, error) {
	var compressionLevelValue any
	if tr.compressionLvl != nil {
		compressionLevelValue = int64(*tr.compressionLvl)
	}

	var blockID int64
	if err := tx.QueryRowContext(
		ctx,
		`INSERT INTO storage_blocks (
			format_version, codec, plaintext_size, stored_size,
			container_id, container_offset, block_hash,
			compression_codec, compression_level, compressed_size, compression_ratio, payload_hash,
			compressed_hash, physical_hash
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		 RETURNING id`,
		1,
		tr.storageCodec,
		int64(len(enc.plaintextEncoded)),
		int64(len(tr.storedPayload)),
		placement.ContainerID,
		placement.Offset,
		enc.blockHash,
		tr.metadata.CompressionCodec,
		compressionLevelValue,
		tr.compressedSize,
		tr.metadata.CompressionRatio,
		// Legacy mirror only: lowercase-hex(block_hash). block_hash remains authoritative.
		tr.metadata.PayloadHash,
		tr.compressedHash,
		tr.physicalHash,
	).Scan(&blockID); err != nil {
		return 0, nil, err
	}

	payloadPrefixBytes := int64(len(enc.plaintextEncoded) - len(enc.encodedBlock.Payload))
	segments := make(map[int64]packedChunkSegment, len(enc.encodedBlock.Entries))
	for _, entry := range enc.encodedBlock.Entries {
		segments[int64(entry.ChunkID)] = packedChunkSegment{
			Offset: payloadPrefixBytes + int64(entry.Offset),
			Size:   int64(entry.Size),
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (chunk_id) DO UPDATE
			 SET block_id = EXCLUDED.block_id,
			     offset_in_block = EXCLUDED.offset_in_block,
			     size_in_block = EXCLUDED.size_in_block`,
			int64(entry.ChunkID),
			blockID,
			int64(entry.Offset),
			int64(entry.Size),
		); err != nil {
			return 0, nil, err
		}
	}

	return blockID, segments, nil
}

// storePackedBlockWithWriter persists one flushed packed block atomically inside tx.
//
// Atomic order (single transaction boundary):
//  1. Build block and encode to plaintext bytes; compute logical hash
//  2. Apply transforms (compress then encrypt when configured)
//  3. Persist payload to container
//  4. Persist storage_blocks + chunk_block_refs metadata
//
// Invariant: no chunk_block_refs row is written before a successful storage_blocks
// insert in the same transaction.
func storePackedBlockWithWriter(
	ctx context.Context,
	tx *sql.Tx,
	writer payloadStatefulWriter,
	transformer blocks.Transformer,
	compression storeRuntimeCompression,
	builder *blocks.BlockBuilder,
) (packedBlockPersistResult, error) {
	if builder == nil || builder.Empty() {
		return packedBlockPersistResult{}, fmt.Errorf("packed block flush requires non-empty builder")
	}

	// Stage 1: Build block + encode + hash.
	enc, err := buildAndEncodePackedBlock(builder)
	if err != nil {
		return packedBlockPersistResult{}, err
	}

	// Stage 2: Apply transforms.
	tr, err := applyPackedBlockTransforms(ctx, transformer, compression, enc)
	if err != nil {
		return packedBlockPersistResult{}, err
	}

	// Stage 3: Persist payload to container.
	placement, err := persistPackedBlockPayload(tx, writer, tr)
	if err != nil {
		return packedBlockPersistResult{}, err
	}

	// Stage 4: Persist metadata.
	blockID, segments, err := persistPackedBlockMetadata(ctx, tx, enc, tr, placement)
	if err != nil {
		return packedBlockPersistResult{}, err
	}

	return packedBlockPersistResult{
		BlockID:      blockID,
		BlockHash:    enc.blockHash,
		StorageCodec: tr.storageCodec,
		LegacyNonce:  tr.legacyNonce,
		Placement:    placement,
		StoredSize:   int64(len(tr.storedPayload)),
		Segments:     segments,
	}, nil
}

// Store payload bytes directly in a container and return offset/size metadata.
func StoreBlockPayload(c container.Container, payload []byte) (offset int64, newSize int64, err error) {
	offset, err = c.Append(payload)
	if err != nil {
		return 0, 0, err
	}

	return offset, offset + int64(len(payload)), nil
}
