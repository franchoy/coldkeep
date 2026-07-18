package storage

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

func TestStoreInterleavingRebuildCleanupDeletesBothMappings(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	chunkID, chunkHash := seedInvalidRebuildCleanupFixture(t, dbconn, containersDir)
	events := claimInvalidRebuildCleanupChunk(t, dbconn, containersDir, chunkID, chunkHash)
	assertChunkMappingsRemoved(t, dbconn, chunkID)
	assertRebuildCleanupHookSequence(t, events)
	assertInterleavingChunkFinalState(t, dbconn, containersDir, chunkHash, 64, interleavingChunkFinalState{
		chunkStatus: filestate.ChunkProcessing,
	})
}

func seedInvalidRebuildCleanupFixture(t *testing.T, dbconn *sql.DB, containersDir string) (int64, string) {
	t.Helper()
	containerID := insertReusableTestContainer(t, dbconn, "rebuild-cleanup.bin", false)
	containerPath := filepath.Join(containersDir, "rebuild-cleanup.bin")
	if err := writeReusableTestContainerFileWithPayload(containerPath, make([]byte, 256)); err != nil {
		t.Fatalf("write reusable container file: %v", err)
	}

	chunkHash := "rebuild-cleanup-hash"
	chunkID := insertReusableTestChunk(t, dbconn, chunkHash, filestate.ChunkCompleted)
	if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = 0 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("zero live_ref_count for rebuild cleanup fixture: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, 64, 64, $2, 65)`,
		chunkID,
		containerID,
	); err != nil {
		t.Fatalf("insert invalid legacy row: %v", err)
	}

	var blockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', 64, 64, $1, 64, x'00')
		 RETURNING id`,
		containerID,
	).Scan(&blockID); err != nil {
		t.Fatalf("insert packed block row: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, 0, 64)`,
		chunkID,
		blockID,
	); err != nil {
		t.Fatalf("insert packed ref: %v", err)
	}
	return chunkID, chunkHash
}

func claimInvalidRebuildCleanupChunk(t *testing.T, dbconn *sql.DB, containersDir string, chunkID int64, chunkHash string) []TestStoreInterleavingHookEvent {
	t.Helper()
	var events []TestStoreInterleavingHookEvent
	ctx := withStoreInterleavingState(context.Background(), &storeInterleavingState{
		hooks: &storeInterleavingHooks{
			onEvent: func(_ context.Context, event storeInterleavingHookEvent) error {
				events = append(events, event)
				return nil
			},
		},
		storeOpID: "rebuild-cleanup-op",
		codec:     "plain",
		fileHash:  chunkHash,
	})
	claimedID, claimedStatus, isNew, err := claimChunkWithContext(ctx, dbconn, chunkHash, 64, string(chunk.DefaultChunkerVersion), containersDir)
	if err != nil {
		t.Fatalf("claim invalid reusable chunk: %v", err)
	}
	if claimedID != chunkID || isNew || claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("unexpected reclaim result: id=%d status=%s isNew=%v", claimedID, claimedStatus, isNew)
	}
	return events
}

func assertChunkMappingsRemoved(t *testing.T, dbconn *sql.DB, chunkID int64) {
	t.Helper()
	packedRows := scanInterleavingCount(t, "count packed rows after rebuild cleanup", dbconn.QueryRow(
		`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = $1`, chunkID,
	))
	if packedRows != 0 {
		t.Fatalf("expected packed rows to be deleted during rebuild cleanup, got %d", packedRows)
	}
	legacyRows := scanInterleavingCount(t, "count legacy rows after rebuild cleanup", dbconn.QueryRow(
		`SELECT COUNT(*) FROM blocks WHERE chunk_id = $1`, chunkID,
	))
	if legacyRows != 0 {
		t.Fatalf("expected legacy rows to be deleted during rebuild cleanup, got %d", legacyRows)
	}
}

func assertRebuildCleanupHookSequence(t *testing.T, events []TestStoreInterleavingHookEvent) {
	t.Helper()
	if len(events) < 2 || events[0].Event != TestStoreInterleavingEventBeforeMarkChunkForRebuild || events[1].Event != TestStoreInterleavingEventAfterMarkChunkForRebuild {
		t.Fatalf("expected rebuild cleanup hook sequence, got %+v", events)
	}
}

func TestStoreInterleavingRebuildCleanupRetainsSharedStorageBlock(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	losingChunkID, blockID, winningHash := seedSharedRebuildCleanupFixture(t, dbconn, containersDir)
	if err := markChunkForRebuildWithContext(context.Background(), dbconn, losingChunkID); err != nil {
		t.Fatalf("mark losing chunk for rebuild: %v", err)
	}
	assertChunkMappingsRemoved(t, dbconn, losingChunkID)
	assertSharedStorageBlockRetained(t, dbconn, blockID)
	assertSharedRebuildSurvivorState(t, dbconn, winningHash, losingChunkID)
}

func seedSharedRebuildCleanupFixture(t *testing.T, dbconn *sql.DB, containersDir string) (int64, int64, string) {
	t.Helper()
	losingPayload := bytes.Repeat([]byte("L"), 64)
	winningPayload := bytes.Repeat([]byte("W"), 64)
	losingHash := storeInterleavingHash(losingPayload)
	winningHash := storeInterleavingHash(winningPayload)
	seeds := seedPackedFixtureBlock(
		t,
		dbconn,
		containersDir,
		packedFixtureChunkSpec{
			hash:          losingHash,
			payload:       losingPayload,
			liveRefCount:  0,
			withCompanion: false,
		},
		packedFixtureChunkSpec{
			hash:          winningHash,
			payload:       winningPayload,
			liveRefCount:  0,
			withCompanion: false,
		},
	)
	return seeds[0].chunkID, seeds[0].blockID, winningHash
}

func assertSharedStorageBlockRetained(t *testing.T, dbconn *sql.DB, blockID int64) {
	t.Helper()
	blockRows := scanInterleavingCount(t, "count shared storage block rows", dbconn.QueryRow(
		`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, blockID,
	))
	if blockRows != 1 {
		t.Fatalf("expected shared storage block to remain for winner, got %d rows", blockRows)
	}
}

func assertSharedRebuildSurvivorState(t *testing.T, dbconn *sql.DB, winningHash string, losingChunkID int64) {
	t.Helper()
	// This synthetic fixture intentionally seeds one packed payload that still
	// encodes both chunks, then removes only one chunk_block_refs row. That
	// leaves the surviving storage_blocks row still referenced, which is the
	// cleanup-retention property under test, but the packed payload can no longer
	// satisfy full payload-level verification for the deleted chunk entry.
	gotWinner := loadInterleavingChunkFinalState(t, dbconn, winningHash, 64)
	wantWinner := interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkCompleted,
		packedMappings:      1,
		legacyMappings:      0,
		logicalFileRefs:     0,
		physicalFileRefs:    0,
		storageBlockRefs:    1,
		storageBlocks:       1,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: false,
	}
	if comparableInterleavingState(gotWinner) != comparableInterleavingState(wantWinner) {
		t.Fatalf("unexpected shared-reference survivor state: got=%+v want=%+v", gotWinner, wantWinner)
	}

	var losingStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, losingChunkID).Scan(&losingStatus); err != nil {
		t.Fatalf("load losing chunk status: %v", err)
	}
	if losingStatus != filestate.ChunkAborted {
		t.Fatalf("expected losing chunk status ABORTED, got %s", losingStatus)
	}
}

func TestStoreInterleavingRebuildCleanupRollbackRestoresRows(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	rollbackPayload := bytes.Repeat([]byte("R"), 64)
	chunkHash := storeInterleavingHash(rollbackPayload)
	seeds := seedPackedFixtureBlock(
		t,
		dbconn,
		containersDir,
		packedFixtureChunkSpec{
			hash:          chunkHash,
			payload:       rollbackPayload,
			liveRefCount:  0,
			withCompanion: true,
		},
	)
	chunkID := seeds[0].chunkID
	if _, err := dbconn.Exec(
		`CREATE TRIGGER fail_storage_block_delete
		 BEFORE DELETE ON storage_blocks
		 BEGIN
		   SELECT RAISE(ABORT, 'forced rebuild rollback');
		 END`,
	); err != nil {
		t.Fatalf("create rollback trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = dbconn.Exec(`DROP TRIGGER IF EXISTS fail_storage_block_delete`)
	})

	err := markChunkForRebuildWithContext(context.Background(), dbconn, chunkID)
	if err == nil || !strings.Contains(err.Error(), "forced rebuild rollback") {
		t.Fatalf("expected forced rollback error, got %v", err)
	}

	assertInterleavingChunkFinalState(t, dbconn, containersDir, chunkHash, 64, interleavingChunkFinalState{
		chunkStatus:         filestate.ChunkCompleted,
		packedMappings:      1,
		legacyMappings:      1,
		logicalFileRefs:     0,
		physicalFileRefs:    0,
		storageBlockRefs:    1,
		storageBlocks:       1,
		orphanStorageBlocks: 0,
		sealedContainers:    0,
		quarantinedConts:    0,
		validCompanionState: true,
	})
}

func TestStoreInterleavingRebuildCleanupIsIdempotent(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	chunkID, chunkHash := seedIdempotentRebuildCleanupFixture(t, dbconn, containersDir)
	markChunkForRebuildOrFail(t, dbconn, chunkID, "first rebuild cleanup")
	markChunkForRebuildOrFail(t, dbconn, chunkID, "second rebuild cleanup")
	assertInterleavingChunkFinalState(t, dbconn, containersDir, chunkHash, 64, interleavingChunkFinalState{
		chunkStatus: filestate.ChunkAborted,
	})
}

func seedIdempotentRebuildCleanupFixture(t *testing.T, dbconn *sql.DB, containersDir string) (int64, string) {
	t.Helper()
	containerID := insertReusableTestContainer(t, dbconn, "idempotent-rebuild.bin", false)
	containerPath := filepath.Join(containersDir, "idempotent-rebuild.bin")
	if err := writeReusableTestContainerFileWithPayload(containerPath, make([]byte, 256)); err != nil {
		t.Fatalf("write idempotent rebuild fixture: %v", err)
	}

	chunkHash := "idempotent-rebuild-hash"
	chunkID := insertReusableTestChunk(t, dbconn, chunkHash, filestate.ChunkCompleted)
	if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = 0 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("zero live_ref_count for idempotence fixture: %v", err)
	}

	var blockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', 64, 64, $1, 64, zeroblob(32))
		 RETURNING id`,
		containerID,
	).Scan(&blockID); err != nil {
		t.Fatalf("insert idempotence storage block: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, 0, 64)`,
		chunkID, blockID,
	); err != nil {
		t.Fatalf("insert idempotence packed ref: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, 64, 64, $2, 64)`,
		chunkID, containerID,
	); err != nil {
		t.Fatalf("insert idempotence legacy row: %v", err)
	}
	return chunkID, chunkHash
}

func markChunkForRebuildOrFail(t *testing.T, dbconn *sql.DB, chunkID int64, label string) {
	t.Helper()
	if err := markChunkForRebuildWithContext(context.Background(), dbconn, chunkID); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

func TestStoreInterleavingRetryClaimSignalsCASBoundary(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	if db.BackendFromDB(dbconn) == db.BackendSQLite {
		t.Skip("sqlite cannot exercise nested retry-CAS transaction flow without backend lock contention; postgres parity test covers this path")
	}
	hash := "retry-claim-hash"
	chunkID := insertAbortedRetryClaimChunk(t, dbconn, hash)
	events := claimAbortedRetryChunk(t, dbconn, hash, chunkID)
	assertRetryClaimCASBoundary(t, events, chunkID)
}

func insertAbortedRetryClaimChunk(t *testing.T, dbconn *sql.DB, hash string) int64 {
	t.Helper()
	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, 0, 0, $4)
		 RETURNING id`,
		hash,
		64,
		filestate.ChunkAborted,
		string(chunk.DefaultChunkerVersion),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert aborted chunk: %v", err)
	}
	return chunkID
}

func claimAbortedRetryChunk(t *testing.T, dbconn *sql.DB, hash string, chunkID int64) []TestStoreInterleavingHookEvent {
	t.Helper()
	var events []TestStoreInterleavingHookEvent
	ctx := withStoreInterleavingState(context.Background(), &storeInterleavingState{
		hooks: &storeInterleavingHooks{
			onEvent: func(_ context.Context, event storeInterleavingHookEvent) error {
				events = append(events, event)
				return nil
			},
		},
		storeOpID: "retry-claim-op",
		codec:     "plain",
		fileHash:  hash,
	})
	claimedID, claimedStatus, isNew, err := claimChunkWithContext(ctx, dbconn, hash, 64, string(chunk.DefaultChunkerVersion), t.TempDir())
	if err != nil {
		t.Fatalf("claim aborted chunk: %v", err)
	}
	if claimedID != chunkID || isNew || claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("unexpected retry claim result: id=%d status=%s isNew=%v", claimedID, claimedStatus, isNew)
	}
	return events
}

func assertRetryClaimCASBoundary(t *testing.T, events []TestStoreInterleavingHookEvent, chunkID int64) {
	t.Helper()
	for _, event := range events {
		if event.ChunkID == chunkID && event.Event == TestStoreInterleavingEventBeforeChunkRetryCAS {
			return
		}
	}
	t.Fatalf("expected retry claim to hit CAS boundary, events=%+v", events)
}
