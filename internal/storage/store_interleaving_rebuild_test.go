package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
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
	losingChunkID, blockID, _ := seedSharedRebuildCleanupFixture(t, dbconn, containersDir)
	err := markChunkForRebuildWithContext(context.Background(), dbconn, losingChunkID)
	if !errors.Is(err, errSharedPackedBlockPartialRebuild) {
		t.Fatalf("expected shared packed-block rebuild refusal, got %v", err)
	}
	assertSharedStorageBlockRetained(t, dbconn, blockID)
	packedRows := scanInterleavingCount(t, "count selected packed rows after refusal", dbconn.QueryRow(
		`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = $1 AND block_id = $2`, losingChunkID, blockID,
	))
	if packedRows != 1 {
		t.Fatalf("expected selected shared-block mapping to remain after refusal, got %d rows", packedRows)
	}
	var losingStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, losingChunkID).Scan(&losingStatus); err != nil {
		t.Fatalf("load selected chunk status after refusal: %v", err)
	}
	if losingStatus != filestate.ChunkCompleted {
		t.Fatalf("expected selected chunk status to remain COMPLETED, got %s", losingStatus)
	}
}

func TestSharedPackedBlockSingleMemberRebuildCannotLeavePartialMembership(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	selectedPayload := bytes.Repeat([]byte("S"), 64)
	siblingPayload := bytes.Repeat([]byte("W"), 64)
	seeds := seedPackedFixtureBlock(
		t,
		dbconn,
		containersDir,
		packedFixtureChunkSpec{
			hash:          storeInterleavingHash(selectedPayload),
			payload:       selectedPayload,
			liveRefCount:  0,
			withCompanion: true,
		},
		packedFixtureChunkSpec{
			hash:          storeInterleavingHash(siblingPayload),
			payload:       siblingPayload,
			liveRefCount:  0,
			withCompanion: true,
		},
	)
	if len(seeds) != 2 || seeds[0].blockID != seeds[1].blockID {
		t.Fatalf("fixture must create exactly two members in one packed block: %+v", seeds)
	}

	blockID := seeds[0].blockID
	memberIDs := []int64{seeds[0].chunkID, seeds[1].chunkID}
	slices.Sort(memberIDs)
	before := loadSharedPackedBlockSnapshot(t, dbconn, containersDir, blockID, memberIDs)
	assertValidSharedPackedBlockSnapshot(t, before, memberIDs)
	selectedCompanionBefore := requireValidSharedPackedCompanion(t, dbconn, seeds[0].chunkID, "selected before rebuild")
	siblingCompanionBefore := requireValidSharedPackedCompanion(t, dbconn, seeds[1].chunkID, "sibling before rebuild")
	if err := verifypkg.VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("baseline shared packed block must pass full verification: %v", err)
	}

	var events []TestStoreInterleavingHookEvent
	ctx := withStoreInterleavingState(context.Background(), &storeInterleavingState{
		hooks: &storeInterleavingHooks{
			onEvent: func(_ context.Context, event storeInterleavingHookEvent) error {
				events = append(events, event)
				return nil
			},
		},
		storeOpID: "shared-packed-single-member-rebuild",
		codec:     "plain",
		fileHash:  seeds[0].hash,
	})
	rebuildErr := markChunkForRebuildWithContext(ctx, dbconn, seeds[0].chunkID)
	if rebuildErr == nil {
		t.Fatal("shared packed-block single-member rebuild must fail closed")
	}
	if !errors.Is(rebuildErr, errSharedPackedBlockPartialRebuild) {
		t.Fatalf("expected classified shared packed-block rebuild refusal, got %v", rebuildErr)
	}
	wantMessage := fmt.Sprintf(
		"cannot rebuild chunk %d independently: packed block %d has 2 active members",
		seeds[0].chunkID,
		blockID,
	)
	if !strings.Contains(rebuildErr.Error(), wantMessage) {
		t.Fatalf("expected stable shared packed-block refusal %q, got %v", wantMessage, rebuildErr)
	}
	assertSharedRebuildRefusalHookSequence(t, events, seeds[0].chunkID)

	after := loadSharedPackedBlockSnapshot(t, dbconn, containersDir, blockID, memberIDs)
	selectedCompanionAfter := requireValidSharedPackedCompanion(t, dbconn, seeds[0].chunkID, "selected after refusal")
	siblingCompanionAfter := requireValidSharedPackedCompanion(t, dbconn, seeds[1].chunkID, "sibling after refusal")
	if !sharedPackedBlockSnapshotsEqual(before, after) {
		t.Fatalf("shared packed-block refusal mutated repository state: before=%+v after=%+v", before, after)
	}
	if !selectedCompanionBefore || !siblingCompanionBefore || !selectedCompanionAfter || !siblingCompanionAfter {
		t.Fatalf(
			"shared packed-block companions must remain valid after refusal: selected_before=%t sibling_before=%t selected_after=%t sibling_after=%t",
			selectedCompanionBefore,
			siblingCompanionBefore,
			selectedCompanionAfter,
			siblingCompanionAfter,
		)
	}
	if err := verifypkg.VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("shared packed block must remain fully verifiable after refusal: %v", err)
	}
}

func TestStoreInterleavingRebuildCleanupAllowsSingleMemberPackedBlock(t *testing.T) {
	dbconn, _ := openSharedStoreInterleavingDB(t)
	containersDir := t.TempDir()
	payload := bytes.Repeat([]byte("S"), 64)
	chunkHash := storeInterleavingHash(payload)
	seeds := seedPackedFixtureBlock(t, dbconn, containersDir, packedFixtureChunkSpec{
		hash:          chunkHash,
		payload:       payload,
		liveRefCount:  0,
		withCompanion: true,
	})
	if len(seeds) != 1 {
		t.Fatalf("fixture must create exactly one packed member: %+v", seeds)
	}
	if err := verifypkg.VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("baseline single-member packed block must pass full verification: %v", err)
	}

	if err := markChunkForRebuildWithContext(context.Background(), dbconn, seeds[0].chunkID); err != nil {
		t.Fatalf("single-member packed block rebuild cleanup: %v", err)
	}
	assertInterleavingChunkFinalState(t, dbconn, containersDir, chunkHash, len(payload), interleavingChunkFinalState{
		chunkStatus: filestate.ChunkAborted,
	})
	blockRows := scanInterleavingCount(t, "count single-member storage block after cleanup", dbconn.QueryRow(
		`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, seeds[0].blockID,
	))
	if blockRows != 0 {
		t.Fatalf("expected orphaned single-member storage block to be deleted, got %d rows", blockRows)
	}
	if err := verifypkg.VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("single-member rebuild cleanup must leave repository verifiable: %v", err)
	}
}

type sharedPackedBlockSnapshot struct {
	blockExists       bool
	relationalMembers []int64
	companionMembers  []int64
	encodedMembers    []int64
	chunkStatuses     []string
	storageMetadata   string
	physicalFileRows  int
	physicalVerified  bool
	physicalError     string
}

func loadSharedPackedBlockSnapshot(
	t *testing.T,
	dbconn *sql.DB,
	containersDir string,
	blockID int64,
	memberIDs []int64,
) sharedPackedBlockSnapshot {
	t.Helper()
	if len(memberIDs) != 2 {
		t.Fatalf("shared packed block diagnostic expects two members, got %v", memberIDs)
	}

	snapshot := sharedPackedBlockSnapshot{
		relationalMembers: loadSharedPackedBlockMemberIDs(t, dbconn, blockID),
		companionMembers:  loadSharedPackedCompanionMemberIDs(t, dbconn, memberIDs),
		chunkStatuses:     loadSharedPackedChunkStatuses(t, dbconn, memberIDs),
		physicalFileRows: scanInterleavingCount(t, "count physical-file metadata for shared block", dbconn.QueryRow(
			`SELECT COUNT(*) FROM physical_file`,
		)),
	}

	var meta verifypkg.BlockStorageMetadata
	var compressionLevel sql.NullInt64
	var compressedSize sql.NullInt64
	var containerCurrentSize int64
	err := dbconn.QueryRow(`
		SELECT sb.format_version, sb.codec, sb.compression_codec, sb.compression_level,
		       sb.plaintext_size, sb.compressed_size, sb.stored_size,
		       sb.container_id, c.filename, c.current_size, c.max_size, sb.container_offset,
		       sb.block_hash, sb.compressed_hash, sb.physical_hash
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE sb.id = $1
	`, blockID).Scan(
		&meta.FormatVersion,
		&meta.Codec,
		&meta.CompressionCodec,
		&compressionLevel,
		&meta.PlaintextSize,
		&compressedSize,
		&meta.StoredSize,
		&meta.ContainerID,
		&meta.ContainerName,
		&containerCurrentSize,
		&meta.ContainerMaxSize,
		&meta.ContainerOffset,
		&meta.LogicalHash,
		&meta.CompressedHash,
		&meta.PhysicalHash,
	)
	if err == sql.ErrNoRows {
		return snapshot
	}
	if err != nil {
		t.Fatalf("load shared packed block %d metadata: %v", blockID, err)
	}
	snapshot.blockExists = true
	meta.BlockID = blockID
	snapshot.storageMetadata = fmt.Sprintf(
		"container_id=%d name=%s current_size=%d max_size=%d offset=%d stored_size=%d logical_hash=%x compressed_hash=%x physical_hash=%x",
		meta.ContainerID,
		meta.ContainerName,
		containerCurrentSize,
		meta.ContainerMaxSize,
		meta.ContainerOffset,
		meta.StoredSize,
		meta.LogicalHash,
		meta.CompressedHash,
		meta.PhysicalHash,
	)
	if compressedSize.Valid {
		value := compressedSize.Int64
		meta.CompressedSize = &value
	}
	if compressionLevel.Valid {
		value := int(compressionLevel.Int64)
		meta.CompressionLevel = &value
	}

	verified, err := verifypkg.VerifyStoredBlock(
		context.Background(),
		meta,
		verifypkg.FilesystemContainerReader{ContainersDir: containersDir},
	)
	if err != nil {
		snapshot.physicalError = err.Error()
		return snapshot
	}
	snapshot.physicalVerified = true
	for _, entry := range verified.DecodedBlock.Entries {
		snapshot.encodedMembers = append(snapshot.encodedMembers, int64(entry.ChunkID))
	}
	slices.Sort(snapshot.encodedMembers)
	return snapshot
}

func loadSharedPackedChunkStatuses(t *testing.T, dbconn *sql.DB, memberIDs []int64) []string {
	t.Helper()
	rows, err := dbconn.Query(
		`SELECT status FROM chunk WHERE id = $1 OR id = $2 ORDER BY id`,
		memberIDs[0],
		memberIDs[1],
	)
	if err != nil {
		t.Fatalf("load shared packed chunk statuses: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var statuses []string
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			t.Fatalf("scan shared packed chunk status: %v", err)
		}
		statuses = append(statuses, status)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate shared packed chunk statuses: %v", err)
	}
	return statuses
}

func loadSharedPackedBlockMemberIDs(t *testing.T, dbconn *sql.DB, blockID int64) []int64 {
	t.Helper()
	rows, err := dbconn.Query(`SELECT chunk_id FROM chunk_block_refs WHERE block_id = $1 ORDER BY chunk_id`, blockID)
	if err != nil {
		t.Fatalf("load relational members for packed block %d: %v", blockID, err)
	}
	defer func() { _ = rows.Close() }()
	return scanSharedPackedMemberIDs(t, rows, "relational packed members")
}

func loadSharedPackedCompanionMemberIDs(t *testing.T, dbconn *sql.DB, memberIDs []int64) []int64 {
	t.Helper()
	rows, err := dbconn.Query(
		`SELECT chunk_id FROM blocks WHERE chunk_id = $1 OR chunk_id = $2 ORDER BY chunk_id`,
		memberIDs[0],
		memberIDs[1],
	)
	if err != nil {
		t.Fatalf("load packed companion members: %v", err)
	}
	defer func() { _ = rows.Close() }()
	return scanSharedPackedMemberIDs(t, rows, "packed companion members")
}

func scanSharedPackedMemberIDs(t *testing.T, rows *sql.Rows, label string) []int64 {
	t.Helper()
	var memberIDs []int64
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			t.Fatalf("scan %s: %v", label, err)
		}
		memberIDs = append(memberIDs, chunkID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s: %v", label, err)
	}
	return memberIDs
}

func assertValidSharedPackedBlockSnapshot(t *testing.T, snapshot sharedPackedBlockSnapshot, memberIDs []int64) {
	t.Helper()
	if !snapshot.blockExists || !snapshot.physicalVerified || snapshot.physicalError != "" {
		t.Fatalf("fixture packed block physical metadata/bytes are invalid: %+v", snapshot)
	}
	if len(memberIDs) < 2 || !slices.Equal(snapshot.relationalMembers, memberIDs) || !slices.Equal(snapshot.companionMembers, memberIDs) || !slices.Equal(snapshot.encodedMembers, memberIDs) {
		t.Fatalf("fixture must begin with identical multi-member relational, companion, and encoded membership: want=%v got=%+v", memberIDs, snapshot)
	}
}

func requireValidSharedPackedCompanion(t *testing.T, dbconn *sql.DB, chunkID int64, label string) bool {
	t.Helper()
	valid, err := validateReusableChunkCompanionMappingWithContext(context.Background(), dbconn, chunkID)
	if err != nil {
		t.Fatalf("%s companion validation: %v", label, err)
	}
	if !valid {
		t.Fatalf("%s companion must be valid: %s", label, describeSharedPackedCompanion(t, dbconn, chunkID))
	}
	return valid
}

func describeSharedPackedCompanion(t *testing.T, dbconn *sql.DB, chunkID int64) string {
	t.Helper()
	var chunkSize, plaintextSize, storedSize, legacyContainerID, legacyOffset int64
	var offsetInBlock, sizeInBlock, packedContainerID, packedContainerOffset, packedPlaintextSize, totalReferencedBytes int64
	var codec string
	if err := dbconn.QueryRow(`
		SELECT c.size, b.codec, b.plaintext_size, b.stored_size, b.container_id, b.block_offset,
		       r.offset_in_block, r.size_in_block, sb.container_id, sb.container_offset,
		       sb.plaintext_size,
		       (SELECT COALESCE(SUM(size_in_block), 0) FROM chunk_block_refs WHERE block_id = r.block_id)
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE c.id = $1
	`, chunkID).Scan(
		&chunkSize,
		&codec,
		&plaintextSize,
		&storedSize,
		&legacyContainerID,
		&legacyOffset,
		&offsetInBlock,
		&sizeInBlock,
		&packedContainerID,
		&packedContainerOffset,
		&packedPlaintextSize,
		&totalReferencedBytes,
	); err != nil {
		return err.Error()
	}
	return fmt.Sprintf(
		"chunk_size=%d codec=%s plaintext_size=%d stored_size=%d legacy_container=%d legacy_offset=%d offset_in_block=%d size_in_block=%d packed_container=%d packed_offset=%d packed_plaintext_size=%d total_referenced=%d derived_prefix=%d expected_legacy_offset=%d",
		chunkSize,
		codec,
		plaintextSize,
		storedSize,
		legacyContainerID,
		legacyOffset,
		offsetInBlock,
		sizeInBlock,
		packedContainerID,
		packedContainerOffset,
		packedPlaintextSize,
		totalReferencedBytes,
		packedPlaintextSize-totalReferencedBytes,
		packedContainerOffset+packedPlaintextSize-totalReferencedBytes+offsetInBlock,
	)
}

func sharedPackedBlockSnapshotsEqual(left, right sharedPackedBlockSnapshot) bool {
	return left.blockExists == right.blockExists &&
		slices.Equal(left.relationalMembers, right.relationalMembers) &&
		slices.Equal(left.companionMembers, right.companionMembers) &&
		slices.Equal(left.encodedMembers, right.encodedMembers) &&
		slices.Equal(left.chunkStatuses, right.chunkStatuses) &&
		left.storageMetadata == right.storageMetadata &&
		left.physicalFileRows == right.physicalFileRows &&
		left.physicalVerified == right.physicalVerified &&
		left.physicalError == right.physicalError
}

func assertSharedRebuildRefusalHookSequence(t *testing.T, events []TestStoreInterleavingHookEvent, chunkID int64) {
	t.Helper()
	if len(events) != 1 || events[0].Event != TestStoreInterleavingEventBeforeMarkChunkForRebuild || events[0].ChunkID != chunkID {
		t.Fatalf("expected rebuild refusal before-mutation hook only, got %+v", events)
	}
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
