package maintenance

import (
	"context"
	"database/sql"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openPackedGCUnitDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestLoadLivePackedBlockIDsResolvesFromLiveChunks(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	res, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('live-packed.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := res.LastInsertId()

	liveChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('live', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live chunk: %v", err)
	}
	liveChunkID, _ := liveChunkRes.LastInsertId()

	deadChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('dead', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}
	deadChunkID, _ := deadChunkRes.LastInsertId()

	blockRes1, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 0, x'01')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block #1: %v", err)
	}
	liveBlockID, _ := blockRes1.LastInsertId()

	blockRes2, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 64, x'02')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block #2: %v", err)
	}
	deadBlockID, _ := blockRes2.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, liveChunkID, liveBlockID); err != nil {
		t.Fatalf("insert chunk_block_refs live: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, deadChunkID, deadBlockID); err != nil {
		t.Fatalf("insert chunk_block_refs dead: %v", err)
	}

	livePacked, err := LoadLivePackedBlockIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLivePackedBlockIDs: %v", err)
	}
	if _, ok := livePacked[liveBlockID]; !ok {
		t.Fatalf("expected live packed block id %d", liveBlockID)
	}
	if _, ok := livePacked[deadBlockID]; ok {
		t.Fatalf("unexpected dead packed block id %d in live set", deadBlockID)
	}
}

func TestLoadLiveLegacyContainerIDsResolvesFromLiveChunks(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	resA, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('legacy-live.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container A: %v", err)
	}
	containerLiveID, _ := resA.LastInsertId()

	resB, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('legacy-dead.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container B: %v", err)
	}
	containerDeadID, _ := resB.LastInsertId()

	liveChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('legacy-live-chunk', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live chunk: %v", err)
	}
	liveChunkID, _ := liveChunkRes.LastInsertId()

	deadChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('legacy-dead-chunk', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}
	deadChunkID, _ := deadChunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, 64, 64, ?, 0)`, liveChunkID, containerLiveID); err != nil {
		t.Fatalf("insert live legacy block: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, 64, 64, ?, 0)`, deadChunkID, containerDeadID); err != nil {
		t.Fatalf("insert dead legacy block: %v", err)
	}

	liveLegacyContainers, err := LoadLiveLegacyContainerIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLiveLegacyContainerIDs: %v", err)
	}
	if _, ok := liveLegacyContainers[containerLiveID]; !ok {
		t.Fatalf("expected legacy live container id %d", containerLiveID)
	}
	if _, ok := liveLegacyContainers[containerDeadID]; ok {
		t.Fatalf("unexpected legacy dead container id %d in live set", containerDeadID)
	}
}

func TestContainerHasReachableChunksIncludesPackedRefs(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('reachable-packed.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('reachable', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	blockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 0, x'03')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block: %v", err)
	}
	blockID, _ := blockRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, chunkID, blockID); err != nil {
		t.Fatalf("insert chunk_block_refs: %v", err)
	}

	reachable := map[int64]struct{}{chunkID: {}}
	hasRetained, err := containerHasReachableChunks(context.Background(), dbconn, containerID, reachable)
	if err != nil {
		t.Fatalf("containerHasReachableChunks: %v", err)
	}
	if !hasRetained {
		t.Fatal("expected reachable packed chunk to retain container")
	}
}

func TestSweepUnreachableChunksDeletesPackedMappings(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('sweep-packed.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('dead-sweep', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	blockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 0, x'04')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block: %v", err)
	}
	blockID, _ := blockRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, chunkID, blockID); err != nil {
		t.Fatalf("insert chunk_block_refs: %v", err)
	}

	if err := SweepUnreachableChunks(context.Background(), dbconn, containerID); err != nil {
		t.Fatalf("SweepUnreachableChunks: %v", err)
	}

	var refs int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = ?`, blockID).Scan(&refs); err != nil {
		t.Fatalf("count refs: %v", err)
	}
	if refs != 0 {
		t.Fatalf("chunk_block_refs remaining = %d, want 0", refs)
	}

	var blocks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = ?`, blockID).Scan(&blocks); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if blocks != 0 {
		t.Fatalf("storage_blocks remaining = %d, want 0", blocks)
	}

	var chunks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id = ?`, chunkID).Scan(&chunks); err != nil {
		t.Fatalf("count chunk: %v", err)
	}
	if chunks != 0 {
		t.Fatalf("chunk rows remaining = %d, want 0", chunks)
	}
}

func TestSweepUnreachableChunksKeepsPackedBlockWhenAnyChunkIsLive(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('sweep-packed-mixed-live.bin', 1, 0, 128, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	liveChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-live-ref', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live chunk: %v", err)
	}
	liveChunkID, _ := liveChunkRes.LastInsertId()

	deadChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-dead-ref', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}
	deadChunkID, _ := deadChunkRes.LastInsertId()

	blockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 128, 128, ?, 0, x'22')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block: %v", err)
	}
	blockID, _ := blockRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, liveChunkID, blockID); err != nil {
		t.Fatalf("insert live chunk_block_ref: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 64, 64)`, deadChunkID, blockID); err != nil {
		t.Fatalf("insert dead chunk_block_ref: %v", err)
	}

	if err := SweepUnreachableChunks(context.Background(), dbconn, containerID); err != nil {
		t.Fatalf("SweepUnreachableChunks: %v", err)
	}

	var blocks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = ?`, blockID).Scan(&blocks); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if blocks != 1 {
		t.Fatalf("storage_blocks remaining = %d, want 1", blocks)
	}

	var refs int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = ?`, blockID).Scan(&refs); err != nil {
		t.Fatalf("count chunk_block_refs: %v", err)
	}
	if refs != 2 {
		t.Fatalf("chunk_block_refs remaining = %d, want 2", refs)
	}

	var liveChunks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id = ?`, liveChunkID).Scan(&liveChunks); err != nil {
		t.Fatalf("count live chunk: %v", err)
	}
	if liveChunks != 1 {
		t.Fatalf("live chunk rows remaining = %d, want 1", liveChunks)
	}

	var deadChunks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id = ?`, deadChunkID).Scan(&deadChunks); err != nil {
		t.Fatalf("count dead chunk: %v", err)
	}
	if deadChunks != 1 {
		t.Fatalf("dead chunk rows remaining = %d, want 1", deadChunks)
	}
}

func TestSweepUnreachableChunksMixedPackedBlockKeepsAllRefsAndChunks(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('sweep-packed-mixed-three.bin', 1, 0, 192, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	liveChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('mixed-live-c1', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live chunk c1: %v", err)
	}
	c1ID, _ := liveChunkRes.LastInsertId()

	deadChunk2Res, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('mixed-dead-c2', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk c2: %v", err)
	}
	c2ID, _ := deadChunk2Res.LastInsertId()

	deadChunk3Res, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('mixed-dead-c3', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk c3: %v", err)
	}
	c3ID, _ := deadChunk3Res.LastInsertId()

	blockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 192, 192, ?, 0, x'55')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block: %v", err)
	}
	blockID, _ := blockRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, c1ID, blockID); err != nil {
		t.Fatalf("insert c1 ref: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 64, 64)`, c2ID, blockID); err != nil {
		t.Fatalf("insert c2 ref: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 128, 64)`, c3ID, blockID); err != nil {
		t.Fatalf("insert c3 ref: %v", err)
	}

	if err := SweepUnreachableChunks(context.Background(), dbconn, containerID); err != nil {
		t.Fatalf("SweepUnreachableChunks: %v", err)
	}

	var blockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = ?`, blockID).Scan(&blockCount); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if blockCount != 1 {
		t.Fatalf("storage_blocks remaining = %d, want 1", blockCount)
	}

	var blockRefs int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = ?`, blockID).Scan(&blockRefs); err != nil {
		t.Fatalf("count block refs: %v", err)
	}
	if blockRefs != 3 {
		t.Fatalf("chunk_block_refs remaining for retained block = %d, want 3", blockRefs)
	}

	for _, tc := range []struct {
		chunkID int64
		name    string
	}{
		{chunkID: c1ID, name: "c1"},
		{chunkID: c2ID, name: "c2"},
		{chunkID: c3ID, name: "c3"},
	} {
		var chunkCount int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id = ?`, tc.chunkID).Scan(&chunkCount); err != nil {
			t.Fatalf("count %s chunk: %v", tc.name, err)
		}
		if chunkCount != 1 {
			t.Fatalf("%s chunk rows remaining = %d, want 1", tc.name, chunkCount)
		}

		var chunkRefCount int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE chunk_id = ? AND block_id = ?`, tc.chunkID, blockID).Scan(&chunkRefCount); err != nil {
			t.Fatalf("count %s ref: %v", tc.name, err)
		}
		if chunkRefCount != 1 {
			t.Fatalf("%s ref rows remaining = %d, want 1", tc.name, chunkRefCount)
		}
	}
}

func TestSweepUnreachableChunksDeletesWholePackedBlockWhenAllChunksDead(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('sweep-packed-all-dead.bin', 1, 0, 128, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	deadChunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-dead-a', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk A: %v", err)
	}
	deadChunkAID, _ := deadChunkARes.LastInsertId()

	deadChunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-dead-b', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk B: %v", err)
	}
	deadChunkBID, _ := deadChunkBRes.LastInsertId()

	blockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 128, 128, ?, 0, x'33')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block: %v", err)
	}
	blockID, _ := blockRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, deadChunkAID, blockID); err != nil {
		t.Fatalf("insert dead chunk A ref: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 64, 64)`, deadChunkBID, blockID); err != nil {
		t.Fatalf("insert dead chunk B ref: %v", err)
	}

	if err := SweepUnreachableChunks(context.Background(), dbconn, containerID); err != nil {
		t.Fatalf("SweepUnreachableChunks: %v", err)
	}

	var blocks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = ?`, blockID).Scan(&blocks); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if blocks != 0 {
		t.Fatalf("storage_blocks remaining = %d, want 0", blocks)
	}

	var refs int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = ?`, blockID).Scan(&refs); err != nil {
		t.Fatalf("count chunk_block_refs: %v", err)
	}
	if refs != 0 {
		t.Fatalf("chunk_block_refs remaining = %d, want 0", refs)
	}

	var chunks int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id IN (?, ?)`, deadChunkAID, deadChunkBID).Scan(&chunks); err != nil {
		t.Fatalf("count chunk rows: %v", err)
	}
	if chunks != 0 {
		t.Fatalf("chunk rows remaining = %d, want 0", chunks)
	}
}

func TestSweepUnreachableChunksMetadataOrderTransactionSafety(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('sweep-packed-tx-safety.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	deadChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-dead-tx', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}
	deadChunkID, _ := deadChunkRes.LastInsertId()

	blockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 0, x'44')`, containerID)
	if err != nil {
		t.Fatalf("insert storage block: %v", err)
	}
	blockID, _ := blockRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, deadChunkID, blockID); err != nil {
		t.Fatalf("insert chunk_block_ref: %v", err)
	}

	// Rollback path: metadata should remain unchanged if transaction does not commit.
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := SweepUnreachableChunks(context.Background(), tx, containerID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SweepUnreachableChunks in tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	var blocksAfterRollback int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = ?`, blockID).Scan(&blocksAfterRollback); err != nil {
		t.Fatalf("count storage_blocks after rollback: %v", err)
	}
	if blocksAfterRollback != 1 {
		t.Fatalf("storage_blocks after rollback = %d, want 1", blocksAfterRollback)
	}

	var refsAfterRollback int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = ?`, blockID).Scan(&refsAfterRollback); err != nil {
		t.Fatalf("count chunk_block_refs after rollback: %v", err)
	}
	if refsAfterRollback != 1 {
		t.Fatalf("chunk_block_refs after rollback = %d, want 1", refsAfterRollback)
	}

	// Commit path: metadata is removed in-order with no dangling refs.
	tx2, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	if err := SweepUnreachableChunks(context.Background(), tx2, containerID); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("SweepUnreachableChunks in tx2: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	var danglingRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk_block_refs cbr
		LEFT JOIN storage_blocks sb ON sb.id = cbr.block_id
		WHERE sb.id IS NULL
	`).Scan(&danglingRefs); err != nil {
		t.Fatalf("count dangling refs: %v", err)
	}
	if danglingRefs != 0 {
		t.Fatalf("dangling chunk_block_refs = %d, want 0", danglingRefs)
	}
}

func TestContainerHasLivePhysicalUnitsSupportsLegacyPackedAndMixed(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	legacyContainerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('legacy-only.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert legacy container: %v", err)
	}
	legacyContainerID, _ := legacyContainerRes.LastInsertId()

	packedContainerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('packed-only.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert packed container: %v", err)
	}
	packedContainerID, _ := packedContainerRes.LastInsertId()

	mixedContainerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('mixed.bin', 1, 0, 128, 1024)`)
	if err != nil {
		t.Fatalf("insert mixed container: %v", err)
	}
	mixedContainerID, _ := mixedContainerRes.LastInsertId()

	liveLegacyChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('legacy-live', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live legacy chunk: %v", err)
	}
	liveLegacyChunkID, _ := liveLegacyChunkRes.LastInsertId()

	livePackedChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-live', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live packed chunk: %v", err)
	}
	livePackedChunkID, _ := livePackedChunkRes.LastInsertId()

	liveMixedPackedChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-live-mixed', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live mixed packed chunk: %v", err)
	}
	liveMixedPackedChunkID, _ := liveMixedPackedChunkRes.LastInsertId()

	deadChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('dead-mixed', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}
	deadChunkID, _ := deadChunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, 64, 64, ?, 0)`, liveLegacyChunkID, legacyContainerID); err != nil {
		t.Fatalf("insert legacy-only block: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, 64, 64, ?, 0)`, deadChunkID, mixedContainerID); err != nil {
		t.Fatalf("insert mixed legacy dead block: %v", err)
	}

	packedOnlyBlockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 0, x'11')`, packedContainerID)
	if err != nil {
		t.Fatalf("insert packed-only block: %v", err)
	}
	packedOnlyBlockID, _ := packedOnlyBlockRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, livePackedChunkID, packedOnlyBlockID); err != nil {
		t.Fatalf("insert packed-only refs: %v", err)
	}

	mixedPackedBlockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 64, x'12')`, mixedContainerID)
	if err != nil {
		t.Fatalf("insert mixed packed block: %v", err)
	}
	mixedPackedBlockID, _ := mixedPackedBlockRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, liveMixedPackedChunkID, mixedPackedBlockID); err != nil {
		t.Fatalf("insert mixed packed refs: %v", err)
	}

	liveLegacyContainers, err := LoadLiveLegacyContainerIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLiveLegacyContainerIDs: %v", err)
	}
	livePackedBlocks, err := LoadLivePackedBlockIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLivePackedBlockIDs: %v", err)
	}
	liveUnits := livePhysicalUnits{
		LegacyLiveContainerIDs: liveLegacyContainers,
		PackedLiveBlockIDs:     livePackedBlocks,
	}

	hasLiveLegacy, err := containerHasLivePhysicalUnits(context.Background(), dbconn, legacyContainerID, liveUnits)
	if err != nil {
		t.Fatalf("containerHasLivePhysicalUnits legacy-only: %v", err)
	}
	if !hasLiveLegacy {
		t.Fatal("expected legacy-only container to be live")
	}

	hasLivePacked, err := containerHasLivePhysicalUnits(context.Background(), dbconn, packedContainerID, liveUnits)
	if err != nil {
		t.Fatalf("containerHasLivePhysicalUnits packed-only: %v", err)
	}
	if !hasLivePacked {
		t.Fatal("expected packed-only container to be live")
	}

	hasLiveMixed, err := containerHasLivePhysicalUnits(context.Background(), dbconn, mixedContainerID, liveUnits)
	if err != nil {
		t.Fatalf("containerHasLivePhysicalUnits mixed: %v", err)
	}
	if !hasLiveMixed {
		t.Fatal("expected mixed container to be live")
	}
}

func TestContainerHasLivePhysicalUnitsFalseWhenNoLiveLegacyOrPacked(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('dead-container.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	deadChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('dead-only', 64, 'COMPLETED', 0, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}
	deadChunkID, _ := deadChunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, 64, 64, ?, 0)`, deadChunkID, containerID); err != nil {
		t.Fatalf("insert dead legacy block: %v", err)
	}

	deadPackedBlockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 0, x'66')`, containerID)
	if err != nil {
		t.Fatalf("insert dead packed block: %v", err)
	}
	deadPackedBlockID, _ := deadPackedBlockRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, deadChunkID, deadPackedBlockID); err != nil {
		t.Fatalf("insert dead packed ref: %v", err)
	}

	liveLegacyContainers, err := LoadLiveLegacyContainerIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLiveLegacyContainerIDs: %v", err)
	}
	livePackedBlocks, err := LoadLivePackedBlockIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLivePackedBlockIDs: %v", err)
	}
	liveUnits := livePhysicalUnits{
		LegacyLiveContainerIDs: liveLegacyContainers,
		PackedLiveBlockIDs:     livePackedBlocks,
	}

	hasLiveUnits, err := containerHasLivePhysicalUnits(context.Background(), dbconn, containerID, liveUnits)
	if err != nil {
		t.Fatalf("containerHasLivePhysicalUnits: %v", err)
	}
	if hasLiveUnits {
		t.Fatal("expected container to be deletable (no live legacy refs and no live packed blocks)")
	}
}

func TestContainerHasLivePhysicalUnitsUsesLivePackedBlockIDsForContainerRetention(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES ('packed-retained-container.bin', 1, 0, 64, 1024)`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := containerRes.LastInsertId()

	liveChunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES ('packed-live-only', 64, 'COMPLETED', 1, 0, 'v2-fastcdc')`)
	if err != nil {
		t.Fatalf("insert live chunk: %v", err)
	}
	liveChunkID, _ := liveChunkRes.LastInsertId()

	packedBlockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'plain', 64, 64, ?, 0, x'77')`, containerID)
	if err != nil {
		t.Fatalf("insert packed block: %v", err)
	}
	packedBlockID, _ := packedBlockRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 64)`, liveChunkID, packedBlockID); err != nil {
		t.Fatalf("insert packed ref: %v", err)
	}

	// Build live units from source-of-truth loaders (equivalent to GC runtime behavior).
	liveLegacyContainers, err := LoadLiveLegacyContainerIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLiveLegacyContainerIDs: %v", err)
	}
	livePackedBlocks, err := LoadLivePackedBlockIDs(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("LoadLivePackedBlockIDs: %v", err)
	}
	liveUnits := livePhysicalUnits{
		LegacyLiveContainerIDs: liveLegacyContainers,
		PackedLiveBlockIDs:     livePackedBlocks,
	}

	hasLiveUnits, err := containerHasLivePhysicalUnits(context.Background(), dbconn, containerID, liveUnits)
	if err != nil {
		t.Fatalf("containerHasLivePhysicalUnits: %v", err)
	}
	if !hasLiveUnits {
		t.Fatal("expected container retained because it contains a live storage_block")
	}
}
