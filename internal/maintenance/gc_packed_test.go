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
