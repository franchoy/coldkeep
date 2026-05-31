package maintenance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	"github.com/franchoy/coldkeep/tests/testdb"
)

func step11InsertLogicalFileChunkAndPhysicalPath(t *testing.T, dbconn *sql.DB, name string, payload []byte, physicalPath string) (int64, int64) {
	t.Helper()

	h := sha256.Sum256(payload)
	hashHex := hex.EncodeToString(h[:])

	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version)
		VALUES ($1, $2, $3, 1, 'COMPLETED', 'v2-fastcdc')
		RETURNING id
	`, name, int64(len(payload)), hashHex).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file %s: %v", name, err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 1, 0, 'v2-fastcdc')
		RETURNING id
	`, hashHex, int64(len(payload))).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk for %s: %v", name, err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk for %s: %v", name, err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
		VALUES ($1, $2, FALSE)
	`, physicalPath, logicalID); err != nil {
		t.Fatalf("insert physical_file for %s: %v", name, err)
	}

	return logicalID, chunkID
}

func step11InsertStandaloneDeadChunk(t *testing.T, dbconn *sql.DB, hash string, size int64) int64 {
	t.Helper()

	var chunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id
	`, hash, size).Scan(&chunkID); err != nil {
		t.Fatalf("insert dead chunk %s: %v", hash, err)
	}

	return chunkID
}

func step11InsertSealedContainerWithPayload(t *testing.T, dbconn *sql.DB, containersDir, filename string, payload []byte) int64 {
	t.Helper()

	containerPath := filepath.Join(containersDir, filename)
	if err := writeTestContainerFileWithPayload(containerPath, payload); err != nil {
		t.Fatalf("write container file %s: %v", filename, err)
	}

	var containerID int64
	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, TRUE, FALSE)
		RETURNING id
	`, filename, int64(container.ContainerHdrLen+len(payload)), container.GetContainerMaxSize()).Scan(&containerID); err != nil {
		t.Fatalf("insert container %s: %v", filename, err)
	}

	return containerID
}

func step11EncodePackedBlockV1(t *testing.T, chunkIDs []int64, chunkPayloads ...[]byte) ([]byte, []byte) {
	t.Helper()

	if len(chunkIDs) == 0 || len(chunkIDs) != len(chunkPayloads) {
		t.Fatalf("invalid packed block fixture shape: chunkIDs=%d payloads=%d", len(chunkIDs), len(chunkPayloads))
	}

	entries := make([]blocks.ChunkEntry, 0, len(chunkIDs))
	payload := make([]byte, 0)
	offset := uint64(0)
	for i, id := range chunkIDs {
		chunk := chunkPayloads[i]
		entries = append(entries, blocks.ChunkEntry{
			ChunkID: uint64(id),
			Offset:  offset,
			Size:    uint64(len(chunk)),
		})
		payload = append(payload, chunk...)
		offset += uint64(len(chunk))
	}

	encoded, err := blocks.EncodePackedBlockV1(entries, payload)
	if err != nil {
		t.Fatalf("encode packed block fixture: %v", err)
	}

	return encoded.Bytes, encoded.BlockHash
}

func step11InsertLegacyCompanionRows(t *testing.T, dbconn *sql.DB, containerID, containerOffset int64, encodedLen int, chunkIDs []int64, chunkPayloads ...[]byte) {
	t.Helper()

	if len(chunkIDs) == 0 || len(chunkIDs) != len(chunkPayloads) {
		t.Fatalf("invalid legacy companion fixture shape: chunkIDs=%d payloads=%d", len(chunkIDs), len(chunkPayloads))
	}

	var totalChunkBytes int64
	for _, payload := range chunkPayloads {
		totalChunkBytes += int64(len(payload))
	}

	payloadPrefix := int64(encodedLen) - totalChunkBytes
	if payloadPrefix < 0 {
		t.Fatalf("invalid encoded block length: encoded=%d total_chunks=%d", encodedLen, totalChunkBytes)
	}

	offsetInPayload := int64(0)
	for i, chunkID := range chunkIDs {
		chunkSize := int64(len(chunkPayloads[i]))
		legacyOffset := containerOffset + payloadPrefix + offsetInPayload
		if _, err := dbconn.Exec(`
			INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
			VALUES ($1, 'plain', 1, $2, $3, $4, $5)
		`, chunkID, chunkSize, chunkSize, containerID, legacyOffset); err != nil {
			t.Fatalf("insert legacy companion block for chunk %d: %v", chunkID, err)
		}
		offsetInPayload += chunkSize
	}
}

func TestStep11PackedOnlyDeadBlockDeletionRunGCAndVerify(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()

	deadPayload := []byte("step11-packed-only-dead")

	deadHash := sha256.Sum256(deadPayload)
	deadChunkID := step11InsertStandaloneDeadChunk(t, dbconn, hex.EncodeToString(deadHash[:]), int64(len(deadPayload)))
	deadEncoded, deadBlockHash := step11EncodePackedBlockV1(t, []int64{deadChunkID}, deadPayload)

	containerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-packed-only-dead.bin", deadEncoded)

	var blockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, physical_hash)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6)
		RETURNING id
	`, int64(len(deadEncoded)), int64(len(deadEncoded)), containerID, int64(container.ContainerHdrLen), deadBlockHash, blocks.HashPhysical(deadEncoded)).Scan(&blockID); err != nil {
		t.Fatalf("insert storage_block: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		VALUES ($1, $2, 0, $3)
	`, deadChunkID, blockID, int64(len(deadPayload))); err != nil {
		t.Fatalf("insert chunk_block_ref: %v", err)
	}
	step11InsertLegacyCompanionRows(t, dbconn, containerID, int64(container.ContainerHdrLen), len(deadEncoded), []int64{deadChunkID}, deadPayload)

	result, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("RunGCWithContainersDirResult: %v", gcErr)
	}
	if result.AffectedContainers != 1 {
		t.Fatalf("AffectedContainers = %d, want 1", result.AffectedContainers)
	}

	var storageBlockRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, blockID).Scan(&storageBlockRows); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if storageBlockRows != 0 {
		t.Fatalf("storage_blocks rows = %d, want 0", storageBlockRows)
	}

	var refRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = $1`, blockID).Scan(&refRows); err != nil {
		t.Fatalf("count chunk_block_refs: %v", err)
	}
	if refRows != 0 {
		t.Fatalf("chunk_block_refs rows = %d, want 0", refRows)
	}

	if err := verify.VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("verify after GC: %v", err)
	}
}

func TestStep11PackedBlockPartiallyLiveRetainedRestoreAndVerify(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()

	c1Payload := []byte("step11-c1-live")
	c2Payload := []byte("step11-c2-dead")
	c3Payload := []byte("step11-c3-dead")

	logicalC1ID, c1ChunkID := step11InsertLogicalFileChunkAndPhysicalPath(t, dbconn, "step11-c1.bin", c1Payload, "/step11/c1.bin")
	h2 := sha256.Sum256(c2Payload)
	h3 := sha256.Sum256(c3Payload)
	c2ChunkID := step11InsertStandaloneDeadChunk(t, dbconn, hex.EncodeToString(h2[:]), int64(len(c2Payload)))
	c3ChunkID := step11InsertStandaloneDeadChunk(t, dbconn, hex.EncodeToString(h3[:]), int64(len(c3Payload)))
	encodedBlock, blockHash := step11EncodePackedBlockV1(t, []int64{c1ChunkID, c2ChunkID, c3ChunkID}, c1Payload, c2Payload, c3Payload)

	containerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-packed-partially-live.bin", encodedBlock)

	var blockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, physical_hash)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6)
		RETURNING id
	`, int64(len(encodedBlock)), int64(len(encodedBlock)), containerID, int64(container.ContainerHdrLen), blockHash, blocks.HashPhysical(encodedBlock)).Scan(&blockID); err != nil {
		t.Fatalf("insert storage_block: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, $2, 0, $3)`, c1ChunkID, blockID, int64(len(c1Payload))); err != nil {
		t.Fatalf("insert c1 ref: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, $2, $3, $4)`, c2ChunkID, blockID, int64(len(c1Payload)), int64(len(c2Payload))); err != nil {
		t.Fatalf("insert c2 ref: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, $2, $3, $4)`, c3ChunkID, blockID, int64(len(c1Payload)+len(c2Payload)), int64(len(c3Payload))); err != nil {
		t.Fatalf("insert c3 ref: %v", err)
	}
	step11InsertLegacyCompanionRows(
		t,
		dbconn,
		containerID,
		int64(container.ContainerHdrLen),
		len(encodedBlock),
		[]int64{c1ChunkID, c2ChunkID, c3ChunkID},
		c1Payload,
		c2Payload,
		c3Payload,
	)

	result, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("RunGCWithContainersDirResult: %v", gcErr)
	}
	if result.AffectedContainers != 0 {
		t.Fatalf("AffectedContainers = %d, want 0", result.AffectedContainers)
	}

	var blockRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, blockID).Scan(&blockRows); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if blockRows != 1 {
		t.Fatalf("storage_blocks rows = %d, want 1", blockRows)
	}

	var blockRefRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = $1`, blockID).Scan(&blockRefRows); err != nil {
		t.Fatalf("count chunk_block_refs: %v", err)
	}
	if blockRefRows != 3 {
		t.Fatalf("chunk_block_refs rows = %d, want 3", blockRefRows)
	}

	outPath := filepath.Join(t.TempDir(), "step11-c1-restored.bin")
	sgctx := storage.StorageContext{DB: dbconn, ContainerDir: containersDir}
	if err := storage.RestoreFileWithStorageContext(sgctx, logicalC1ID, outPath); err != nil {
		t.Fatalf("restore c1: %v", err)
	}
	restored, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored c1: %v", err)
	}
	if string(restored) != string(c1Payload) {
		t.Fatalf("restored c1 payload mismatch: got %q want %q", string(restored), string(c1Payload))
	}

	if err := verify.VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("verify after GC: %v", err)
	}
}

func TestStep11SnapshotRetainsPackedBlockAndRestoreSucceeds(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()

	payload := []byte("step11-snapshot-packed-retained")

	logicalID, chunkID := step11InsertLogicalFileChunkAndPhysicalPath(t, dbconn, "step11-snapshot-packed.bin", payload, "/step11/snapshot-packed.bin")
	encodedBlock, blockHash := step11EncodePackedBlockV1(t, []int64{chunkID}, payload)

	containerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-snapshot-packed.bin", encodedBlock)

	var blockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, physical_hash)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6)
		RETURNING id
	`, int64(len(encodedBlock)), int64(len(encodedBlock)), containerID, int64(container.ContainerHdrLen), blockHash, blocks.HashPhysical(encodedBlock)).Scan(&blockID); err != nil {
		t.Fatalf("insert storage_block: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		VALUES ($1, $2, 0, $3)
	`, chunkID, blockID, int64(len(payload))); err != nil {
		t.Fatalf("insert chunk_block_ref: %v", err)
	}
	step11InsertLegacyCompanionRows(t, dbconn, containerID, int64(container.ContainerHdrLen), len(encodedBlock), []int64{chunkID}, payload)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('step11-snapshot-packed', NOW(), 'full')`); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "step11-snapshot-packed", "snap/step11-snapshot-packed.bin", logicalID)

	if _, err := dbconn.Exec(`DELETE FROM physical_file WHERE logical_file_id = $1`, logicalID); err != nil {
		t.Fatalf("delete physical_file rows: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = 0 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("set logical ref_count to 0: %v", err)
	}

	result, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("RunGCWithContainersDirResult: %v", gcErr)
	}
	if result.AffectedContainers != 0 {
		t.Fatalf("AffectedContainers = %d, want 0", result.AffectedContainers)
	}

	var blockRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, blockID).Scan(&blockRows); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if blockRows != 1 {
		t.Fatalf("storage_blocks rows = %d, want 1", blockRows)
	}

	restoreDir := t.TempDir()
	sgctx := storage.StorageContext{DB: dbconn, ContainerDir: containersDir}
	restoreResult, err := snapshot.RestoreSnapshot(context.Background(), dbconn, "step11-snapshot-packed", nil, snapshot.RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationPrefix,
		Destination:     restoreDir,
		Overwrite:       true,
		NoMetadata:      true,
		StorageContext:  &sgctx,
	})
	if err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if restoreResult.RestoredFiles != 1 {
		t.Fatalf("RestoredFiles = %d, want 1", restoreResult.RestoredFiles)
	}

	restoreTarget := filepath.Join(restoreDir, "snap", "step11-snapshot-packed.bin")
	restored, err := os.ReadFile(restoreTarget)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("restored payload mismatch: got %q want %q", string(restored), string(payload))
	}
}

func TestStep11MixedLegacyAndPackedRepoRestoreRemainingAndVerify(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()

	legacyKeepPayload := []byte("step11-legacy-keep")
	legacyDropPayload := []byte("step11-legacy-drop")
	packedKeepPayload := []byte("step11-packed-keep")
	packedDropPayload := []byte("step11-packed-drop")

	legacyKeepLogicalID, legacyKeepChunkID := step11InsertLogicalFileChunkAndPhysicalPath(t, dbconn, "legacy-keep.bin", legacyKeepPayload, "/step11/legacy-keep.bin")
	packedKeepLogicalID, packedKeepChunkID := step11InsertLogicalFileChunkAndPhysicalPath(t, dbconn, "packed-keep.bin", packedKeepPayload, "/step11/packed-keep.bin")

	hLegacyDrop := sha256.Sum256(legacyDropPayload)
	hPackedDrop := sha256.Sum256(packedDropPayload)
	legacyDropChunkID := step11InsertStandaloneDeadChunk(t, dbconn, hex.EncodeToString(hLegacyDrop[:]), int64(len(legacyDropPayload)))
	packedDropChunkID := step11InsertStandaloneDeadChunk(t, dbconn, hex.EncodeToString(hPackedDrop[:]), int64(len(packedDropPayload)))

	legacyKeepContainerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-legacy-keep.bin", legacyKeepPayload)
	legacyDropContainerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-legacy-drop.bin", legacyDropPayload)
	packedKeepEncoded, keepBlockHash := step11EncodePackedBlockV1(t, []int64{packedKeepChunkID}, packedKeepPayload)
	packedDropEncoded, dropBlockHash := step11EncodePackedBlockV1(t, []int64{packedDropChunkID}, packedDropPayload)
	packedKeepContainerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-packed-keep.bin", packedKeepEncoded)
	packedDropContainerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-packed-drop.bin", packedDropEncoded)

	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		VALUES ($1, 'plain', 1, $2, $3, $4, $5)
	`, legacyKeepChunkID, int64(len(legacyKeepPayload)), int64(len(legacyKeepPayload)), legacyKeepContainerID, int64(container.ContainerHdrLen)); err != nil {
		t.Fatalf("insert legacy keep block: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		VALUES ($1, 'plain', 1, $2, $3, $4, $5)
	`, legacyDropChunkID, int64(len(legacyDropPayload)), int64(len(legacyDropPayload)), legacyDropContainerID, int64(container.ContainerHdrLen)); err != nil {
		t.Fatalf("insert legacy drop block: %v", err)
	}

	var packedKeepBlockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, physical_hash)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6)
		RETURNING id
	`, int64(len(packedKeepEncoded)), int64(len(packedKeepEncoded)), packedKeepContainerID, int64(container.ContainerHdrLen), keepBlockHash, blocks.HashPhysical(packedKeepEncoded)).Scan(&packedKeepBlockID); err != nil {
		t.Fatalf("insert packed keep storage_block: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		VALUES ($1, $2, 0, $3)
	`, packedKeepChunkID, packedKeepBlockID, int64(len(packedKeepPayload))); err != nil {
		t.Fatalf("insert packed keep ref: %v", err)
	}
	step11InsertLegacyCompanionRows(t, dbconn, packedKeepContainerID, int64(container.ContainerHdrLen), len(packedKeepEncoded), []int64{packedKeepChunkID}, packedKeepPayload)

	var packedDropBlockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, physical_hash)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6)
		RETURNING id
	`, int64(len(packedDropEncoded)), int64(len(packedDropEncoded)), packedDropContainerID, int64(container.ContainerHdrLen), dropBlockHash, blocks.HashPhysical(packedDropEncoded)).Scan(&packedDropBlockID); err != nil {
		t.Fatalf("insert packed drop storage_block: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		VALUES ($1, $2, 0, $3)
	`, packedDropChunkID, packedDropBlockID, int64(len(packedDropPayload))); err != nil {
		t.Fatalf("insert packed drop ref: %v", err)
	}
	step11InsertLegacyCompanionRows(t, dbconn, packedDropContainerID, int64(container.ContainerHdrLen), len(packedDropEncoded), []int64{packedDropChunkID}, packedDropPayload)

	gcResult, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("RunGCWithContainersDirResult: %v", gcErr)
	}
	if gcResult.AffectedContainers != 2 {
		t.Fatalf("AffectedContainers = %d, want 2 (legacy drop + packed drop)", gcResult.AffectedContainers)
	}

	sgctx := storage.StorageContext{DB: dbconn, ContainerDir: containersDir}
	legacyOut := filepath.Join(t.TempDir(), "legacy-keep-restored.bin")
	if err := storage.RestoreFileWithStorageContext(sgctx, legacyKeepLogicalID, legacyOut); err != nil {
		t.Fatalf("restore legacy keep: %v", err)
	}
	legacyRestored, err := os.ReadFile(legacyOut)
	if err != nil {
		t.Fatalf("read legacy restored: %v", err)
	}
	if string(legacyRestored) != string(legacyKeepPayload) {
		t.Fatalf("legacy restored payload mismatch: got %q want %q", string(legacyRestored), string(legacyKeepPayload))
	}

	packedOut := filepath.Join(t.TempDir(), "packed-keep-restored.bin")
	if err := storage.RestoreFileWithStorageContext(sgctx, packedKeepLogicalID, packedOut); err != nil {
		t.Fatalf("restore packed keep: %v", err)
	}
	packedRestored, err := os.ReadFile(packedOut)
	if err != nil {
		t.Fatalf("read packed restored: %v", err)
	}
	if string(packedRestored) != string(packedKeepPayload) {
		t.Fatalf("packed restored payload mismatch: got %q want %q", string(packedRestored), string(packedKeepPayload))
	}

	if err := verify.VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("verify after mixed GC: %v", err)
	}
}

func TestStep11ContainerWithMixedPhysicalUnitsRetainedWhileEitherKindLive(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()

	legacyPayload := []byte("step11-mixed-legacy")
	packedPayload := []byte("step11-mixed-packed")

	hLegacy := sha256.Sum256(legacyPayload)
	hPacked := sha256.Sum256(packedPayload)

	var legacyChunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 1, 0, 'v2-fastcdc')
		RETURNING id
	`, hex.EncodeToString(hLegacy[:]), int64(len(legacyPayload))).Scan(&legacyChunkID); err != nil {
		t.Fatalf("insert legacy chunk: %v", err)
	}

	var packedChunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id
	`, hex.EncodeToString(hPacked[:]), int64(len(packedPayload))).Scan(&packedChunkID); err != nil {
		t.Fatalf("insert packed chunk: %v", err)
	}

	packedEncoded, packedBlockHash := step11EncodePackedBlockV1(t, []int64{packedChunkID}, packedPayload)
	containerPayload := append(append([]byte{}, legacyPayload...), packedEncoded...)

	containerID := step11InsertSealedContainerWithPayload(t, dbconn, containersDir, "step11-mixed-units.bin", containerPayload)

	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		VALUES ($1, 'plain', 1, $2, $3, $4, $5)
	`, legacyChunkID, int64(len(legacyPayload)), int64(len(legacyPayload)), containerID, int64(container.ContainerHdrLen)); err != nil {
		t.Fatalf("insert legacy block: %v", err)
	}

	var packedBlockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, physical_hash)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6)
		RETURNING id
	`, int64(len(packedEncoded)), int64(len(packedEncoded)), containerID, int64(container.ContainerHdrLen+len(legacyPayload)), packedBlockHash, blocks.HashPhysical(packedEncoded)).Scan(&packedBlockID); err != nil {
		t.Fatalf("insert packed block: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		VALUES ($1, $2, 0, $3)
	`, packedChunkID, packedBlockID, int64(len(packedPayload))); err != nil {
		t.Fatalf("insert packed ref: %v", err)
	}
	step11InsertLegacyCompanionRows(
		t,
		dbconn,
		containerID,
		int64(container.ContainerHdrLen+len(legacyPayload)),
		len(packedEncoded),
		[]int64{packedChunkID},
		packedPayload,
	)

	resultA, err := RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("GC phase A: %v", err)
	}
	if resultA.AffectedContainers != 0 {
		t.Fatalf("phase A affected containers = %d, want 0", resultA.AffectedContainers)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = 0 WHERE id = $1`, legacyChunkID); err != nil {
		t.Fatalf("set legacy chunk dead: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = 1 WHERE id = $1`, packedChunkID); err != nil {
		t.Fatalf("set packed chunk live: %v", err)
	}

	resultB, err := RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("GC phase B: %v", err)
	}
	if resultB.AffectedContainers != 0 {
		t.Fatalf("phase B affected containers = %d, want 0", resultB.AffectedContainers)
	}

	var containerRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&containerRows); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if containerRows != 1 {
		t.Fatalf("container rows = %d, want 1", containerRows)
	}
}
