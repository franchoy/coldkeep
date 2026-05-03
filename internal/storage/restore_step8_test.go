package storage

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

type restoreChunkSeed struct {
	id      int64
	payload []byte
}

func setupStep8DB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func insertChunkForRestore(t *testing.T, dbconn *sql.DB, payload []byte, chunkerVersion string) restoreChunkSeed {
	t.Helper()
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, $4)
		 RETURNING id`,
		hash,
		int64(len(payload)),
		filestate.ChunkCompleted,
		chunkerVersion,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	return restoreChunkSeed{id: chunkID, payload: payload}
}

func insertLogicalFileForRestore(t *testing.T, dbconn *sql.DB, originalName string, chunks []restoreChunkSeed, chunkerVersion string) int64 {
	t.Helper()
	all := make([]byte, 0)
	for _, c := range chunks {
		all = append(all, c.payload...)
	}
	sum := sha256.Sum256(all)
	fileHash := hex.EncodeToString(sum[:])

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		originalName,
		int64(len(all)),
		fileHash,
		filestate.LogicalFileCompleted,
		chunkerVersion,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	for i, c := range chunks {
		if _, err := dbconn.Exec(
			`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
			 VALUES ($1, $2, $3)`,
			fileID,
			c.id,
			i,
		); err != nil {
			t.Fatalf("insert file_chunk %d: %v", i, err)
		}
	}

	return fileID
}

func insertContainerWithPayload(t *testing.T, dbconn *sql.DB, containersDir, filename string, payload []byte) int64 {
	t.Helper()
	containerPath := filepath.Join(containersDir, filename)
	if err := writeReusableTestContainerFileWithPayload(containerPath, payload); err != nil {
		t.Fatalf("write container payload %s: %v", filename, err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, TRUE)
		 RETURNING id`,
		filename,
		int64(container.ContainerHdrLen+len(payload)),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container %s: %v", filename, err)
	}

	return containerID
}

func insertLegacyBlocksRows(t *testing.T, dbconn *sql.DB, containerID int64, chunks []restoreChunkSeed) {
	t.Helper()
	for _, c := range chunks {
		if _, err := dbconn.Exec(
			`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
			 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
			c.id,
			int64(len(c.payload)),
			int64(len(c.payload)),
			[]byte{},
			containerID,
			int64(container.ContainerHdrLen),
		); err != nil {
			t.Fatalf("insert legacy blocks row for chunk %d: %v", c.id, err)
		}
	}
}

type packedBlockFixture struct {
	blockID int64
}

func insertPackedStorageBlock(t *testing.T, dbconn *sql.DB, containersDir, filename string, chunks []restoreChunkSeed) packedBlockFixture {
	t.Helper()

	packed := make([]blocks.PackedChunk, 0, len(chunks))
	for _, c := range chunks {
		packed = append(packed, blocks.PackedChunk{ChunkID: uint64(c.id), Data: c.payload})
	}

	encoded, err := blocks.EncodePackedBlockV1FromChunks(packed)
	if err != nil {
		t.Fatalf("encode packed block: %v", err)
	}

	containerID := insertContainerWithPayload(t, dbconn, containersDir, filename, encoded.Bytes)

	var blockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (
			format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id`,
		1,
		"plain",
		int64(len(encoded.Bytes)),
		int64(len(encoded.Bytes)),
		containerID,
		int64(container.ContainerHdrLen),
		encoded.BlockHash,
	).Scan(&blockID); err != nil {
		t.Fatalf("insert storage_blocks row: %v", err)
	}

	for i, c := range chunks {
		entry := encoded.Entries[i]
		if _, err := dbconn.Exec(
			`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
			 VALUES ($1, $2, $3, $4)`,
			c.id,
			blockID,
			int64(entry.Offset),
			int64(entry.Size),
		); err != nil {
			t.Fatalf("insert chunk_block_refs for chunk %d: %v", c.id, err)
		}

		// Keep legacy metadata rows present because pin/load recipe still joins blocks table.
		if _, err := dbconn.Exec(
			`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
			 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
			c.id,
			int64(len(c.payload)),
			int64(len(c.payload)),
			[]byte{},
			containerID,
			int64(container.ContainerHdrLen),
		); err != nil {
			t.Fatalf("insert companion blocks row for packed chunk %d: %v", c.id, err)
		}
	}

	return packedBlockFixture{blockID: blockID}
}

func restoreAndReadBytes(t *testing.T, dbconn *sql.DB, fileID int64, containersDir string) []byte {
	t.Helper()
	outPath := filepath.Join(t.TempDir(), fmt.Sprintf("restore-%d.bin", fileID))
	if _, err := restoreFileWithDBAndDir(dbconn, fileID, outPath, containersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored output file %d: %v", fileID, err)
	}
	return got
}

// Step 8 / Test 1: v1.7 repo restore must stay byte-identical with no regression.
func TestStep8RestoreV17RepoNoRegression(t *testing.T) {
	dbconn := setupStep8DB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()

	payloadA := []byte("legacy-v17-part-a-")
	payloadB := []byte("legacy-v17-part-b")
	chunks := []restoreChunkSeed{
		insertChunkForRestore(t, dbconn, payloadA, "v1-simple-rolling"),
		insertChunkForRestore(t, dbconn, payloadB, "v1-simple-rolling"),
	}

	containerIDa := insertContainerWithPayload(t, dbconn, containersDir, "legacy-a.bin", payloadA)
	containerIDb := insertContainerWithPayload(t, dbconn, containersDir, "legacy-b.bin", payloadB)
	insertLegacyBlocksRows(t, dbconn, containerIDa, []restoreChunkSeed{chunks[0]})
	insertLegacyBlocksRows(t, dbconn, containerIDb, []restoreChunkSeed{chunks[1]})

	fileID := insertLogicalFileForRestore(t, dbconn, "legacy-v17.bin", chunks, "v1-simple-rolling")

	got := restoreAndReadBytes(t, dbconn, fileID, containersDir)
	want := append(append([]byte{}, payloadA...), payloadB...)
	if !bytes.Equal(got, want) {
		t.Fatalf("v1.7 restore regression: got=%q want=%q", string(got), string(want))
	}
}

// Step 8 / Test 2: synthetic packed block (manual metadata insertion) restores correctly.
func TestStep8RestoreSyntheticPackedBlock(t *testing.T) {
	dbconn := setupStep8DB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()

	payloadA := []byte("packed-synthetic-A")
	payloadB := []byte("packed-synthetic-BB")
	chunks := []restoreChunkSeed{
		insertChunkForRestore(t, dbconn, payloadA, "v2-fastcdc"),
		insertChunkForRestore(t, dbconn, payloadB, "v2-fastcdc"),
	}

	insertPackedStorageBlock(t, dbconn, containersDir, "packed-synth.bin", chunks)
	fileID := insertLogicalFileForRestore(t, dbconn, "packed-synth.bin", chunks, "v2-fastcdc")

	got := restoreAndReadBytes(t, dbconn, fileID, containersDir)
	want := append(append([]byte{}, payloadA...), payloadB...)
	if !bytes.Equal(got, want) {
		t.Fatalf("packed synthetic restore mismatch: got=%q want=%q", string(got), string(want))
	}
}

// Step 8 / Test 3: mixed repository (legacy chunks + packed blocks) restores both files correctly.
func TestStep8RestoreMixedRepoV17AndV18(t *testing.T) {
	dbconn := setupStep8DB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()

	// v1.7 legacy file
	legacyPayload := []byte("mixed-legacy-payload")
	legacyChunk := insertChunkForRestore(t, dbconn, legacyPayload, "v1-simple-rolling")
	legacyContainerID := insertContainerWithPayload(t, dbconn, containersDir, "mixed-legacy.bin", legacyPayload)
	insertLegacyBlocksRows(t, dbconn, legacyContainerID, []restoreChunkSeed{legacyChunk})
	legacyFileID := insertLogicalFileForRestore(t, dbconn, "mixed-legacy-file.bin", []restoreChunkSeed{legacyChunk}, "v1-simple-rolling")

	// v1.8 packed file
	packedA := []byte("mixed-packed-A")
	packedB := []byte("mixed-packed-B")
	packedChunks := []restoreChunkSeed{
		insertChunkForRestore(t, dbconn, packedA, "v2-fastcdc"),
		insertChunkForRestore(t, dbconn, packedB, "v2-fastcdc"),
	}
	insertPackedStorageBlock(t, dbconn, containersDir, "mixed-packed.bin", packedChunks)
	packedFileID := insertLogicalFileForRestore(t, dbconn, "mixed-packed-file.bin", packedChunks, "v2-fastcdc")

	legacyGot := restoreAndReadBytes(t, dbconn, legacyFileID, containersDir)
	if !bytes.Equal(legacyGot, legacyPayload) {
		t.Fatalf("mixed repo legacy restore mismatch: got=%q want=%q", string(legacyGot), string(legacyPayload))
	}

	packedGot := restoreAndReadBytes(t, dbconn, packedFileID, containersDir)
	packedWant := append(append([]byte{}, packedA...), packedB...)
	if !bytes.Equal(packedGot, packedWant) {
		t.Fatalf("mixed repo packed restore mismatch: got=%q want=%q", string(packedGot), string(packedWant))
	}
}

// Step 8 / Test 4: repeated block usage must hit cache and avoid duplicate block reads.
func TestStep8RestoreRepeatedBlockUsageUsesCache(t *testing.T) {
	dbconn := setupStep8DB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()

	// Non-contiguous usage pattern:
	// chunk0 -> block1, chunk1 -> block2, chunk2 -> block1
	chunk0 := insertChunkForRestore(t, dbconn, []byte("repeat-A"), "v2-fastcdc")
	chunk1 := insertChunkForRestore(t, dbconn, []byte("repeat-B"), "v2-fastcdc")
	chunk2 := insertChunkForRestore(t, dbconn, []byte("repeat-C"), "v2-fastcdc")

	insertPackedStorageBlock(t, dbconn, containersDir, "repeat-block1.bin", []restoreChunkSeed{chunk0, chunk2})
	insertPackedStorageBlock(t, dbconn, containersDir, "repeat-block2.bin", []restoreChunkSeed{chunk1})

	fileID := insertLogicalFileForRestore(t, dbconn, "repeat-cache.bin", []restoreChunkSeed{chunk0, chunk1, chunk2}, "v2-fastcdc")

	hookCalls := 0
	hookChunkIDs := make([]int64, 0, 4)
	TestRestoreBeforeChunkReadHook = func(_ *sql.DB, chunkID int64) error {
		hookCalls++
		hookChunkIDs = append(hookChunkIDs, chunkID)
		return nil
	}
	defer func() { TestRestoreBeforeChunkReadHook = nil }()

	got := restoreAndReadBytes(t, dbconn, fileID, containersDir)
	want := []byte("repeat-Arepeat-Brepeat-C")
	if !bytes.Equal(got, want) {
		t.Fatalf("repeated-block restore mismatch: got=%q want=%q", string(got), string(want))
	}

	if hookCalls != 2 {
		t.Fatalf("expected 2 block read misses (block1 + block2), got %d", hookCalls)
	}
	if len(hookChunkIDs) != 2 {
		t.Fatalf("expected 2 hook chunk IDs, got %d", len(hookChunkIDs))
	}
	if hookChunkIDs[0] != chunk0.id || hookChunkIDs[1] != chunk1.id {
		t.Fatalf("unexpected cache-miss read order: got=%v want=[%d %d]", hookChunkIDs, chunk0.id, chunk1.id)
	}
}

// Step 8 / Test 5: corrupted packed block should fail cleanly with decode error.
func TestStep8RestoreCorruptedPackedBlockFailsCleanly(t *testing.T) {
	dbconn := setupStep8DB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()

	payload := []byte("corrupted-packed-chunk")
	seed := insertChunkForRestore(t, dbconn, payload, "v2-fastcdc")

	// Corrupted block bytes: not valid block format payload.
	corruptedBytes := []byte("not-a-valid-v18-block-format")
	containerID := insertContainerWithPayload(t, dbconn, containersDir, "corrupted-packed.bin", corruptedBytes)

	var storageBlockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (
			format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id`,
		1,
		"plain",
		int64(len(corruptedBytes)),
		int64(len(corruptedBytes)),
		containerID,
		int64(container.ContainerHdrLen),
		blocks.ComputeBlockHash(corruptedBytes),
	).Scan(&storageBlockID); err != nil {
		t.Fatalf("insert corrupted storage block: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, 0, $3)`,
		seed.id,
		storageBlockID,
		int64(len(payload)),
	); err != nil {
		t.Fatalf("insert chunk_block_refs for corrupted block: %v", err)
	}

	// Companion legacy metadata row required by pin/load query.
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		seed.id,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert companion blocks row for corrupted case: %v", err)
	}

	fileID := insertLogicalFileForRestore(t, dbconn, "corrupted-packed.bin", []restoreChunkSeed{seed}, "v2-fastcdc")
	outPath := filepath.Join(t.TempDir(), "corrupted-out.bin")
	_, err := restoreFileWithDBAndDir(dbconn, fileID, outPath, containersDir, RestoreOptions{Overwrite: true})
	if err == nil {
		t.Fatalf("expected restore failure for corrupted packed block")
	}
	if !strings.Contains(err.Error(), "decode block") {
		t.Fatalf("expected decode block failure, got: %v", err)
	}
}
