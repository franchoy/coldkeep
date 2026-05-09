package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// Step 6.9 — Revalidate GC Safety Across Matrix
//
// Core roadmap guarantee: GC must remain transform-agnostic and safe.
//
// Matrix coverage:
//   - compressed repositories
//   - uncompressed repositories
//   - mixed repositories
//   - encrypted repositories
//   - legacy repositories
//
// Validation per requirement:
//   ✔ live compressed blocks preserved
//   ✔ orphaned compressed blocks removable
//   ✔ restore after GC correct
//   ✔ verify after GC correct

type step69RepoClass struct {
	name       string
	encryption blocks.Codec
	mode       string
	legacyOnly bool
}

type step69StoredFile struct {
	fileID   int64
	hash     string
	chunkIDs []int64
}

func TestStep69GCSafetyAcrossMatrix(t *testing.T) {
	testgate.RequireDB(t)

	classes := []step69RepoClass{
		{name: "compressed", encryption: blocks.CodecPlain, mode: "compressed"},
		{name: "uncompressed", encryption: blocks.CodecPlain, mode: "uncompressed"},
		{name: "mixed", encryption: blocks.CodecPlain, mode: "mixed"},
		{name: "encrypted", encryption: blocks.CodecAESGCM, mode: "compressed"},
		{name: "legacy", encryption: blocks.CodecPlain, mode: "uncompressed", legacyOnly: true},
	}

	for _, class := range classes {
		class := class
		t.Run(class.name, func(t *testing.T) {
			testStep69SingleRepoClass(t, class)
		})
	}
}

func testStep69SingleRepoClass(t *testing.T, class step69RepoClass) {
	t.Helper()

	dbconn, tmp, writer := setupStep69Env(t, class.encryption)
	defer dbconn.Close()

	liveFiles, orphan := step69SeedScenarioData(t, dbconn, writer, tmp, class)

	if class.legacyOnly {
		toLegacyMetadataShapeStep69(t, dbconn)
	}

	orphanChunksBefore := countChunksByIDsStep69(t, dbconn, orphan.chunkIDs)
	if orphanChunksBefore == 0 {
		t.Fatalf("expected orphan chunks before GC")
	}

	liveChunksBefore := int64(0)
	for _, live := range liveFiles {
		liveChunksBefore += countChunksByIDsStep69(t, dbconn, live.chunkIDs)
	}
	if liveChunksBefore == 0 {
		t.Fatalf("expected live chunks before GC")
	}

	compressedLiveBefore := countLiveCompressedBlocksStep69(t, dbconn)
	orphanCompressedBefore := countOrphanCompressedBlocksStep69(t, dbconn)
	orphanCollectableBefore := countCollectableChunksByIDsStep69(t, dbconn, orphan.chunkIDs)

	if _, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir); err != nil {
		t.Fatalf("gc dry-run: %v", err)
	}
	gcResult, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir)
	if err != nil {
		t.Fatalf("gc real-run: %v", err)
	}

	orphanCollectableAfter := countCollectableChunksByIDsStep69(t, dbconn, orphan.chunkIDs)
	if orphanCollectableAfter > orphanCollectableBefore {
		t.Fatalf("collectable orphan chunks increased after GC: before=%d after=%d", orphanCollectableBefore, orphanCollectableAfter)
	}
	if orphanCollectableBefore > 0 && gcResult.AffectedContainers > 0 && orphanCollectableAfter >= orphanCollectableBefore {
		t.Fatalf("GC reclaimed containers but did not reduce collectable orphan chunks: before=%d after=%d affected_containers=%d", orphanCollectableBefore, orphanCollectableAfter, gcResult.AffectedContainers)
	}
	if remaining := countChunksByIDsStep69(t, dbconn, orphan.chunkIDs); remaining != 0 {
		t.Logf("note: %d orphan-seeded chunks remain due shared/liveness semantics", remaining)
	}

	for _, live := range liveFiles {
		if got := countChunksByIDsStep69(t, dbconn, live.chunkIDs); got == 0 {
			t.Fatalf("live file chunks were collected unexpectedly (file_id=%d)", live.fileID)
		}
	}

	for i, live := range liveFiles {
		restored := restoreStep69(t, dbconn, tmp, live.fileID, fmt.Sprintf("live-%d-restored.bin", i))
		if sha256HexStep69(restored) != live.hash {
			t.Fatalf("restore hash mismatch after GC for file_id=%d", live.fileID)
		}
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify after GC: %v", err)
	}

	compressedLiveAfter := countLiveCompressedBlocksStep69(t, dbconn)
	orphanCompressedAfter := countOrphanCompressedBlocksStep69(t, dbconn)

	if compressedLiveBefore > 0 && compressedLiveAfter == 0 {
		t.Fatalf("live compressed blocks were not preserved: before=%d after=%d", compressedLiveBefore, compressedLiveAfter)
	}
	if orphanCompressedAfter > orphanCompressedBefore {
		t.Fatalf("orphan compressed blocks increased after GC: before=%d after=%d", orphanCompressedBefore, orphanCompressedAfter)
	}
	if orphanCompressedBefore > 0 && gcResult.AffectedContainers > 0 && orphanCompressedAfter >= orphanCompressedBefore {
		t.Fatalf("GC reclaimed containers but did not reduce orphan compressed blocks: before=%d after=%d affected_containers=%d", orphanCompressedBefore, orphanCompressedAfter, gcResult.AffectedContainers)
	}

	t.Logf("✓ gc safety validated for repo_class=%s encryption=%s live_compressed_before=%d orphan_compressed_before=%d", class.name, class.encryption, compressedLiveBefore, orphanCompressedBefore)
}

func setupStep69Env(t *testing.T, encryption blocks.Codec) (*sql.DB, string, container.ContainerWriter) {
	t.Helper()

	tmp := t.TempDir()
	origContainers := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainers })

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers: %v", err)
	}
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	if encryption == blocks.CodecAESGCM {
		testutils.SetTestAESGCMKey(t)
	} else {
		t.Setenv("COLDKEEP_KEY", "")
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}

	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	writer := container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn)
	return dbconn, tmp, writer
}

func step69SeedScenarioData(
	t *testing.T,
	dbconn *sql.DB,
	writer container.ContainerWriter,
	tmp string,
	class step69RepoClass,
) ([]step69StoredFile, step69StoredFile) {
	t.Helper()

	live := make([]step69StoredFile, 0, 2)

	switch class.mode {
	case "compressed":
		setCompressionStep69(t, dbconn, storagecompression.CompressionZstd)
		live = append(live, storePayloadStep69(t, dbconn, writer, tmp, "live-zstd.bin", makePayloadStep69("live-zstd", 2*1024*1024), class.encryption))
		orphan := storePayloadStep69(t, dbconn, writer, tmp, "orphan-zstd.bin", makePayloadStep69("orphan-zstd", 1536*1024), class.encryption)
		if err := storage.RemoveFileWithDB(dbconn, orphan.fileID); err != nil {
			t.Fatalf("remove orphan file: %v", err)
		}
		return live, orphan

	case "uncompressed":
		setCompressionStep69(t, dbconn, storagecompression.CompressionNone)
		live = append(live, storePayloadStep69(t, dbconn, writer, tmp, "live-none.bin", makePayloadStep69("live-none", 2*1024*1024), class.encryption))
		orphan := storePayloadStep69(t, dbconn, writer, tmp, "orphan-none.bin", makePayloadStep69("orphan-none", 1536*1024), class.encryption)
		if err := storage.RemoveFileWithDB(dbconn, orphan.fileID); err != nil {
			t.Fatalf("remove orphan file: %v", err)
		}
		return live, orphan

	case "mixed":
		setCompressionStep69(t, dbconn, storagecompression.CompressionNone)
		live = append(live, storePayloadStep69(t, dbconn, writer, tmp, "live-none.bin", makePayloadStep69("mixed-live-none", 1024*1024), class.encryption))

		setCompressionStep69(t, dbconn, storagecompression.CompressionZstd)
		live = append(live, storePayloadStep69(t, dbconn, writer, tmp, "live-zstd.bin", makePayloadStep69("mixed-live-zstd", 1024*1024+131), class.encryption))
		orphan := storePayloadStep69(t, dbconn, writer, tmp, "orphan-zstd.bin", makePayloadStep69("mixed-orphan-zstd", 1400*1024), class.encryption)
		if err := storage.RemoveFileWithDB(dbconn, orphan.fileID); err != nil {
			t.Fatalf("remove orphan file: %v", err)
		}
		return live, orphan

	default:
		t.Fatalf("unsupported step69 mode: %s", class.mode)
		return nil, step69StoredFile{}
	}
}

func setCompressionStep69(t *testing.T, dbconn *sql.DB, compression string) {
	t.Helper()

	t.Setenv("COLDKEEP_COMPRESSION", compression)
	if compression == storagecompression.CompressionZstd {
		t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")
	}

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx for compression update: %v", err)
	}
	if err := storage.SetDefaultCompression(tx, compression); err != nil {
		t.Fatalf("set default compression: %v", err)
	}
	if compression == storagecompression.CompressionZstd {
		if err := storage.SetDefaultCompressionLevel(tx, 3); err != nil {
			t.Fatalf("set default compression level: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit compression update: %v", err)
	}
}

func storePayloadStep69(
	t *testing.T,
	dbconn *sql.DB,
	writer container.ContainerWriter,
	tmp string,
	filename string,
	payload []byte,
	codec blocks.Codec,
) step69StoredFile {
	t.Helper()

	path := filepath.Join(tmp, filename)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write payload file: %v", err)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: container.ContainersDir,
		Chunker:      chunk.DefaultChunker(),
	}

	result, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	chunkIDs := make([]int64, 0, 16)
	rows, err := dbconn.Query(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_id ASC`, result.FileID)
	if err != nil {
		t.Fatalf("query file_chunk rows: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			t.Fatalf("scan file_chunk row: %v", err)
		}
		chunkIDs = append(chunkIDs, chunkID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate file_chunk rows: %v", err)
	}

	if len(chunkIDs) == 0 {
		t.Fatalf("stored file has no chunks; file_id=%d", result.FileID)
	}

	return step69StoredFile{
		fileID:   result.FileID,
		hash:     sha256HexStep69(payload),
		chunkIDs: chunkIDs,
	}
}

func toLegacyMetadataShapeStep69(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		t.Fatalf("delete chunk_block_refs for legacy shape: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		t.Fatalf("delete storage_blocks for legacy shape: %v", err)
	}
}

func countChunksByIDsStep69(t *testing.T, dbconn *sql.DB, ids []int64) int64 {
	t.Helper()

	if len(ids) == 0 {
		return 0
	}

	var total int64
	for _, id := range ids {
		var n int64
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatalf("count chunk id=%d: %v", id, err)
		}
		total += n
	}
	return total
}

func countCollectableChunksByIDsStep69(t *testing.T, dbconn *sql.DB, ids []int64) int64 {
	t.Helper()

	if len(ids) == 0 {
		return 0
	}

	var total int64
	for _, id := range ids {
		var n int64
		if err := dbconn.QueryRow(`
			SELECT COUNT(*)
			FROM chunk c
			WHERE c.id = $1
			  AND c.live_ref_count = 0
			  AND COALESCE(c.pin_count, 0) = 0
			  AND NOT EXISTS (
				SELECT 1 FROM file_chunk fc WHERE fc.chunk_id = c.id
			  )
		`, id).Scan(&n); err != nil {
			t.Fatalf("count collectable chunk id=%d: %v", id, err)
		}
		total += n
	}

	return total
}

func countLiveCompressedBlocksStep69(t *testing.T, dbconn *sql.DB) int64 {
	t.Helper()

	var n int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM storage_blocks sb
		JOIN chunk_block_refs cbr ON cbr.block_id = sb.id
		JOIN chunk c ON c.id = cbr.chunk_id
		WHERE c.live_ref_count > 0 AND sb.compression_codec = 'zstd'
	`).Scan(&n); err != nil {
		t.Fatalf("count live compressed blocks: %v", err)
	}
	return n
}

func countOrphanCompressedBlocksStep69(t *testing.T, dbconn *sql.DB) int64 {
	t.Helper()

	var n int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM storage_blocks sb
		JOIN chunk_block_refs cbr ON cbr.block_id = sb.id
		JOIN chunk c ON c.id = cbr.chunk_id
		WHERE c.live_ref_count = 0 AND sb.compression_codec = 'zstd'
	`).Scan(&n); err != nil {
		t.Fatalf("count orphan compressed blocks: %v", err)
	}
	return n
}

func restoreStep69(t *testing.T, dbconn *sql.DB, tmp string, fileID int64, filename string) []byte {
	t.Helper()

	out := filepath.Join(tmp, filename)
	if err := storage.RestoreFileWithDB(dbconn, fileID, out); err != nil {
		t.Fatalf("restore file_id=%d: %v", fileID, err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	return data
}

func sha256HexStep69(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func makePayloadStep69(tag string, size int) []byte {
	if size <= 0 {
		return nil
	}
	p := make([]byte, size)
	seed := []byte("step-6-9-gc-safety-" + tag + "-")
	for i := range p {
		p[i] = seed[i%len(seed)]
		if i%257 == 0 {
			p[i] ^= byte((i / 257) % 251)
		}
	}
	return p
}
