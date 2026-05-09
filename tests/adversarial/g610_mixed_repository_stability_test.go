package main

import (
	"context"
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
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// Step 6.10 — Validate Mixed Repository Stability
//
// Core roadmap guarantee: Mixed repositories are normal behavior and must be stable.
//
// Evolution scenario under test:
//   v1.8 blocks
//   -> Phase 5 uncompressed blocks
//   -> zstd compressed blocks
//   -> encryption mode changes
//   -> store-if-smaller fallbacks
//
// Validation targets:
//   ✔ mixed repositories stable
//   ✔ per-block metadata fully sufficient
//   ✔ repository defaults never required for reads

type step610StoredFile struct {
	name   string
	fileID int64
	hash   string
}

func TestStep610MixedRepositoryStability(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, tmp, writer := setupStep610Env(t)
	defer dbconn.Close()

	stored := make([]step610StoredFile, 0, 7)

	// 1) Seed baseline data, then convert metadata shape to legacy-only to emulate v1.8 blocks.
	setCompressionStep610(t, dbconn, storagecompression.CompressionNone)
	stored = append(stored,
		storeStep610(t, dbconn, writer, tmp, "v18-legacy-a.bin", makePayloadStep610("v18-legacy-a", 780*1024), blocks.CodecPlain),
		storeStep610(t, dbconn, writer, tmp, "v18-legacy-b.bin", makePayloadStep610("v18-legacy-b", 930*1024), blocks.CodecPlain),
	)

	var oldMaxChunkID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM chunk`).Scan(&oldMaxChunkID); err != nil {
		t.Fatalf("query old max chunk id: %v", err)
	}

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		t.Fatalf("delete chunk_block_refs for legacy emulation: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		t.Fatalf("delete storage_blocks for legacy emulation: %v", err)
	}
	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("reopen legacy-style repository: %v", err)
	}

	// 2) Phase 5 uncompressed blocks.
	setCompressionStep610(t, dbconn, storagecompression.CompressionNone)
	stored = append(stored,
		storeStep610(t, dbconn, writer, tmp, "phase5-none.bin", makePayloadStep610("phase5-none", 1200*1024), blocks.CodecPlain),
	)

	// 3) zstd compressed blocks (compressible payload should produce zstd blocks).
	setCompressionStep610(t, dbconn, storagecompression.CompressionZstd)
	stored = append(stored,
		storeStep610(t, dbconn, writer, tmp, "zstd-compressible.bin", makeHighlyCompressiblePayloadStep610(1700*1024), blocks.CodecPlain),
	)

	// 4) Encryption mode changes.
	stored = append(stored,
		storeStep610(t, dbconn, writer, tmp, "enc-aes.bin", makePayloadStep610("enc-aes", 900*1024), blocks.CodecAESGCM),
		storeStep610(t, dbconn, writer, tmp, "enc-plain-after-aes.bin", makePayloadStep610("enc-plain-after-aes", 860*1024), blocks.CodecPlain),
	)

	// 5) Store-if-smaller fallback under zstd default.
	fallbackFile := storeStep610(t, dbconn, writer, tmp, "fallback-incompressible.bin", makePseudoRandomPayloadStep610(1900*1024), blocks.CodecPlain)
	stored = append(stored, fallbackFile)

	// Validate mixed layout contains both legacy and packed paths.
	assertStep610MixedMetadataShape(t, dbconn, oldMaxChunkID)

	// Validate store-if-smaller fallback happened for the incompressible file.
	var fallbackNoneCodecBlocks int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM file_chunk fc
		JOIN chunk_block_refs cbr ON cbr.chunk_id = fc.chunk_id
		JOIN storage_blocks sb ON sb.id = cbr.block_id
		WHERE fc.logical_file_id = $1 AND sb.compression_codec = 'none'
	`, fallbackFile.fileID).Scan(&fallbackNoneCodecBlocks); err != nil {
		t.Fatalf("count fallback blocks for incompressible file: %v", err)
	}
	if fallbackNoneCodecBlocks == 0 {
		t.Fatalf("expected store-if-smaller fallback blocks for incompressible file")
	}

	// Ensure per-block metadata is fully populated for packed rows.
	var invalidPackedRows int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE codec IS NULL
		   OR compression_codec IS NULL
		   OR block_hash IS NULL
	`).Scan(&invalidPackedRows); err != nil {
		t.Fatalf("count invalid packed metadata rows: %v", err)
	}
	if invalidPackedRows != 0 {
		t.Fatalf("expected all packed rows to have sufficient per-block metadata, invalid_rows=%d", invalidPackedRows)
	}

	// Change repository defaults to a different mode before reads.
	setCompressionStep610(t, dbconn, storagecompression.CompressionNone)
	t.Setenv("COLDKEEP_COMPRESSION", storagecompression.CompressionNone)
	t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "9")

	// restore everything: repository defaults must not be required for reads.
	for i, f := range stored {
		restored := restoreStep610(t, dbconn, tmp, f.fileID, fmt.Sprintf("restored-%02d-%s", i, f.name))
		if sha256HexStep610(restored) != f.hash {
			t.Fatalf("restore mismatch with changed defaults for %s (file_id=%d)", f.name, f.fileID)
		}
	}

	// verify everything
	t.Setenv("COLDKEEP_VERIFY_STRICT_SEGMENTS", "1")
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify after mixed evolution: %v", err)
	}

	// stats everything
	statsBeforeGC, err := maintenance.RunStatsResultWithDB(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("stats before GC: %v", err)
	}
	if statsBeforeGC.BlockStats.LegacyBlocks == 0 {
		t.Fatalf("expected legacy blocks in mixed repository stats")
	}
	if statsBeforeGC.BlockStats.PackedBlocks == 0 {
		t.Fatalf("expected packed blocks in mixed repository stats")
	}
	if statsBeforeGC.BlockStats.CompressionCodecBreakdown["none"] == 0 {
		t.Fatalf("expected compression codec breakdown to include none")
	}
	if statsBeforeGC.BlockStats.CompressionCodecBreakdown["zstd"] == 0 {
		t.Fatalf("expected compression codec breakdown to include zstd")
	}

	// GC everything (on active mixed repository state) then verify/stats again.
	if err := storage.RemoveFileWithDB(dbconn, fallbackFile.fileID); err != nil {
		t.Fatalf("remove fallback file before gc: %v", err)
	}
	if _, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir); err != nil {
		t.Fatalf("gc dry-run: %v", err)
	}
	if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
		t.Fatalf("gc real-run: %v", err)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify after gc: %v", err)
	}

	statsAfterGC, err := maintenance.RunStatsResultWithDB(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("stats after GC: %v", err)
	}
	if statsAfterGC.BlockStats.LegacyBlocks == 0 {
		t.Fatalf("expected legacy blocks to remain after gc")
	}
	if statsAfterGC.BlockStats.PackedBlocks == 0 {
		t.Fatalf("expected packed blocks to remain after gc")
	}

	t.Logf("✓ mixed repositories stable")
	t.Logf("✓ per-block metadata fully sufficient")
	t.Logf("✓ repository defaults never required for reads")
}

func setupStep610Env(t *testing.T) (*sql.DB, string, container.ContainerWriter) {
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
	testutils.SetTestAESGCMKey(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	writer := container.NewLocalWriterWithDirAndDB(tmp, container.GetContainerMaxSize(), dbconn)
	return dbconn, tmp, writer
}

func setCompressionStep610(t *testing.T, dbconn *sql.DB, codec string) {
	t.Helper()

	t.Setenv("COLDKEEP_COMPRESSION", codec)
	if codec == storagecompression.CompressionZstd {
		t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")
	}

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin config tx: %v", err)
	}
	if err := storage.SetDefaultCompression(tx, codec); err != nil {
		t.Fatalf("set default compression: %v", err)
	}
	if codec == storagecompression.CompressionZstd {
		if err := storage.SetDefaultCompressionLevel(tx, 3); err != nil {
			t.Fatalf("set default compression level: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit config tx: %v", err)
	}
}

func storeStep610(
	t *testing.T,
	dbconn *sql.DB,
	writer container.ContainerWriter,
	tmp string,
	name string,
	payload []byte,
	codec blocks.Codec,
) step610StoredFile {
	t.Helper()

	path := filepath.Join(tmp, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write payload %s: %v", name, err)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmp,
		Chunker:      chunk.DefaultChunker(),
	}

	res, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store %s: %v", name, err)
	}

	return step610StoredFile{name: name, fileID: res.FileID, hash: sha256HexStep610(payload)}
}

func restoreStep610(t *testing.T, dbconn *sql.DB, tmp string, fileID int64, outName string) []byte {
	t.Helper()

	outPath := filepath.Join(tmp, outName)
	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file_id=%d: %v", fileID, err)
	}
	b, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file_id=%d: %v", fileID, err)
	}
	return b
}

func assertStep610MixedMetadataShape(t *testing.T, dbconn *sql.DB, oldMaxChunkID int64) {
	t.Helper()

	var oldLegacyBlocks int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		WHERE c.id <= $1
	`, oldMaxChunkID).Scan(&oldLegacyBlocks); err != nil {
		t.Fatalf("count old legacy blocks: %v", err)
	}
	if oldLegacyBlocks == 0 {
		t.Fatalf("expected legacy blocks rows for old chunks")
	}

	var oldPackedRefs int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id <= $1
	`, oldMaxChunkID).Scan(&oldPackedRefs); err != nil {
		t.Fatalf("count old chunk packed refs: %v", err)
	}
	if oldPackedRefs != 0 {
		t.Fatalf("expected old chunks to avoid packed refs, got=%d", oldPackedRefs)
	}

	var newPackedRefs int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id > $1
	`, oldMaxChunkID).Scan(&newPackedRefs); err != nil {
		t.Fatalf("count new chunk packed refs: %v", err)
	}
	if newPackedRefs == 0 {
		t.Fatalf("expected new chunks to use packed refs")
	}
}

func sha256HexStep610(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func makePayloadStep610(tag string, size int) []byte {
	if size <= 0 {
		return nil
	}
	b := make([]byte, size)
	seed := []byte("step-6-10-" + tag + "-")
	for i := range b {
		b[i] = seed[i%len(seed)]
		if i%251 == 0 {
			b[i] ^= byte((i / 251) % 251)
		}
	}
	return b
}

func makeHighlyCompressiblePayloadStep610(size int) []byte {
	if size <= 0 {
		return nil
	}
	b := make([]byte, size)
	pat := []byte("compressible-step-6-10-pattern-")
	for i := range b {
		b[i] = pat[i%len(pat)]
	}
	return b
}

func makePseudoRandomPayloadStep610(size int) []byte {
	if size <= 0 {
		return nil
	}
	b := make([]byte, size)
	var x uint64 = 0x9E3779B97F4A7C15
	for i := range b {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		b[i] = byte(x)
	}
	return b
}
