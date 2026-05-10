package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// Step 6.13 — Validate Observability & Stats Semantics
//
// Core guarantee:
//   Stats must reflect reality correctly. All observability formulas (logical_bytes,
//   compressed_bytes, stored_bytes, compression_ratio, physical_ratio) must be
//   mathematically correct and handle edge cases (mixed repos, legacy nulls, etc).
//
// Required validations:
//   ✔ ratios mathematically correct
//   ✔ mixed repos handled correctly
//   ✔ fallback blocks counted correctly

func storeTestFileVia613(t *testing.T, scx storage.StorageContext, tmpdir, filename string, payload []byte, codec blocks.Codec) {
	filepath := filepath.Join(tmpdir, filename)
	if err := os.WriteFile(filepath, payload, 0644); err != nil {
		_ = scx.DB.Close()
		t.Fatalf("write file %s: %v", filename, err)
	}

	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(scx, filepath, codec, false); err != nil {
		_ = scx.DB.Close()
		t.Fatalf("store file %s: %v", filename, err)
	}
}

func setCompressionVia613(t *testing.T, dbconn *sql.DB, codec string, level int) {
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx for compression setup: %v", err)
	}

	if err := storage.SetDefaultCompression(tx, codec); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SetDefaultCompression failed: %v", err)
	}

	if err := storage.SetDefaultCompressionLevel(tx, level); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SetDefaultCompressionLevel failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit compression setup: %v", err)
	}
}

func setupStep613StatsContext(t *testing.T) (*sql.DB, storage.StorageContext, string) {
	t.Helper()
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	containersDir := filepath.Join(tmpdir, "containers")
	if err := os.MkdirAll(containersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers dir: %v", err)
	}
	origContainers := container.ContainersDir
	container.ContainersDir = containersDir
	t.Cleanup(func() { container.ContainersDir = origContainers })
	// Ensure storage internals and env resolve to the test-local container root.
	t.Setenv("COLDKEEP_STORAGE_DIR", containersDir)
	testutils.ResetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}

	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
	}

	return dbconn, scx, tmpdir
}

func collectExpectedBlockStats613(t *testing.T, dbconn *sql.DB) (logical, compressed, stored int64, compressedBlocks, uncompressedBlocks int64) {
	t.Helper()

	err := dbconn.QueryRow(`
		SELECT
			COALESCE(SUM(plaintext_size), 0),
			COALESCE(SUM(COALESCE(compressed_size, CASE WHEN COALESCE(compression_codec, 'none') = 'none' THEN plaintext_size END, stored_size)), 0),
			COALESCE(SUM(stored_size), 0),
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) != 'none' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) = 'none' THEN 1 ELSE 0 END), 0)
		FROM storage_blocks
	`).Scan(&logical, &compressed, &stored, &compressedBlocks, &uncompressedBlocks)
	if err != nil {
		t.Fatalf("collect expected block stats: %v", err)
	}

	return logical, compressed, stored, compressedBlocks, uncompressedBlocks
}

func TestStep613StatsRatiosMathematicallyCorrect(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()
	setCompressionVia613(t, dbconn, "none", 1)

	// Store 3 uncompressed files: 100, 200, 300 bytes
	storeTestFileVia613(t, scx, tmpdir, "file1.txt", bytes.Repeat([]byte("a"), 100), blocks.CodecPlain)
	storeTestFileVia613(t, scx, tmpdir, "file2.txt", bytes.Repeat([]byte("b"), 200), blocks.CodecPlain)
	storeTestFileVia613(t, scx, tmpdir, "file3.txt", bytes.Repeat([]byte("c"), 300), blocks.CodecPlain)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	expectedLogical, expectedCompressed, expectedStored, expectedCompressedBlocks, expectedUncompressedBlocks := collectExpectedBlockStats613(t, dbconn)
	if stats.LogicalBytes != expectedLogical {
		t.Errorf("LogicalBytes: expected %d, got %d", expectedLogical, stats.LogicalBytes)
	}

	if stats.CompressedBytes != expectedCompressed {
		t.Errorf("CompressedBytes (uncompressed): expected %d, got %d", expectedCompressed, stats.CompressedBytes)
	}

	if stats.StoredBytes != expectedStored {
		t.Errorf("StoredBytes: expected %d, got %d", expectedStored, stats.StoredBytes)
	}

	// compressionFactor = logical/compressed (factor ≥1.0 when effective compression;
	// distinct from benchmark CompressionRatio = compressed/logical, 0-1 size fraction).
	compressionFactor := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
	if math.Abs(compressionFactor-1.0) > 0.001 {
		t.Errorf("uncompressed: compressionFactor (logical/compressed) should be ~1.0, got %.3f", compressionFactor)
	}

	// physicalFactor = logical/stored (≤1.0 because stored includes block format overhead).
	physicalFactor := float64(stats.LogicalBytes) / float64(stats.StoredBytes)
	if physicalFactor <= 0 || physicalFactor > 1.0 {
		t.Errorf("physicalFactor (logical/stored) out of bounds (0,1.0]: %.3f", physicalFactor)
	}

	if stats.UncompressedBlocks != expectedUncompressedBlocks {
		t.Errorf("UncompressedBlocks: expected %d, got %d", expectedUncompressedBlocks, stats.UncompressedBlocks)
	}
	if stats.CompressedBlocks != expectedCompressedBlocks {
		t.Errorf("CompressedBlocks: expected %d, got %d", expectedCompressedBlocks, stats.CompressedBlocks)
	}
}

func TestStep613StatsCompressedBytesRatio(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	setCompressionVia613(t, dbconn, "zstd", 3)

	// Store highly compressible data
	storeTestFileVia613(t, scx, tmpdir, "compressible.txt", bytes.Repeat([]byte("x"), 10000), blocks.CodecPlain)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	expectedLogical, expectedCompressed, _, expectedCompressedBlocks, _ := collectExpectedBlockStats613(t, dbconn)
	if stats.LogicalBytes != expectedLogical {
		t.Errorf("LogicalBytes: expected %d, got %d", expectedLogical, stats.LogicalBytes)
	}

	if stats.CompressedBytes != expectedCompressed {
		t.Errorf("CompressedBytes: expected %d, got %d", expectedCompressed, stats.CompressedBytes)
	}

	if stats.CompressedBytes >= stats.LogicalBytes {
		t.Errorf("CompressedBytes should be < LogicalBytes for compressible data")
	}

	// compressionFactor = logical/compressed; > 1.0 means compression reduced size.
	compressionFactor := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
	if compressionFactor <= 1.0 {
		t.Errorf("compressionFactor (logical/compressed) should be > 1.0 for compressed data, got %.3f", compressionFactor)
	}

	if stats.CompressedBlocks != expectedCompressedBlocks {
		t.Errorf("CompressedBlocks: expected %d, got %d", expectedCompressedBlocks, stats.CompressedBlocks)
	}
}

func TestStep613StatsMixedRepository(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	// Store v1.8 style uncompressed
	setCompressionVia613(t, dbconn, "none", 1)
	storeTestFileVia613(t, scx, tmpdir, "legacy.txt", bytes.Repeat([]byte("l"), 1000), blocks.CodecPlain)

	// Switch to compression and store new data
	setCompressionVia613(t, dbconn, "zstd", 3)
	storeTestFileVia613(t, scx, tmpdir, "new.txt", bytes.Repeat([]byte("y"), 5000), blocks.CodecPlain)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	expectedLogical, _, _, expectedCompressedBlocks, expectedUncompressedBlocks := collectExpectedBlockStats613(t, dbconn)
	if stats.LogicalBytes != expectedLogical {
		t.Errorf("LogicalBytes: expected %d, got %d", expectedLogical, stats.LogicalBytes)
	}

	if stats.UncompressedBlocks != expectedUncompressedBlocks {
		t.Errorf("UncompressedBlocks: expected %d, got %d", expectedUncompressedBlocks, stats.UncompressedBlocks)
	}
	if stats.CompressedBlocks != expectedCompressedBlocks {
		t.Errorf("CompressedBlocks: expected %d, got %d", expectedCompressedBlocks, stats.CompressedBlocks)
	}
}

func TestStep613StatsMultiChunkFile(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	// Large file spans multiple chunks
	storeTestFileVia613(t, scx, tmpdir, "large.txt", bytes.Repeat([]byte("c"), 5000000), blocks.CodecPlain)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	expectedLogical, _, _, _, _ := collectExpectedBlockStats613(t, dbconn)
	if stats.LogicalBytes != expectedLogical {
		t.Errorf("LogicalBytes: expected %d, got %d", expectedLogical, stats.LogicalBytes)
	}
}

func TestStep613StatsLegacyNullHashesSafe(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	setCompressionVia613(t, dbconn, "zstd", 3)
	storeTestFileVia613(t, scx, tmpdir, "test.txt", bytes.Repeat([]byte("d"), 10000), blocks.CodecPlain)

	// Verify blocks have block_hash
	rows, err := dbconn.Query("SELECT COUNT(*), COUNT(CASE WHEN block_hash IS NULL THEN 1 END) FROM storage_blocks")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var total, nullCount int64
	if rows.Next() {
		if err := rows.Scan(&total, &nullCount); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
	}

	if total == 0 {
		t.Errorf("Expected blocks, got 0")
	}
	if nullCount > 0 {
		t.Errorf("block_hash should not be null, got %d null rows", nullCount)
	}

	// Collect stats should not crash
	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	expectedLogical, _, _, _, _ := collectExpectedBlockStats613(t, dbconn)
	if stats.LogicalBytes != expectedLogical {
		t.Errorf("LogicalBytes: expected %d, got %d", expectedLogical, stats.LogicalBytes)
	}
}

func TestStep613StatsAESGCMEncryption(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	os.Setenv("COLDKEEP_KEY", "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd")
	defer os.Unsetenv("COLDKEEP_KEY")

	setCompressionVia613(t, dbconn, "zstd", 3)
	storeTestFileVia613(t, scx, tmpdir, "encrypted.txt", bytes.Repeat([]byte("e"), 8000), blocks.CodecAESGCM)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	expectedLogical, expectedCompressed, _, expectedCompressedBlocks, _ := collectExpectedBlockStats613(t, dbconn)
	if stats.LogicalBytes != expectedLogical {
		t.Errorf("LogicalBytes: expected %d, got %d", expectedLogical, stats.LogicalBytes)
	}

	if stats.CompressedBytes != expectedCompressed {
		t.Errorf("CompressedBytes: expected %d, got %d", expectedCompressed, stats.CompressedBytes)
	}
	if stats.CompressedBytes >= stats.LogicalBytes {
		t.Errorf("Should compress: %d >= %d", stats.CompressedBytes, stats.LogicalBytes)
	}
	if stats.CompressedBlocks != expectedCompressedBlocks {
		t.Errorf("CompressedBlocks: expected %d, got %d", expectedCompressedBlocks, stats.CompressedBlocks)
	}
}

func TestStep613StatsRatioBoundaries(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	setCompressionVia613(t, dbconn, "zstd", 3)

	for i := 0; i < 5; i++ {
		storeTestFileVia613(t, scx, tmpdir, fmt.Sprintf("file%d.txt", i), bytes.Repeat([]byte{byte(65 + i)}, 1000), blocks.CodecPlain)
	}

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	if stats.LogicalBytes <= 0 || stats.CompressedBytes <= 0 || stats.StoredBytes <= 0 {
		t.Errorf("All byte counts should be positive: L=%d C=%d S=%d", stats.LogicalBytes, stats.CompressedBytes, stats.StoredBytes)
	}

	// compressionFactor = logical/compressed (≥1.0 with any effective compression).
	// physicalFactor = logical/stored (≤compressionFactor because stored includes overhead beyond compression).
	compressionFactor := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
	physicalFactor := float64(stats.LogicalBytes) / float64(stats.StoredBytes)

	if compressionFactor < 1.0 {
		t.Errorf("compressionFactor (logical/compressed) should be >= 1.0, got %.3f", compressionFactor)
	}
	if physicalFactor <= 0 {
		t.Errorf("physicalFactor (logical/stored) should be > 0, got %.3f", physicalFactor)
	}
	if physicalFactor > compressionFactor {
		t.Errorf("physicalFactor (%.3f) should be <= compressionFactor (%.3f): stored includes transform overhead", physicalFactor, compressionFactor)
	}
}

func TestStep613StatsByteAccountingConsistency(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	setCompressionVia613(t, dbconn, "zstd", 3)
	storeTestFileVia613(t, scx, tmpdir, "test1.txt", bytes.Repeat([]byte("a"), 2000), blocks.CodecPlain)
	storeTestFileVia613(t, scx, tmpdir, "test2.txt", bytes.Repeat([]byte("b"), 3000), blocks.CodecPlain)

	// Direct query
	var sumLogical, sumCompressed, sumStored int64
	err := dbconn.QueryRow(`
		SELECT
			COALESCE(SUM(plaintext_size), 0),
			COALESCE(SUM(CASE 
				WHEN compression_codec = 'none' THEN plaintext_size
				ELSE compressed_size
			END), 0),
			COALESCE(SUM(stored_size), 0)
		FROM storage_blocks
	`).Scan(&sumLogical, &sumCompressed, &sumStored)
	if err != nil {
		t.Fatalf("Direct query failed: %v", err)
	}

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	if stats.LogicalBytes != sumLogical {
		t.Errorf("LogicalBytes mismatch: stats=%d, direct=%d", stats.LogicalBytes, sumLogical)
	}
	if stats.CompressedBytes != sumCompressed {
		t.Errorf("CompressedBytes mismatch: stats=%d, direct=%d", stats.CompressedBytes, sumCompressed)
	}
	if stats.StoredBytes != sumStored {
		t.Errorf("StoredBytes mismatch: stats=%d, direct=%d", stats.StoredBytes, sumStored)
	}

	if sumLogical > 0 && sumCompressed > 0 {
		// compressionFactor (logical/compressed) from direct query vs CollectBlockStats must agree.
		factorFormula := float64(sumLogical) / float64(sumCompressed)
		factorStats := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
		if math.Abs(factorFormula-factorStats) > 0.001 {
			t.Errorf("compressionFactor mismatch: direct=%.3f, stats=%.3f", factorFormula, factorStats)
		}
	}
}

func TestStep613StatsEmptyRepository(t *testing.T) {
	dbconn, _, _ := setupStep613StatsContext(t)
	defer dbconn.Close()

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	if stats.LogicalBytes != 0 || stats.CompressedBytes != 0 || stats.StoredBytes != 0 {
		t.Errorf("Empty repo should have zero bytes: L=%d C=%d S=%d", stats.LogicalBytes, stats.CompressedBytes, stats.StoredBytes)
	}
	if (stats.CompressedBlocks + stats.UncompressedBlocks) != 0 {
		t.Errorf("Empty repo should have zero blocks")
	}
}

func TestStep613StatsAccumulation(t *testing.T) {
	dbconn, scx, tmpdir := setupStep613StatsContext(t)
	defer dbconn.Close()

	setCompressionVia613(t, dbconn, "zstd", 3)

	storeTestFileVia613(t, scx, tmpdir, "f1.txt", bytes.Repeat([]byte("1"), 1000), blocks.CodecPlain)
	stats1, _ := maintenance.CollectBlockStats(context.Background(), dbconn)
	expectedLogical1, _, _, _, _ := collectExpectedBlockStats613(t, dbconn)

	storeTestFileVia613(t, scx, tmpdir, "f2.txt", bytes.Repeat([]byte("2"), 2000), blocks.CodecPlain)
	stats2, _ := maintenance.CollectBlockStats(context.Background(), dbconn)
	expectedLogical2, _, _, _, _ := collectExpectedBlockStats613(t, dbconn)

	storeTestFileVia613(t, scx, tmpdir, "f3.txt", bytes.Repeat([]byte("3"), 3000), blocks.CodecPlain)
	stats3, _ := maintenance.CollectBlockStats(context.Background(), dbconn)
	expectedLogical3, _, _, _, _ := collectExpectedBlockStats613(t, dbconn)

	if stats1.LogicalBytes != expectedLogical1 {
		t.Errorf("After store 1: expected %d, got %d", expectedLogical1, stats1.LogicalBytes)
	}
	if stats2.LogicalBytes != expectedLogical2 {
		t.Errorf("After store 2: expected %d, got %d", expectedLogical2, stats2.LogicalBytes)
	}
	if stats3.LogicalBytes != expectedLogical3 {
		t.Errorf("After store 3: expected %d, got %d", expectedLogical3, stats3.LogicalBytes)
	}
	if stats1.LogicalBytes >= stats2.LogicalBytes || stats2.LogicalBytes >= stats3.LogicalBytes {
		t.Errorf("LogicalBytes should accumulate monotonically: got %d, %d, %d", stats1.LogicalBytes, stats2.LogicalBytes, stats3.LogicalBytes)
	}

	if stats1.CompressedBlocks+stats1.UncompressedBlocks != 1 || stats2.CompressedBlocks+stats2.UncompressedBlocks != 2 || stats3.CompressedBlocks+stats3.UncompressedBlocks != 3 {
		t.Errorf("Block accumulation failed")
	}

	for i, s := range []*maintenance.BlockStats{&stats1, &stats2, &stats3} {
		if s.CompressedBytes <= 0 {
			t.Errorf("Stats %d: CompressedBytes should be > 0", i)
		}
		ratio := float64(s.LogicalBytes) / float64(s.CompressedBytes)
		if ratio < 1.0 {
			t.Errorf("Stats %d: ratio should be >= 1.0, got %.3f", i, ratio)
		}
	}
}
