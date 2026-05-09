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

func TestStep613StatsRatiosMathematicallyCorrect(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

	// Store 3 uncompressed files: 100, 200, 300 bytes
	storeTestFileVia613(t, scx, tmpdir, "file1.txt", bytes.Repeat([]byte("a"), 100), blocks.CodecPlain)
	storeTestFileVia613(t, scx, tmpdir, "file2.txt", bytes.Repeat([]byte("b"), 200), blocks.CodecPlain)
	storeTestFileVia613(t, scx, tmpdir, "file3.txt", bytes.Repeat([]byte("c"), 300), blocks.CodecPlain)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	expectedLogical := int64(600)
	if stats.LogicalBytes != expectedLogical {
		t.Errorf("LogicalBytes: expected %d, got %d", expectedLogical, stats.LogicalBytes)
	}

	if stats.CompressedBytes != expectedLogical {
		t.Errorf("CompressedBytes (uncompressed): expected %d, got %d", expectedLogical, stats.CompressedBytes)
	}

	if stats.StoredBytes < expectedLogical {
		t.Errorf("StoredBytes should be >= LogicalBytes: %d < %d", stats.StoredBytes, expectedLogical)
	}

	ratio := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
	if math.Abs(ratio-1.0) > 0.001 {
		t.Errorf("Uncompressed CompressionRatio should be ~1.0, got %.3f", ratio)
	}

	physicalRatio := float64(stats.LogicalBytes) / float64(stats.StoredBytes)
	if physicalRatio <= 0 || physicalRatio > 1.0 {
		t.Errorf("PhysicalRatio out of bounds (0,1.0]: %.3f", physicalRatio)
	}

	if stats.UncompressedBlocks != 3 {
		t.Errorf("UncompressedBlocks: expected 3, got %d", stats.UncompressedBlocks)
	}
	if stats.CompressedBlocks != 0 {
		t.Errorf("CompressedBlocks: expected 0, got %d", stats.CompressedBlocks)
	}
}

func TestStep613StatsCompressedBytesRatio(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

	setCompressionVia613(t, dbconn, "zstd", 3)

	// Store highly compressible data
	storeTestFileVia613(t, scx, tmpdir, "compressible.txt", bytes.Repeat([]byte("x"), 10000), blocks.CodecPlain)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	if stats.LogicalBytes != 10000 {
		t.Errorf("LogicalBytes: expected 10000, got %d", stats.LogicalBytes)
	}

	if stats.CompressedBytes >= stats.LogicalBytes {
		t.Errorf("CompressedBytes should be < LogicalBytes for compressible data")
	}

	ratio := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
	if ratio <= 1.0 {
		t.Errorf("CompressionRatio should be > 1.0 for compressed data, got %.3f", ratio)
	}

	if stats.CompressedBlocks != 1 {
		t.Errorf("CompressedBlocks: expected 1, got %d", stats.CompressedBlocks)
	}
}

func TestStep613StatsMixedRepository(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

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

	if stats.LogicalBytes != 6000 {
		t.Errorf("LogicalBytes: expected 6000, got %d", stats.LogicalBytes)
	}

	if stats.UncompressedBlocks != 1 {
		t.Errorf("UncompressedBlocks: expected 1, got %d", stats.UncompressedBlocks)
	}
	if stats.CompressedBlocks != 1 {
		t.Errorf("CompressedBlocks: expected 1, got %d", stats.CompressedBlocks)
	}
}

func TestStep613StatsMultiChunkFile(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

	// Large file spans multiple chunks
	storeTestFileVia613(t, scx, tmpdir, "large.txt", bytes.Repeat([]byte("c"), 5000000), blocks.CodecPlain)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	if stats.LogicalBytes != 5000000 {
		t.Errorf("LogicalBytes: expected 5000000, got %d", stats.LogicalBytes)
	}
}

func TestStep613StatsLegacyNullHashesSafe(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

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

	if stats.LogicalBytes != 10000 {
		t.Errorf("LogicalBytes: expected 10000, got %d", stats.LogicalBytes)
	}
}

func TestStep613StatsAESGCMEncryption(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	os.Setenv("COLDKEEP_KEY", "aabbccddaabbccddaabbccddaabbccdd")
	defer os.Unsetenv("COLDKEEP_KEY")

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

	setCompressionVia613(t, dbconn, "zstd", 3)
	storeTestFileVia613(t, scx, tmpdir, "encrypted.txt", bytes.Repeat([]byte("e"), 8000), blocks.CodecAESGCM)

	stats, err := maintenance.CollectBlockStats(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats failed: %v", err)
	}

	if stats.LogicalBytes != 8000 {
		t.Errorf("LogicalBytes: expected 8000, got %d", stats.LogicalBytes)
	}

	if stats.CompressedBytes >= stats.LogicalBytes {
		t.Errorf("Should compress: %d >= %d", stats.CompressedBytes, stats.LogicalBytes)
	}
	if stats.CompressedBlocks != 1 {
		t.Errorf("CompressedBlocks: expected 1, got %d", stats.CompressedBlocks)
	}
}

func TestStep613StatsRatioBoundaries(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

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

	compressionRatio := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
	physicalRatio := float64(stats.LogicalBytes) / float64(stats.StoredBytes)

	if compressionRatio < 1.0 {
		t.Errorf("CompressionRatio should be >= 1.0, got %.3f", compressionRatio)
	}
	if physicalRatio < 1.0 || physicalRatio > 1.0 {
		t.Errorf("PhysicalRatio should be <= 1.0, got %.3f", physicalRatio)
	}
	if physicalRatio > compressionRatio {
		t.Errorf("PhysicalRatio (%.3f) should be <= CompressionRatio (%.3f)", physicalRatio, compressionRatio)
	}
}

func TestStep613StatsByteAccountingConsistency(t *testing.T) {
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

	setCompressionVia613(t, dbconn, "zstd", 3)
	storeTestFileVia613(t, scx, tmpdir, "test1.txt", bytes.Repeat([]byte("a"), 2000), blocks.CodecPlain)
	storeTestFileVia613(t, scx, tmpdir, "test2.txt", bytes.Repeat([]byte("b"), 3000), blocks.CodecPlain)

	// Direct query
	var sumLogical, sumCompressed, sumStored int64
	err = dbconn.QueryRow(`
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
		ratioFormula := float64(sumLogical) / float64(sumCompressed)
		ratioStats := float64(stats.LogicalBytes) / float64(stats.CompressedBytes)
		if math.Abs(ratioFormula-ratioStats) > 0.001 {
			t.Errorf("Ratio mismatch: direct=%.3f, stats=%.3f", ratioFormula, ratioStats)
		}
	}
}

func TestStep613StatsEmptyRepository(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
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
	testgate.RequireDB(t)

	tmpdir := t.TempDir()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer dbconn.Close()

	scx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: tmpdir,
	}

	setCompressionVia613(t, dbconn, "zstd", 3)

	storeTestFileVia613(t, scx, tmpdir, "f1.txt", bytes.Repeat([]byte("1"), 1000), blocks.CodecPlain)
	stats1, _ := maintenance.CollectBlockStats(context.Background(), dbconn)

	storeTestFileVia613(t, scx, tmpdir, "f2.txt", bytes.Repeat([]byte("2"), 2000), blocks.CodecPlain)
	stats2, _ := maintenance.CollectBlockStats(context.Background(), dbconn)

	storeTestFileVia613(t, scx, tmpdir, "f3.txt", bytes.Repeat([]byte("3"), 3000), blocks.CodecPlain)
	stats3, _ := maintenance.CollectBlockStats(context.Background(), dbconn)

	if stats1.LogicalBytes != 1000 {
		t.Errorf("After store 1: expected 1000, got %d", stats1.LogicalBytes)
	}
	if stats2.LogicalBytes != 3000 {
		t.Errorf("After store 2: expected 3000, got %d", stats2.LogicalBytes)
	}
	if stats3.LogicalBytes != 6000 {
		t.Errorf("After store 3: expected 6000, got %d", stats3.LogicalBytes)
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
