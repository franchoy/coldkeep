package main

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// Step 6.8 — Revalidate Dedup Semantics
//
// Core roadmap guarantee: Compression must not reduce dedup effectiveness.
//
// Validation per requirement:
//   ✔ chunk identities unchanged
//   ✔ dedup graph unchanged
//   ✔ no duplicate chunk storage introduced
//   ✔ restore unchanged
//
// Test strategy:
//   1. Store identical/overlapping files in two repositories (compression none vs zstd)
//   2. Compare chunk counts, chunk hashes, dedup ratios
//   3. Verify logical dedup graph is identical
//   4. Confirm physical storage differs but logical structure is the same

func TestStep68DedupSemanticCompressionIndependence(t *testing.T) {
	testgate.RequireDB(t)

	// Test both encryption options to ensure dedup works regardless of encryption
	for _, encryption := range []string{"plain", "aes-gcm"} {
		t.Run("encryption-"+encryption, func(t *testing.T) {
			testStep68DedupMatrixVariant(t, encryption)
		})
	}
}

// testStep68DedupMatrixVariant tests dedup semantics for one encryption mode across compression modes.
func testStep68DedupMatrixVariant(t *testing.T, encryptionCodec string) {
	t.Helper()

	// Create two repositories: one with compression=none, one with compression=zstd
	resultsNone := testStep68RunDedupScenario(t, encryptionCodec, storagecompression.CompressionNone)
	resultsZstd := testStep68RunDedupScenario(t, encryptionCodec, storagecompression.CompressionZstd)

	// Compare chunk counts
	if resultsNone.chunkCount != resultsZstd.chunkCount {
		t.Fatalf("chunk count mismatch: none=%d zstd=%d", resultsNone.chunkCount, resultsZstd.chunkCount)
	}
	t.Logf("✓ chunk count identical: %d", resultsNone.chunkCount)

	// Compare file_chunk relationships
	if resultsNone.fileChunkCount != resultsZstd.fileChunkCount {
		t.Fatalf("file_chunk count mismatch: none=%d zstd=%d", resultsNone.fileChunkCount, resultsZstd.fileChunkCount)
	}
	t.Logf("✓ file_chunk count identical: %d", resultsNone.fileChunkCount)

	// Compare chunk hashes
	if len(resultsNone.chunkHashes) != len(resultsZstd.chunkHashes) {
		t.Fatalf("chunk hash count mismatch: none=%d zstd=%d", len(resultsNone.chunkHashes), len(resultsZstd.chunkHashes))
	}

	// Verify all chunk hashes match
	for i, hashNone := range resultsNone.chunkHashes {
		hashZstd := resultsZstd.chunkHashes[i]
		if hashNone != hashZstd {
			t.Fatalf("chunk hash mismatch at position %d: none=%s zstd=%s", i, hashNone, hashZstd)
		}
	}
	t.Logf("✓ chunk hashes identical: %d chunks", len(resultsNone.chunkHashes))

	// Compare dedup ratios (total file size / unique chunk size)
	ratioNone := float64(resultsNone.totalFileSize) / float64(resultsNone.uniqueChunkSize)
	ratioZstd := float64(resultsZstd.totalFileSize) / float64(resultsZstd.uniqueChunkSize)

	if ratioNone < 1.0 || ratioZstd < 1.0 {
		t.Logf("warning: dedup ratio < 1 (none=%.2f zstd=%.2f); may indicate test data mismatch", ratioNone, ratioZstd)
	}

	if ratioNone != ratioZstd {
		t.Fatalf("dedup ratio mismatch: none=%.2f zstd=%.2f", ratioNone, ratioZstd)
	}
	t.Logf("✓ dedup ratio identical: %.2f (total=%d unique=%d)", ratioNone, resultsNone.totalFileSize, resultsNone.uniqueChunkSize)

	// Compare file references
	if resultsNone.fileReferenceCount != resultsZstd.fileReferenceCount {
		t.Fatalf("file reference count mismatch: none=%d zstd=%d", resultsNone.fileReferenceCount, resultsZstd.fileReferenceCount)
	}
	t.Logf("✓ file reference count identical: %d", resultsNone.fileReferenceCount)

	// Verify restores are identical
	if !bytes.Equal(resultsNone.restoredBytes, resultsZstd.restoredBytes) {
		t.Fatalf("restored bytes differ between compression modes")
	}
	t.Logf("✓ restored bytes identical across compression modes")

	t.Logf("✓ dedup semantics validated for encryption=%s", encryptionCodec)
}

// step68DedupResults holds metrics for a dedup test scenario.
type step68DedupResults struct {
	chunkCount         int64
	fileChunkCount     int64
	chunkHashes        []string
	totalFileSize      int64
	uniqueChunkSize    int64
	fileReferenceCount int64
	restoredBytes      []byte
}

// testStep68RunDedupScenario stores duplicate/overlapping files and returns dedup metrics.
func testStep68RunDedupScenario(t *testing.T, encryptionCodec, compressionCodec string) step68DedupResults {
	t.Helper()

	// Setup
	tmp := t.TempDir()
	origContainers := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainers })

	os.MkdirAll(container.ContainersDir, 0o755)
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	if encryptionCodec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	} else {
		os.Setenv("COLDKEEP_KEY", "")
	}

	os.Setenv("COLDKEEP_COMPRESSION", compressionCodec)
	if compressionCodec == storagecompression.CompressionZstd {
		os.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	// Set compression config
	tx, _ := dbconn.Begin()
	storage.SetDefaultCompression(tx, compressionCodec)
	if compressionCodec == storagecompression.CompressionZstd {
		storage.SetDefaultCompressionLevel(tx, 3)
	}
	tx.Commit()

	// Create test payloads
	payload1 := []byte("duplicate-content-file-1-for-dedup-test")
	payload2 := []byte("duplicate-content-file-1-for-dedup-test")                      // Exact duplicate
	payload3 := []byte("duplicate-content-file-1-for-dedup-test-with-suffix-appended") // Partial overlap
	payload4 := []byte("different-content-file-4")

	// Store files
	codec := blocks.Codec(encryptionCodec)
	writer := container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn)

	file1ID := storeStep68File(t, dbconn, writer, tmp, "file1.bin", payload1, codec)
	_ = storeStep68File(t, dbconn, writer, tmp, "file2.bin", payload2, codec) // Duplicate of file1
	_ = storeStep68File(t, dbconn, writer, tmp, "file3.bin", payload3, codec) // Partial overlap
	_ = storeStep68File(t, dbconn, writer, tmp, "file4.bin", payload4, codec) // Different content

	// Query dedup metrics
	var chunkCount, fileChunkCount, fileReferenceCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunkCount); err != nil {
		t.Fatalf("count chunks: %v", err)
	}

	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk`).Scan(&fileChunkCount); err != nil {
		t.Fatalf("count file_chunk: %v", err)
	}

	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file`).Scan(&fileReferenceCount); err != nil {
		t.Fatalf("count file refs: %v", err)
	}

	// Get chunk hashes in order to verify dedup graph
	rows, err := dbconn.Query(`
		SELECT c.chunk_hash FROM chunk c
		ORDER BY c.id ASC
	`)
	if err != nil {
		t.Fatalf("query chunk hashes: %v", err)
	}
	defer rows.Close()

	var chunkHashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			t.Fatalf("scan chunk hash: %v", err)
		}
		chunkHashes = append(chunkHashes, hash)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunks: %v", err)
	}

	// Calculate dedup metrics
	totalFileSize := int64(len(payload1) + len(payload2) + len(payload3) + len(payload4))

	var uniqueChunkSize int64
	if err := dbconn.QueryRow(`SELECT COALESCE(SUM(size), 0) FROM chunk`).Scan(&uniqueChunkSize); err != nil {
		t.Fatalf("sum chunk sizes: %v", err)
	}

	// Restore one file to verify content is unchanged
	restored := restoreStep68File(t, dbconn, tmp, file1ID, "restored.bin")

	return step68DedupResults{
		chunkCount:         chunkCount,
		fileChunkCount:     fileChunkCount,
		chunkHashes:        chunkHashes,
		totalFileSize:      totalFileSize,
		uniqueChunkSize:    uniqueChunkSize,
		fileReferenceCount: fileReferenceCount,
		restoredBytes:      restored,
	}
}

// storeStep68File stores a file and returns its file ID.
func storeStep68File(
	t *testing.T,
	dbconn *sql.DB,
	writer container.ContainerWriter,
	workDir string,
	filename string,
	payload []byte,
	codec blocks.Codec,
) int64 {
	t.Helper()

	path := filepath.Join(workDir, filename)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
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

	return result.FileID
}

// restoreStep68File restores a file and returns the bytes.
func restoreStep68File(t *testing.T, dbconn *sql.DB, workDir string, fileID int64, filename string) []byte {
	t.Helper()

	path := filepath.Join(workDir, filename)
	if err := storage.RestoreFileWithDB(dbconn, fileID, path); err != nil {
		t.Fatalf("restore file: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored: %v", err)
	}

	return data
}

// TestStep68CrossCompressionDedupConsistency tests that dedup works identically across all compression/encryption modes.
func TestStep68CrossCompressionDedupConsistency(t *testing.T) {
	testgate.RequireDB(t)

	if testing.Short() {
		t.Skip("skipping cross-compression dedup in short mode")
	}

	// Test all 4 mode combinations
	modes := []struct {
		name        string
		encryption  string
		compression string
	}{
		{"plain-none", "plain", storagecompression.CompressionNone},
		{"plain-zstd", "plain", storagecompression.CompressionZstd},
		{"aes-none", "aes-gcm", storagecompression.CompressionNone},
		{"aes-zstd", "aes-gcm", storagecompression.CompressionZstd},
	}

	var baseline step68DedupResults
	for i, mode := range modes {
		result := testStep68RunDedupScenario(t, mode.encryption, mode.compression)

		if i == 0 {
			baseline = result
		} else {
			// Verify all modes produce identical dedup metrics
			if result.chunkCount != baseline.chunkCount {
				t.Fatalf("%s: chunk count mismatch vs baseline: got %d want %d", mode.name, result.chunkCount, baseline.chunkCount)
			}
			if result.fileChunkCount != baseline.fileChunkCount {
				t.Fatalf("%s: file_chunk count mismatch vs baseline: got %d want %d", mode.name, result.fileChunkCount, baseline.fileChunkCount)
			}
			if result.fileReferenceCount != baseline.fileReferenceCount {
				t.Fatalf("%s: file reference count mismatch vs baseline: got %d want %d", mode.name, result.fileReferenceCount, baseline.fileReferenceCount)
			}

			// Chunk hashes should be identical
			if len(result.chunkHashes) != len(baseline.chunkHashes) {
				t.Fatalf("%s: chunk hash count mismatch: got %d want %d", mode.name, len(result.chunkHashes), len(baseline.chunkHashes))
			}

			for j, hash := range result.chunkHashes {
				if hash != baseline.chunkHashes[j] {
					t.Fatalf("%s: chunk hash %d mismatch: got %s want %s", mode.name, j, hash, baseline.chunkHashes[j])
				}
			}

			t.Logf("✓ %s dedup metrics match baseline", mode.name)
		}
	}

	t.Logf("✓ dedup consistency validated across all compression/encryption modes")
}
