package storage

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
)

func writeFeatureGateInputFile(t *testing.T, data []byte) string {
	t.Helper()
	inPath := filepath.Join(t.TempDir(), "feature-gate-input.bin")
	if err := os.WriteFile(inPath, data, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}
	return inPath
}

func readStoredBlockCompressionMetaForFile(t *testing.T, dbconn *sql.DB, fileID int64) (codec string, plaintextSize, compressedSize, storedSize int64) {
	t.Helper()
	if err := dbconn.QueryRow(`
		SELECT b.compression_codec, b.plaintext_size, b.compressed_size, b.stored_size
		FROM storage_blocks b
		JOIN chunk_block_refs r ON r.block_id = b.id
		JOIN file_chunk fc ON fc.chunk_id = r.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY b.id ASC
		LIMIT 1
	`, fileID).Scan(&codec, &plaintextSize, &compressedSize, &storedSize); err != nil {
		t.Fatalf("read storage block compression metadata for file %d: %v", fileID, err)
	}
	return codec, plaintextSize, compressedSize, storedSize
}

func TestCompressionFixtureDefaultsToNone(t *testing.T) {
	repo := NewTestRepository(t, WithCompression("none"))
	payload := bytes.Repeat([]byte("none-default-feature-gate-"), 128)

	inPath := writeFeatureGateInputFile(t, payload)
	result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store with explicit none compression fixture: %v", err)
	}

	codec, plaintextSize, compressedSize, storedSize := readStoredBlockCompressionMetaForFile(t, repo.DB, result.FileID)
	if codec != "none" {
		t.Fatalf("expected compression_codec=none in default fixture, got %q", codec)
	}
	if plaintextSize <= 0 || compressedSize <= 0 || storedSize <= 0 {
		t.Fatalf("expected positive size metadata, got plaintext=%d compressed=%d stored=%d", plaintextSize, compressedSize, storedSize)
	}
	if compressedSize != plaintextSize {
		t.Fatalf("expected none compression to keep compressed_size==plaintext_size, got compressed=%d plaintext=%d", compressedSize, plaintextSize)
	}
}

func TestFeatureGatedCompressionZstdStoreAndRestore(t *testing.T) {
	RequireTestCompression(t, "zstd")

	repo := NewTestRepository(t, WithCompression("zstd"), WithCompressionLevel(3))
	payload := bytes.Repeat([]byte("zstd-feature-gate-repetitive-payload-"), 4096)

	inPath := writeFeatureGateInputFile(t, payload)
	result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store with zstd feature gate: %v", err)
	}

	codec, plaintextSize, compressedSize, _ := readStoredBlockCompressionMetaForFile(t, repo.DB, result.FileID)
	if codec != "zstd" {
		t.Fatalf("expected compression_codec=zstd, got %q", codec)
	}
	if compressedSize >= plaintextSize {
		t.Fatalf("expected zstd to reduce repetitive payload size, got compressed=%d plaintext=%d", compressedSize, plaintextSize)
	}

	outPath := filepath.Join(t.TempDir(), "feature-gate-zstd-restore.bin")
	if _, err := restoreFileWithDBAndDir(repo.DB, result.FileID, outPath, repo.ContainersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore zstd feature-gated file: %v", err)
	}

	restored, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !bytes.Equal(restored, payload) {
		t.Fatalf("restored payload mismatch for zstd feature-gated test")
	}
}
