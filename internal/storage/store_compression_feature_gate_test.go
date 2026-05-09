package storage

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
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

type blockCompressionMetadata struct {
	Codec          string
	CompressionLvl sql.NullInt64
	PlaintextSize  int64
	CompressedSize int64
	StoredSize     int64
	BlockHash      []byte
	CompressedHash []byte
	PhysicalHash   []byte
}

func readStoredBlockMetaWithHashesForFile(t *testing.T, dbconn *sql.DB, fileID int64) blockCompressionMetadata {
	t.Helper()

	var row blockCompressionMetadata
	if err := dbconn.QueryRow(`
		SELECT b.compression_codec, b.compression_level,
		       b.plaintext_size, b.compressed_size, b.stored_size,
		       b.block_hash, b.compressed_hash, b.physical_hash
		FROM storage_blocks b
		JOIN chunk_block_refs r ON r.block_id = b.id
		JOIN file_chunk fc ON fc.chunk_id = r.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY b.id ASC
		LIMIT 1
	`, fileID).Scan(
		&row.Codec,
		&row.CompressionLvl,
		&row.PlaintextSize,
		&row.CompressedSize,
		&row.StoredSize,
		&row.BlockHash,
		&row.CompressedHash,
		&row.PhysicalHash,
	); err != nil {
		t.Fatalf("read storage block metadata with hashes for file %d: %v", fileID, err)
	}
	return row
}

func deterministicNoise(size int) []byte {
	out := make([]byte, size)
	var x uint32 = 0x9e3779b9
	for i := 0; i < size; i++ {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		out[i] = byte(x)
	}
	return out
}

func TestFeatureGatedCompressionStoreIfSmallerDeterministicCases(t *testing.T) {
	RequireTestCompression(t, "zstd")

	repo := NewTestRepository(t, WithCompression("zstd"), WithCompressionLevel(3))

	testCases := []struct {
		name          string
		payload       []byte
		expectedCodec string
	}{
		{
			name:          "compressible",
			payload:       bytes.Repeat([]byte("store-if-smaller-compressible-"), 8192),
			expectedCodec: storagecompression.CompressionZstd,
		},
		{
			name:          "random",
			payload:       deterministicNoise(64 * 1024),
			expectedCodec: storagecompression.CompressionNone,
		},
		{
			name:          "tiny",
			payload:       []byte("tiny"),
			expectedCodec: storagecompression.CompressionNone,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			inPath := writeFeatureGateInputFile(t, tc.payload)
			result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, inPath, blocks.CodecPlain)
			if err != nil {
				t.Fatalf("store payload: %v", err)
			}

			meta := readStoredBlockMetaWithHashesForFile(t, repo.DB, result.FileID)
			if meta.Codec != tc.expectedCodec {
				t.Fatalf("expected codec=%q, got %q", tc.expectedCodec, meta.Codec)
			}
			if meta.PlaintextSize <= 0 || meta.CompressedSize <= 0 || meta.StoredSize <= 0 {
				t.Fatalf("expected positive sizes, got plaintext=%d compressed=%d stored=%d", meta.PlaintextSize, meta.CompressedSize, meta.StoredSize)
			}
			if len(meta.BlockHash) != 32 || len(meta.CompressedHash) != 32 || len(meta.PhysicalHash) != 32 {
				t.Fatalf("expected 32-byte hashes, got logical=%d compressed=%d physical=%d", len(meta.BlockHash), len(meta.CompressedHash), len(meta.PhysicalHash))
			}

			if meta.Codec == storagecompression.CompressionNone {
				if meta.CompressedSize != meta.PlaintextSize {
					t.Fatalf("codec=none invariant violated: compressed=%d plaintext=%d", meta.CompressedSize, meta.PlaintextSize)
				}
				if !bytes.Equal(meta.CompressedHash, meta.BlockHash) {
					t.Fatalf("codec=none hash invariant violated: compressed_hash != block_hash")
				}
				if meta.CompressionLvl.Valid {
					t.Fatalf("expected NULL compression_level for codec=none, got %d", meta.CompressionLvl.Int64)
				}
			} else {
				if meta.CompressedSize >= meta.PlaintextSize {
					t.Fatalf("codec=zstd invariant violated: compressed=%d plaintext=%d", meta.CompressedSize, meta.PlaintextSize)
				}
				if !meta.CompressionLvl.Valid || meta.CompressionLvl.Int64 != 3 {
					t.Fatalf("expected compression_level=3 for codec=zstd, got %+v", meta.CompressionLvl)
				}
			}

			if meta.StoredSize != meta.CompressedSize {
				t.Fatalf("plain transform invariant violated: stored_size=%d compressed_size=%d", meta.StoredSize, meta.CompressedSize)
			}
			if !bytes.Equal(meta.PhysicalHash, meta.CompressedHash) {
				t.Fatalf("plain transform invariant violated: physical_hash != compressed_hash")
			}

			outPath := filepath.Join(t.TempDir(), tc.name+"-restored.bin")
			if _, err := restoreFileWithDBAndDir(repo.DB, result.FileID, outPath, repo.ContainersDir, RestoreOptions{Overwrite: true}); err != nil {
				t.Fatalf("restore payload: %v", err)
			}

			restored, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read restored payload: %v", err)
			}
			if !bytes.Equal(restored, tc.payload) {
				t.Fatalf("restored payload mismatch")
			}

			if err := verifypkg.VerifyFileStandardWithContainersDir(repo.DB, int(result.FileID), repo.ContainersDir); err != nil {
				t.Fatalf("verify file standard: %v", err)
			}
		})
	}
}
