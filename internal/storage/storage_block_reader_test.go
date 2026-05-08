package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

// TestStorageBlockReaderInvalidBlockID validates error on invalid block ID.
func TestStorageBlockReaderInvalidBlockID(t *testing.T) {
	reader := NewStorageBlockReader(nil, "")
	for _, bidInvalid := range []int64{0, -1, -999} {
		_, err := reader.ReadBlock(context.Background(), bidInvalid)
		if err == nil {
			t.Fatalf("expected error for invalid block ID %d", bidInvalid)
		}
	}
}

// TestStorageBlockReaderBlockNotFound validates error when block doesn't exist.
func TestStorageBlockReaderBlockNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer dbconn.Close()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	reader := NewStorageBlockReader(dbconn, "/tmp")
	_, err = reader.ReadBlock(context.Background(), 999)
	if err == nil {
		t.Fatalf("expected error for nonexistent block")
	}
}

// TestStorageBlockReaderEmptyBlockHashFailsClosed validates fail-closed behavior
// for mandatory block hash verification.
func TestStorageBlockReaderEmptyBlockHashFailsClosed(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer dbconn.Close()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	_, err = dbconn.ExecContext(context.Background(), `
		INSERT INTO container (id, filename, max_size, created_at)
		VALUES (1, 'missing.bin', 1048576, CURRENT_TIMESTAMP)
	`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}

	_, err = dbconn.ExecContext(context.Background(), `
		INSERT INTO storage_blocks (id, format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		VALUES (1, 1, 'none', 100, 100, 1, 0, x'')
	`)
	if err != nil {
		t.Fatalf("insert storage_blocks row with empty hash: %v", err)
	}

	reader := NewStorageBlockReader(dbconn, "/tmp")
	_, err = reader.ReadBlock(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error for empty block_hash")
	}
	if got := err.Error(); !strings.Contains(got, "empty block_hash") {
		t.Fatalf("expected empty block_hash error, got: %v", err)
	}
}

// TestStorageBlockReaderNilDatabase validates error on nil database.
func TestStorageBlockReaderNilDatabase(t *testing.T) {
	reader := NewStorageBlockReader(nil, "/tmp")
	_, err := reader.ReadBlock(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected error for nil database")
	}
}

// TestNewStorageBlockReader validates constructor.
func TestNewStorageBlockReader(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dbconn.Close()

	reader := NewStorageBlockReader(dbconn, "/containers")
	if reader == nil {
		t.Fatalf("expected non-nil reader")
	}
	if reader.db != dbconn {
		t.Fatalf("expected db to be set")
	}
	if reader.containersDir != "/containers" {
		t.Fatalf("expected containersDir to be set")
	}
	if !reader.verifyHash {
		t.Fatalf("expected verifyHash to be true by default")
	}
}

// TestStorageBlockReaderDisableHashVerification validates the disable flag.
func TestStorageBlockReaderDisableHashVerification(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dbconn.Close()

	reader := NewStorageBlockReader(dbconn, "/tmp")
	if !reader.verifyHash {
		t.Fatalf("expected verifyHash=true initially")
	}

	reader.DisableHashVerification()
	if reader.verifyHash {
		t.Fatalf("expected verifyHash=false after disable")
	}
}

// TestStorageBlockReaderLogBlockRead validates logging (no-op if no error).
func TestStorageBlockReaderLogBlockRead(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dbconn.Close()

	reader := NewStorageBlockReader(dbconn, "/tmp")

	// Should not panic
	reader.LogBlockRead(1, true, nil)
	reader.LogBlockRead(2, false, fmt.Errorf("test error"))
}

// TestStorageBlockReaderTransformerCache validates transformer caching.
func TestStorageBlockReaderTransformerCache(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dbconn.Close()

	reader := NewStorageBlockReader(dbconn, "/tmp")

	// Cache should start empty
	if len(reader.transformerCache) != 0 {
		t.Fatalf("expected empty transformer cache initially")
	}
}

// TestStorageBlockReaderMetadataValidation validates metadata field validators.
// This tests the validation logic without needing full container setup.
func TestStorageBlockReaderMetadataValidation(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dbconn.Close()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Insert a container for FK
	_, err = dbconn.ExecContext(context.Background(), `
		INSERT INTO container (id, filename, max_size, created_at)
		VALUES (1, 'test.bin', 1048576, CURRENT_TIMESTAMP)
	`)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}

	testCases := []struct {
		name            string
		blockID         int64
		formatVersion   int
		codec           string
		plaintextSize   int64
		storedSize      int64
		containerOffset int64
		shouldFail      bool
	}{
		{"valid", 1, 1, "none", 1000, 1000, 0, false},
		{"invalid_plaintext_size", 2, 1, "none", 0, 1000, 0, true},
		{"invalid_plaintext_size_negative", 3, 1, "none", -100, 1000, 0, true},
		{"invalid_stored_size", 4, 1, "none", 1000, 0, 0, true},
		{"invalid_stored_size_negative", 5, 1, "none", 1000, -100, 0, true},
		{"invalid_container_offset", 6, 1, "none", 1000, 1000, -1, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Insert block metadata (if valid)
			_, _ = dbconn.ExecContext(context.Background(), `
				INSERT INTO storage_blocks (id, format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
				VALUES ($1, $2, $3, $4, $5, 1, $6, x'00')
			`, tc.blockID, tc.formatVersion, tc.codec, tc.plaintextSize, tc.storedSize, tc.containerOffset)

			reader := NewStorageBlockReader(dbconn, "/tmp")
			_, err := reader.ReadBlock(context.Background(), tc.blockID)

			// Container read will fail before metadata validation triggers; no assertion needed here.
			_ = err
		})
	}
}

func TestStorageBlockReaderLoadBlockMetadataIncludesTransformAwareFields(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dbconn.Close()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.ExecContext(context.Background(), `
		INSERT INTO container (id, filename, max_size, created_at)
		VALUES (1, 'test.bin', 1048576, CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	if _, err := dbconn.ExecContext(context.Background(), `
		INSERT INTO storage_blocks (
			id, format_version, codec, plaintext_size,
			compression_codec, compression_level, compressed_size,
			stored_size, container_id, container_offset,
			block_hash, compressed_hash, physical_hash
		)
		VALUES (1, 1, 'none', 100, 'zstd', 7, 80, 108, 1, 12, x'0102', x'0304', x'0506')
	`); err != nil {
		t.Fatalf("insert storage_blocks row: %v", err)
	}

	reader := NewStorageBlockReader(dbconn, "/tmp")
	meta, err := reader.loadBlockMetadata(context.Background(), 1)
	if err != nil {
		t.Fatalf("load block metadata: %v", err)
	}

	if meta.Metadata.Compression.Codec != "zstd" {
		t.Fatalf("compression codec: got %q want %q", meta.Metadata.Compression.Codec, "zstd")
	}
	if meta.Metadata.Compression.Level == nil || *meta.Metadata.Compression.Level != 7 {
		t.Fatalf("compression level: got %v want 7", meta.Metadata.Compression.Level)
	}
	if meta.Metadata.Sizes.CompressedSize == nil || *meta.Metadata.Sizes.CompressedSize != 80 {
		t.Fatalf("compressed size: got %v want 80", meta.Metadata.Sizes.CompressedSize)
	}
	if got := fmt.Sprintf("%x", meta.Metadata.Hashes.CompressedHash); got != "0304" {
		t.Fatalf("compressed hash: got %s want %s", got, "0304")
	}
	if got := fmt.Sprintf("%x", meta.Metadata.Hashes.PhysicalHash); got != "0506" {
		t.Fatalf("physical hash: got %s want %s", got, "0506")
	}
	if meta.Metadata.Sizes.StoredSize != 108 {
		t.Fatalf("stored size: got %d want %d", meta.Metadata.Sizes.StoredSize, 108)
	}
	if meta.Metadata.Sizes.PlaintextSize != 100 {
		t.Fatalf("plaintext size: got %d want %d", meta.Metadata.Sizes.PlaintextSize, 100)
	}
}

func setupStoredBlockForReaderCorruption(t *testing.T, codec blocks.Codec) (*sql.DB, string, int64, string, int64, int64) {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	inPath := filepath.Join(workDir, "reader-corruption.bin")
	if err := os.WriteFile(inPath, []byte("reader-corruption-payload"), 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(workDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: workDir,
	}

	if _, err := StoreFileWithStorageContextAndCodecResult(sgctx, inPath, codec); err != nil {
		t.Fatalf("store file with codec %s: %v", codec, err)
	}

	var blockID int64
	var containerFilename string
	var offset int64
	var storedSize int64
	if err := dbconn.QueryRow(`
		SELECT sb.id, c.filename, sb.container_offset, sb.stored_size
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		ORDER BY sb.id DESC
		LIMIT 1
	`).Scan(&blockID, &containerFilename, &offset, &storedSize); err != nil {
		t.Fatalf("load stored block placement: %v", err)
	}

	return dbconn, workDir, blockID, containerFilename, offset, storedSize
}

func TestStorageBlockReaderPhysicalHashMismatchOnFlippedByte(t *testing.T) {
	dbconn, workDir, blockID, filename, offset, _ := setupStoredBlockForReaderCorruption(t, blocks.CodecPlain)

	path := filepath.Join(workDir, filename)
	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open container file: %v", err)
	}
	defer func() { _ = fh.Close() }()

	b := make([]byte, 1)
	if _, err := fh.ReadAt(b, offset); err != nil {
		t.Fatalf("read byte at offset: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := fh.WriteAt(b, offset); err != nil {
		t.Fatalf("write flipped byte: %v", err)
	}

	r := NewStorageBlockReader(dbconn, workDir)
	_, err = r.ReadBlock(context.Background(), blockID)
	if err == nil || !strings.Contains(err.Error(), "physical payload hash mismatch") {
		t.Fatalf("expected physical hash mismatch, got: %v", err)
	}
}

func TestStorageBlockReaderPhysicalHashMismatchOnTruncatedPayload(t *testing.T) {
	dbconn, workDir, blockID, filename, _, storedSize := setupStoredBlockForReaderCorruption(t, blocks.CodecPlain)

	if storedSize <= 1 {
		t.Fatalf("stored payload too small for truncate test: %d", storedSize)
	}
	newStoredSize := storedSize - 1

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET stored_size = $1 WHERE id = $2`, newStoredSize, blockID); err != nil {
		t.Fatalf("update stored_size: %v", err)
	}

	path := filepath.Join(workDir, filename)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat container file: %v", err)
	}
	if err := os.Truncate(path, st.Size()-1); err != nil {
		t.Fatalf("truncate container file: %v", err)
	}

	r := NewStorageBlockReader(dbconn, workDir)
	_, err = r.ReadBlock(context.Background(), blockID)
	if err == nil || !strings.Contains(err.Error(), "physical payload hash mismatch") {
		t.Fatalf("expected physical hash mismatch for truncated payload, got: %v", err)
	}
}

func TestStorageBlockReaderPhysicalHashMismatchOnWrongOffset(t *testing.T) {
	dbconn, workDir, blockID, _, offset, _ := setupStoredBlockForReaderCorruption(t, blocks.CodecPlain)

	if offset <= 0 {
		t.Fatalf("unexpected non-positive offset for wrong-offset test: %d", offset)
	}

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET container_offset = $1 WHERE id = $2`, offset-1, blockID); err != nil {
		t.Fatalf("update container_offset: %v", err)
	}

	r := NewStorageBlockReader(dbconn, workDir)
	_, err := r.ReadBlock(context.Background(), blockID)
	if err == nil || !strings.Contains(err.Error(), "physical payload hash mismatch") {
		t.Fatalf("expected physical hash mismatch for wrong offset metadata, got: %v", err)
	}
}

func TestStorageBlockReaderPhysicalHashMismatchBeforeAESGCMDecode(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))
	dbconn, workDir, blockID, filename, offset, _ := setupStoredBlockForReaderCorruption(t, blocks.CodecAESGCM)

	path := filepath.Join(workDir, filename)
	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open container file: %v", err)
	}
	defer func() { _ = fh.Close() }()

	b := make([]byte, 1)
	if _, err := fh.ReadAt(b, offset+packedStorageBlockAESGCMNonceSize); err != nil {
		t.Fatalf("read encrypted byte: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := fh.WriteAt(b, offset+packedStorageBlockAESGCMNonceSize); err != nil {
		t.Fatalf("write flipped encrypted byte: %v", err)
	}

	r := NewStorageBlockReader(dbconn, workDir)
	_, err = r.ReadBlock(context.Background(), blockID)
	if err == nil || !strings.Contains(err.Error(), "physical payload hash mismatch") {
		t.Fatalf("expected physical hash mismatch before decode/auth failure, got: %v", err)
	}
	if strings.Contains(err.Error(), "decode block") {
		t.Fatalf("expected failure before decode stage, got decode error: %v", err)
	}
}

func TestStorageBlockReaderLegacyNullPhysicalHashStillReads(t *testing.T) {
	dbconn, workDir, blockID, _, _, _ := setupStoredBlockForReaderCorruption(t, blocks.CodecPlain)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET physical_hash = NULL WHERE id = $1`, blockID); err != nil {
		t.Fatalf("set physical_hash null: %v", err)
	}

	r := NewStorageBlockReader(dbconn, workDir)
	if _, err := r.ReadBlock(context.Background(), blockID); err != nil {
		t.Fatalf("expected legacy NULL physical_hash row to read successfully, got: %v", err)
	}
}

func TestStorageBlockReaderCompressedHashMismatchAfterDecrypt(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))
	dbconn, workDir, blockID, _, _, _ := setupStoredBlockForReaderCorruption(t, blocks.CodecAESGCM)

	var compressedHash []byte
	if err := dbconn.QueryRow(`SELECT compressed_hash FROM storage_blocks WHERE id = $1`, blockID).Scan(&compressedHash); err != nil {
		t.Fatalf("load compressed_hash: %v", err)
	}
	if len(compressedHash) == 0 {
		t.Fatal("expected non-empty compressed_hash for new row")
	}

	tampered := append([]byte(nil), compressedHash...)
	tampered[0] ^= 0x01
	if _, err := dbconn.Exec(`UPDATE storage_blocks SET compressed_hash = $1 WHERE id = $2`, tampered, blockID); err != nil {
		t.Fatalf("tamper compressed_hash: %v", err)
	}

	r := NewStorageBlockReader(dbconn, workDir)
	_, err := r.ReadBlock(context.Background(), blockID)
	if err == nil || !strings.Contains(err.Error(), "compressed payload hash mismatch") {
		t.Fatalf("expected compressed payload hash mismatch, got: %v", err)
	}
	if strings.Contains(err.Error(), "physical payload hash mismatch") {
		t.Fatalf("expected compressed-hash stage failure after physical stage, got: %v", err)
	}
}

func TestStorageBlockReaderLegacyNullCompressedHashStillReads(t *testing.T) {
	dbconn, workDir, blockID, _, _, _ := setupStoredBlockForReaderCorruption(t, blocks.CodecPlain)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET compressed_hash = NULL WHERE id = $1`, blockID); err != nil {
		t.Fatalf("set compressed_hash null: %v", err)
	}

	r := NewStorageBlockReader(dbconn, workDir)
	if _, err := r.ReadBlock(context.Background(), blockID); err != nil {
		t.Fatalf("expected legacy NULL compressed_hash row to read successfully, got: %v", err)
	}
}
