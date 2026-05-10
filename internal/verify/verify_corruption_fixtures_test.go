package verify

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

type verifyCorruptionRepo struct {
	dbconn        *sql.DB
	containersDir string
}

func CorruptContainerByte(t *testing.T, repo verifyCorruptionRepo, blockID int64, byteOffset int64) {
	t.Helper()

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, repo.dbconn, blockID, repo.containersDir)
	if byteOffset < 0 || byteOffset >= storedSize {
		t.Fatalf("corrupt container byte offset out of range block_id=%d offset=%d stored_size=%d", blockID, byteOffset, storedSize)
	}

	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	payload[byteOffset] ^= 0xFF
	overwritePackedStoredBytesForTest(t, path, offset, payload)
}

func TruncateContainerPayload(t *testing.T, repo verifyCorruptionRepo, blockID int64, nBytes int64) {
	t.Helper()

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, repo.dbconn, blockID, repo.containersDir)
	if nBytes <= 0 || nBytes >= storedSize {
		t.Fatalf("truncate bytes out of range block_id=%d n_bytes=%d stored_size=%d", blockID, nBytes, storedSize)
	}

	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	truncated := payload[:len(payload)-int(nBytes)]
	overwritePackedStoredBytesForTest(t, path, offset, truncated)
	UpdateStorageBlockField(t, repo, blockID, "stored_size", int64(len(truncated)))
}

func UpdateStorageBlockField(t *testing.T, repo verifyCorruptionRepo, blockID int64, field string, value any) {
	t.Helper()

	allowed := map[string]struct{}{
		"block_hash":        {},
		"compressed_hash":   {},
		"physical_hash":     {},
		"codec":             {},
		"compression_codec": {},
		"stored_size":       {},
		"compressed_size":   {},
		"plaintext_size":    {},
	}
	if _, ok := allowed[field]; !ok {
		t.Fatalf("unsupported storage_blocks field for corruption fixture: %q", field)
	}

	query := fmt.Sprintf("UPDATE storage_blocks SET %s = $1 WHERE id = $2", field)
	res, err := repo.dbconn.Exec(query, value, blockID)
	if err != nil {
		t.Fatalf("update storage_blocks.%s for block %d: %v", field, blockID, err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected for storage_blocks.%s update: %v", field, err)
	}
	if rowsAffected != 1 {
		t.Fatalf("expected exactly one updated storage_blocks row for block %d, got %d", blockID, rowsAffected)
	}
}

func UpdateChunkBlockRefField(t *testing.T, repo verifyCorruptionRepo, blockID int64, chunkID int64, field string, value any) {
	t.Helper()

	allowed := map[string]struct{}{
		"offset_in_block": {},
		"size_in_block":   {},
	}
	if _, ok := allowed[field]; !ok {
		t.Fatalf("unsupported chunk_block_refs field for corruption fixture: %q", field)
	}

	query := fmt.Sprintf("UPDATE chunk_block_refs SET %s = $1 WHERE block_id = $2 AND chunk_id = $3", field)
	res, err := repo.dbconn.Exec(query, value, blockID, chunkID)
	if err != nil {
		t.Fatalf("update chunk_block_refs.%s for block %d chunk %d: %v", field, blockID, chunkID, err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected for chunk_block_refs.%s update: %v", field, err)
	}
	if rowsAffected != 1 {
		t.Fatalf("expected exactly one updated chunk_block_refs row for block %d chunk %d, got %d", blockID, chunkID, rowsAffected)
	}
}

func firstChunkIDForBlock(t *testing.T, dbconn *sql.DB, blockID int64) int64 {
	t.Helper()

	var chunkID int64
	if err := dbconn.QueryRow(`SELECT chunk_id FROM chunk_block_refs WHERE block_id = $1 ORDER BY chunk_id LIMIT 1`, blockID).Scan(&chunkID); err != nil {
		t.Fatalf("query first chunk id for block %d: %v", blockID, err)
	}
	return chunkID
}

func assertPhysicalStageVerifyFailure(t *testing.T, err error, blockID int64) {
	t.Helper()

	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}

	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %T %v", err, err)
	}
	if vf.Stage != VerifyStagePhysicalPayload {
		t.Fatalf("expected stage %q, got %q (err=%v)", VerifyStagePhysicalPayload, vf.Stage, err)
	}
	if vf.BlockID == nil || *vf.BlockID != blockID {
		t.Fatalf("expected block_id=%d in failure, got: %+v", blockID, vf)
	}
	if vf.ContainerID == nil {
		t.Fatalf("expected container_id in failure, got: %+v", vf)
	}
	if vf.Offset == nil {
		t.Fatalf("expected offset in failure, got: %+v", vf)
	}
	if !strings.HasPrefix(err.Error(), verifyErrPhysicalHashMismatch+":") {
		t.Fatalf("expected category prefix %q, got: %v", verifyErrPhysicalHashMismatch+":", err)
	}
	if strings.Contains(err.Error(), "stage=decrypt") || strings.Contains(err.Error(), "cipher: message authentication failed") {
		t.Fatalf("expected failure before decrypt stage, got: %v", err)
	}
}

func assertDecryptStageVerifyFailure(t *testing.T, err error, blockID int64) {
	t.Helper()

	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}

	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %T %v", err, err)
	}
	if vf.Stage != VerifyStageDecrypt {
		t.Fatalf("expected stage %q, got %q (err=%v)", VerifyStageDecrypt, vf.Stage, err)
	}
	if vf.BlockID == nil || *vf.BlockID != blockID {
		t.Fatalf("expected block_id=%d in failure, got: %+v", blockID, vf)
	}
	if vf.ContainerID == nil {
		t.Fatalf("expected container_id in failure, got: %+v", vf)
	}
	if vf.Offset == nil {
		t.Fatalf("expected offset in failure, got: %+v", vf)
	}
	if !strings.HasPrefix(err.Error(), verifyErrMetadataInvalid+":") {
		t.Fatalf("expected category prefix %q, got: %v", verifyErrMetadataInvalid+":", err)
	}
	if !strings.Contains(err.Error(), "cipher: message authentication failed") {
		t.Fatalf("expected AES-GCM auth failure detail, got: %v", err)
	}
	if strings.Contains(err.Error(), "stage=decompress") || strings.Contains(err.Error(), "stage=logical_hash") {
		t.Fatalf("expected decrypt-stage failure, got mislabeled stage: %v", err)
	}
	if strings.HasPrefix(err.Error(), verifyErrCompressedHashMismatch+":") || strings.HasPrefix(err.Error(), verifyErrBlockHashMismatch+":") {
		t.Fatalf("expected decrypt-stage metadata_invalid, got later-stage mismatch: %v", err)
	}
}

func assertCompressedHashStageVerifyFailure(t *testing.T, err error, blockID int64) {
	t.Helper()

	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}

	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %T %v", err, err)
	}
	if vf.Stage != VerifyStageCompressedHash {
		t.Fatalf("expected stage %q, got %q (err=%v)", VerifyStageCompressedHash, vf.Stage, err)
	}
	if vf.BlockID == nil || *vf.BlockID != blockID {
		t.Fatalf("expected block_id=%d in failure, got: %+v", blockID, vf)
	}
	if vf.ContainerID == nil {
		t.Fatalf("expected container_id in failure, got: %+v", vf)
	}
	if vf.Offset == nil {
		t.Fatalf("expected offset in failure, got: %+v", vf)
	}
	if !strings.HasPrefix(err.Error(), verifyErrCompressedHashMismatch+":") {
		t.Fatalf("expected category prefix %q, got: %v", verifyErrCompressedHashMismatch+":", err)
	}
	if strings.Contains(err.Error(), "stage=decompress") || strings.Contains(err.Error(), "stage=logical_hash") {
		t.Fatalf("expected compressed_hash-stage failure before decompression/logical hash, got: %v", err)
	}
	if strings.Contains(err.Error(), "cipher: message authentication failed") {
		t.Fatalf("expected decrypt to succeed and fail at compressed_hash stage, got: %v", err)
	}
}

func assertDecompressStageVerifyFailure(t *testing.T, err error, blockID int64, detailSubstring string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}

	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %T %v", err, err)
	}
	if vf.Stage != VerifyStageDecompress {
		t.Fatalf("expected stage %q, got %q (err=%v)", VerifyStageDecompress, vf.Stage, err)
	}
	if vf.BlockID == nil || *vf.BlockID != blockID {
		t.Fatalf("expected block_id=%d in failure, got: %+v", blockID, vf)
	}
	if vf.ContainerID == nil {
		t.Fatalf("expected container_id in failure, got: %+v", vf)
	}
	if vf.Offset == nil {
		t.Fatalf("expected offset in failure, got: %+v", vf)
	}
	if !strings.HasPrefix(err.Error(), verifyErrMetadataInvalid+":") {
		t.Fatalf("expected category prefix %q, got: %v", verifyErrMetadataInvalid+":", err)
	}
	if detailSubstring != "" && !strings.Contains(err.Error(), detailSubstring) {
		t.Fatalf("expected error containing %q, got: %v", detailSubstring, err)
	}
	if strings.Contains(err.Error(), "stage=logical_hash") || strings.Contains(err.Error(), "stage=block_decode") {
		t.Fatalf("expected decompression-stage failure before logical/decode stages, got: %v", err)
	}
}

func assertLogicalHashStageVerifyFailure(t *testing.T, err error, blockID int64) {
	t.Helper()

	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}

	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %T %v", err, err)
	}
	if vf.Stage != VerifyStageLogicalHash {
		t.Fatalf("expected stage %q, got %q (err=%v)", VerifyStageLogicalHash, vf.Stage, err)
	}
	if vf.BlockID == nil || *vf.BlockID != blockID {
		t.Fatalf("expected block_id=%d in failure, got: %+v", blockID, vf)
	}
	if vf.ContainerID == nil {
		t.Fatalf("expected container_id in failure, got: %+v", vf)
	}
	if vf.Offset == nil {
		t.Fatalf("expected offset in failure, got: %+v", vf)
	}
	if !strings.HasPrefix(err.Error(), verifyErrBlockHashMismatch+":") {
		t.Fatalf("expected category prefix %q, got: %v", verifyErrBlockHashMismatch+":", err)
	}
	if !strings.Contains(err.Error(), "logical block hash mismatch") {
		t.Fatalf("expected logical hash mismatch detail, got: %v", err)
	}
	if strings.Contains(err.Error(), "stage=block_decode") || strings.Contains(err.Error(), "decode logical block") {
		t.Fatalf("expected logical_hash-stage failure before decode stage, got: %v", err)
	}
}

func assertBlockDecodeStageVerifyFailure(t *testing.T, err error, blockID int64) {
	t.Helper()

	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}

	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %T %v", err, err)
	}
	if vf.Stage != VerifyStageBlockDecode {
		t.Fatalf("expected stage %q, got %q (err=%v)", VerifyStageBlockDecode, vf.Stage, err)
	}
	if vf.BlockID == nil || *vf.BlockID != blockID {
		t.Fatalf("expected block_id=%d in failure, got: %+v", blockID, vf)
	}
	if vf.ContainerID == nil {
		t.Fatalf("expected container_id in failure, got: %+v", vf)
	}
	if vf.Offset == nil {
		t.Fatalf("expected offset in failure, got: %+v", vf)
	}
	if !strings.HasPrefix(err.Error(), verifyErrUnsupportedBlock+":") {
		t.Fatalf("expected category prefix %q, got: %v", verifyErrUnsupportedBlock+":", err)
	}
	if !strings.Contains(err.Error(), "decode logical block") {
		t.Fatalf("expected decode logical block detail, got: %v", err)
	}
	if strings.Contains(err.Error(), "stage=chunk_refs") {
		t.Fatalf("expected malformed block to fail at decode stage first, got: %v", err)
	}
}

func assertChunkRefsStageVerifyFailure(t *testing.T, err error, blockID int64, category string, detailSubstring string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}

	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %T %v", err, err)
	}
	if vf.Stage != VerifyStageChunkRefs {
		t.Fatalf("expected stage %q, got %q (err=%v)", VerifyStageChunkRefs, vf.Stage, err)
	}
	if vf.BlockID == nil || *vf.BlockID != blockID {
		t.Fatalf("expected block_id=%d in failure, got: %+v", blockID, vf)
	}
	if vf.ContainerID == nil {
		t.Fatalf("expected container_id in failure, got: %+v", vf)
	}
	if vf.Offset == nil {
		t.Fatalf("expected offset in failure, got: %+v", vf)
	}
	if category != "" && !strings.HasPrefix(err.Error(), category+":") {
		t.Fatalf("expected category prefix %q, got: %v", category+":", err)
	}
	if detailSubstring != "" && !strings.Contains(err.Error(), detailSubstring) {
		t.Fatalf("expected error containing %q, got: %v", detailSubstring, err)
	}
}

func blockChunkIDs(t *testing.T, dbconn *sql.DB, blockID int64) []int64 {
	t.Helper()

	rows, err := dbconn.Query(`SELECT chunk_id FROM chunk_block_refs WHERE block_id = $1 ORDER BY offset_in_block ASC`, blockID)
	if err != nil {
		t.Fatalf("query chunk ids for block %d: %v", blockID, err)
	}
	defer func() { _ = rows.Close() }()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan chunk id for block %d: %v", blockID, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk ids for block %d: %v", blockID, err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected at least one chunk ref for block %d", blockID)
	}
	return ids
}

func rewritePackedBlockAndHashesForDecodeFixture(t *testing.T, repo verifyCorruptionRepo, blockID int64, mutate func([]byte)) {
	t.Helper()

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, repo.dbconn, blockID, repo.containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	mutate(payload)
	overwritePackedStoredBytesForTest(t, path, offset, payload)

	UpdateStorageBlockField(t, repo, blockID, "stored_size", int64(len(payload)))
	UpdateStorageBlockField(t, repo, blockID, "plaintext_size", int64(len(payload)))
	UpdateStorageBlockField(t, repo, blockID, "block_hash", blocks.HashLogical(payload))
	UpdateStorageBlockField(t, repo, blockID, "compressed_hash", blocks.HashCompressed(payload))
	UpdateStorageBlockField(t, repo, blockID, "physical_hash", blocks.HashPhysical(payload))
}

func TestCorruptionFixtureCorruptContainerByteDetectsPhysicalHashMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-physical-stage")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	CorruptContainerByte(t, repo, blockID, 0)

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrPhysicalHashMismatch+":") {
		t.Fatalf("expected physical_hash_mismatch from persisted-byte corruption, got: %v", err)
	}
}

func TestCorruptionFixturePhysicalStageFlipPersistedByteFailsBeforeDecrypt(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))

	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-physical-flip-before-decrypt")}, blocks.CodecAESGCM, storagecompression.CompressionZstd)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, repo.containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	if len(payload) < packedStorageBlockAESGCMNonceSize+1 {
		t.Fatalf("expected encrypted payload length > nonce prefix, got=%d", len(payload))
	}
	payload[packedStorageBlockAESGCMNonceSize] ^= 0xFF
	overwritePackedStoredBytesForTest(t, path, offset, payload)

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertPhysicalStageVerifyFailure(t, err, blockID)
}

func TestCorruptionFixturePhysicalStageTruncatePersistedPayload(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-physical-truncate")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	TruncateContainerPayload(t, repo, blockID, 1)

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertPhysicalStageVerifyFailure(t, err, blockID)
}

func TestCorruptionFixturePhysicalStageWrongBytesAtBlockOffset(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-physical-overwrite")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, repo.containersDir)
	wrong := bytes.Repeat([]byte{0xAC}, int(storedSize))
	overwritePackedStoredBytesForTest(t, path, offset, wrong)

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertPhysicalStageVerifyFailure(t, err, blockID)
}

func TestCorruptionFixturePhysicalStageDBPhysicalHashMismatch(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))

	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-physical-db-mismatch")}, blocks.CodecAESGCM, storagecompression.CompressionZstd)

	UpdateStorageBlockField(t, repo, blockID, "physical_hash", bytes.Repeat([]byte{0x00}, 32))

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertPhysicalStageVerifyFailure(t, err, blockID)
}

func TestCorruptionFixtureDecryptStageLegacyNullPhysicalHashDetectsAEADFailure(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))

	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-decrypt-auth-failure")}, blocks.CodecAESGCM, storagecompression.CompressionZstd)

	UpdateStorageBlockField(t, repo, blockID, "physical_hash", nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, repo.containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	if len(payload) <= packedStorageBlockAESGCMNonceSize {
		t.Fatalf("expected encrypted payload larger than nonce prefix, got=%d", len(payload))
	}
	payload[packedStorageBlockAESGCMNonceSize] ^= 0xFF
	overwritePackedStoredBytesForTest(t, path, offset, payload)

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertDecryptStageVerifyFailure(t, err, blockID)
}

func TestCorruptionFixtureDecryptStageUnencryptedBlocksSkipCleanly(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	_, _ = seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-decrypt-skip-plain")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	if err := verifyBlockPayloads(dbconn, repo.containersDir); err != nil {
		t.Fatalf("expected plain codec block verification to skip decrypt stage cleanly, got: %v", err)
	}
}

func TestCorruptionFixtureCompressedHashStageEncryptedBlockMismatchBeforeDecompress(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))

	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-compressed-hash-stage-encrypted")}, blocks.CodecAESGCM, storagecompression.CompressionZstd)

	UpdateStorageBlockField(t, repo, blockID, "compressed_hash", bytes.Repeat([]byte{0x00}, 32))

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertCompressedHashStageVerifyFailure(t, err, blockID)
}

func TestCorruptionFixtureDecompressStageMalformedCompressedPayloadAfterHashFixtureUpdate(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-decompress-malformed")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	path, offset, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, repo.containersDir)
	corruptedCompressed := []byte("not-a-valid-zstd-stream")
	overwritePackedStoredBytesForTest(t, path, offset, corruptedCompressed)

	UpdateStorageBlockField(t, repo, blockID, "stored_size", int64(len(corruptedCompressed)))
	UpdateStorageBlockField(t, repo, blockID, "compressed_size", int64(len(corruptedCompressed)))
	UpdateStorageBlockField(t, repo, blockID, "physical_hash", blocks.HashPhysical(corruptedCompressed))
	UpdateStorageBlockField(t, repo, blockID, "compressed_hash", blocks.HashCompressed(corruptedCompressed))

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("verifyBlockPayloads must not panic on malformed compressed payload: %v", r)
		}
	}()

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertDecompressStageVerifyFailure(t, err, blockID, "decompress codec=zstd")
}

func TestCorruptionFixtureLogicalHashStageMismatchAfterSuccessfulDecryptAndDecompress(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))

	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-logical-hash-stage")}, blocks.CodecAESGCM, storagecompression.CompressionZstd)

	UpdateStorageBlockField(t, repo, blockID, "block_hash", bytes.Repeat([]byte{0x00}, 32))

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	assertLogicalHashStageVerifyFailure(t, err, blockID)
}

func TestCorruptionFixtureBlockDecodeStageMalformedLogicalPayloadShapes(t *testing.T) {
	tests := []struct {
		name   string
		chunks [][]byte
		mutate func([]byte)
	}{
		{
			name:   "magic invalid",
			chunks: [][]byte{[]byte("decode-magic")},
			mutate: func(payload []byte) {
				binary.LittleEndian.PutUint32(payload[0:4], 0)
			},
		},
		{
			name:   "version unsupported",
			chunks: [][]byte{[]byte("decode-version")},
			mutate: func(payload []byte) {
				binary.LittleEndian.PutUint16(payload[4:6], 99)
			},
		},
		{
			name:   "chunk count mismatch",
			chunks: [][]byte{[]byte("decode-count")},
			mutate: func(payload []byte) {
				binary.LittleEndian.PutUint32(payload[8:12], 2)
			},
		},
		{
			name:   "chunk offsets invalid",
			chunks: [][]byte{[]byte("ABCD")},
			mutate: func(payload []byte) {
				binary.LittleEndian.PutUint64(payload[28:36], 1)
			},
		},
		{
			name:   "chunk lengths exceed plaintext size",
			chunks: [][]byte{[]byte("ABCD")},
			mutate: func(payload []byte) {
				binary.LittleEndian.PutUint64(payload[36:44], 99)
			},
		},
		{
			name:   "duplicate or overlapping chunk spans",
			chunks: [][]byte{[]byte("AA"), []byte("BB")},
			mutate: func(payload []byte) {
				// Second entry offset starts at 0 instead of 2, creating overlap.
				binary.LittleEndian.PutUint64(payload[52:60], 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbconn := openVerifyTestDB(t)
			defer func() { _ = dbconn.Close() }()

			repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
			blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, tc.chunks, nil)

			rewritePackedBlockAndHashesForDecodeFixture(t, repo, blockID, tc.mutate)

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("verifyBlockPayloads must not panic on malformed logical block fixture: %v", r)
				}
			}()

			err := verifyBlockPayloads(dbconn, repo.containersDir)
			assertBlockDecodeStageVerifyFailure(t, err, blockID)
		})
	}
}

func TestCorruptionFixtureDBHashFieldsMapToExpectedStages(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		value    []byte
		category string
	}{
		{name: "physical hash", field: "physical_hash", value: bytes.Repeat([]byte{0x00}, 32), category: verifyErrPhysicalHashMismatch},
		{name: "compressed hash", field: "compressed_hash", value: bytes.Repeat([]byte{0x00}, 32), category: verifyErrCompressedHashMismatch},
		{name: "logical block hash", field: "block_hash", value: bytes.Repeat([]byte{0x00}, 32), category: verifyErrBlockHashMismatch},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbconn := openVerifyTestDB(t)
			defer func() { _ = dbconn.Close() }()

			repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
			blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-db-hash-stage")}, blocks.CodecPlain, storagecompression.CompressionZstd)
			UpdateStorageBlockField(t, repo, blockID, tc.field, tc.value)

			err := verifyBlockPayloads(dbconn, repo.containersDir)
			if err == nil || !strings.HasPrefix(err.Error(), tc.category+":") {
				t.Fatalf("expected %s after mutating %s, got: %v", tc.category, tc.field, err)
			}
		})
	}
}

func TestCorruptionFixtureCodecMetadataFailuresArePrecise(t *testing.T) {
	tests := []struct {
		name        string
		field       string
		value       string
		containsAny []string
	}{
		{name: "compression codec metadata", field: "compression_codec", value: "broken-compressor", containsAny: []string{"resolve compression codec"}},
		{name: "encryption codec metadata", field: "codec", value: "aes-gcm", containsAny: []string{"load transformer", "decrypt payload"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbconn := openVerifyTestDB(t)
			defer func() { _ = dbconn.Close() }()

			if tc.field == "compression_codec" {
				if _, err := dbconn.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
					t.Fatalf("enable sqlite ignore_check_constraints for corruption fixture: %v", err)
				}
				defer func() {
					_, _ = dbconn.Exec(`PRAGMA ignore_check_constraints = OFF`)
				}()
			}

			repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
			blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-codec-metadata")}, blocks.CodecPlain, storagecompression.CompressionZstd)
			UpdateStorageBlockField(t, repo, blockID, tc.field, tc.value)

			err := verifyBlockPayloads(dbconn, repo.containersDir)
			if err == nil || !strings.HasPrefix(err.Error(), verifyErrMetadataInvalid+":") {
				t.Fatalf("expected metadata_invalid after mutating %s, got: %v", tc.field, err)
			}

			matched := false
			for _, want := range tc.containsAny {
				if strings.Contains(err.Error(), want) {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("expected metadata_invalid containing one of %q after mutating %s, got: %v", tc.containsAny, tc.field, err)
			}
		})
	}
}

func TestCorruptionFixtureSizeMetadataFailuresArePrecise(t *testing.T) {
	t.Run("stored_size", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-size-stored")}, blocks.CodecPlain, storagecompression.CompressionZstd)

		UpdateStorageBlockField(t, repo, blockID, "stored_size", int64(1))

		err := verifyBlockPayloads(dbconn, repo.containersDir)
		if err == nil || !strings.HasPrefix(err.Error(), verifyErrPhysicalHashMismatch+":") {
			t.Fatalf("expected physical_hash_mismatch from stored_size corruption, got: %v", err)
		}
	})

	t.Run("compressed_size", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-size-compressed")}, blocks.CodecPlain, storagecompression.CompressionZstd)

		UpdateStorageBlockField(t, repo, blockID, "compressed_size", int64(1))

		err := verifyBlockPayloads(dbconn, repo.containersDir)
		if err == nil || !strings.HasPrefix(err.Error(), verifyErrMetadataInvalid+":") || !strings.Contains(err.Error(), "compressed size mismatch") {
			t.Fatalf("expected compressed size mismatch metadata_invalid, got: %v", err)
		}
	})

	t.Run("plaintext_size", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-size-plaintext")}, blocks.CodecPlain, storagecompression.CompressionZstd)

		UpdateStorageBlockField(t, repo, blockID, "plaintext_size", int64(1))

		err := verifyBlockPayloads(dbconn, repo.containersDir)
		assertDecompressStageVerifyFailure(t, err, blockID, "decompression size mismatch")
	})
}

func TestCorruptionFixtureTruncateContainerPayloadTargetsDecodeStage(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("fixture-truncate-decode")}, nil)

	TruncateContainerPayload(t, repo, blockID, 3)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, repo.containersDir)
	truncated := readPackedStoredBytesForTest(t, path, offset, storedSize)
	UpdateStorageBlockField(t, repo, blockID, "block_hash", blocks.HashLogical(truncated))
	UpdateStorageBlockField(t, repo, blockID, "plaintext_size", int64(len(truncated)))

	err := verifyBlockPayloads(dbconn, repo.containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrUnsupportedBlock+":") || !strings.Contains(err.Error(), "decode logical block") {
		t.Fatalf("expected unsupported_block_format decode failure for truncated payload, got: %v", err)
	}
}

func TestCorruptionFixtureChunkRefOffsetsAndLengths(t *testing.T) {
	t.Run("offset overflow", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("ABCD")}, nil)
		chunkID := firstChunkIDForBlock(t, dbconn, blockID)

		UpdateChunkBlockRefField(t, repo, blockID, chunkID, "offset_in_block", int64(1))

		err := verifyBlockPayloads(dbconn, repo.containersDir)
		if err == nil || (!strings.Contains(err.Error(), "segment out of payload bounds") && !strings.Contains(err.Error(), "chunk_block_ref references chunk not in encoded block table")) {
			t.Fatalf("expected out-of-bounds-style chunk ref error, got: %v", err)
		}
	})

	t.Run("zero length", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("ABCD")}, nil)
		chunkID := firstChunkIDForBlock(t, dbconn, blockID)

		if _, err := dbconn.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
			t.Fatalf("disable sqlite check constraints: %v", err)
		}
		UpdateChunkBlockRefField(t, repo, blockID, chunkID, "size_in_block", int64(0))

		err := verifyChunkBlockRefs(dbconn)
		if err == nil || !strings.Contains(err.Error(), "invalid chunk_block_refs ranges") {
			t.Fatalf("expected invalid chunk_block_refs ranges error, got: %v", err)
		}
	})
}

func TestCorruptionFixtureChunkRefsStageConsistencyFailures(t *testing.T) {
	t.Run("chunk index mismatch", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("AA"), []byte("BB")}, nil)

		ids := blockChunkIDs(t, dbconn, blockID)
		res, err := dbconn.Exec(`
			INSERT INTO chunk (chunk_hash, size, status)
			VALUES ($1, $2, 'COMPLETED')
		`, strings.Repeat("9", 64), int64(2))
		if err != nil {
			t.Fatalf("insert unreferenced chunk for chunk-index mismatch fixture: %v", err)
		}
		newChunkID, err := res.LastInsertId()
		if err != nil {
			t.Fatalf("load inserted chunk id: %v", err)
		}

		if _, err := dbconn.Exec(`UPDATE chunk_block_refs SET chunk_id = $1 WHERE block_id = $2 AND chunk_id = $3`, newChunkID, blockID, ids[0]); err != nil {
			t.Fatalf("mutate chunk ref chunk_id mismatch: %v", err)
		}

		err = verifyBlockPayloads(dbconn, repo.containersDir)
		assertChunkRefsStageVerifyFailure(t, err, blockID, verifyErrMetadataInvalid, "chunk_block_ref references chunk not in encoded block table")
	})

	t.Run("chunk offset mismatch", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("ABCD")}, nil)
		chunkID := firstChunkIDForBlock(t, dbconn, blockID)

		UpdateChunkBlockRefField(t, repo, blockID, chunkID, "offset_in_block", int64(1))

		err := verifyBlockPayloads(dbconn, repo.containersDir)
		assertChunkRefsStageVerifyFailure(t, err, blockID, verifyErrMetadataInvalid, "chunk_block_ref references chunk not in encoded block table")
	})

	t.Run("chunk length mismatch", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("ABCD")}, nil)
		chunkID := firstChunkIDForBlock(t, dbconn, blockID)

		UpdateChunkBlockRefField(t, repo, blockID, chunkID, "size_in_block", int64(1))

		err := verifyBlockPayloads(dbconn, repo.containersDir)
		assertChunkRefsStageVerifyFailure(t, err, blockID, verifyErrMetadataInvalid, "size mismatch")
	})

	t.Run("chunk hash mapping mismatch", func(t *testing.T) {
		dbconn := openVerifyTestDB(t)
		defer func() { _ = dbconn.Close() }()

		repo := verifyCorruptionRepo{dbconn: dbconn, containersDir: t.TempDir()}
		blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, repo.containersDir, [][]byte{[]byte("ABCD")}, nil)
		chunkID := firstChunkIDForBlock(t, dbconn, blockID)

		if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("0", 64), chunkID); err != nil {
			t.Fatalf("mutate chunk hash mapping: %v", err)
		}

		err := verifyBlockPayloads(dbconn, repo.containersDir)
		assertChunkRefsStageVerifyFailure(t, err, blockID, verifyErrChunkHashMismatch, "hash mismatch")
	})
}
