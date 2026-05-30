package verify

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
)

func TestBlockVerifyStageConstants(t *testing.T) {
	if VerifyStagePhysicalPayload != "physical_payload" {
		t.Fatalf("unexpected VerifyStagePhysicalPayload=%q", VerifyStagePhysicalPayload)
	}
	if VerifyStageCompressedHash != "compressed_hash" {
		t.Fatalf("unexpected VerifyStageCompressedHash=%q", VerifyStageCompressedHash)
	}
	if VerifyStageLogicalHash != "logical_hash" {
		t.Fatalf("unexpected VerifyStageLogicalHash=%q", VerifyStageLogicalHash)
	}
}

func TestVerifyPhysicalPayloadStageIsNoOp(t *testing.T) {
	// Legacy (blocks table) rows have isPackedBlock=false (the zero value).
	// NULL physical_hash on a legacy row must be silently skipped.
	p := blockStagePayloads{
		storedBytes:      []byte("any bytes"),
		compressedBytes:  nil,
		plaintextEncoded: []byte("plaintext"),
		// isPackedBlock defaults to false — legacy path
	}
	loc := verifyBlockLocation{blockID: 42, containerID: 7, offset: 11}
	if err := verifyPhysicalPayloadStage(context.Background(), loc, p); err != nil {
		t.Fatalf("expected nil from physical stage legacy skip, got: %v", err)
	}
}

func TestVerifyPhysicalPayloadStageFailsForPackedBlockWithNullHash(t *testing.T) {
	// Packed (storage_blocks) rows must have physical_hash set.
	// NULL physical_hash on a packed block is a metadata integrity failure.
	p := blockStagePayloads{
		storedBytes:      []byte("packed-block-bytes"),
		isPackedBlock:    true,
		compressionCodec: "none",
		// PhysicalHash intentionally nil to simulate incomplete metadata
	}
	loc := verifyBlockLocation{blockID: 99, containerID: 8, offset: 0}
	err := verifyPhysicalPayloadStage(context.Background(), loc, p)
	if err == nil {
		t.Fatal("expected physical stage to fail-closed for packed block with nil physical_hash")
	}
	if !strings.Contains(err.Error(), verifyErrMetadataInvalid) {
		t.Fatalf("expected metadata_invalid category, got: %v", err)
	}
}

func TestVerifyPhysicalPayloadStageDetectsMismatch(t *testing.T) {
	p := blockStagePayloads{
		storedBytes:      []byte("stored-bytes"),
		compressedBytes:  []byte("compressed-bytes"),
		plaintextEncoded: []byte("plaintext"),
		hashes: blocks.BlockHashes{
			PhysicalHash: blocks.HashPhysical([]byte("different-bytes")),
		},
	}
	loc := verifyBlockLocation{blockID: 42, containerID: 7, offset: 11}
	err := verifyPhysicalPayloadStage(context.Background(), loc, p)
	if err == nil {
		t.Fatal("expected physical stage mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), verifyErrPhysicalHashMismatch) {
		t.Fatalf("expected physical hash mismatch category, got: %v", err)
	}
}

func TestVerifyCompressedPayloadStageSkipsLegacyNull(t *testing.T) {
	// Legacy (blocks table) rows have isPackedBlock=false (the zero value).
	// NULL compressed_hash on a legacy row must be silently skipped.
	p := blockStagePayloads{
		storedBytes:      []byte("any bytes"),
		compressedBytes:  nil,
		plaintextEncoded: []byte("plaintext"),
		// isPackedBlock defaults to false — legacy path
	}
	loc := verifyBlockLocation{blockID: 42, containerID: 7, offset: 11}
	if err := verifyCompressedPayloadStage(context.Background(), loc, p); err != nil {
		t.Fatalf("expected nil from compressed stage legacy skip, got: %v", err)
	}
}

func TestVerifyCompressedPayloadStageSkipsPackedNoneCodecNullHash(t *testing.T) {
	// Packed blocks with compression_codec=none legitimately have no compressed_hash
	// because there is no separate compressed representation to hash.
	p := blockStagePayloads{
		storedBytes:      []byte("packed-none-bytes"),
		isPackedBlock:    true,
		compressionCodec: "none",
		// CompressedHash nil for none-codec block — valid
	}
	loc := verifyBlockLocation{blockID: 100, containerID: 8, offset: 0}
	if err := verifyCompressedPayloadStage(context.Background(), loc, p); err != nil {
		t.Fatalf("expected nil for packed none-codec block with nil compressed_hash, got: %v", err)
	}
}

func TestVerifyCompressedPayloadStageFailsForCompressedPackedBlockWithNullHash(t *testing.T) {
	// Packed compressed blocks (e.g. zstd) must have compressed_hash set.
	// NULL compressed_hash on a compressed packed block is a metadata failure.
	p := blockStagePayloads{
		storedBytes:      []byte("compressed-packed-bytes"),
		isPackedBlock:    true,
		compressionCodec: "zstd",
		// CompressedHash intentionally nil to simulate incomplete metadata
	}
	loc := verifyBlockLocation{blockID: 101, containerID: 9, offset: 0}
	err := verifyCompressedPayloadStage(context.Background(), loc, p)
	if err == nil {
		t.Fatal("expected compressed stage to fail-closed for zstd packed block with nil compressed_hash")
	}
	if !strings.Contains(err.Error(), verifyErrMetadataInvalid) {
		t.Fatalf("expected metadata_invalid category, got: %v", err)
	}
}

func TestVerifyCompressedPayloadStageDetectsMismatch(t *testing.T) {
	p := blockStagePayloads{
		storedBytes:      []byte("stored-bytes"),
		compressedBytes:  []byte("compressed-bytes"),
		plaintextEncoded: []byte("plaintext"),
		hashes: blocks.BlockHashes{
			CompressedHash: blocks.HashCompressed([]byte("different-bytes")),
		},
	}
	loc := verifyBlockLocation{blockID: 42, containerID: 7, offset: 11}
	err := verifyCompressedPayloadStage(context.Background(), loc, p)
	if err == nil {
		t.Fatal("expected compressed stage mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), verifyErrCompressedHashMismatch) {
		t.Fatalf("expected compressed hash mismatch category, got: %v", err)
	}
}

func buildTestEncodedBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	encoded, err := blocks.EncodeBlock(&blocks.EncodedBlock{
		Header: blocks.BlockHeader{
			Magic:         blocks.BlockMagicV1,
			Version:       blocks.BlockFormatVersionV1,
			Codec:         blocks.BlockCodecNoneV1,
			ChunkCount:    1,
			PlaintextSize: uint64(len(payload)),
		},
		Entries: []blocks.ChunkEntry{{ChunkID: 1, Offset: 0, Size: uint64(len(payload))}},
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	return encoded
}

func TestVerifyLogicalPayloadStagePassesOnCorrectHash(t *testing.T) {
	encoded := buildTestEncodedBytes(t, []byte("test-block-payload"))
	sum := sha256.Sum256(encoded)
	p := blockStagePayloads{storedBytes: encoded, plaintextEncoded: encoded}
	loc := verifyBlockLocation{blockID: 1, containerID: 1, offset: 0}
	if err := verifyLogicalPayloadStage(context.Background(), loc, sum[:], p); err != nil {
		t.Fatalf("expected logical stage to pass on correct hash, got: %v", err)
	}
}

func TestVerifyLogicalPayloadStageFailsOnHashMismatch(t *testing.T) {
	encoded := buildTestEncodedBytes(t, []byte("test-block-payload"))
	wrongHash := make([]byte, 32)
	p := blockStagePayloads{storedBytes: encoded, plaintextEncoded: encoded}
	loc := verifyBlockLocation{blockID: 1, containerID: 1, offset: 0}
	if err := verifyLogicalPayloadStage(context.Background(), loc, wrongHash, p); err == nil {
		t.Fatal("expected logical stage to fail on hash mismatch, got nil")
	}
}
