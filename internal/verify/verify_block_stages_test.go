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
	p := blockStagePayloads{
		storedBytes:      []byte("any bytes"),
		compressedBytes:  nil,
		plaintextEncoded: []byte("plaintext"),
	}
	loc := verifyBlockLocation{blockID: 42, containerID: 7, offset: 11}
	if err := verifyPhysicalPayloadStage(context.Background(), loc, p); err != nil {
		t.Fatalf("expected nil from physical stage legacy skip, got: %v", err)
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
	p := blockStagePayloads{
		storedBytes:      []byte("any bytes"),
		compressedBytes:  nil,
		plaintextEncoded: []byte("plaintext"),
	}
	loc := verifyBlockLocation{blockID: 42, containerID: 7, offset: 11}
	if err := verifyCompressedPayloadStage(context.Background(), loc, p); err != nil {
		t.Fatalf("expected nil from compressed stage legacy skip, got: %v", err)
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

func TestRunBlockVerifyStagesPassesEndToEnd(t *testing.T) {
	encoded := buildTestEncodedBytes(t, []byte("end-to-end-stage-test"))
	sum := sha256.Sum256(encoded)
	p := blockStagePayloads{
		storedBytes:      encoded,
		compressedBytes:  encoded,
		plaintextEncoded: encoded,
		hashes: blocks.BlockHashes{
			LogicalHash:    sum[:],
			CompressedHash: blocks.HashCompressed(encoded),
			PhysicalHash:   blocks.HashPhysical(encoded),
		},
	}
	loc := verifyBlockLocation{blockID: 1, containerID: 1, offset: 0}
	if err := runBlockVerifyStages(context.Background(), nil, loc, sum[:], p); err != nil {
		t.Fatalf("expected runBlockVerifyStages to pass, got: %v", err)
	}
}

func TestRunBlockVerifyStagesFailsWhenLogicalHashMismatches(t *testing.T) {
	encoded := buildTestEncodedBytes(t, []byte("mismatch-test"))
	wrongHash := make([]byte, 32)
	p := blockStagePayloads{storedBytes: encoded, plaintextEncoded: encoded}
	loc := verifyBlockLocation{blockID: 1, containerID: 1, offset: 0}
	if err := runBlockVerifyStages(context.Background(), nil, loc, wrongHash, p); err == nil {
		t.Fatal("expected runBlockVerifyStages to fail on logical hash mismatch, got nil")
	}
}
