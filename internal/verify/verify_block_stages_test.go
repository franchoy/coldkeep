package verify

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
)

func TestBlockVerifyStageConstants(t *testing.T) {
	if StagePhysicalPayload >= StageCompressedPayload {
		t.Errorf("expected StagePhysicalPayload < StageCompressedPayload")
	}
	if StageCompressedPayload >= StageLogicalPayload {
		t.Errorf("expected StageCompressedPayload < StageLogicalPayload")
	}
}

func TestVerifyPhysicalPayloadStageIsNoOp(t *testing.T) {
	p := blockStagePayloads{
		storedBytes:      []byte("any bytes"),
		compressedBytes:  nil,
		plaintextEncoded: []byte("plaintext"),
	}
	if err := verifyPhysicalPayloadStage(context.Background(), 42, p); err != nil {
		t.Fatalf("expected nil from physical stage no-op, got: %v", err)
	}
}

func TestVerifyCompressedPayloadStageIsNoOp(t *testing.T) {
	p := blockStagePayloads{
		storedBytes:      []byte("any bytes"),
		compressedBytes:  nil,
		plaintextEncoded: []byte("plaintext"),
	}
	if err := verifyCompressedPayloadStage(context.Background(), 42, p); err != nil {
		t.Fatalf("expected nil from compressed stage no-op, got: %v", err)
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
		Entries:  []blocks.ChunkEntry{{ChunkID: 1, Offset: 0, Size: uint64(len(payload))}},
		Payload:  payload,
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
	if err := verifyLogicalPayloadStage(context.Background(), 1, sum[:], p); err != nil {
		t.Fatalf("expected logical stage to pass on correct hash, got: %v", err)
	}
}

func TestVerifyLogicalPayloadStageFailsOnHashMismatch(t *testing.T) {
	encoded := buildTestEncodedBytes(t, []byte("test-block-payload"))
	wrongHash := make([]byte, 32)
	p := blockStagePayloads{storedBytes: encoded, plaintextEncoded: encoded}
	if err := verifyLogicalPayloadStage(context.Background(), 1, wrongHash, p); err == nil {
		t.Fatal("expected logical stage to fail on hash mismatch, got nil")
	}
}

func TestRunBlockVerifyStagesPassesEndToEnd(t *testing.T) {
	encoded := buildTestEncodedBytes(t, []byte("end-to-end-stage-test"))
	sum := sha256.Sum256(encoded)
	p := blockStagePayloads{storedBytes: encoded, plaintextEncoded: encoded}
	if err := runBlockVerifyStages(context.Background(), nil, 1, sum[:], p); err != nil {
		t.Fatalf("expected runBlockVerifyStages to pass, got: %v", err)
	}
}

func TestRunBlockVerifyStagesFailsWhenLogicalHashMismatches(t *testing.T) {
	encoded := buildTestEncodedBytes(t, []byte("mismatch-test"))
	wrongHash := make([]byte, 32)
	p := blockStagePayloads{storedBytes: encoded, plaintextEncoded: encoded}
	if err := runBlockVerifyStages(context.Background(), nil, 1, wrongHash, p); err == nil {
		t.Fatal("expected runBlockVerifyStages to fail on logical hash mismatch, got nil")
	}
}
