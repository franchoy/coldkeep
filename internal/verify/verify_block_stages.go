package verify

import (
	"bytes"
	"context"
	"encoding/hex"
	"log"

	"github.com/franchoy/coldkeep/internal/blocks"
)

type verifyBlockLocation struct {
	blockID     int64
	containerID int64
	offset      int64
}

// blockStagePayloads carries the accumulated payload bytes as they flow through
// the three verification stages.
type blockStagePayloads struct {
	storedBytes      []byte // Layer 3: raw on-disk bytes
	compressedBytes  []byte // Layer 2: post-decrypt, pre-decompress bytes
	plaintextEncoded []byte // Layer 1: decrypted/decoded plaintext block bytes
	hashes           blocks.BlockHashes
}

// verifyPhysicalPayloadStage is Stage 1 of the block verification pipeline.
// If physical_hash is present, verify SHA-256 of raw stored bytes.
// Compatibility rule: NULL/empty physical_hash means legacy row and this stage is skipped.
func verifyPhysicalPayloadStage(_ context.Context, loc verifyBlockLocation, payloads blockStagePayloads) error {
	if len(payloads.hashes.PhysicalHash) == 0 {
		log.Printf("DEBUG verify stage=%s block_id=%d container_id=%d offset=%d: physical hash unavailable for legacy block", VerifyStagePhysicalPayload, loc.blockID, loc.containerID, loc.offset)
		return nil
	}
	computed := blocks.HashPhysical(payloads.storedBytes)
	if !bytes.Equal(computed, payloads.hashes.PhysicalHash) {
		meta := verifyBlockFailureMeta(VerifyStagePhysicalPayload, loc.blockID, loc.containerID, loc.offset)
		meta.expectedHash = hex.EncodeToString(payloads.hashes.PhysicalHash)
		meta.actualHash = hex.EncodeToString(computed)
		return verifyStageError(
			verifyErrPhysicalHashMismatch,
			meta,
			"verifyBlockPayloads: physical payload hash mismatch",
			nil,
		)
	}
	return nil
}

// verifyCompressedPayloadStage is Stage 2 of the block verification pipeline.
// If compressed_hash is present, verify SHA-256 of the pre-decompression payload.
// Compatibility rule: NULL/empty compressed_hash means legacy row and this stage is skipped.
func verifyCompressedPayloadStage(_ context.Context, loc verifyBlockLocation, payloads blockStagePayloads) error {
	if len(payloads.hashes.CompressedHash) == 0 {
		log.Printf("DEBUG verify stage=%s block_id=%d container_id=%d offset=%d: compressed hash unavailable for legacy block", VerifyStageCompressedHash, loc.blockID, loc.containerID, loc.offset)
		return nil
	}
	computed := blocks.HashCompressed(payloads.compressedBytes)
	if !bytes.Equal(computed, payloads.hashes.CompressedHash) {
		meta := verifyBlockFailureMeta(VerifyStageCompressedHash, loc.blockID, loc.containerID, loc.offset)
		meta.expectedHash = hex.EncodeToString(payloads.hashes.CompressedHash)
		meta.actualHash = hex.EncodeToString(computed)
		return verifyStageError(
			verifyErrCompressedHashMismatch,
			meta,
			"verifyBlockPayloads: compressed payload hash mismatch",
			nil,
		)
	}
	return nil
}

// verifyLogicalPayloadStage is Stage 3 of the block verification pipeline.
// It validates sha256(plaintextEncoded) == block_hash after decrypt/decompress.
func verifyLogicalPayloadStage(_ context.Context, loc verifyBlockLocation, expectedHash []byte, payloads blockStagePayloads) error {
	if err := blocks.VerifyBlockHash(payloads.plaintextEncoded, expectedHash); err != nil {
		meta := verifyBlockFailureMeta(VerifyStageLogicalHash, loc.blockID, loc.containerID, loc.offset)
		meta.expectedHash = hex.EncodeToString(expectedHash)
		meta.actualHash = hex.EncodeToString(blocks.HashLogical(payloads.plaintextEncoded))
		return verifyStageError(verifyErrBlockHashMismatch, meta,
			"verifyBlockPayloads: logical block hash mismatch", err)
	}
	return nil
}
