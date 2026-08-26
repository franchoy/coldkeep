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
	// isPackedBlock distinguishes packed storage_blocks rows (must have physical
	// and compressed hashes set) from true legacy blocks rows (hash columns absent
	// by design). Set to true by VerifyStoredBlock for all packed-path blocks.
	isPackedBlock bool
	// compressionCodec is the per-block compression codec string, used to
	// determine whether compressed_hash is expected for packed blocks.
	compressionCodec string
}

// verifyPhysicalPayloadStage is Stage 1 of the block verification pipeline.
// If physical_hash is present, verify SHA-256 of raw stored bytes.
// Compatibility rule: NULL/empty physical_hash on a legacy (blocks table) row
// skips this stage. Packed (storage_blocks) rows must always carry physical_hash;
// a NULL value on a packed row is treated as a metadata integrity failure.
func verifyPhysicalPayloadStage(ctx context.Context, loc verifyBlockLocation, payloads blockStagePayloads) error {
	if len(payloads.hashes.PhysicalHash) == 0 {
		if payloads.isPackedBlock {
			// Packed blocks must have physical_hash recorded at write time.
			// A NULL value indicates incomplete metadata — fail closed.
			meta := verifyBlockFailureMeta(VerifyStagePhysicalPayload, loc.blockID, loc.containerID, loc.offset)
			return verifyStageError(
				verifyErrMetadataInvalid,
				meta,
				"verifyBlockPayloads: physical_hash missing for packed block",
				nil,
			)
		}
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
	observePayloadVerificationStage(ctx, loc.blockID, payloads, verificationObservedPhysicalHash)
	return nil
}

// verifyCompressedPayloadStage is Stage 2 of the block verification pipeline.
// If compressed_hash is present, verify SHA-256 of the pre-decompression payload.
// Compatibility rule: NULL/empty compressed_hash on a legacy (blocks table) row
// or a packed row with no compression (codec=none) skips this stage. Packed rows
// with a non-none compression codec must carry compressed_hash; a NULL value is
// a metadata integrity failure.
func verifyCompressedPayloadStage(ctx context.Context, loc verifyBlockLocation, payloads blockStagePayloads) error {
	if len(payloads.hashes.CompressedHash) == 0 {
		if payloads.isPackedBlock && payloads.compressionCodec != "" && payloads.compressionCodec != "none" {
			// Packed compressed blocks must have compressed_hash recorded at write time.
			// A NULL value on a compressed packed block indicates incomplete metadata — fail closed.
			meta := verifyBlockFailureMeta(VerifyStageCompressedHash, loc.blockID, loc.containerID, loc.offset)
			return verifyStageError(
				verifyErrMetadataInvalid,
				meta,
				"verifyBlockPayloads: compressed_hash missing for compressed packed block",
				nil,
			)
		}
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
	observePayloadVerificationStage(ctx, loc.blockID, payloads, verificationObservedCompressedHash)
	return nil
}

// verifyLogicalPayloadStage is Stage 3 of the block verification pipeline.
// It validates sha256(plaintextEncoded) == block_hash after decrypt/decompress.
func verifyLogicalPayloadStage(ctx context.Context, loc verifyBlockLocation, expectedHash []byte, payloads blockStagePayloads) error {
	if err := blocks.VerifyBlockHash(payloads.plaintextEncoded, expectedHash); err != nil {
		meta := verifyBlockFailureMeta(VerifyStageLogicalHash, loc.blockID, loc.containerID, loc.offset)
		meta.expectedHash = hex.EncodeToString(expectedHash)
		meta.actualHash = hex.EncodeToString(blocks.HashLogical(payloads.plaintextEncoded))
		return verifyStageError(verifyErrBlockHashMismatch, meta,
			"verifyBlockPayloads: logical block hash mismatch", err)
	}
	observePayloadVerificationStage(ctx, loc.blockID, payloads, verificationObservedLogicalHash)
	return nil
}
