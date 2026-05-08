package verify

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/blocks"
)

// verify_block_stages.go introduces the three-stage block payload verification
// pipeline (Step 1.6). External verify behavior is UNCHANGED.
//
// Stage order (outermost to innermost):
//
//	StagePhysicalPayload   -- raw stored bytes on disk           (no-op today)
//	StageCompressedPayload -- compressed-but-encrypted payload   (no-op today)
//	StageLogicalPayload    -- decrypted plaintext block hash      (ACTIVE)

// blockStagePayloads carries the accumulated payload bytes as they flow through
// the three verification stages.
type blockStagePayloads struct {
	storedBytes      []byte // Layer 3: raw on-disk bytes
	compressedBytes  []byte // Layer 2: future compressed payload (nil today)
	plaintextEncoded []byte // Layer 1: decrypted/decoded plaintext block bytes
	hashes           blocks.BlockHashes
}

// verifyPhysicalPayloadStage is Stage 1 of the block verification pipeline.
// Current: explicit no-op. physical_hash is populated for new rows, but legacy
// rows may still be NULL. Compatibility rule: NULL physical_hash means legacy
// row and this stage is skipped.
func verifyPhysicalPayloadStage(_ context.Context, _ int64, _ blockStagePayloads) error {
	return nil
}

// verifyCompressedPayloadStage is Stage 2 of the block verification pipeline.
// Current: explicit no-op. compressed_hash is populated for new rows, but
// legacy rows may still be NULL. Compatibility rule: NULL compressed_hash means
// legacy row and this stage is skipped.
func verifyCompressedPayloadStage(_ context.Context, _ int64, _ blockStagePayloads) error {
	return nil
}

// verifyLogicalPayloadStage is Stage 3 of the block verification pipeline.
// This is the currently active hash check: sha256(plaintextEncoded) == block_hash.
func verifyLogicalPayloadStage(_ context.Context, blockID int64, expectedHash []byte, payloads blockStagePayloads) error {
	if err := blocks.VerifyBlockHash(payloads.plaintextEncoded, expectedHash); err != nil {
		return verifyCategoryError(verifyErrBlockHashMismatch,
			fmt.Sprintf("verifyBlockPayloads: logical block hash mismatch for block %d", blockID), err)
	}
	return nil
}

// runBlockVerifyStages executes all three verification stages in order.
// Stages 1 and 2 are no-ops today. Only Stage 3 (logical) is active.
func runBlockVerifyStages(ctx context.Context, dbconn *sql.DB, blockID int64, expectedHash []byte, payloads blockStagePayloads) error {
	if err := verifyPhysicalPayloadStage(ctx, blockID, payloads); err != nil {
		return err
	}
	if err := verifyCompressedPayloadStage(ctx, blockID, payloads); err != nil {
		return err
	}
	if err := verifyLogicalPayloadStage(ctx, blockID, expectedHash, payloads); err != nil {
		return err
	}
	_ = dbconn
	return nil
}
