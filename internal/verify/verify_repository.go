package verify

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/db"
)

// VerifyRepository provides the Phase 5 layered verification orchestration for
// repositories that may include v1.8 packed-block storage.
//
// Layer order:
//  1. metadata integrity (chunk reachability)
//  2. physical block integrity (storage_blocks + chunk_block_refs)
//  3. decoded block integrity (payload stage placeholder)
//  4. chunk slice integrity (payload stage placeholder)
//  5. legacy compatibility checks
func VerifyRepository(dbconn *sql.DB, containersDir string) error {
	if err := verifyChunkReachability(dbconn); err != nil {
		return err
	}
	if err := verifyStorageBlocks(dbconn); err != nil {
		return err
	}
	if err := verifyChunkBlockRefs(dbconn); err != nil {
		return err
	}
	if err := verifyBlockPayloads(dbconn, containersDir); err != nil {
		return err
	}
	if err := verifyLegacyCompatibility(dbconn); err != nil {
		return err
	}
	return nil
}

func verifyChunkReachability(dbconn *sql.DB) error {
	if err := runPhysicalIntegrityChecks(dbconn); err != nil {
		return fmt.Errorf("verifyChunkReachability: %w", err)
	}
	return nil
}

func verifyStorageBlocks(dbconn *sql.DB) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking storage_blocks metadata integrity...")

	var missingContainerRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks sb
		LEFT JOIN container c ON c.id = sb.container_id
		WHERE c.id IS NULL
	`).Scan(&missingContainerRows); err != nil {
		return fmt.Errorf("verifyStorageBlocks: query missing containers: %w", err)
	}
	if missingContainerRows > 0 {
		return fmt.Errorf("verifyStorageBlocks: storage_blocks rows with missing container refs=%d", missingContainerRows)
	}

	var missingHashRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE block_hash IS NULL OR length(block_hash) = 0
	`).Scan(&missingHashRows); err != nil {
		return fmt.Errorf("verifyStorageBlocks: query missing block_hash rows: %w", err)
	}
	if missingHashRows > 0 {
		return fmt.Errorf("verifyStorageBlocks: storage_blocks rows with empty block_hash=%d", missingHashRows)
	}

	log.Println(" SUCCESS ")
	return nil
}

func verifyChunkBlockRefs(dbconn *sql.DB) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking chunk_block_refs structural integrity...")

	var missingBlockRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk_block_refs r
		LEFT JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE sb.id IS NULL
	`).Scan(&missingBlockRows); err != nil {
		return fmt.Errorf("verifyChunkBlockRefs: query missing storage_blocks: %w", err)
	}
	if missingBlockRows > 0 {
		return fmt.Errorf("verifyChunkBlockRefs: chunk_block_refs rows with missing storage_blocks=%d", missingBlockRows)
	}

	var missingChunkRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk_block_refs r
		LEFT JOIN chunk c ON c.id = r.chunk_id
		WHERE c.id IS NULL
	`).Scan(&missingChunkRows); err != nil {
		return fmt.Errorf("verifyChunkBlockRefs: query missing chunk rows: %w", err)
	}
	if missingChunkRows > 0 {
		return fmt.Errorf("verifyChunkBlockRefs: chunk_block_refs rows with missing chunks=%d", missingChunkRows)
	}

	var invalidRanges int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk_block_refs
		WHERE offset_in_block < 0 OR size_in_block <= 0
	`).Scan(&invalidRanges); err != nil {
		return fmt.Errorf("verifyChunkBlockRefs: query invalid ranges: %w", err)
	}
	if invalidRanges > 0 {
		return fmt.Errorf("verifyChunkBlockRefs: invalid chunk_block_refs ranges=%d", invalidRanges)
	}

	log.Println(" SUCCESS ")
	return nil
}

func verifyBlockPayloads(dbconn *sql.DB, containersDir string) error {
	// Phase 5 Step 1 scope: define layered flow and keep payload validation
	// behavior unchanged for now. Deep byte-level verification remains in
	// VerifySystemDeepWithContainersDir and will be progressively moved here.
	return nil
}

func verifyLegacyCompatibility(dbconn *sql.DB) error {
	if err := runLogicalReconstructionChecks(dbconn); err != nil {
		return fmt.Errorf("verifyLegacyCompatibility: %w", err)
	}
	return nil
}
