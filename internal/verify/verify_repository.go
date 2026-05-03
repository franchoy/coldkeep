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
	if err := verifyFileChunkRelationships(dbconn); err != nil {
		return err
	}
	return nil
}

func verifyFileChunkRelationships(dbconn *sql.DB) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking file_chunk -> chunk relationships...")

	var missingChunkRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM file_chunk fc
		LEFT JOIN chunk c ON c.id = fc.chunk_id
		WHERE c.id IS NULL
	`).Scan(&missingChunkRows); err != nil {
		return fmt.Errorf("verifyFileChunkRelationships: query missing chunk refs: %w", err)
	}
	if missingChunkRows > 0 {
		return fmt.Errorf("verifyFileChunkRelationships: file_chunk rows with missing chunk refs=%d", missingChunkRows)
	}

	log.Println(" SUCCESS ")
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

	if err := verifyChunkPhysicalLocationRules(ctx, dbconn); err != nil {
		return err
	}

	log.Println(" SUCCESS ")
	return nil
}

func verifyChunkPhysicalLocationRules(ctx context.Context, dbconn *sql.DB) error {
	type chunkLocationShape struct {
		chunkID    int64
		chunkSize  int64
		hasPacked  bool
		legacyRows int64
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT
			c.id,
			c.size,
			EXISTS(SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = c.id) AS has_packed,
			(SELECT COUNT(*) FROM blocks b WHERE b.chunk_id = c.id) AS legacy_rows
		FROM chunk c
		WHERE c.status = 'COMPLETED'
	`)
	if err != nil {
		return fmt.Errorf("verifyChunkBlockRefs: query chunk location shape: %w", err)
	}
	defer func() { _ = rows.Close() }()

	shapes := make([]chunkLocationShape, 0)
	for rows.Next() {
		var s chunkLocationShape
		if err := rows.Scan(&s.chunkID, &s.chunkSize, &s.hasPacked, &s.legacyRows); err != nil {
			return fmt.Errorf("verifyChunkBlockRefs: scan chunk location shape: %w", err)
		}
		shapes = append(shapes, s)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verifyChunkBlockRefs: iterate chunk location shape: %w", err)
	}

	for _, s := range shapes {

		switch {
		case s.hasPacked && s.legacyRows == 0:
			// Pure v1.8 mapping is valid.
			continue
		case !s.hasPacked && s.legacyRows == 1:
			// Pure legacy mapping is valid.
			continue
		case s.hasPacked && s.legacyRows == 1:
			ok, err := isValidMigrationCompanionMapping(ctx, dbconn, s.chunkID, s.chunkSize)
			if err != nil {
				return fmt.Errorf("verifyChunkBlockRefs: check migration companion for chunk %d: %w", s.chunkID, err)
			}
			if !ok {
				return fmt.Errorf("verifyChunkBlockRefs: chunk %d has both packed and legacy mappings outside migration companion contract", s.chunkID)
			}
		default:
			return fmt.Errorf("verifyChunkBlockRefs: chunk %d has invalid physical location shape packed=%t legacy_rows=%d", s.chunkID, s.hasPacked, s.legacyRows)
		}
	}

	return nil
}

func isValidMigrationCompanionMapping(ctx context.Context, dbconn *sql.DB, chunkID, chunkSize int64) (bool, error) {
	var blockID int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT block_id FROM chunk_block_refs WHERE chunk_id = $1`,
		chunkID,
	).Scan(&blockID); err != nil {
		return false, err
	}

	var packedContainerID int64
	var packedContainerOffset int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT container_id, container_offset FROM storage_blocks WHERE id = $1`,
		blockID,
	).Scan(&packedContainerID, &packedContainerOffset); err != nil {
		return false, err
	}

	var codec string
	var formatVersion int64
	var plaintextSize int64
	var storedSize int64
	var legacyContainerID int64
	var legacyOffset int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT codec, format_version, plaintext_size, stored_size, container_id, block_offset
		 FROM blocks
		 WHERE chunk_id = $1`,
		chunkID,
	).Scan(&codec, &formatVersion, &plaintextSize, &storedSize, &legacyContainerID, &legacyOffset); err != nil {
		return false, err
	}

	if codec != "plain" || formatVersion != 1 {
		return false, nil
	}
	if plaintextSize != chunkSize || storedSize != chunkSize {
		return false, nil
	}
	if legacyContainerID != packedContainerID || legacyOffset != packedContainerOffset {
		return false, nil
	}

	return true, nil
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
