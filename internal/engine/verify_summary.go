package engine

import (
	"context"
	"database/sql"
	"fmt"
)

func collectVerifyResult(ctx context.Context, dbconn *sql.DB, target string, fileID int64) (VerifyResult, error) {
	if dbconn == nil {
		return VerifyResult{}, fmt.Errorf("verify summary DB connection is nil")
	}
	switch target {
	case "system":
		return countVerifyResultForSystem(ctx, dbconn)
	case "file":
		return countVerifyResultForFile(ctx, dbconn, fileID)
	default:
		return VerifyResult{}, fmt.Errorf("unknown verify target: %s", target)
	}
}

func countVerifyResultForSystem(ctx context.Context, dbconn *sql.DB) (VerifyResult, error) {
	var result VerifyResult
	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN physical_hash IS NOT NULL AND length(physical_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN compressed_hash IS NOT NULL AND length(compressed_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN block_hash IS NOT NULL AND length(block_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) != 'none' THEN 1 ELSE 0 END), 0)
		FROM storage_blocks
	`).Scan(
		&result.BlocksChecked,
		&result.PhysicalHashChecked,
		&result.CompressedHashChecked,
		&result.LogicalHashChecked,
		&result.CompressedBlocksChecked,
	); err != nil {
		return VerifyResult{}, err
	}

	var legacyBlocks int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blocks b
		WHERE NOT EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = b.chunk_id)
	`).Scan(&legacyBlocks); err != nil {
		return VerifyResult{}, err
	}
	result.BlocksChecked += legacyBlocks
	return result, nil
}

func countVerifyResultForFile(ctx context.Context, dbconn *sql.DB, fileID int64) (VerifyResult, error) {
	var result VerifyResult
	if err := dbconn.QueryRowContext(ctx, `
		WITH target_blocks AS (
			SELECT DISTINCT sb.id, sb.physical_hash, sb.compressed_hash, sb.block_hash, sb.compression_codec
			FROM file_chunk fc
			JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
			JOIN storage_blocks sb ON sb.id = r.block_id
			JOIN container c ON c.id = sb.container_id
			WHERE fc.logical_file_id = $1
			  AND c.quarantine = FALSE
		)
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN physical_hash IS NOT NULL AND length(physical_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN compressed_hash IS NOT NULL AND length(compressed_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN block_hash IS NOT NULL AND length(block_hash) > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) != 'none' THEN 1 ELSE 0 END), 0)
		FROM target_blocks
	`, fileID).Scan(
		&result.BlocksChecked,
		&result.PhysicalHashChecked,
		&result.CompressedHashChecked,
		&result.LogicalHashChecked,
		&result.CompressedBlocksChecked,
	); err != nil {
		return VerifyResult{}, err
	}

	var legacyBlocks int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM file_chunk fc
		JOIN blocks b ON b.chunk_id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		  AND NOT EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = b.chunk_id)
	`, fileID).Scan(&legacyBlocks); err != nil {
		return VerifyResult{}, err
	}
	result.BlocksChecked += legacyBlocks
	return result, nil
}
