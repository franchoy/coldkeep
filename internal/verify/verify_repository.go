package verify

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
)

const (
	verifyErrMetadataMissing          = "metadata_missing"
	verifyErrMetadataInvalid          = "metadata_invalid"
	verifyErrPhysicalMissing          = "physical_missing"
	verifyErrPhysicalHashMismatch     = "physical_hash_mismatch"
	verifyErrCompressedHashMismatch   = "compressed_hash_mismatch"
	verifyErrBlockHashMismatch        = "block_hash_mismatch"
	verifyErrChunkHashMismatch        = "chunk_hash_mismatch"
	verifyErrUnsupportedBlock         = "unsupported_block_format"
	packedStorageBlockAESGCMNonceSize = 12
)

func verifyCategoryError(category, detail string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%s: %s", category, detail)
	}
	return fmt.Errorf("%s: %s: %w", category, detail, cause)
}

// VerifyRepository provides layered verification orchestration for
// repositories that may include v1.8 packed-block storage.
//
// Layer order:
//  1. metadata integrity (chunk reachability)
//  2. physical block integrity (storage_blocks + chunk_block_refs)
//  3. decoded block integrity (payload hash and codec validation)
//  4. chunk slice integrity (chunk-to-block slice validation)
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
	if err := verifyPackedManifestIndex(dbconn); err != nil {
		return err
	}
	if err := verifyPackedBounds(dbconn); err != nil {
		return err
	}
	if err := verifyBlockPayloads(dbconn, containersDir); err != nil {
		return err
	}
	if err := verifyLegacyCompatibility(dbconn, containersDir); err != nil {
		return err
	}
	return nil
}

// VerifyRepositoryFast runs the lightweight repository checks.
// Fast mode is intended for quick operational health checks:
// metadata graph + packed block hash integrity.
func VerifyRepositoryFast(dbconn *sql.DB, containersDir string) error {
	if err := verifyChunkReachability(dbconn); err != nil {
		return err
	}
	if err := verifyStorageBlocks(dbconn); err != nil {
		return err
	}
	if err := verifyChunkBlockRefs(dbconn); err != nil {
		return err
	}
	if err := verifyPackedManifestIndex(dbconn); err != nil {
		return err
	}
	if err := verifyPackedBounds(dbconn); err != nil {
		return err
	}
	if err := verifyBlockPayloadsFast(dbconn, containersDir); err != nil {
		return err
	}
	return nil
}

func verifyPackedManifestIndex(dbconn *sql.DB) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking packed manifest/index metadata consistency...")

	var blocksWithoutRefs int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks sb
		WHERE NOT EXISTS (
			SELECT 1
			FROM chunk_block_refs r
			WHERE r.block_id = sb.id
		)
	`).Scan(&blocksWithoutRefs); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyPackedManifestIndex: query storage_blocks without chunk_block_refs", err)
	}
	if blocksWithoutRefs > 0 {
		return verifyCategoryError(verifyErrMetadataMissing, fmt.Sprintf("verifyPackedManifestIndex: storage_blocks missing chunk_block_refs=%d", blocksWithoutRefs), nil)
	}

	var conflictingOffsetRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT block_id, offset_in_block
			FROM chunk_block_refs
			GROUP BY block_id, offset_in_block
			HAVING COUNT(*) > 1
		) t
	`).Scan(&conflictingOffsetRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyPackedManifestIndex: query conflicting offsets", err)
	}
	if conflictingOffsetRows > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyPackedManifestIndex: conflicting chunk_block_refs offsets=%d", conflictingOffsetRows), nil)
	}

	var conflictingChunkRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT block_id, chunk_id
			FROM chunk_block_refs
			GROUP BY block_id, chunk_id
			HAVING COUNT(*) > 1
		) t
	`).Scan(&conflictingChunkRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyPackedManifestIndex: query conflicting chunk entries", err)
	}
	if conflictingChunkRows > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyPackedManifestIndex: conflicting chunk_block_refs entries=%d", conflictingChunkRows), nil)
	}

	log.Println(" SUCCESS ")
	return nil
}

// validatePackedRange checks that a numeric range [offset, offset+length) is
// valid within a container of the given size.
// The overflow-safe check (length > size-offset) avoids int64 wraparound.
func validatePackedRange(label string, offset, length, size int64) error {
	if label == "" {
		label = "packed range"
	}
	if offset < 0 {
		return fmt.Errorf("%s offset must be non-negative", label)
	}
	if length < 0 {
		return fmt.Errorf("%s length must be non-negative", label)
	}
	if size < 0 {
		return fmt.Errorf("%s container size must be non-negative", label)
	}
	if offset > size {
		return fmt.Errorf("%s offset exceeds container size", label)
	}
	if length > size-offset {
		return fmt.Errorf("%s range exceeds container size", label)
	}
	return nil
}

// verifyPackedBounds validates that chunk_block_refs offset/length metadata is
// within the bounds of the parent storage_block's plaintext_size.
// This runs after verifyPackedManifestIndex and before verifyBlockPayloads so
// that unsafe ranges fail before any read, seek, or allocation depends on them.
func verifyPackedBounds(dbconn *sql.DB) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking packed offset/length/bounds metadata...")

	var outOfBoundsRefs int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk_block_refs r
		JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE r.size_in_block > sb.plaintext_size - r.offset_in_block
	`).Scan(&outOfBoundsRefs); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyPackedBounds: query out-of-bounds chunk_block_refs", err)
	}
	if outOfBoundsRefs > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyPackedBounds: chunk_block_refs range exceeds plaintext_size=%d", outOfBoundsRefs), nil)
	}

	log.Println(" SUCCESS ")
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
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyStorageBlocks: query missing containers", err)
	}
	if missingContainerRows > 0 {
		return verifyCategoryError(verifyErrMetadataMissing, fmt.Sprintf("verifyStorageBlocks: storage_blocks rows with missing container refs=%d", missingContainerRows), nil)
	}

	var invalidFieldRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE format_version != 1
		   OR lower(trim(codec)) NOT IN ('none', 'aes-gcm')
		   OR plaintext_size <= 0
		   OR stored_size <= 0
		   OR container_offset < 0
	`).Scan(&invalidFieldRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyStorageBlocks: query invalid field rows", err)
	}
	if invalidFieldRows > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyStorageBlocks: storage_blocks rows with invalid metadata fields=%d", invalidFieldRows), nil)
	}

	var missingHashRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE block_hash IS NULL OR length(block_hash) = 0
	`).Scan(&missingHashRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyStorageBlocks: query missing block_hash rows", err)
	}
	if missingHashRows > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyStorageBlocks: storage_blocks rows with empty block_hash=%d", missingHashRows), nil)
	}

	const expectedBlockHashLen = 32
	var invalidHashLenRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE length(block_hash) != $1
	`, expectedBlockHashLen).Scan(&invalidHashLenRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyStorageBlocks: query invalid block_hash length rows", err)
	}
	if invalidHashLenRows > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyStorageBlocks: storage_blocks rows with invalid block_hash length=%d expected=%d", invalidHashLenRows, expectedBlockHashLen), nil)
	}

	var impossibleLocationRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE c.quarantine = FALSE
		  AND sb.container_offset + sb.stored_size > c.current_size
	`).Scan(&impossibleLocationRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyStorageBlocks: query impossible container locations", err)
	}
	if impossibleLocationRows > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyStorageBlocks: storage_blocks rows with impossible container ranges=%d", impossibleLocationRows), nil)
	}

	// Step 1.6 metadata awareness: record how many rows have payload_hash populated.
	// payload_hash is a deprecated lowercase-hex mirror of block_hash retained
	// for compatibility/observability only. Presence is informational and never
	// an integrity requirement.
	var payloadHashPresentRows int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM storage_blocks WHERE payload_hash IS NOT NULL AND payload_hash != ''`).Scan(&payloadHashPresentRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyStorageBlocks: query payload_hash presence", err)
	}
	log.Printf("  storage_blocks legacy metadata: payload_hash (deprecated mirror) present in %d rows", payloadHashPresentRows)

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
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: query missing storage_blocks", err)
	}
	if missingBlockRows > 0 {
		return verifyCategoryError(verifyErrMetadataMissing, fmt.Sprintf("verifyChunkBlockRefs: chunk_block_refs rows with missing storage_blocks=%d", missingBlockRows), nil)
	}

	var missingChunkRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk_block_refs r
		LEFT JOIN chunk c ON c.id = r.chunk_id
		WHERE c.id IS NULL
	`).Scan(&missingChunkRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: query missing chunk rows", err)
	}
	if missingChunkRows > 0 {
		return verifyCategoryError(verifyErrMetadataMissing, fmt.Sprintf("verifyChunkBlockRefs: chunk_block_refs rows with missing chunks=%d", missingChunkRows), nil)
	}

	var invalidRanges int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk_block_refs
		WHERE offset_in_block < 0 OR size_in_block <= 0
	`).Scan(&invalidRanges); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: query invalid ranges", err)
	}
	if invalidRanges > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyChunkBlockRefs: invalid chunk_block_refs ranges=%d", invalidRanges), nil)
	}

	var completedNoPhysicalRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk c
		WHERE c.status = 'COMPLETED'
		  AND NOT EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = c.id)
		  AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.chunk_id = c.id)
	`).Scan(&completedNoPhysicalRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: query completed chunks without physical location", err)
	}
	if completedNoPhysicalRows > 0 {
		return verifyCategoryError(verifyErrMetadataMissing, fmt.Sprintf("verifyChunkBlockRefs: completed chunks with no physical location=%d", completedNoPhysicalRows), nil)
	}

	var multiplePackedRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT chunk_id
			FROM chunk_block_refs
			GROUP BY chunk_id
			HAVING COUNT(*) > 1
		) t
	`).Scan(&multiplePackedRows); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: query chunks with multiple packed refs", err)
	}
	if multiplePackedRows > 0 {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyChunkBlockRefs: chunks with multiple packed refs=%d", multiplePackedRows), nil)
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
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: query chunk location shape", err)
	}
	defer func() { _ = rows.Close() }()

	shapes := make([]chunkLocationShape, 0)
	for rows.Next() {
		var s chunkLocationShape
		if err := rows.Scan(&s.chunkID, &s.chunkSize, &s.hasPacked, &s.legacyRows); err != nil {
			return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: scan chunk location shape", err)
		}
		shapes = append(shapes, s)
	}
	if err := rows.Err(); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyChunkBlockRefs: iterate chunk location shape", err)
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
				return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyChunkBlockRefs: check migration companion for chunk %d", s.chunkID), err)
			}
			if !ok {
				return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyChunkBlockRefs: chunk %d has both packed and legacy mappings outside migration companion contract", s.chunkID), nil)
			}
		default:
			return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyChunkBlockRefs: chunk %d has invalid physical location shape packed=%t legacy_rows=%d", s.chunkID, s.hasPacked, s.legacyRows), nil)
		}
	}

	return nil
}

func isValidMigrationCompanionMapping(ctx context.Context, dbconn *sql.DB, chunkID, chunkSize int64) (bool, error) {
	var blockID int64
	var offsetInBlock int64
	var sizeInBlock int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT block_id, offset_in_block, size_in_block FROM chunk_block_refs WHERE chunk_id = $1`,
		chunkID,
	).Scan(&blockID, &offsetInBlock, &sizeInBlock); err != nil {
		return false, err
	}

	var packedContainerID int64
	var packedContainerOffset int64
	var packedPlaintextSize int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT container_id, container_offset, plaintext_size FROM storage_blocks WHERE id = $1`,
		blockID,
	).Scan(&packedContainerID, &packedContainerOffset, &packedPlaintextSize); err != nil {
		return false, err
	}

	var totalReferencedBytes int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(size_in_block), 0) FROM chunk_block_refs WHERE block_id = $1`,
		blockID,
	).Scan(&totalReferencedBytes); err != nil {
		return false, err
	}
	payloadPrefixBytes := packedPlaintextSize - totalReferencedBytes
	if payloadPrefixBytes < 0 {
		return false, nil
	}

	var codec string
	var formatVersion int64
	var plaintextSize int64
	var storedSize int64
	var nonce []byte
	var legacyContainerID int64
	var legacyOffset int64
	if err := dbconn.QueryRowContext(ctx,
		`SELECT codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset
		 FROM blocks
		 WHERE chunk_id = $1`,
		chunkID,
	).Scan(&codec, &formatVersion, &plaintextSize, &storedSize, &nonce, &legacyContainerID, &legacyOffset); err != nil {
		return false, err
	}

	if formatVersion != 1 {
		return false, nil
	}

	switch codec {
	case "plain":
		if plaintextSize != chunkSize || storedSize != chunkSize {
			return false, nil
		}
		if sizeInBlock != chunkSize {
			return false, nil
		}
		expectedLegacyOffset := packedContainerOffset + payloadPrefixBytes + offsetInBlock
		if legacyContainerID != packedContainerID || legacyOffset != expectedLegacyOffset {
			return false, nil
		}
	case "aes-gcm":
		if plaintextSize != chunkSize {
			return false, nil
		}
		if storedSize <= 0 {
			return false, nil
		}
		if len(nonce) != 12 {
			return false, nil
		}
		if legacyContainerID != packedContainerID || legacyOffset != packedContainerOffset {
			return false, nil
		}
	default:
		return false, nil
	}

	return true, nil
}

func verifyBlockPayloads(dbconn *sql.DB, containersDir string) error {
	return verifyBlockPayloadsMode(dbconn, containersDir, true)
}

func verifyBlockPayloadsFast(dbconn *sql.DB, containersDir string) error {
	return verifyBlockPayloadsMode(dbconn, containersDir, false)
}

func verifyBlockPayloadsMode(dbconn *sql.DB, containersDir string, includeDeepContentChecks bool) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking packed block payload and segment integrity...")

	rows, err := dbconn.QueryContext(ctx, `
		SELECT sb.id, sb.container_id, sb.format_version, sb.codec, sb.plaintext_size, sb.compressed_size, sb.compression_codec, sb.compression_level, sb.container_offset, sb.stored_size,
		       sb.block_hash, sb.compressed_hash, sb.physical_hash, c.filename, c.max_size
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE c.quarantine = FALSE
		ORDER BY sb.id
	`)
	if err != nil {
		return fmt.Errorf("verifyBlockPayloads: query storage blocks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type blockRow struct {
		id               int64
		containerID      int64
		formatVersion    int64
		codec            string
		plaintextSize    int64
		compressedSize   sql.NullInt64
		compressionCodec string
		compressionLevel sql.NullInt64
		containerOffset  int64
		storedSize       int64
		logicalHash      []byte
		compressedHash   []byte
		physicalHash     []byte
		filename         string
		maxSize          int64
	}

	blocksToVerify := make([]blockRow, 0)
	for rows.Next() {
		var b blockRow
		if err := rows.Scan(
			&b.id,
			&b.containerID,
			&b.formatVersion,
			&b.codec,
			&b.plaintextSize,
			&b.compressedSize,
			&b.compressionCodec,
			&b.compressionLevel,
			&b.containerOffset,
			&b.storedSize,
			&b.logicalHash,
			&b.compressedHash,
			&b.physicalHash,
			&b.filename,
			&b.maxSize,
		); err != nil {
			return verifyCategoryError(verifyErrMetadataInvalid, "verifyBlockPayloads: scan storage block row", err)
		}
		blocksToVerify = append(blocksToVerify, b)
	}
	if err := rows.Err(); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyBlockPayloads: iterate storage block rows", err)
	}

	strictMode := verifyStrictPackedSegmentsEnabled()
	reader := FilesystemContainerReader{ContainersDir: containersDir}

	for _, b := range blocksToVerify {
		var compressionLevel *int
		if b.compressionLevel.Valid {
			v := int(b.compressionLevel.Int64)
			compressionLevel = &v
		}
		var compressedSize *int64
		if b.compressedSize.Valid {
			v := b.compressedSize.Int64
			compressedSize = &v
		}

		verified, err := VerifyStoredBlock(ctx, BlockStorageMetadata{
			BlockID:          b.id,
			ContainerID:      b.containerID,
			ContainerOffset:  b.containerOffset,
			ContainerName:    b.filename,
			ContainerMaxSize: b.maxSize,
			FormatVersion:    b.formatVersion,
			Codec:            b.codec,
			PlaintextSize:    b.plaintextSize,
			CompressedSize:   compressedSize,
			StoredSize:       b.storedSize,
			CompressionCodec: b.compressionCodec,
			CompressionLevel: compressionLevel,
			LogicalHash:      b.logicalHash,
			CompressedHash:   b.compressedHash,
			PhysicalHash:     b.physicalHash,
		}, reader)
		if err != nil {
			return err
		}

		if !includeDeepContentChecks {
			continue
		}

		decoded := verified.DecodedBlock
		if err := verifyDecodedBlockHeaderAndTable(b.id, b.formatVersion, b.codec, decoded); err != nil {
			return err
		}
		if err := verifyDecodedChunkSliceHashes(ctx, dbconn, b.id, b.containerID, b.containerOffset, decoded); err != nil {
			return err
		}
		if err := verifyDecodedBlockSegmentsAgainstRefs(ctx, dbconn, b.id, b.containerID, b.containerOffset, decoded, strictMode); err != nil {
			return err
		}
	}

	log.Println(" SUCCESS ")
	return nil
}

func resolveVerifyStorageBlockCodec(raw string) (blocks.Codec, error) {
	codecText := strings.ToLower(strings.TrimSpace(raw))
	if codecText == "none" {
		return blocks.CodecPlain, nil
	}
	if codecText == "" {
		return "", fmt.Errorf("empty codec")
	}
	codec, err := blocks.ParseCodec(codecText)
	if err != nil {
		return "", err
	}
	return codec, nil
}

type verifyChunkRefSegment struct {
	chunkID   int64
	offset    uint64
	size      uint64
	chunkSize int64
}

func verifyDecodedBlockHeaderAndTable(blockID int64, formatVersion int64, codec string, decoded *blocks.EncodedBlock) error {
	if decoded == nil {
		return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded block is nil", blockID), nil)
	}

	if decoded.Header.Version != blocks.BlockFormatVersionV1 {
		return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded header version invalid=%d", blockID, decoded.Header.Version), nil)
	}
	if int64(decoded.Header.Version) != formatVersion {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyBlockPayloads: block %d decoded header version mismatch metadata=%d decoded=%d", blockID, formatVersion, decoded.Header.Version), nil)
	}

	if decoded.Header.Codec != blocks.BlockCodecNoneV1 {
		return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded codec invalid=%d", blockID, decoded.Header.Codec), nil)
	}
	codecText := strings.ToLower(strings.TrimSpace(codec))
	if codecText != "none" && codecText != "aes-gcm" {
		return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyBlockPayloads: block %d storage codec unsupported for packed verify=%q", blockID, codec), nil)
	}

	if decoded.Header.ChunkCount == 0 {
		return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded chunk count must be > 0", blockID), nil)
	}
	if int(decoded.Header.ChunkCount) != len(decoded.Entries) {
		return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded chunk count mismatch header=%d entries=%d", blockID, decoded.Header.ChunkCount, len(decoded.Entries)), nil)
	}

	payloadSize := uint64(len(decoded.Payload))
	if decoded.Header.PlaintextSize != payloadSize {
		return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded payload size mismatch header=%d payload=%d", blockID, decoded.Header.PlaintextSize, payloadSize), nil)
	}

	expectedOffset := uint64(0)
	for i, entry := range decoded.Entries {
		if entry.Size == 0 {
			return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded chunk table bounds invalid index=%d reason=zero-size", blockID, i), nil)
		}
		if entry.Offset != expectedOffset {
			return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded chunk table bounds invalid index=%d expected_offset=%d got=%d", blockID, i, expectedOffset, entry.Offset), nil)
		}
		if _, err := blocks.SliceChunkFromPayload(decoded.Payload, entry); err != nil {
			return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded chunk table bounds invalid index=%d", blockID, i), err)
		}
		expectedOffset = entry.Offset + entry.Size
	}
	if expectedOffset != payloadSize {
		return verifyCategoryError(verifyErrUnsupportedBlock, fmt.Sprintf("verifyBlockPayloads: block %d decoded chunk table bounds invalid final_end=%d payload=%d", blockID, expectedOffset, payloadSize), nil)
	}

	return nil
}

func verifyDecodedChunkSliceHashes(ctx context.Context, dbconn *sql.DB, blockID int64, containerID int64, containerOffset int64, decoded *blocks.EncodedBlock) error {
	meta := verifyBlockFailureMeta(VerifyStageChunkRefs, blockID, containerID, containerOffset)

	for i, entry := range decoded.Entries {
		chunkBytes, err := blocks.SliceChunkFromPayload(decoded.Payload, entry)
		if err != nil {
			return verifyStageError(verifyErrUnsupportedBlock, meta, fmt.Sprintf("verifyBlockPayloads: block %d chunk %d slice failed at entry=%d", blockID, entry.ChunkID, i), err)
		}

		sum := sha256.Sum256(chunkBytes)
		computed := hex.EncodeToString(sum[:])

		var expected string
		if err := dbconn.QueryRowContext(ctx, `SELECT chunk_hash FROM chunk WHERE id = $1`, int64(entry.ChunkID)).Scan(&expected); err != nil {
			if err == sql.ErrNoRows {
				return verifyStageError(verifyErrMetadataMissing, meta, fmt.Sprintf("verifyBlockPayloads: block %d chunk %d from decoded entry=%d missing chunk row", blockID, entry.ChunkID, i), nil)
			}
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: block %d load chunk hash for chunk %d", blockID, entry.ChunkID), err)
		}

		if !strings.EqualFold(strings.TrimSpace(expected), computed) {
			return verifyStageError(verifyErrChunkHashMismatch, meta, fmt.Sprintf("verifyBlockPayloads: block %d chunk %d hash mismatch computed=%s expected=%s", blockID, entry.ChunkID, computed, strings.TrimSpace(expected)), nil)
		}
	}

	return nil
}

func verifyDecodedBlockSegmentsAgainstRefs(ctx context.Context, dbconn *sql.DB, blockID int64, containerID int64, containerOffset int64, decoded *blocks.EncodedBlock, strictMode bool) error {
	meta := verifyBlockFailureMeta(VerifyStageChunkRefs, blockID, containerID, containerOffset)

	rows, err := dbconn.QueryContext(ctx, `
		SELECT r.chunk_id, r.offset_in_block, r.size_in_block, c.size
		FROM chunk_block_refs r
		JOIN chunk c ON c.id = r.chunk_id
		WHERE r.block_id = $1
		ORDER BY r.offset_in_block ASC
	`, blockID)
	if err != nil {
		return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: query chunk refs for block %d", blockID), err)
	}
	defer func() { _ = rows.Close() }()

	segments := make([]verifyChunkRefSegment, 0)
	for rows.Next() {
		var chunkID int64
		var offset int64
		var size int64
		var chunkSize int64
		if err := rows.Scan(&chunkID, &offset, &size, &chunkSize); err != nil {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: scan chunk ref for block %d", blockID), err)
		}
		if offset < 0 {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: block %d has negative offset_in_block for chunk %d", blockID, chunkID), nil)
		}
		if size <= 0 {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: block %d has non-positive size_in_block for chunk %d", blockID, chunkID), nil)
		}
		if chunkSize > 0 && size != chunkSize {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: block %d chunk %d size mismatch ref=%d chunk.size=%d", blockID, chunkID, size, chunkSize), nil)
		}
		segments = append(segments, verifyChunkRefSegment{
			chunkID:   chunkID,
			offset:    uint64(offset),
			size:      uint64(size),
			chunkSize: chunkSize,
		})
	}
	if err := rows.Err(); err != nil {
		return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: iterate chunk refs for block %d", blockID), err)
	}
	decodedEntriesByKey := make(map[verifyChunkRefSegment]struct{}, len(decoded.Entries))
	for _, e := range decoded.Entries {
		decodedEntriesByKey[verifyChunkRefSegment{
			chunkID: int64(e.ChunkID),
			offset:  e.Offset,
			size:    e.Size,
		}] = struct{}{}
	}

	refsByKey := make(map[verifyChunkRefSegment]struct{}, len(segments))
	for _, s := range segments {
		refsByKey[verifyChunkRefSegment{
			chunkID: s.chunkID,
			offset:  s.offset,
			size:    s.size,
		}] = struct{}{}
	}

	for _, s := range segments {
		k := verifyChunkRefSegment{chunkID: s.chunkID, offset: s.offset, size: s.size}
		if _, ok := decodedEntriesByKey[k]; !ok {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: block %d chunk_block_ref references chunk not in encoded block table chunk=%d offset=%d size=%d", blockID, s.chunkID, s.offset, s.size), nil)
		}
	}

	for _, e := range decoded.Entries {
		k := verifyChunkRefSegment{chunkID: int64(e.ChunkID), offset: e.Offset, size: e.Size}
		if _, ok := refsByKey[k]; !ok {
			return verifyStageError(verifyErrMetadataMissing, meta, fmt.Sprintf("verifyBlockPayloads: block %d encoded block table contains chunk not in chunk_block_refs chunk=%d offset=%d size=%d", blockID, e.ChunkID, e.Offset, e.Size), nil)
		}
	}

	payloadSize := uint64(len(decoded.Payload))
	for _, s := range segments {
		end := s.offset + s.size
		if end < s.offset || end > payloadSize {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: block %d chunk %d segment out of payload bounds offset=%d size=%d payload=%d", blockID, s.chunkID, s.offset, s.size, payloadSize), nil)
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		if segments[i].offset == segments[j].offset {
			return segments[i].chunkID < segments[j].chunkID
		}
		return segments[i].offset < segments[j].offset
	})

	prevEnd := uint64(0)
	for i, s := range segments {
		if i > 0 && s.offset < prevEnd {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: block %d has overlapping segments around chunk %d", blockID, s.chunkID), nil)
		}
		prevEnd = s.offset + s.size
	}

	if strictMode {
		if len(decoded.Entries) != len(segments) {
			return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: strict mode block %d entry count mismatch decoded=%d refs=%d", blockID, len(decoded.Entries), len(segments)), nil)
		}
		for i := range decoded.Entries {
			e := decoded.Entries[i]
			s := segments[i]
			if int64(e.ChunkID) != s.chunkID || e.Offset != s.offset || e.Size != s.size {
				return verifyStageError(verifyErrMetadataInvalid, meta, fmt.Sprintf("verifyBlockPayloads: strict mode block %d entry mismatch at index %d", blockID, i), nil)
			}
		}
	}

	return nil
}

func verifyStrictPackedSegmentsEnabled() bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv("COLDKEEP_VERIFY_STRICT_SEGMENTS")))
	if raw == "" {
		return false
	}
	if raw == "true" || raw == "yes" || raw == "on" {
		return true
	}
	v, err := strconv.ParseBool(raw)
	if err == nil {
		return v
	}
	return false
}

func verifyLegacyChunkHashes(dbconn *sql.DB, containersDir string) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking legacy block payload hash integrity...")

	rows, err := dbconn.QueryContext(ctx, `
		SELECT
			b.chunk_id,
			b.block_offset,
			b.stored_size,
			b.plaintext_size,
			c.chunk_hash,
			b.codec,
			b.format_version,
			b.nonce,
			cctr.filename,
			cctr.max_size
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		JOIN container cctr ON cctr.id = b.container_id
		WHERE c.status = 'COMPLETED'
		  AND NOT EXISTS (
			SELECT 1
			FROM chunk_block_refs r
			WHERE r.chunk_id = c.id
		  )
		ORDER BY cctr.id, b.block_offset
	`)
	if err != nil {
		return fmt.Errorf("verifyLegacyChunkHashes: query legacy blocks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	transformers := make(map[blocks.Codec]blocks.Transformer)
	openContainers := make(map[string]*container.FileContainer)
	defer func() {
		for _, fc := range openContainers {
			_ = fc.Close()
		}
	}()

	for rows.Next() {
		var chunkID int64
		var blockOffset int64
		var storedSize int64
		var plaintextSize int64
		var expectedChunkHash string
		var codecRaw string
		var formatVersion int
		var nonce []byte
		var filename string
		var maxSize int64

		if err := rows.Scan(&chunkID, &blockOffset, &storedSize, &plaintextSize, &expectedChunkHash, &codecRaw, &formatVersion, &nonce, &filename, &maxSize); err != nil {
			return verifyCategoryError(verifyErrMetadataInvalid, "verifyLegacyChunkHashes: scan legacy block row", err)
		}

		codec, err := blocks.ParseCodec(strings.ToLower(strings.TrimSpace(codecRaw)))
		if err != nil {
			return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyLegacyChunkHashes: chunk %d invalid codec=%q", chunkID, codecRaw), err)
		}

		transformer := transformers[codec]
		if transformer == nil {
			transformer, err = blocks.GetBlockTransformer(codec)
			if err != nil {
				return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyLegacyChunkHashes: chunk %d get transformer codec=%s", chunkID, codec), err)
			}
			transformers[codec] = transformer
		}

		fc := openContainers[filename]
		if fc == nil {
			containerPath, pathErr := container.SafeContainerPath(containersDir, filename)
			if pathErr != nil {
				return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyLegacyChunkHashes: invalid container filename %q", filename), pathErr)
			}
			fc, err = container.OpenReadOnlyContainer(containerPath, maxSize)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return verifyCategoryError(verifyErrPhysicalMissing, fmt.Sprintf("verifyLegacyChunkHashes: open container %q", filename), err)
				}
				return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyLegacyChunkHashes: open container %q", filename), err)
			}
			openContainers[filename] = fc
		}

		payload, err := container.ReadPayloadAt(fc, blockOffset, storedSize)
		if err != nil {
			return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyLegacyChunkHashes: read chunk %d payload", chunkID), err)
		}

		plaintext, err := transformer.Decode(ctx, blocks.DecodeInput{
			ChunkHash: expectedChunkHash,
			Descriptor: blocks.Descriptor{
				ChunkID:       chunkID,
				Codec:         codec,
				FormatVersion: formatVersion,
				PlaintextSize: plaintextSize,
				StoredSize:    storedSize,
				Nonce:         nonce,
				ContainerID:   0,
				BlockOffset:   blockOffset,
			},
			Payload: payload,
		})
		if err != nil {
			return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyLegacyChunkHashes: decode chunk %d payload", chunkID), err)
		}
		if int64(len(plaintext)) != plaintextSize {
			return verifyCategoryError(verifyErrMetadataInvalid, fmt.Sprintf("verifyLegacyChunkHashes: chunk %d plaintext size mismatch metadata=%d decoded=%d", chunkID, plaintextSize, len(plaintext)), nil)
		}

		sum := sha256.Sum256(plaintext)
		computed := hex.EncodeToString(sum[:])
		if !strings.EqualFold(strings.TrimSpace(expectedChunkHash), computed) {
			return verifyCategoryError(verifyErrChunkHashMismatch, fmt.Sprintf("verifyLegacyChunkHashes: chunk %d hash mismatch computed=%s expected=%s", chunkID, computed, strings.TrimSpace(expectedChunkHash)), nil)
		}
	}
	if err := rows.Err(); err != nil {
		return verifyCategoryError(verifyErrMetadataInvalid, "verifyLegacyChunkHashes: iterate legacy block rows", err)
	}

	log.Println("Legacy block payload hash integrity check: OK")
	return nil
}

func verifyLegacyCompatibility(dbconn *sql.DB, containersDir string) error {
	if err := verifyLegacyChunkHashes(dbconn, containersDir); err != nil {
		return fmt.Errorf("verifyLegacyCompatibility: %w", err)
	}
	if err := runLogicalReconstructionChecks(dbconn); err != nil {
		return fmt.Errorf("verifyLegacyCompatibility: %w", err)
	}
	return nil
}
