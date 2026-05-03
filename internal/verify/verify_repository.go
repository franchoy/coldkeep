package verify

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
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

	var invalidFieldRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE format_version != 1
		   OR codec != 'none'
		   OR plaintext_size <= 0
		   OR stored_size <= 0
		   OR container_offset < 0
	`).Scan(&invalidFieldRows); err != nil {
		return fmt.Errorf("verifyStorageBlocks: query invalid field rows: %w", err)
	}
	if invalidFieldRows > 0 {
		return fmt.Errorf("verifyStorageBlocks: storage_blocks rows with invalid metadata fields=%d", invalidFieldRows)
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

	const expectedBlockHashLen = 32
	var invalidHashLenRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE length(block_hash) != $1
	`, expectedBlockHashLen).Scan(&invalidHashLenRows); err != nil {
		return fmt.Errorf("verifyStorageBlocks: query invalid block_hash length rows: %w", err)
	}
	if invalidHashLenRows > 0 {
		return fmt.Errorf("verifyStorageBlocks: storage_blocks rows with invalid block_hash length=%d expected=%d", invalidHashLenRows, expectedBlockHashLen)
	}

	var impossibleLocationRows int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE sb.container_offset + sb.stored_size > c.current_size
	`).Scan(&impossibleLocationRows); err != nil {
		return fmt.Errorf("verifyStorageBlocks: query impossible container locations: %w", err)
	}
	if impossibleLocationRows > 0 {
		return fmt.Errorf("verifyStorageBlocks: storage_blocks rows with impossible container ranges=%d", impossibleLocationRows)
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
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	log.Printf("Checking packed block payload and segment integrity...")

	rows, err := dbconn.QueryContext(ctx, `
		SELECT sb.id, sb.format_version, sb.codec, sb.plaintext_size, sb.container_offset, sb.stored_size, sb.block_hash, c.filename, c.max_size
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		ORDER BY sb.id
	`)
	if err != nil {
		return fmt.Errorf("verifyBlockPayloads: query storage blocks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type blockRow struct {
		id              int64
		formatVersion   int64
		codec           string
		plaintextSize   int64
		containerOffset int64
		storedSize      int64
		blockHash       []byte
		filename        string
		maxSize         int64
	}

	blocksToVerify := make([]blockRow, 0)
	for rows.Next() {
		var b blockRow
		if err := rows.Scan(&b.id, &b.formatVersion, &b.codec, &b.plaintextSize, &b.containerOffset, &b.storedSize, &b.blockHash, &b.filename, &b.maxSize); err != nil {
			return fmt.Errorf("verifyBlockPayloads: scan storage block row: %w", err)
		}
		blocksToVerify = append(blocksToVerify, b)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verifyBlockPayloads: iterate storage block rows: %w", err)
	}

	strictMode := verifyStrictPackedSegmentsEnabled()
	transformers := make(map[blocks.Codec]blocks.Transformer)

	for _, b := range blocksToVerify {
		path := filepath.Join(containersDir, b.filename)
		fc, err := container.OpenReadOnlyContainer(path, b.maxSize)
		if err != nil {
			return fmt.Errorf("verifyBlockPayloads: open container for block %d: %w", b.id, err)
		}

		storedBytes, readErr := container.ReadPayloadAt(fc, b.containerOffset, b.storedSize)
		closeErr := fc.Close()
		if readErr != nil {
			return fmt.Errorf("verifyBlockPayloads: read payload for block %d: %w", b.id, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("verifyBlockPayloads: close container for block %d: %w", b.id, closeErr)
		}

		codec, err := resolveVerifyStorageBlockCodec(b.codec)
		if err != nil {
			return fmt.Errorf("verifyBlockPayloads: block %d invalid codec metadata: %w", b.id, err)
		}

		transformer := transformers[codec]
		if transformer == nil {
			transformer, err = blocks.GetBlockTransformer(codec)
			if err != nil {
				return fmt.Errorf("verifyBlockPayloads: block %d get transformer codec=%s: %w", b.id, codec, err)
			}
			transformers[codec] = transformer
		}

		plaintextEncoded, err := transformer.Decode(ctx, blocks.DecodeInput{
			Descriptor: blocks.Descriptor{
				ChunkID:       0,
				Codec:         codec,
				FormatVersion: int(b.formatVersion),
				PlaintextSize: b.plaintextSize,
				StoredSize:    b.storedSize,
				ContainerID:   0,
				BlockOffset:   b.containerOffset,
			},
			Payload: storedBytes,
		})
		if err != nil {
			return fmt.Errorf("verifyBlockPayloads: block %d transform/decrypt failed: %w", b.id, err)
		}
		if int64(len(plaintextEncoded)) != b.plaintextSize {
			return fmt.Errorf("verifyBlockPayloads: block %d plaintext size mismatch metadata=%d decoded=%d", b.id, b.plaintextSize, len(plaintextEncoded))
		}

		if err := blocks.VerifyBlockHash(plaintextEncoded, b.blockHash); err != nil {
			return fmt.Errorf("verifyBlockPayloads: block %d hash mismatch: %w", b.id, err)
		}

		decoded, err := blocks.DecodeBlock(plaintextEncoded)
		if err != nil {
			return fmt.Errorf("verifyBlockPayloads: decode block %d: %w", b.id, err)
		}
		if err := verifyDecodedBlockHeaderAndTable(b.id, b.formatVersion, b.codec, decoded); err != nil {
			return err
		}
		if err := verifyDecodedChunkSliceHashes(ctx, dbconn, b.id, decoded); err != nil {
			return err
		}

		if err := verifyDecodedBlockSegmentsAgainstRefs(ctx, dbconn, b.id, decoded, strictMode); err != nil {
			return err
		}
	}

	log.Println(" SUCCESS ")
	return nil
}

type verifyChunkRefSegment struct {
	chunkID   int64
	offset    uint64
	size      uint64
	chunkSize int64
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

func verifyDecodedBlockHeaderAndTable(blockID int64, formatVersion int64, codec string, decoded *blocks.EncodedBlock) error {
	if decoded == nil {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded block is nil", blockID)
	}

	if decoded.Header.Version != blocks.BlockFormatVersionV1 {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded header version invalid=%d", blockID, decoded.Header.Version)
	}
	if int64(decoded.Header.Version) != formatVersion {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded header version mismatch metadata=%d decoded=%d", blockID, formatVersion, decoded.Header.Version)
	}

	if decoded.Header.Codec != blocks.BlockCodecNoneV1 {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded codec invalid=%d", blockID, decoded.Header.Codec)
	}
	if strings.ToLower(strings.TrimSpace(codec)) != "none" {
		return fmt.Errorf("verifyBlockPayloads: block %d storage codec unsupported for packed verify=%q", blockID, codec)
	}

	if decoded.Header.ChunkCount == 0 {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded chunk count must be > 0", blockID)
	}
	if int(decoded.Header.ChunkCount) != len(decoded.Entries) {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded chunk count mismatch header=%d entries=%d", blockID, decoded.Header.ChunkCount, len(decoded.Entries))
	}

	payloadSize := uint64(len(decoded.Payload))
	if decoded.Header.PlaintextSize != payloadSize {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded payload size mismatch header=%d payload=%d", blockID, decoded.Header.PlaintextSize, payloadSize)
	}

	expectedOffset := uint64(0)
	for i, entry := range decoded.Entries {
		if entry.Size == 0 {
			return fmt.Errorf("verifyBlockPayloads: block %d decoded chunk table bounds invalid index=%d reason=zero-size", blockID, i)
		}
		if entry.Offset != expectedOffset {
			return fmt.Errorf("verifyBlockPayloads: block %d decoded chunk table bounds invalid index=%d expected_offset=%d got=%d", blockID, i, expectedOffset, entry.Offset)
		}
		if _, err := blocks.SliceChunkFromPayload(decoded.Payload, entry); err != nil {
			return fmt.Errorf("verifyBlockPayloads: block %d decoded chunk table bounds invalid index=%d: %w", blockID, i, err)
		}
		expectedOffset = entry.Offset + entry.Size
	}
	if expectedOffset != payloadSize {
		return fmt.Errorf("verifyBlockPayloads: block %d decoded chunk table bounds invalid final_end=%d payload=%d", blockID, expectedOffset, payloadSize)
	}

	return nil
}

func verifyDecodedChunkSliceHashes(ctx context.Context, dbconn *sql.DB, blockID int64, decoded *blocks.EncodedBlock) error {
	for i, entry := range decoded.Entries {
		chunkBytes, err := blocks.SliceChunkFromPayload(decoded.Payload, entry)
		if err != nil {
			return fmt.Errorf("verifyBlockPayloads: block %d chunk %d slice failed at entry=%d: %w", blockID, entry.ChunkID, i, err)
		}

		sum := sha256.Sum256(chunkBytes)
		computed := hex.EncodeToString(sum[:])

		var expected string
		if err := dbconn.QueryRowContext(ctx, `SELECT chunk_hash FROM chunk WHERE id = $1`, int64(entry.ChunkID)).Scan(&expected); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("verifyBlockPayloads: block %d chunk %d from decoded entry=%d missing chunk row", blockID, entry.ChunkID, i)
			}
			return fmt.Errorf("verifyBlockPayloads: block %d load chunk hash for chunk %d: %w", blockID, entry.ChunkID, err)
		}

		if !strings.EqualFold(strings.TrimSpace(expected), computed) {
			return fmt.Errorf("verifyBlockPayloads: block %d chunk %d hash mismatch computed=%s expected=%s", blockID, entry.ChunkID, computed, strings.TrimSpace(expected))
		}
	}

	return nil
}

func verifyDecodedBlockSegmentsAgainstRefs(ctx context.Context, dbconn *sql.DB, blockID int64, decoded *blocks.EncodedBlock, strictMode bool) error {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT r.chunk_id, r.offset_in_block, r.size_in_block, c.size
		FROM chunk_block_refs r
		JOIN chunk c ON c.id = r.chunk_id
		WHERE r.block_id = $1
		ORDER BY r.offset_in_block ASC
	`, blockID)
	if err != nil {
		return fmt.Errorf("verifyBlockPayloads: query chunk refs for block %d: %w", blockID, err)
	}
	defer func() { _ = rows.Close() }()

	segments := make([]verifyChunkRefSegment, 0)
	for rows.Next() {
		var chunkID int64
		var offset int64
		var size int64
		var chunkSize int64
		if err := rows.Scan(&chunkID, &offset, &size, &chunkSize); err != nil {
			return fmt.Errorf("verifyBlockPayloads: scan chunk ref for block %d: %w", blockID, err)
		}
		if offset < 0 {
			return fmt.Errorf("verifyBlockPayloads: block %d has negative offset_in_block for chunk %d", blockID, chunkID)
		}
		if size <= 0 {
			return fmt.Errorf("verifyBlockPayloads: block %d has non-positive size_in_block for chunk %d", blockID, chunkID)
		}
		if chunkSize > 0 && size != chunkSize {
			return fmt.Errorf("verifyBlockPayloads: block %d chunk %d size mismatch ref=%d chunk.size=%d", blockID, chunkID, size, chunkSize)
		}
		segments = append(segments, verifyChunkRefSegment{
			chunkID:   chunkID,
			offset:    uint64(offset),
			size:      uint64(size),
			chunkSize: chunkSize,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verifyBlockPayloads: iterate chunk refs for block %d: %w", blockID, err)
	}
	if len(segments) != len(decoded.Entries) {
		return fmt.Errorf("verifyBlockPayloads: block %d chunk count mismatch decoded=%d refs=%d", blockID, len(decoded.Entries), len(segments))
	}

	payloadSize := uint64(len(decoded.Payload))
	for _, s := range segments {
		end := s.offset + s.size
		if end < s.offset || end > payloadSize {
			return fmt.Errorf("verifyBlockPayloads: block %d chunk %d segment out of payload bounds offset=%d size=%d payload=%d", blockID, s.chunkID, s.offset, s.size, payloadSize)
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
			return fmt.Errorf("verifyBlockPayloads: block %d has overlapping segments around chunk %d", blockID, s.chunkID)
		}
		prevEnd = s.offset + s.size
	}

	if strictMode {
		if len(decoded.Entries) != len(segments) {
			return fmt.Errorf("verifyBlockPayloads: strict mode block %d entry count mismatch decoded=%d refs=%d", blockID, len(decoded.Entries), len(segments))
		}
		for i := range decoded.Entries {
			e := decoded.Entries[i]
			s := segments[i]
			if int64(e.ChunkID) != s.chunkID || e.Offset != s.offset || e.Size != s.size {
				return fmt.Errorf("verifyBlockPayloads: strict mode block %d entry mismatch at index %d", blockID, i)
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

func verifyLegacyCompatibility(dbconn *sql.DB) error {
	if err := runLogicalReconstructionChecks(dbconn); err != nil {
		return fmt.Errorf("verifyLegacyCompatibility: %w", err)
	}
	return nil
}
