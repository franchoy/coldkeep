package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// LoadChunkPlacements returns one complete placement per logical recipe entry.
// Packed references are authoritative when a chunk also has a legacy companion
// row; otherwise the legacy block is returned. The method never returns a
// partial recipe.
func (s *Service) LoadChunkPlacements(ctx context.Context, logicalFileID int64) ([]ChunkPlacementRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, placementCatalogError(err)
	}
	if logicalFileID <= 0 {
		return nil, NewError(ErrorInvalidArgument, "load chunk placements", "positive_logical_file_id", "logical file ID must be positive", nil)
	}

	var totalSize int64
	if err := s.db.QueryRowContext(ctx, `SELECT total_size FROM logical_file WHERE id = $1`, logicalFileID).Scan(&totalSize); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewError(ErrorNotFound, "load chunk placements", "", fmt.Sprintf("logical file %d not found", logicalFileID), err)
		}
		return nil, placementCatalogError(fmt.Errorf("query logical file %d: %w", logicalFileID, err))
	}

	rows, err := s.db.QueryContext(ctx, chunkPlacementQuery, logicalFileID)
	if err != nil {
		return nil, placementCatalogError(fmt.Errorf("query logical file %d placements: %w", logicalFileID, err))
	}
	defer func() { _ = rows.Close() }()

	placements := make([]ChunkPlacementRef, 0)
	var expectedOrder int64
	for rows.Next() {
		scanned, err := scanChunkPlacement(rows)
		if err != nil {
			return nil, placementCatalogError(fmt.Errorf("scan logical file %d placement: %w", logicalFileID, err))
		}
		placement, err := scanned.toPlacement()
		if err != nil {
			return nil, err
		}
		if placement.ChunkOrder != expectedOrder {
			return nil, NewError(ErrorInvariantViolation, "load chunk placements", "contiguous_chunk_order", fmt.Sprintf("logical file %d has chunk order %d; expected %d", logicalFileID, placement.ChunkOrder, expectedOrder), nil)
		}
		if err := ValidateChunkPlacement(placement); err != nil {
			return nil, err
		}
		placements = append(placements, placement)
		expectedOrder++
	}
	if err := rows.Err(); err != nil {
		return nil, placementCatalogError(fmt.Errorf("iterate logical file %d placements: %w", logicalFileID, err))
	}
	if len(placements) == 0 && totalSize != 0 {
		return nil, NewError(ErrorInvariantViolation, "load chunk placements", "nonempty_file_has_recipe", fmt.Sprintf("logical file %d has size %d but no chunk recipe", logicalFileID, totalSize), nil)
	}
	return placements, nil
}

const chunkPlacementQuery = `
SELECT
  fc.chunk_order,
  c.id, c.chunk_hash, c.size, c.chunker_version, c.status,
  r.block_id, r.offset_in_block, r.size_in_block,
  sb.format_version, sb.codec, sb.plaintext_size,
  sb.compression_codec, sb.compression_level, sb.compressed_size,
  sb.stored_size, sb.block_hash, sb.compressed_hash, sb.physical_hash,
  sb.container_id, sb.container_offset,
  pc.filename, pc.sealed, pc.sealing, pc.container_hash, pc.quarantine,
  pc.current_size, pc.max_size,
  b.id, b.codec, b.format_version, b.plaintext_size, b.stored_size,
  b.nonce, b.container_id, b.block_offset,
  lc.filename, lc.sealed, lc.sealing, lc.container_hash, lc.quarantine,
  lc.current_size, lc.max_size
FROM file_chunk fc
JOIN chunk c ON c.id = fc.chunk_id
LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
LEFT JOIN storage_blocks sb ON sb.id = r.block_id
LEFT JOIN container pc ON pc.id = sb.container_id
LEFT JOIN blocks b ON b.chunk_id = c.id
LEFT JOIN container lc ON lc.id = b.container_id
WHERE fc.logical_file_id = $1
ORDER BY fc.chunk_order ASC`

type chunkPlacementScan struct {
	chunkOrder, chunkID, chunkSize                    int64
	chunkHash, chunkerVersion, chunkStatus            string
	packedBlockID, packedOffset, packedSize           sql.NullInt64
	packedFormat                                      sql.NullInt64
	packedCodec, compressionCodec                     sql.NullString
	packedPlaintext, compressionLevel, compressedSize sql.NullInt64
	packedStored                                      sql.NullInt64
	blockHash, compressedHash, physicalHash           []byte
	packedContainerID, packedContainerOffset          sql.NullInt64
	packedContainer                                   nullableContainer
	legacyBlockID, legacyFormat, legacyPlaintext      sql.NullInt64
	legacyCodec                                       sql.NullString
	legacyStored                                      sql.NullInt64
	legacyNonce                                       []byte
	legacyContainerID, legacyContainerOffset          sql.NullInt64
	legacyContainer                                   nullableContainer
}

type nullableContainer struct {
	filename, hash               sql.NullString
	sealed, sealing, quarantined sql.NullBool
	currentSize, maxSize         sql.NullInt64
}

func scanChunkPlacement(rows *sql.Rows) (chunkPlacementScan, error) {
	var row chunkPlacementScan
	err := rows.Scan(
		&row.chunkOrder,
		&row.chunkID, &row.chunkHash, &row.chunkSize, &row.chunkerVersion, &row.chunkStatus,
		&row.packedBlockID, &row.packedOffset, &row.packedSize,
		&row.packedFormat, &row.packedCodec, &row.packedPlaintext,
		&row.compressionCodec, &row.compressionLevel, &row.compressedSize,
		&row.packedStored, &row.blockHash, &row.compressedHash, &row.physicalHash,
		&row.packedContainerID, &row.packedContainerOffset,
		&row.packedContainer.filename, &row.packedContainer.sealed, &row.packedContainer.sealing,
		&row.packedContainer.hash, &row.packedContainer.quarantined,
		&row.packedContainer.currentSize, &row.packedContainer.maxSize,
		&row.legacyBlockID, &row.legacyCodec, &row.legacyFormat, &row.legacyPlaintext, &row.legacyStored,
		&row.legacyNonce, &row.legacyContainerID, &row.legacyContainerOffset,
		&row.legacyContainer.filename, &row.legacyContainer.sealed, &row.legacyContainer.sealing,
		&row.legacyContainer.hash, &row.legacyContainer.quarantined,
		&row.legacyContainer.currentSize, &row.legacyContainer.maxSize,
	)
	return row, err
}

func (row chunkPlacementScan) toPlacement() (ChunkPlacementRef, error) {
	placement := ChunkPlacementRef{
		ChunkOrder: row.chunkOrder, ChunkID: row.chunkID, ChunkHash: row.chunkHash,
		ChunkSize: row.chunkSize, ChunkerVersion: row.chunkerVersion, ChunkStatus: row.chunkStatus,
	}
	if row.packedBlockID.Valid {
		packed, err := row.packedPlacement()
		if err != nil {
			return ChunkPlacementRef{}, err
		}
		placement.Kind, placement.Packed = PlacementPacked, packed
		return placement, nil
	}
	if row.legacyBlockID.Valid {
		legacy, err := row.legacyPlacement()
		if err != nil {
			return ChunkPlacementRef{}, err
		}
		placement.Kind, placement.Legacy = PlacementLegacy, legacy
		return placement, nil
	}
	return ChunkPlacementRef{}, invalidPlacement(placement, "chunk has neither packed nor legacy placement metadata")
}

func (row chunkPlacementScan) packedPlacement() (*PackedChunkPlacement, error) {
	if !row.packedOffset.Valid || !row.packedSize.Valid || !row.packedFormat.Valid || !row.packedCodec.Valid || !row.packedPlaintext.Valid || !row.compressionCodec.Valid || !row.packedStored.Valid || !row.packedContainerID.Valid || !row.packedContainerOffset.Valid {
		return nil, invalidPlacement(ChunkPlacementRef{ChunkID: row.chunkID}, "packed placement metadata is incomplete")
	}
	container, err := row.packedContainer.value(row.packedContainerID.Int64)
	if err != nil {
		return nil, invalidPlacement(ChunkPlacementRef{ChunkID: row.chunkID}, err.Error())
	}
	return &PackedChunkPlacement{
		BlockID: row.packedBlockID.Int64, FormatVersion: int(row.packedFormat.Int64), Codec: row.packedCodec.String,
		PlaintextSize: row.packedPlaintext.Int64, CompressionCodec: row.compressionCodec.String,
		CompressionLevel: optionalInt(row.compressionLevel), CompressedSize: optionalInt64(row.compressedSize),
		StoredSize: row.packedStored.Int64, BlockHash: cloneBytes(row.blockHash),
		CompressedHash: cloneBytes(row.compressedHash), PhysicalHash: cloneBytes(row.physicalHash),
		Container: container, ContainerOffset: row.packedContainerOffset.Int64,
		OffsetInBlock: row.packedOffset.Int64, SizeInBlock: row.packedSize.Int64,
	}, nil
}

func (row chunkPlacementScan) legacyPlacement() (*LegacyChunkPlacement, error) {
	if !row.legacyCodec.Valid || !row.legacyFormat.Valid || !row.legacyPlaintext.Valid || !row.legacyStored.Valid || !row.legacyContainerID.Valid || !row.legacyContainerOffset.Valid {
		return nil, invalidPlacement(ChunkPlacementRef{ChunkID: row.chunkID}, "legacy placement metadata is incomplete")
	}
	container, err := row.legacyContainer.value(row.legacyContainerID.Int64)
	if err != nil {
		return nil, invalidPlacement(ChunkPlacementRef{ChunkID: row.chunkID}, err.Error())
	}
	return &LegacyChunkPlacement{
		BlockID: row.legacyBlockID.Int64, Codec: row.legacyCodec.String, FormatVersion: int(row.legacyFormat.Int64),
		PlaintextSize: row.legacyPlaintext.Int64, StoredSize: row.legacyStored.Int64, Nonce: cloneBytes(row.legacyNonce),
		Container: container, ContainerOffset: row.legacyContainerOffset.Int64,
	}, nil
}

func (container nullableContainer) value(id int64) (ContainerPlacementRef, error) {
	if !container.filename.Valid || !container.sealed.Valid || !container.sealing.Valid || !container.quarantined.Valid || !container.currentSize.Valid || !container.maxSize.Valid {
		return ContainerPlacementRef{}, fmt.Errorf("container %d metadata is incomplete", id)
	}
	return ContainerPlacementRef{
		ID: id, Filename: container.filename.String, Sealed: container.sealed.Bool,
		Sealing: container.sealing.Bool, ContainerHash: container.hash.String,
		Quarantined: container.quarantined.Bool, CurrentSize: container.currentSize.Int64,
		MaxSize: container.maxSize.Int64,
	}, nil
}

func optionalInt(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	converted := int(value.Int64)
	return &converted
}

func optionalInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	converted := value.Int64
	return &converted
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func placementCatalogError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError(ErrorCancelled, "load chunk placements", "", "chunk placement load cancelled", err)
	}
	return NewError(ErrorOperationFailed, "load chunk placements", "", "chunk placement query failed", err)
}
