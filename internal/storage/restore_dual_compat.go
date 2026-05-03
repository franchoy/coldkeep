package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/blocks"
)

// DualCompatChunkResolver implements dual v1.7/v1.8 chunk resolution.
// It queries chunk_block_refs (v1.8) and falls back to legacy block mapping (v1.7).
//
// v1.7 layout: each chunk stored as a single "block" (conceptually).
//
//	storage_chunks.block_offset contains the chunk bytes.
//	Legacy: blockID ≡ chunkID (direct mapping).
//
// v1.8 layout: chunks packed inside physical blocks.
//
//	chunk_block_refs.(block_id, offset, size) define placement.
//	Advanced: many chunks → one physical block.
//
// This resolver bridges both models seamlessly.
type DualCompatChunkResolver struct {
	db *sql.DB
}

// NewDualCompatChunkResolver creates a resolver supporting both v1.7 and v1.8 layouts.
func NewDualCompatChunkResolver(db *sql.DB) *DualCompatChunkResolver {
	return &DualCompatChunkResolver{db: db}
}

// ResolveChunk implements blocks.ChunkResolver.
// Returns ChunkSegment by checking v1.8 first, then falling back to v1.7.
func (r *DualCompatChunkResolver) ResolveChunk(ctx context.Context, chunkID int64) (*blocks.ChunkSegment, error) {
	if r.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	// Phase 3.2 Step 1: Check v1.8 chunk_block_refs table
	// If a chunk has an entry in chunk_block_refs, it's stored in a v1.8 packed block.
	v18Segment, err := r.resolveV18ChunkBlockRef(ctx, chunkID)
	if err != nil && err != sql.ErrNoRows {
		// Hard error, not just "not found"
		return nil, fmt.Errorf("query chunk_block_refs for chunk %d: %w", chunkID, err)
	}

	if err == nil && v18Segment != nil {
		// v1.8 hit: chunk is in a packed block
		return v18Segment, nil
	}

	// Phase 3.2 Step 2: Fallback to v1.7 legacy mapping
	// If no v1.8 entry, treat the chunk as a single "block" (v1.7 model).
	// In v1.7, chunk stores directly; we map chunkID → blockID and rely on
	// legacy restore logic for the actual read.
	v17Segment := &blocks.ChunkSegment{
		ChunkID: chunkID,
		BlockID: 0, // v1.7 marker: triggers legacy direct-chunk path
		Offset:  0,
		Size:    0,
	}

	return v17Segment, nil
}

// resolveV18ChunkBlockRef queries chunk_block_refs for a v1.8 packed-block entry.
// Returns nil, nil if not found (not an error — means v1.7 fallback applies).
// Returns error only on database issues.
func (r *DualCompatChunkResolver) resolveV18ChunkBlockRef(ctx context.Context, chunkID int64) (*blocks.ChunkSegment, error) {
	query := `
		SELECT block_id, offset_in_block, size_in_block
		FROM chunk_block_refs
		WHERE chunk_id = $1
		LIMIT 1
	`

	var blockID int64
	var offsetInBlock int64
	var sizeInBlock int64

	err := r.db.QueryRowContext(ctx, query, chunkID).Scan(&blockID, &offsetInBlock, &sizeInBlock)
	if err == sql.ErrNoRows {
		// Not found in v1.8 table — v1.7 fallback
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, fmt.Errorf("scan chunk_block_refs: %w", err)
	}

	if blockID <= 0 {
		return nil, fmt.Errorf("chunk_block_refs has invalid block_id %d for chunk %d", blockID, chunkID)
	}

	if offsetInBlock < 0 || sizeInBlock <= 0 {
		return nil, fmt.Errorf("chunk_block_refs has invalid offset %d or size %d for chunk %d", offsetInBlock, sizeInBlock, chunkID)
	}

	return &blocks.ChunkSegment{
		ChunkID: chunkID,
		BlockID: blockID,
		Offset:  offsetInBlock,
		Size:    sizeInBlock,
	}, nil
}

// LegacyBlockChunkResolver returns the v1.7 marker for all chunks (no DB queries).
// Used for v1.7-only repositories with no chunk_block_refs table.
type LegacyBlockChunkResolver struct{}

// ResolveChunk returns the v1.7 marker (BlockID == 0) signaling legacy direct-chunk path.
func (r *LegacyBlockChunkResolver) ResolveChunk(ctx context.Context, chunkID int64) (*blocks.ChunkSegment, error) {
	return &blocks.ChunkSegment{
		ChunkID: chunkID,
		BlockID: 0, // v1.7 marker
		Offset:  0,
		Size:    0,
	}, nil
}

// NewDualCompatRestoreService creates a restore service for mixed v1.7+v1.8 repositories.
// It uses a DualCompatChunkResolver to automatically detect and handle both layouts.
func NewDualCompatRestoreService(db *sql.DB) *RestoreService {
	return &RestoreService{
		ChunkResolver: NewDualCompatChunkResolver(db),
		BlockReader:   nil, // Will be wired in Phase 4 when write path is integrated
	}
}

// NewLegacyRestoreService creates a restore service for v1.7-only repositories.
// It uses LegacyBlockChunkResolver which always returns v1.7 markers.
func NewLegacyRestoreService() *RestoreService {
	return &RestoreService{
		ChunkResolver: &LegacyBlockChunkResolver{},
		BlockReader:   nil,
	}
}
