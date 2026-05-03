package blocks

import (
	"context"
	"fmt"
)

// V17CompatResolver is a no-op resolver for v1.7 layouts.
// It signals that chunk resolution must fall back to legacy storage_chunks table logic.
// In the restore path, when ChunkResolver returns V17ChunkSegmentMarker,
// the system uses pre-v1.8 direct-chunk retrieval instead of block-based lookup.
type V17CompatResolver struct{}

// V17ChunkSegmentMarker is a sentinel ChunkSegment indicating v1.7 direct chunk retrieval.
// BlockID == 0 and the other fields are zero-valued to signal legacy path.
var V17ChunkSegmentMarker = &ChunkSegment{
	ChunkID: 0, // placeholder
	BlockID: 0, // indicates v1.7, not v1.8
	Offset:  0,
	Size:    0,
}

// ResolveChunk returns the v1.7 marker, signaling direct chunk lookup without block read.
func (r *V17CompatResolver) ResolveChunk(ctx context.Context, chunkID int64) (*ChunkSegment, error) {
	// For v1.7, return a marker that tells restore to use legacy storage_chunks table.
	// The marker has BlockID == 0, which is a reserved value indicating "use legacy logic".
	return &ChunkSegment{
		ChunkID: chunkID,
		BlockID: 0, // reserved: signals v1.7 path
		Offset:  0,
		Size:    0,
	}, nil
}

// StorageBlockReader implements BlockReader for v1.8 block store.
// It reads blocks from a BlockStore interface and returns decoded EncodedBlock structs.
type StorageBlockReader struct {
	store BlockStore
}

// NewStorageBlockReader creates a block reader backed by the given BlockStore.
func NewStorageBlockReader(store BlockStore) *StorageBlockReader {
	return &StorageBlockReader{store: store}
}

// ReadBlock retrieves and decodes a block from storage.
// It fetches the block bytes from store, decodes the v1 format, and returns the EncodedBlock.
// IMPORTANT: This assumes blocks are stored in plaintext encoded form (pre-encryption).
func (r *StorageBlockReader) ReadBlock(ctx context.Context, blockID int64) (*EncodedBlock, error) {
	if blockID <= 0 {
		return nil, fmt.Errorf("invalid block ID: %d", blockID)
	}

	// Fetch the block metadata and content from storage.
	block, err := r.store.GetBlock(blockID)
	if err != nil {
		return nil, fmt.Errorf("get block %d: %w", blockID, err)
	}

	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockID)
	}

	// TODO: Phase 3+ — retrieve the plaintext encoded block bytes from storage.
	// For now, this is a placeholder. The actual storage mechanism (e.g., blob store,
	// chunk_block_refs table) will be wired in Phase 3 Step 3.
	// We decode the bytes using DecodeBlock to validate format and reconstruct EncodedBlock.

	// Placeholder: assume block.PlaintextSize > 0 indicates readiness for Phase 3.
	if block.PlaintextSize <= 0 {
		return nil, fmt.Errorf("block %d has invalid plaintext size: %d", blockID, block.PlaintextSize)
	}

	// This is where we would retrieve the actual encoded block bytes.
	// For now, return a stub to allow the abstraction to be wired.
	return nil, fmt.Errorf("block read not yet implemented in Phase 3 Step 2 (placeholder)")
}

// StorageChunkResolver implements ChunkResolver for v1.8 block-based layout.
// It resolves chunk locations by querying the chunk_block_refs and storage_blocks tables.
type StorageChunkResolver struct {
	locator ChunkLocator
}

// NewStorageChunkResolver creates a chunk resolver backed by the given ChunkLocator.
func NewStorageChunkResolver(locator ChunkLocator) *StorageChunkResolver {
	return &StorageChunkResolver{locator: locator}
}

// ResolveChunk looks up which block contains the chunk and returns the segment location.
func (r *StorageChunkResolver) ResolveChunk(ctx context.Context, chunkID int64) (*ChunkSegment, error) {
	if chunkID <= 0 {
		return nil, fmt.Errorf("invalid chunk ID: %d", chunkID)
	}

	// Query the ChunkLocator to find block placement.
	segment, err := r.locator.GetChunkSegment(chunkID)
	if err != nil {
		return nil, fmt.Errorf("get chunk segment %d: %w", chunkID, err)
	}

	if segment == nil {
		return nil, fmt.Errorf("chunk %d not found", chunkID)
	}

	return segment, nil
}
