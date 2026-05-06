package storage

import (
	"context"
	"fmt"
	"log"

	"github.com/franchoy/coldkeep/internal/blocks"
)

// RestoreService provides restore operations with pluggable chunk resolution.
// It handles both v1.7 (direct legacy chunks) and v1.8 (packed block-based) layouts.
type RestoreService struct {
	// ChunkResolver determines where restore finds chunks (block-based or legacy).
	// For v1.7 compatibility, returns a marker with BlockID == 0.
	// For v1.8 reads, returns ChunkSegment with actual BlockID, Offset, Size.
	ChunkResolver blocks.ChunkResolver

	// BlockReader decodes blocks for v1.8 block-based reads.
	// Only invoked when ChunkResolver returns a non-zero BlockID.
	BlockReader blocks.BlockReader
}

// NewV17CompatRestoreService creates a restore service for v1.7-only layouts.
// ChunkResolver returns the v1.7 marker; BlockReader is unused.
func NewV17CompatRestoreService() *RestoreService {
	return &RestoreService{
		ChunkResolver: &blocks.V17CompatResolver{},
		BlockReader:   nil, // not used in v1.7 mode
	}
}

// ResolveChunkLocation returns the physical location of a chunk.
// For v1.7 (BlockID == 0), the caller uses legacy storage_chunks table.
// For v1.8 (BlockID > 0), the caller fetches the block and slices the chunk.
func (s *RestoreService) ResolveChunkLocation(ctx context.Context, chunkID int64) (*blocks.ChunkSegment, error) {
	if s.ChunkResolver == nil {
		return nil, fmt.Errorf("chunk resolver not configured")
	}

	seg, err := s.ChunkResolver.ResolveChunk(ctx, chunkID)
	if err != nil {
		return nil, fmt.Errorf("resolve chunk %d: %w", chunkID, err)
	}

	if seg == nil {
		return nil, fmt.Errorf("chunk %d not found", chunkID)
	}

	return seg, nil
}

// ReadChunkFromBlock fetches a chunk payload from a v1.8 packed block using BlockReader.
// Called when ChunkSegment.BlockID > 0 (v1.8 block-based layout).
func (s *RestoreService) ReadChunkFromBlock(ctx context.Context, blockID int64, offset, size int64) ([]byte, error) {
	if s.BlockReader == nil {
		return nil, fmt.Errorf("block reader not configured (required for v1.8 reads)")
	}

	if blockID <= 0 {
		return nil, fmt.Errorf("invalid block ID for v1.8 read: %d", blockID)
	}

	block, err := s.BlockReader.ReadBlock(ctx, blockID)
	if err != nil {
		return nil, fmt.Errorf("read block %d: %w", blockID, err)
	}

	if block == nil {
		return nil, fmt.Errorf("block %d returned nil", blockID)
	}

	// Slice the chunk from the block payload
	chunk, err := blocks.SliceChunkFromPayload(block.Payload, blocks.ChunkEntry{
		Offset: uint64(offset),
		Size:   uint64(size),
	})
	if err != nil {
		return nil, fmt.Errorf("slice chunk from block %d: %w", blockID, err)
	}

	return chunk, nil
}

// InspectChunkResolution logs the resolution path for debugging Phase 3 migrations.
func (s *RestoreService) InspectChunkResolution(ctx context.Context, chunkID int64) {
	seg, err := s.ResolveChunkLocation(ctx, chunkID)
	if err != nil {
		log.Printf("event=chunk_resolution_error action=inspect chunk_id=%d err=%v", chunkID, err)
		return
	}

	if seg.BlockID == 0 {
		log.Printf("event=chunk_resolution_v17 action=inspect chunk_id=%d path=legacy_storage_chunks", chunkID)
	} else {
		log.Printf("event=chunk_resolution_v18 action=inspect chunk_id=%d block_id=%d offset=%d size=%d", chunkID, seg.BlockID, seg.Offset, seg.Size)
	}
}
