package storage

import (
	"context"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
)

// TestPhase3ReadPathV17CompatibilityPreserved validates that v1.7 restore paths remain unchanged.
// This test ensures backward compatibility during Phase 3 introduction.
func TestPhase3ReadPathV17CompatibilityPreserved(t *testing.T) {
	// Create a v1.7 compat service
	svc := NewV17CompatRestoreService()

	// Resolve a chunk - should return v1.7 marker
	seg, err := svc.ResolveChunkLocation(context.Background(), 42)
	if err != nil {
		t.Fatalf("resolve chunk: %v", err)
	}

	// BlockID == 0 signals "use legacy v1.7 direct-chunk path"
	if seg.BlockID != 0 {
		t.Fatalf("v1.7 compat resolver must return BlockID=0, got %d", seg.BlockID)
	}

	// ChunkID must be preserved for legacy lookup
	if seg.ChunkID != 42 {
		t.Fatalf("v1.7 compat resolver must preserve ChunkID, expected 42 got %d", seg.ChunkID)
	}

	// Attempting to read from block should fail (BlockReader is nil for v1.7)
	_, err = svc.ReadChunkFromBlock(context.Background(), 1, 0, 100)
	if err == nil {
		t.Fatalf("v1.7 service should not support block reads")
	}
}

// TestPhase3ReadPathV18BlockReading validates that v1.8 block reads work correctly.
// This test demonstrates the new block-based chunk resolution introducing in Phase 3.
func TestPhase3ReadPathV18BlockReading(t *testing.T) {
	// Create a mock block with one chunk
	mockBlock := &blocks.EncodedBlock{
		Header: blocks.BlockHeader{
			Magic:         blocks.BlockMagicV1,
			Version:       blocks.BlockFormatVersionV1,
			Codec:         blocks.BlockCodecNoneV1,
			ChunkCount:    1,
			PlaintextSize: 25,
		},
		Entries: []blocks.ChunkEntry{
			{ChunkID: 999, Offset: 0, Size: 25},
		},
		Payload: []byte("HelloWorld1234567890ABCDE"),
	}

	// Create a mock resolver that returns v1.8 segment
	mockResolver := &mockChunkResolverV18{
		segment: &blocks.ChunkSegment{
			ChunkID: 999,
			BlockID: 7, // v1.8: non-zero block ID
			Offset:  0,
			Size:    25,
		},
	}

	// Create a mock reader that returns the block
	mockReader := &mockBlockReaderV18{
		blocks: map[int64]*blocks.EncodedBlock{
			7: mockBlock,
		},
	}

	// Create v1.8 service
	svc := &RestoreService{
		ChunkResolver: mockResolver,
		BlockReader:   mockReader,
	}

	// Resolve chunk location - should return v1.8 segment
	seg, err := svc.ResolveChunkLocation(context.Background(), 999)
	if err != nil {
		t.Fatalf("resolve chunk: %v", err)
	}
	if seg.BlockID != 7 {
		t.Fatalf("v1.8 resolver must return non-zero BlockID, got %d", seg.BlockID)
	}

	// Read chunk from block
	chunk, err := svc.ReadChunkFromBlock(context.Background(), seg.BlockID, seg.Offset, seg.Size)
	if err != nil {
		t.Fatalf("read chunk from block: %v", err)
	}

	// Validate chunk content
	expected := "HelloWorld1234567890ABCDE"
	if string(chunk) != expected {
		t.Fatalf("chunk content mismatch: expected %q, got %q", expected, string(chunk))
	}
}

// TestPhase3ReadPathChunkToBlockSegmentFlow validates the full abstraction flow.
// Chunk → ChunkResolver → ChunkSegment → BlockReader → payload slice
func TestPhase3ReadPathChunkToBlockSegmentFlow(t *testing.T) {
	// Create a single chunk in a block for simplicity
	mockBlock := &blocks.EncodedBlock{
		Header: blocks.BlockHeader{
			Magic:         blocks.BlockMagicV1,
			Version:       blocks.BlockFormatVersionV1,
			Codec:         blocks.BlockCodecNoneV1,
			ChunkCount:    1,
			PlaintextSize: 20,
		},
		Entries: []blocks.ChunkEntry{
			{ChunkID: 100, Offset: 0, Size: 20},
		},
		Payload: []byte("BLOCK_CONTAINS_DATA!"),
	}

	mockResolver := &mockChunkResolverV18{
		segment: &blocks.ChunkSegment{
			ChunkID: 100,
			BlockID: 10,
			Offset:  0,
			Size:    20,
		},
	}

	mockReader := &mockBlockReaderV18{
		blocks: map[int64]*blocks.EncodedBlock{10: mockBlock},
	}

	svc := &RestoreService{
		ChunkResolver: mockResolver,
		BlockReader:   mockReader,
	}

	// Full flow: Resolve → Read
	seg, err := svc.ResolveChunkLocation(context.Background(), 100)
	if err != nil {
		t.Fatalf("resolve chunk: %v", err)
	}

	chunk, err := svc.ReadChunkFromBlock(context.Background(), seg.BlockID, seg.Offset, seg.Size)
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}

	if string(chunk) != "BLOCK_CONTAINS_DATA!" {
		t.Fatalf("chunk mismatch: expected \"BLOCK_CONTAINS_DATA!\", got %q", string(chunk))
	}
}

// Mock implementations

type mockChunkResolverV18 struct {
	segment *blocks.ChunkSegment
	err     error
}

func (r *mockChunkResolverV18) ResolveChunk(ctx context.Context, chunkID int64) (*blocks.ChunkSegment, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.segment, nil
}

type mockBlockReaderV18 struct {
	blocks map[int64]*blocks.EncodedBlock
	err    error
}

func (r *mockBlockReaderV18) ReadBlock(ctx context.Context, blockID int64) (*blocks.EncodedBlock, error) {
	if r.err != nil {
		return nil, r.err
	}
	block, ok := r.blocks[blockID]
	if !ok {
		return nil, nil
	}
	return block, nil
}
