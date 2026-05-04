package blocks

import (
	"context"
	"testing"
)

func TestV17CompatResolverReturnsMarker(t *testing.T) {
	resolver := &V17CompatResolver{}
	seg, err := resolver.ResolveChunk(context.Background(), 123)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if seg == nil {
		t.Fatalf("expected non-nil segment")
	}
	// BlockID == 0 signals v1.7 path
	if seg.BlockID != 0 {
		t.Fatalf("expected BlockID=0 (v1.7 marker), got %d", seg.BlockID)
	}
	if seg.ChunkID != 123 {
		t.Fatalf("expected ChunkID=123, got %d", seg.ChunkID)
	}
}

func TestStorageChunkResolverRejectsInvalidChunkID(t *testing.T) {
	resolver := NewStorageChunkResolver(nil)
	_, err := resolver.ResolveChunk(context.Background(), 0)
	if err == nil {
		t.Fatalf("expected error for invalid chunk ID 0")
	}
	_, err = resolver.ResolveChunk(context.Background(), -1)
	if err == nil {
		t.Fatalf("expected error for invalid chunk ID -1")
	}
}

func TestStorageChunkResolverGetSegmentError(t *testing.T) {
	// Mock ChunkLocator that returns an error
	mockLocator := &mockChunkLocator{
		errOnGet: true,
	}
	resolver := NewStorageChunkResolver(mockLocator)
	_, err := resolver.ResolveChunk(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected error from failed segment retrieval")
	}
}

func TestStorageChunkResolverSegmentNotFound(t *testing.T) {
	// Mock ChunkLocator that returns nil segment
	mockLocator := &mockChunkLocator{
		nilSegment: true,
	}
	resolver := NewStorageChunkResolver(mockLocator)
	_, err := resolver.ResolveChunk(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected error for missing segment")
	}
}

func TestStorageChunkResolverSuccess(t *testing.T) {
	// Mock ChunkLocator that returns a valid segment
	expectedSeg := &ChunkSegment{
		ChunkID: 99,
		BlockID: 7,
		Offset:  100,
		Size:    256,
	}
	mockLocator := &mockChunkLocator{
		segment: expectedSeg,
	}
	resolver := NewStorageChunkResolver(mockLocator)
	seg, err := resolver.ResolveChunk(context.Background(), 99)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if seg.BlockID != 7 || seg.Offset != 100 || seg.Size != 256 {
		t.Fatalf("expected segment {BlockID: 7, Offset: 100, Size: 256}, got %+v", seg)
	}
}

// Mock implementations for testing

type mockChunkLocator struct {
	errOnGet   bool
	nilSegment bool
	segment    *ChunkSegment
}

func (m *mockChunkLocator) GetChunkSegment(chunkID int64) (*ChunkSegment, error) {
	if m.errOnGet {
		return nil, ErrBlockFormatInvalidLayout // any error will do
	}
	if m.nilSegment {
		return nil, nil
	}
	if m.segment != nil {
		return m.segment, nil
	}
	return &ChunkSegment{
		ChunkID: chunkID,
		BlockID: 1,
		Offset:  0,
		Size:    64,
	}, nil
}
