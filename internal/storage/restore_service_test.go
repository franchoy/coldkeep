package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
)

func TestNewV17CompatRestoreServiceHasV17Resolver(t *testing.T) {
	svc := NewV17CompatRestoreService()
	if svc.ChunkResolver == nil {
		t.Fatalf("expected non-nil ChunkResolver")
	}
	_, ok := svc.ChunkResolver.(*blocks.V17CompatResolver)
	if !ok {
		t.Fatalf("expected V17CompatResolver, got %T", svc.ChunkResolver)
	}
}

func TestRestoreServiceResolveChunkLocationV17(t *testing.T) {
	svc := NewV17CompatRestoreService()
	seg, err := svc.ResolveChunkLocation(context.Background(), 123)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if seg == nil {
		t.Fatalf("expected non-nil segment")
	}
	// v1.7 markers have BlockID == 0
	if seg.BlockID != 0 {
		t.Fatalf("expected BlockID=0 (v1.7), got %d", seg.BlockID)
	}
}

func TestRestoreServiceResolveChunkLocationNilResolver(t *testing.T) {
	svc := &RestoreService{ChunkResolver: nil}
	_, err := svc.ResolveChunkLocation(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected error for nil resolver")
	}
}

func TestRestoreServiceResolveChunkLocationError(t *testing.T) {
	failingResolver := &failingChunkResolver{
		err: errors.New("resolver error"),
	}
	svc := &RestoreService{ChunkResolver: failingResolver}
	_, err := svc.ResolveChunkLocation(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected resolver error to propagate")
	}
}

func TestRestoreServiceReadChunkFromBlockNilReader(t *testing.T) {
	svc := &RestoreService{BlockReader: nil}
	_, err := svc.ReadChunkFromBlock(context.Background(), 1, 0, 100)
	if err == nil {
		t.Fatalf("expected error for nil block reader")
	}
}

func TestRestoreServiceReadChunkFromBlockInvalidBlockID(t *testing.T) {
	svc := &RestoreService{BlockReader: &mockBlockReader{}}
	for _, bid := range []int64{0, -1} {
		_, err := svc.ReadChunkFromBlock(context.Background(), bid, 0, 100)
		if err == nil {
			t.Fatalf("expected error for invalid block ID %d", bid)
		}
	}
}

func TestRestoreServiceReadChunkFromBlockSuccess(t *testing.T) {
	mockReader := &mockBlockReader{
		block: &blocks.EncodedBlock{
			Header: blocks.BlockHeader{
				Magic:         blocks.BlockMagicV1,
				Version:       blocks.BlockFormatVersionV1,
				Codec:         blocks.BlockCodecNoneV1,
				ChunkCount:    1,
				PlaintextSize: 10,
			},
			Entries: []blocks.ChunkEntry{{ChunkID: 1, Offset: 0, Size: 10}},
			Payload: []byte("0123456789"),
		},
	}
	svc := &RestoreService{BlockReader: mockReader}
	chunk, err := svc.ReadChunkFromBlock(context.Background(), 5, 0, 10)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if string(chunk) != "0123456789" {
		t.Fatalf("expected \"0123456789\", got %q", string(chunk))
	}
}

func TestRestoreServiceInspectChunkResolutionV17(t *testing.T) {
	svc := NewV17CompatRestoreService()
	// Should not panic
	svc.InspectChunkResolution(context.Background(), 1)
}

// Mock implementations for testing

type failingChunkResolver struct {
	err error
}

func (r *failingChunkResolver) ResolveChunk(ctx context.Context, chunkID int64) (*blocks.ChunkSegment, error) {
	return nil, r.err
}

type mockBlockReader struct {
	block *blocks.EncodedBlock
	err   error
}

func (r *mockBlockReader) ReadBlock(ctx context.Context, blockID int64) (*blocks.EncodedBlock, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.block, nil
}
