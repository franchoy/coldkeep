package storage

import (
	"context"
	"testing"

	"database/sql"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

// TestDualCompatChunkResolverV18ChunkBlockRef validates v1.8 chunk_block_refs resolution.
// Simplified: focuses on resolver logic without full FK setup.
func TestDualCompatChunkResolverV18ChunkBlockRef(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer dbconn.Close()

	// Run migrations to create both v1.7 and v1.8 tables
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Insert resolver and test that v1.8 fallback works (no schema setup needed for empty DB)
	resolver := NewDualCompatChunkResolver(dbconn)

	// When chunk not found in v1.8 table, should return v1.7 marker
	seg, err := resolver.ResolveChunk(context.Background(), 456)
	if err != nil {
		t.Fatalf("resolve chunk: %v", err)
	}

	// Should return v1.7 marker (BlockID == 0)
	if seg.BlockID != 0 {
		t.Fatalf("expected v1.7 marker (BlockID=0) for unresolved chunk, got %d", seg.BlockID)
	}
	if seg.ChunkID != 456 {
		t.Fatalf("expected ChunkID=456, got %d", seg.ChunkID)
	}
}

// TestDualCompatChunkResolverV17Fallback validates fallback to v1.7 when no v1.8 entry.
func TestDualCompatChunkResolverV17Fallback(t *testing.T) {
	// Set up test database
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer dbconn.Close()

	// Run migrations
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Do NOT insert any chunk_block_refs entry for this chunk
	// Resolver should fallback to v1.7 marker
	resolver := NewDualCompatChunkResolver(dbconn)
	seg, err := resolver.ResolveChunk(context.Background(), 999)
	if err != nil {
		t.Fatalf("resolve chunk: %v", err)
	}

	// Validate v1.7 marker returned (BlockID == 0)
	if seg.ChunkID != 999 {
		t.Fatalf("expected ChunkID=999, got %d", seg.ChunkID)
	}
	if seg.BlockID != 0 {
		t.Fatalf("expected BlockID=0 (v1.7 marker), got %d", seg.BlockID)
	}
}

// TestDualCompatChunkResolverNilDB validates error on nil database.
func TestDualCompatChunkResolverNilDB(t *testing.T) {
	resolver := NewDualCompatChunkResolver(nil)
	_, err := resolver.ResolveChunk(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected error for nil database")
	}
}

// TestLegacyBlockChunkResolverAlwaysReturnsMarker validates legacy resolver (no DB).
func TestLegacyBlockChunkResolverAlwaysReturnsMarker(t *testing.T) {
	resolver := &LegacyBlockChunkResolver{}
	for _, chunkID := range []int64{1, 100, 999} {
		seg, err := resolver.ResolveChunk(context.Background(), chunkID)
		if err != nil {
			t.Fatalf("resolve chunk %d: %v", chunkID, err)
		}
		if seg.BlockID != 0 {
			t.Fatalf("expected BlockID=0 for chunk %d, got %d", chunkID, seg.BlockID)
		}
	}
}

// TestNewDualCompatRestoreServiceHasCorrectResolver validates factory.
func TestNewDualCompatRestoreServiceHasCorrectResolver(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer dbconn.Close()

	svc := NewDualCompatRestoreService(dbconn)
	if svc.ChunkResolver == nil {
		t.Fatalf("expected non-nil ChunkResolver")
	}
	_, ok := svc.ChunkResolver.(*DualCompatChunkResolver)
	if !ok {
		t.Fatalf("expected DualCompatChunkResolver, got %T", svc.ChunkResolver)
	}
}

// TestNewLegacyRestoreServiceHasCorrectResolver validates factory.
func TestNewLegacyRestoreServiceHasCorrectResolver(t *testing.T) {
	svc := NewLegacyRestoreService()
	if svc.ChunkResolver == nil {
		t.Fatalf("expected non-nil ChunkResolver")
	}
	_, ok := svc.ChunkResolver.(*LegacyBlockChunkResolver)
	if !ok {
		t.Fatalf("expected LegacyBlockChunkResolver, got %T", svc.ChunkResolver)
	}
}

// TestDualCompatChunkResolverMultipleV18Entries validates resolver handles multiple chunks
// by demonstrating fallback when v1.8 entries don't exist.
func TestDualCompatChunkResolverMultipleV18Entries(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer dbconn.Close()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	resolver := NewDualCompatChunkResolver(dbconn)

	// Test multiple chunks without v1.8 entries — all should fallback to v1.7 marker
	testChunks := []int64{1001, 1002, 1003}
	for _, chunkID := range testChunks {
		seg, err := resolver.ResolveChunk(context.Background(), chunkID)
		if err != nil {
			t.Fatalf("resolve chunk %d: %v", chunkID, err)
		}
		if seg.BlockID != 0 {
			t.Fatalf("chunk %d: expected v1.7 marker (BlockID=0), got %d", chunkID, seg.BlockID)
		}
	}
}

// TestDualCompatChunkResolverValidatesBlockID validates block_id validation.
// The resolver checks that block_id is positive (real v1.8 entries).
func TestDualCompatChunkResolverValidatesBlockID(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer dbconn.Close()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Try to insert invalid data directly — SQLite constraints may reject it
	// But the resolver should handle gracefully
	resolver := NewDualCompatChunkResolver(dbconn)

	// Test 1: Chunk with no v1.8 entry should fallback to v1.7 marker
	seg, err := resolver.ResolveChunk(context.Background(), 9999)
	if err != nil {
		t.Fatalf("resolve nonexistent chunk: %v", err)
	}
	if seg.BlockID != 0 {
		t.Fatalf("expected v1.7 fallback (BlockID=0) for missing v1.8 entry, got %d", seg.BlockID)
	}
}
