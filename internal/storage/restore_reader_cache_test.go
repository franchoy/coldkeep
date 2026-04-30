package storage

import (
	"context"
	"testing"
)

func TestRestoreReaderCacheInitialization(t *testing.T) {
	const fileID = 42
	cache := newRestoreReaderCache(fileID)

	if cache == nil {
		t.Fatal("cache should not be nil")
	}
	if cache.fileID != fileID {
		t.Errorf("cache.fileID: expected %d, got %d", fileID, cache.fileID)
	}
	if len(cache.readers) != 0 {
		t.Errorf("cache.readers should be empty on init, got %d", len(cache.readers))
	}
}

func TestRestoreReaderCacheHandlesOpenError(t *testing.T) {
	cache := newRestoreReaderCache(1)
	ctx := context.Background()

	// Try to open non-existent container
	reader, err := cache.GetReader(ctx, "/nonexistent/container.bin", 512)
	if err == nil {
		t.Fatal("expected error for non-existent container, got nil")
	}
	if reader != nil {
		t.Fatal("reader should be nil on error")
	}

	// Verify the failed container is not cached
	if len(cache.readers) != 0 {
		t.Errorf("cache should not cache failed opens, got %d readers", len(cache.readers))
	}

	if err := cache.Close(); err != nil {
		t.Logf("cache.Close: %v", err)
	}
}

func TestRestoreReaderCacheCloseState(t *testing.T) {
	cache := newRestoreReaderCache(1)

	if len(cache.readers) != 0 {
		t.Errorf("cache should start empty, got %d readers", len(cache.readers))
	}

	// Close empty cache should be safe
	if err := cache.Close(); err != nil {
		t.Logf("cache.Close on empty cache: %v", err)
	}

	if len(cache.readers) != 0 {
		t.Errorf("cache should be empty after close, got %d readers", len(cache.readers))
	}
}
