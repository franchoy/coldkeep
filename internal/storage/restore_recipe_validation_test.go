package storage

import (
	"testing"
)

// TestValidateRestoreRecipeOrderingEmptyRecipe verifies that empty recipes are valid
func TestValidateRestoreRecipeOrderingEmptyRecipe(t *testing.T) {
	recipe := restoreRecipe{
		LogicalFileID:  1,
		OriginalName:   "empty.txt",
		ExpectedHash:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Chunks:         []restoreChunk{},
		PinnedChunkIDs: []int64{},
	}

	if err := validateRestoreRecipeOrdering(&recipe); err != nil {
		t.Fatalf("empty recipe should be valid, got error: %v", err)
	}
}

// TestValidateRestoreRecipeOrderingValidContiguous verifies that properly ordered chunks pass
func TestValidateRestoreRecipeOrderingValidContiguous(t *testing.T) {
	recipe := restoreRecipe{
		LogicalFileID: 1,
		OriginalName:  "file.bin",
		ExpectedHash:  "abcd1234",
		Chunks: []restoreChunk{
			{Index: 0, ID: 100, Hash: "hash0", PlaintextSize: 100},
			{Index: 1, ID: 101, Hash: "hash1", PlaintextSize: 100},
			{Index: 2, ID: 102, Hash: "hash2", PlaintextSize: 100},
		},
		PinnedChunkIDs: []int64{100, 101, 102},
	}

	if err := validateRestoreRecipeOrdering(&recipe); err != nil {
		t.Fatalf("valid contiguous recipe should pass validation, got error: %v", err)
	}
}

// TestValidateRestoreRecipeOrderingMissingChunk verifies that gaps are detected
func TestValidateRestoreRecipeOrderingMissingChunk(t *testing.T) {
	recipe := restoreRecipe{
		LogicalFileID: 1,
		OriginalName:  "file.bin",
		ExpectedHash:  "abcd1234",
		Chunks: []restoreChunk{
			{Index: 0, ID: 100, Hash: "hash0"},
			{Index: 2, ID: 102, Hash: "hash2"}, // index 1 is missing!
		},
		PinnedChunkIDs: []int64{100, 102},
	}

	if err := validateRestoreRecipeOrdering(&recipe); err == nil {
		t.Fatalf("recipe with gap should fail validation")
	}
}

// TestValidateRestoreRecipeOrderingWrongIndex verifies that incorrect indices are detected
func TestValidateRestoreRecipeOrderingWrongIndex(t *testing.T) {
	recipe := restoreRecipe{
		LogicalFileID: 1,
		OriginalName:  "file.bin",
		ExpectedHash:  "abcd1234",
		Chunks: []restoreChunk{
			{Index: 0, ID: 100, Hash: "hash0"},
			{Index: 5, ID: 101, Hash: "hash1"}, // wrong index!
			{Index: 2, ID: 102, Hash: "hash2"},
		},
		PinnedChunkIDs: []int64{100, 101, 102},
	}

	if err := validateRestoreRecipeOrdering(&recipe); err == nil {
		t.Fatalf("recipe with wrong index should fail validation")
	}
}

// TestValidateRestoreRecipeOrderingNegativeIndex verifies that negative indices are caught
func TestValidateRestoreRecipeOrderingNegativeIndex(t *testing.T) {
	recipe := restoreRecipe{
		LogicalFileID: 1,
		OriginalName:  "file.bin",
		ExpectedHash:  "abcd1234",
		Chunks: []restoreChunk{
			{Index: -1, ID: 100, Hash: "hash0"}, // negative index!
		},
		PinnedChunkIDs: []int64{100},
	}

	if err := validateRestoreRecipeOrdering(&recipe); err == nil {
		t.Fatalf("recipe with negative index should fail validation")
	}
}

// TestValidateRestoreRecipeOrderingMismatchedChunkAndPinnedCounts verifies mismatch detection
func TestValidateRestoreRecipeOrderingMismatchedCounts(t *testing.T) {
	recipe := restoreRecipe{
		LogicalFileID: 1,
		OriginalName:  "file.bin",
		ExpectedHash:  "abcd1234",
		Chunks: []restoreChunk{
			{Index: 0, ID: 100, Hash: "hash0"},
			{Index: 1, ID: 101, Hash: "hash1"},
		},
		PinnedChunkIDs: []int64{100}, // only 1 pinned, but 2 chunks!
	}

	if err := validateRestoreRecipeOrdering(&recipe); err == nil {
		t.Fatalf("recipe with mismatched chunk/pinned counts should fail validation")
	}
}

// TestValidateRestoreRecipeOrderingSingleChunk verifies single-chunk recipes work
func TestValidateRestoreRecipeOrderingSingleChunk(t *testing.T) {
	recipe := restoreRecipe{
		LogicalFileID: 1,
		OriginalName:  "single.bin",
		ExpectedHash:  "abcd1234",
		Chunks: []restoreChunk{
			{Index: 0, ID: 100, Hash: "hash0", PlaintextSize: 1000},
		},
		PinnedChunkIDs: []int64{100},
	}

	if err := validateRestoreRecipeOrdering(&recipe); err != nil {
		t.Fatalf("single-chunk recipe should be valid, got error: %v", err)
	}
}

// TestBuildRestoreRecipeCreatesValidOrdering verifies builder creates correctly ordered recipes
func TestBuildRestoreRecipeCreatesValidOrdering(t *testing.T) {
	chunkRows := []restoreChunkRow{
		{
			chunkOrder:        0,
			chunkID:           100,
			expectedChunkHash: "hash0",
			plaintextSize:     100,
		},
		{
			chunkOrder:        1,
			chunkID:           101,
			expectedChunkHash: "hash1",
			plaintextSize:     100,
		},
		{
			chunkOrder:        2,
			chunkID:           102,
			expectedChunkHash: "hash2",
			plaintextSize:     100,
		},
	}

	pinnedChunkIDs := []int64{100, 101, 102}
	recipe := buildRestoreRecipe(1, "file.bin", "expectedhash", 300, chunkRows, pinnedChunkIDs)

	// Validate the recipe is well-formed
	if err := validateRestoreRecipeOrdering(&recipe); err != nil {
		t.Fatalf("built recipe should have valid ordering: %v", err)
	}

	// Verify recipe size
	if len(recipe.Chunks) != 3 {
		t.Fatalf("recipe should have 3 chunks, got %d", len(recipe.Chunks))
	}

	// Verify semantic mapping
	for i, chunk := range recipe.Chunks {
		if chunk.Index != int64(i) {
			t.Fatalf("chunk %d has wrong index %d", i, chunk.Index)
		}
	}
}
