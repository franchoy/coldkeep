package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
)

func TestPrepareChunksWithContextComputesExpectedHashesDeterministically(t *testing.T) {
	results := []chunk.Result{
		{Info: chunk.Info{Offset: 0}, Data: []byte("alpha")},
		{Info: chunk.Info{Offset: 5}, Data: []byte("beta")},
		{Info: chunk.Info{Offset: 9}, Data: []byte("gamma")},
	}

	prepared1, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext first call: %v", err)
	}
	prepared2, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext second call: %v", err)
	}

	if len(prepared1) != len(results) {
		t.Fatalf("prepared length mismatch: got %d want %d", len(prepared1), len(results))
	}
	if len(prepared2) != len(results) {
		t.Fatalf("prepared(second) length mismatch: got %d want %d", len(prepared2), len(results))
	}

	for i := range results {
		sum := sha256.Sum256(results[i].Data)
		want := hex.EncodeToString(sum[:])
		if prepared1[i].Hash != want {
			t.Fatalf("hash mismatch at index %d: got %q want %q", i, prepared1[i].Hash, want)
		}
		if prepared2[i].Hash != want {
			t.Fatalf("hash mismatch on second call at index %d: got %q want %q", i, prepared2[i].Hash, want)
		}
	}
}

func TestPrepareChunksWithContextPreservesProvidedHash(t *testing.T) {
	provided := "already-computed-hash"
	results := []chunk.Result{
		{Info: chunk.Info{Offset: 0, Hash: provided}, Data: []byte("payload")},
	}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV2FastCDC))
	if err != nil {
		t.Fatalf("prepareChunksWithContext: %v", err)
	}
	if len(prepared) != 1 {
		t.Fatalf("prepared length mismatch: got %d want 1", len(prepared))
	}
	if prepared[0].Hash != provided {
		t.Fatalf("provided hash should be preserved: got %q want %q", prepared[0].Hash, provided)
	}
}
