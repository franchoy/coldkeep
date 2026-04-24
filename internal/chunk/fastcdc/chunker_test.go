package fastcdc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk/shared"
)

func writeFixture(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
	return path
}

func runChunker(t *testing.T, name string, data []byte) []shared.Result {
	t.Helper()
	path := writeFixture(t, name, data)
	results, err := New().ChunkFile(path)
	if err != nil {
		t.Fatalf("ChunkFile(%s): %v", name, err)
	}
	return results
}

func chunkHashes(results []shared.Result) []string {
	hashes := make([]string, len(results))
	for i, r := range results {
		sum := sha256.Sum256(r.Data)
		hashes[i] = hex.EncodeToString(sum[:])
	}
	return hashes
}

func assertInvariants(t *testing.T, src []byte, results []shared.Result) {
	t.Helper()

	var cursor int64
	for i, r := range results {
		if r.Info.Offset != cursor {
			t.Fatalf("chunk %d offset=%d want=%d", i, r.Info.Offset, cursor)
		}
		if r.Info.Size != int64(len(r.Data)) {
			t.Fatalf("chunk %d size metadata mismatch: info=%d len=%d", i, r.Info.Size, len(r.Data))
		}
		if int(r.Info.Size) > MaxChunkSize {
			t.Fatalf("chunk %d exceeds MaxChunkSize: %d > %d", i, r.Info.Size, MaxChunkSize)
		}
		if i < len(results)-1 && int(r.Info.Size) < MinChunkSize {
			t.Fatalf("interior chunk %d below MinChunkSize: %d < %d", i, r.Info.Size, MinChunkSize)
		}
		cursor += r.Info.Size
	}

	reassembled := make([]byte, 0, len(src))
	for _, r := range results {
		reassembled = append(reassembled, r.Data...)
	}
	if !bytes.Equal(reassembled, src) {
		t.Fatalf("reassembled bytes mismatch: got=%d want=%d", len(reassembled), len(src))
	}
	if cursor != int64(len(src)) {
		t.Fatalf("sum(chunk sizes)=%d want=%d", cursor, len(src))
	}
}

func TestChunkerVersionIdentity(t *testing.T) {
	if got := New().Version(); got != Version {
		t.Fatalf("Version()=%q want=%q", got, Version)
	}
}

func TestEmptyFileReturnsNoChunks(t *testing.T) {
	results := runChunker(t, "empty.bin", []byte{})
	if len(results) != 0 {
		t.Fatalf("expected 0 chunks for empty input, got %d", len(results))
	}
}

func TestDeterministicChunkBoundariesAndData(t *testing.T) {
	src := bytes.Repeat([]byte("fastcdc-phase5-pattern-"), 180000) // ~3.78 MiB

	r1 := runChunker(t, "run1.bin", src)
	r2 := runChunker(t, "run2.bin", src)

	if len(r1) == 0 {
		t.Fatal("expected non-empty chunk set")
	}
	if len(r1) != len(r2) {
		t.Fatalf("chunk count mismatch across runs: %d vs %d", len(r1), len(r2))
	}

	h1 := chunkHashes(r1)
	h2 := chunkHashes(r2)
	for i := range h1 {
		if h1[i] != h2[i] {
			t.Fatalf("chunk %d hash mismatch across runs: %s vs %s", i, h1[i], h2[i])
		}
	}

	assertInvariants(t, src, r1)
	assertInvariants(t, src, r2)
}

func TestMaxChunkCapEnforced(t *testing.T) {
	src := bytes.Repeat([]byte{0xFF}, MaxChunkSize*2+17)
	results := runChunker(t, "max-cap.bin", src)
	if len(results) < 2 {
		t.Fatalf("expected at least 2 chunks for >Max input, got %d", len(results))
	}
	for i, r := range results {
		if int(r.Info.Size) > MaxChunkSize {
			t.Fatalf("chunk %d size=%d exceeds MaxChunkSize=%d", i, r.Info.Size, MaxChunkSize)
		}
	}
	assertInvariants(t, src, results)
}

func TestShouldCutZoneBehavior(t *testing.T) {
	// 0 -> min: never cut
	if shouldCut(MinChunkSize-1, 0) {
		t.Fatal("expected no cut below MinChunkSize")
	}

	// min -> avg: strict mask gate
	if shouldCut(MinChunkSize, 1) {
		t.Fatal("expected strict-zone non-cut for non-matching fingerprint")
	}
	if !shouldCut(MinChunkSize, 0) {
		t.Fatal("expected strict-zone cut when strictMask matches")
	}

	// avg -> max: normal mask gate
	if shouldCut(AvgChunkSize, 1) {
		t.Fatal("expected normal-zone non-cut for non-matching fingerprint")
	}
	if !shouldCut(AvgChunkSize, 0) {
		t.Fatal("expected normal-zone cut when normalMask matches")
	}

	// max: force cut
	if !shouldCut(MaxChunkSize, 1) {
		t.Fatal("expected force cut at MaxChunkSize")
	}
}
