package simplecdc

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestChunkFileSplitsAtMaxChunkSizeWhenRollingMaskNeverMatches(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "input.bin")

	data := bytes.Repeat([]byte{255}, MaxChunkSize*3+7)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	chunker := New()
	chunks, err := chunker.ChunkFile(path)
	if err != nil {
		t.Fatalf("chunk file: %v", err)
	}

	if len(chunks) != 4 {
		t.Fatalf("chunk count mismatch: got %d want %d", len(chunks), 4)
	}

	if got := len(chunks[0]); got != MaxChunkSize {
		t.Fatalf("chunk 0 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := len(chunks[1]); got != MaxChunkSize {
		t.Fatalf("chunk 1 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := len(chunks[2]); got != MaxChunkSize {
		t.Fatalf("chunk 2 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := len(chunks[3]); got != 7 {
		t.Fatalf("chunk 3 size mismatch: got %d want %d", got, 7)
	}
}
