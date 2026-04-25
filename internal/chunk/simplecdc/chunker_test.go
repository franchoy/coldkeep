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

	if got := len(chunks[0].Data); got != MaxChunkSize {
		t.Fatalf("chunk 0 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := chunks[0].Info.Offset; got != 0 {
		t.Fatalf("chunk 0 offset mismatch: got %d want %d", got, 0)
	}
	if got := len(chunks[1].Data); got != MaxChunkSize {
		t.Fatalf("chunk 1 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := chunks[1].Info.Offset; got != MaxChunkSize {
		t.Fatalf("chunk 1 offset mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := len(chunks[2].Data); got != MaxChunkSize {
		t.Fatalf("chunk 2 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := chunks[2].Info.Offset; got != MaxChunkSize*2 {
		t.Fatalf("chunk 2 offset mismatch: got %d want %d", got, MaxChunkSize*2)
	}
	if got := len(chunks[3].Data); got != 7 {
		t.Fatalf("chunk 3 size mismatch: got %d want %d", got, 7)
	}
	if got := chunks[3].Info.Offset; got != MaxChunkSize*3 {
		t.Fatalf("chunk 3 offset mismatch: got %d want %d", got, MaxChunkSize*3)
	}
}
