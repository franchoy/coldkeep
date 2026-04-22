package chunk

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDefaultChunkerVersion(t *testing.T) {
	if got := DefaultChunker().Version(); got != string(VersionV1SimpleRolling) {
		t.Fatalf("default chunker version mismatch: got %q want %q", got, VersionV1SimpleRolling)
	}
}

func TestChunkFileMatchesDefaultChunker(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "input.bin")

	data := bytes.Repeat([]byte{255}, MaxChunkSize*2+123)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	chunksViaCompatibility, err := ChunkFile(path)
	if err != nil {
		t.Fatalf("chunk via ChunkFile: %v", err)
	}

	chunksViaDefault, err := DefaultChunker().ChunkFile(path)
	if err != nil {
		t.Fatalf("chunk via DefaultChunker: %v", err)
	}

	if !reflect.DeepEqual(chunksViaCompatibility, chunksViaDefault) {
		t.Fatal("ChunkFile output diverged from default chunker output")
	}
}
