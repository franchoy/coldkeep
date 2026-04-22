package chunk

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDefaultChunkerVersion(t *testing.T) {
	if got := DefaultChunker().Version(); got != VersionV1SimpleRolling {
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
	payloadsViaDefault := make([][]byte, 0, len(chunksViaDefault))
	for _, result := range chunksViaDefault {
		payloadsViaDefault = append(payloadsViaDefault, result.Data)
	}

	if !reflect.DeepEqual(chunksViaCompatibility, payloadsViaDefault) {
		t.Fatal("ChunkFile output diverged from default chunker output")
	}
}

func TestDefaultChunkerReturnsStableMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "input.bin")

	data := bytes.Repeat([]byte{255}, MaxChunkSize*2+123)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	results, err := DefaultChunker().ChunkFile(path)
	if err != nil {
		t.Fatalf("chunk via DefaultChunker: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("chunk count mismatch: got %d want %d", len(results), 3)
	}

	if got := results[0].Info.Offset; got != 0 {
		t.Fatalf("chunk 0 offset mismatch: got %d want %d", got, 0)
	}
	if got := results[0].Info.Size; got != MaxChunkSize {
		t.Fatalf("chunk 0 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := results[1].Info.Offset; got != MaxChunkSize {
		t.Fatalf("chunk 1 offset mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := results[1].Info.Size; got != MaxChunkSize {
		t.Fatalf("chunk 1 size mismatch: got %d want %d", got, MaxChunkSize)
	}
	if got := results[2].Info.Offset; got != MaxChunkSize*2 {
		t.Fatalf("chunk 2 offset mismatch: got %d want %d", got, MaxChunkSize*2)
	}
	if got := results[2].Info.Size; got != 123 {
		t.Fatalf("chunk 2 size mismatch: got %d want %d", got, 123)
	}

	if !bytes.Equal(results[0].Data, data[:MaxChunkSize]) {
		t.Fatal("chunk 0 data mismatch")
	}
	if !bytes.Equal(results[1].Data, data[MaxChunkSize:MaxChunkSize*2]) {
		t.Fatal("chunk 1 data mismatch")
	}
	if !bytes.Equal(results[2].Data, data[MaxChunkSize*2:]) {
		t.Fatal("chunk 2 data mismatch")
	}
}
