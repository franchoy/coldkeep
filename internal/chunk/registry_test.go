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

func TestNewRegistryRejectsUnknownDefault(t *testing.T) {
	_, err := NewRegistry("nonexistent-version")
	if err == nil {
		t.Fatal("expected error for unknown default version, got nil")
	}
}

func TestNewRegistryRejectsDuplicateChunker(t *testing.T) {
	_, err := NewRegistry(VersionV1SimpleRolling,
		DefaultChunker(),
		DefaultChunker(),
	)
	if err == nil {
		t.Fatal("expected error for duplicate chunker registration, got nil")
	}
}

func TestRegistryGetAndDefaultVersion(t *testing.T) {
	r, err := NewRegistry(VersionV1SimpleRolling, DefaultChunker())
	if err != nil {
		t.Fatalf("unexpected registry construction error: %v", err)
	}

	if got := r.DefaultVersion(); got != VersionV1SimpleRolling {
		t.Fatalf("DefaultVersion mismatch: got %q want %q", got, VersionV1SimpleRolling)
	}

	c, ok := r.Get(VersionV1SimpleRolling)
	if !ok {
		t.Fatal("expected Get to find registered chunker")
	}
	if c.Version() != VersionV1SimpleRolling {
		t.Fatalf("chunker version mismatch: got %q", c.Version())
	}

	_, ok = r.Get("nonexistent")
	if ok {
		t.Fatal("expected Get to return false for unknown version")
	}
}

func TestRegistryMustGetPanicsOnMissing(t *testing.T) {
	r, err := NewRegistry(VersionV1SimpleRolling, DefaultChunker())
	if err != nil {
		t.Fatalf("unexpected registry construction error: %v", err)
	}

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("expected MustGet to panic for unknown version")
		}
	}()
	r.MustGet("nonexistent")
}

func TestNewDefaultRegistryIsValid(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry returned error: %v", err)
	}

	if got := r.DefaultVersion(); got != VersionV1SimpleRolling {
		t.Fatalf("default version mismatch: got %q want %q", got, VersionV1SimpleRolling)
	}

	c := r.Default()
	if c.Version() != VersionV1SimpleRolling {
		t.Fatalf("default chunker version mismatch: got %q", c.Version())
	}
}

func TestDefaultRegistryResolvesV2FastCDC(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry returned error: %v", err)
	}

	// v2-fastcdc must be resolvable by version key.
	c, ok := r.Get(VersionV2FastCDC)
	if !ok {
		t.Fatalf("expected v2-fastcdc to be registered; Get returned false")
	}
	if c.Version() != VersionV2FastCDC {
		t.Fatalf("resolved chunker version: got %q want %q", c.Version(), VersionV2FastCDC)
	}

	// Default must still be v1.
	if got := r.DefaultVersion(); got != VersionV1SimpleRolling {
		t.Fatalf("default version must remain v1 after v2 registration: got %q", got)
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
