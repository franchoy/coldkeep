package compression

import (
	"bytes"
	"compress/gzip"
	"errors"
	"math/rand"
	"strings"
	"testing"
)

func TestNewZstdCompressorRejectsInvalidLevel(t *testing.T) {
	_, err := NewZstdCompressor(0)
	if err == nil {
		t.Fatal("expected invalid compression level error")
	}
	if !errors.Is(err, ErrInvalidCompressionLevel) {
		t.Fatalf("expected ErrInvalidCompressionLevel, got: %v", err)
	}
}

func TestZstdRoundTripPayloadMatrix(t *testing.T) {
	compressor, err := NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "empty payload", payload: []byte{}},
		{name: "tiny payload", payload: []byte("hi")},
		{name: "repetitive 1mb", payload: bytes.Repeat([]byte("coldkeep-phase3-"), (1024*1024)/len("coldkeep-phase3-"))},
		{name: "random payload", payload: randomDeterministicBytes(256 * 1024)},
		{name: "already-compressed-like payload", payload: gzipLikePayload(t)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := compressor.Compress(tc.payload)
			if err != nil {
				t.Fatalf("Compress: %v", err)
			}
			recovered, err := compressor.Decompress(compressed, int64(len(tc.payload)))
			if err != nil {
				t.Fatalf("Decompress: %v", err)
			}
			if !bytes.Equal(recovered, tc.payload) {
				t.Fatalf("round-trip mismatch for %s", tc.name)
			}
		})
	}
}

func TestZstdInvalidCompressedInputReturnsCleanError(t *testing.T) {
	compressor, err := NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}

	_, err = compressor.Decompress([]byte("not-zstd"), -1)
	if err == nil {
		t.Fatal("expected error for invalid zstd payload")
	}
	if !errors.Is(err, ErrDecompressionFailed) {
		t.Fatalf("expected ErrDecompressionFailed, got: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, `compression_codec="zstd"`) {
		t.Fatalf("expected codec context in error, got: %v", err)
	}
	if !strings.Contains(msg, "block_id=0") {
		t.Fatalf("expected block id context in error, got: %v", err)
	}
	if !strings.Contains(msg, "invalid compressed input") {
		t.Fatalf("expected clean invalid-input marker, got: %v", err)
	}
}

func TestZstdDecompressSizeMismatchReturnsTypedError(t *testing.T) {
	compressor, err := NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}

	payload := []byte("size-mismatch-case")
	compressed, err := compressor.Compress(payload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	_, err = compressor.Decompress(compressed, int64(len(payload)+5))
	if err == nil {
		t.Fatal("expected size mismatch error")
	}
	if !errors.Is(err, ErrCompressionSizeMismatch) {
		t.Fatalf("expected ErrCompressionSizeMismatch, got: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, `compression_codec="zstd"`) {
		t.Fatalf("expected codec context in error, got: %v", err)
	}
	if !strings.Contains(msg, "expected_size=") || !strings.Contains(msg, "actual_size=") {
		t.Fatalf("expected size context in error, got: %v", err)
	}
}

func randomDeterministicBytes(n int) []byte {
	r := rand.New(rand.NewSource(1701))
	b := make([]byte, n)
	_, _ = r.Read(b)
	return b
}

func gzipLikePayload(t *testing.T) []byte {
	t.Helper()

	base := bytes.Repeat([]byte("already-compressed-like-source"), 4096)
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	if _, err := w.Write(base); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return out.Bytes()
}
