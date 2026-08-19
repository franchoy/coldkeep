package compression

import (
	"bytes"
	"compress/gzip"
	"errors"
	"math"
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

	_, err = compressor.Decompress([]byte("not-zstd"), 1)
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

func TestZstdDecompressRejectsExpectedSizeOutsideAbsoluteBound(t *testing.T) {
	tests := []struct {
		expectedSize int64
		maxOutput    int64
	}{
		{expectedSize: -1, maxOutput: MaxDecompressedBlockSize},
		{expectedSize: MaxDecompressedBlockSize + 1, maxOutput: MaxDecompressedBlockSize},
		{expectedSize: math.MaxInt64, maxOutput: 64},
	}
	for _, tc := range tests {
		_, err := decompressZstdBounded([]byte("not-zstd"), tc.expectedSize, tc.maxOutput)
		if !errors.Is(err, ErrCompressionSizeMismatch) {
			t.Fatalf("expected pre-decode size mismatch for expected=%d max=%d, got: %v", tc.expectedSize, tc.maxOutput, err)
		}
		if errors.Is(err, ErrDecompressionFailed) {
			t.Fatalf("decoder ran before expectation validation for expected=%d max=%d: %v", tc.expectedSize, tc.maxOutput, err)
		}
	}
}

func TestZstdDecompressRejectsOutputBeyondExpectedSize(t *testing.T) {
	compressor, err := NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	payload := bytes.Repeat([]byte("a"), 1024)
	compressed, err := compressor.Compress(payload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	_, err = decompressZstdBounded(compressed, 64, 64<<10)
	if !errors.Is(err, ErrCompressionSizeMismatch) {
		t.Fatalf("expected bounded size mismatch, got: %v", err)
	}
}

func TestZstdDecompressRejectsTruncatedInput(t *testing.T) {
	compressor, err := NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	payload := bytes.Repeat([]byte("truncated-zstd"), 32)
	compressed, err := compressor.Compress(payload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	compressed = compressed[:len(compressed)-1]

	_, err = compressor.Decompress(compressed, int64(len(payload)))
	if !errors.Is(err, ErrDecompressionFailed) {
		t.Fatalf("expected ErrDecompressionFailed, got: %v", err)
	}
}

func TestZstdDecompressBoundsConcatenatedFramesAcrossAggregateOutput(t *testing.T) {
	compressor, err := NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	payload := bytes.Repeat([]byte("frame"), 32)
	frame, err := compressor.Compress(payload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	concatenated := append(append([]byte(nil), frame...), frame...)

	_, err = decompressZstdBounded(concatenated, int64(len(payload)), 64<<10)
	if !errors.Is(err, ErrCompressionSizeMismatch) {
		t.Fatalf("expected aggregate output bound failure, got: %v", err)
	}

	want := append(append([]byte(nil), payload...), payload...)
	got, err := decompressZstdBounded(concatenated, int64(len(want)), 64<<10)
	if err != nil {
		t.Fatalf("exact concatenated decode: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("concatenated decode mismatch")
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
