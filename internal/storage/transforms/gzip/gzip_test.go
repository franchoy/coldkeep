package gzip_test

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"testing"

	gziptransform "github.com/franchoy/coldkeep/internal/storage/transforms/gzip"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

func compressible(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 16) // low entropy — highly compressible
	}
	return b
}

// ---------------------------------------------------------------------------
// Core round-trip tests
// ---------------------------------------------------------------------------

func TestGzipRoundTripDefaultCompression(t *testing.T) {
	tr := gziptransform.NewDefault()
	original := compressible(128 * 1024)

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(recovered, original) {
		t.Fatalf("round-trip mismatch: len(original)=%d len(recovered)=%d", len(original), len(recovered))
	}
}

func TestGzipRoundTripBestSpeed(t *testing.T) {
	tr := gziptransform.New(gzip.BestSpeed)
	original := compressible(64 * 1024)

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(recovered, original) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestGzipRoundTripBestCompression(t *testing.T) {
	tr := gziptransform.New(gzip.BestCompression)
	original := compressible(32 * 1024)

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(recovered, original) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestGzipRoundTripRandomBytes(t *testing.T) {
	tr := gziptransform.NewDefault()
	original := randomBytes(t, 64*1024)

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(recovered, original) {
		t.Fatalf("round-trip mismatch for random bytes")
	}
}

func TestGzipRoundTripEmpty(t *testing.T) {
	tr := gziptransform.NewDefault()
	original := []byte{}

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode empty: %v", err)
	}

	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode empty: %v", err)
	}

	if !bytes.Equal(recovered, original) {
		t.Fatalf("empty round-trip mismatch: got %x", recovered)
	}
}

func TestGzipRoundTripSingleByte(t *testing.T) {
	tr := gziptransform.NewDefault()
	original := []byte{0xFF}

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode single byte: %v", err)
	}

	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode single byte: %v", err)
	}

	if !bytes.Equal(recovered, original) {
		t.Fatalf("single-byte round-trip mismatch")
	}
}

// ---------------------------------------------------------------------------
// Compression ratio test
// ---------------------------------------------------------------------------

func TestGzipHighlyCompressibleDataShrinks(t *testing.T) {
	tr := gziptransform.NewDefault()
	// Zeroed buffer of 1 MiB is maximally compressible.
	original := make([]byte, 1024*1024)

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if len(compressed) >= len(original) {
		t.Fatalf("expected compressed size < %d, got %d", len(original), len(compressed))
	}
	t.Logf("compressible 1 MiB: %d → %d bytes (%.1f%%)", len(original), len(compressed), 100.0*float64(len(compressed))/float64(len(original)))
}

func TestGzipRandomDataMayExpand(t *testing.T) {
	tr := gziptransform.NewDefault()
	// Random data (max entropy) typically won't shrink but must still round-trip.
	original := randomBytes(t, 4*1024)

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// No assertion on compression ratio — random data can expand due to gzip header.
	// Just verify round-trip fidelity.
	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(recovered, original) {
		t.Fatalf("random-data round-trip mismatch")
	}
}

// ---------------------------------------------------------------------------
// Error path tests
// ---------------------------------------------------------------------------

func TestGzipDecodeInvalidInput(t *testing.T) {
	tr := gziptransform.NewDefault()

	_, err := tr.Decode([]byte("this is not gzip data"))
	if err == nil {
		t.Fatal("expected error decoding non-gzip bytes, got nil")
	}
}

func TestGzipDecodeEmptyInput(t *testing.T) {
	tr := gziptransform.NewDefault()

	_, err := tr.Decode([]byte{})
	if err == nil {
		t.Fatal("expected error decoding empty input, got nil")
	}
}

func TestGzipInvalidLevel(t *testing.T) {
	// Level 99 is outside the valid range [NoCompression..BestCompression].
	tr := gziptransform.New(99)
	_, err := tr.Encode([]byte("test"))
	if err == nil {
		t.Fatal("expected error for invalid compression level, got nil")
	}
}

// ---------------------------------------------------------------------------
// Name and interface tests
// ---------------------------------------------------------------------------

func TestGzipName(t *testing.T) {
	tr := gziptransform.NewDefault()
	if tr.Name() != "gzip" {
		t.Fatalf("expected Name()=\"gzip\", got %q", tr.Name())
	}
}

func TestGzipConstantName(t *testing.T) {
	if gziptransform.Name != "gzip" {
		t.Fatalf("expected gzip.Name==\"gzip\", got %q", gziptransform.Name)
	}
}

// ---------------------------------------------------------------------------
// Determinism test
// ---------------------------------------------------------------------------

func TestGzipEncodeIsDeterministicForSameInput(t *testing.T) {
	tr := gziptransform.NewDefault()
	input := compressible(64 * 1024)

	a, err := tr.Encode(input)
	if err != nil {
		t.Fatalf("Encode (1): %v", err)
	}
	b, err := tr.Encode(input)
	if err != nil {
		t.Fatalf("Encode (2): %v", err)
	}

	// gzip output is deterministic for same input and level — no random element.
	if !bytes.Equal(a, b) {
		t.Fatal("Encode produced different outputs for the same input")
	}
}

// ---------------------------------------------------------------------------
// Large payload test
// ---------------------------------------------------------------------------

func TestGzipLargePayloadRoundTrip(t *testing.T) {
	tr := gziptransform.NewDefault()
	original := compressible(4 * 1024 * 1024) // 4 MiB

	compressed, err := tr.Encode(original)
	if err != nil {
		t.Fatalf("Encode 4 MiB: %v", err)
	}

	recovered, err := tr.Decode(compressed)
	if err != nil {
		t.Fatalf("Decode 4 MiB: %v", err)
	}

	if !bytes.Equal(recovered, original) {
		t.Fatalf("4 MiB round-trip mismatch")
	}
	t.Logf("4 MiB round-trip: %d → %d bytes (%.1f%%)", len(original), len(compressed), 100.0*float64(len(compressed))/float64(len(original)))
}
