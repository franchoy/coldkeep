package compression

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestLookupReturnsExpectedCodecs(t *testing.T) {
	tests := []struct {
		name      string
		codec     string
		expected  string
		wantError bool
	}{
		{name: "none", codec: CompressionNone, expected: CompressionNone},
		{name: "empty defaults to none", codec: "", expected: CompressionNone},
		{name: "zstd", codec: CompressionZstd, expected: CompressionZstd},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compressor, err := Lookup(tc.codec)
			if tc.wantError {
				if err == nil {
					t.Fatalf("expected error for codec=%q", tc.codec)
				}
				return
			}

			if err != nil {
				t.Fatalf("Lookup(%q): %v", tc.codec, err)
			}
			if compressor.Codec() != tc.expected {
				t.Fatalf("codec mismatch: got=%q want=%q", compressor.Codec(), tc.expected)
			}
		})
	}
}

func TestLookupUnknownCodecReturnsClearError(t *testing.T) {
	_, err := Lookup("brotli")
	if err == nil {
		t.Fatal("expected unsupported codec error")
	}
	if !errors.Is(err, ErrUnsupportedCompressionCodec) {
		t.Fatalf("expected ErrUnsupportedCompressionCodec, got: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, `compression_codec="brotli"`) {
		t.Fatalf("expected codec context in error, got: %v", err)
	}
	if !strings.Contains(msg, "block_id=0") {
		t.Fatalf("expected block id context in error, got: %v", err)
	}
}

func TestNoneRoundTripUnchanged(t *testing.T) {
	compressor, err := Lookup(CompressionNone)
	if err != nil {
		t.Fatalf("Lookup none: %v", err)
	}

	original := []byte("coldkeep-phase3-none-codec")
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("none compress: %v", err)
	}
	recovered, err := compressor.Decompress(compressed, int64(len(original)))
	if err != nil {
		t.Fatalf("none decompress: %v", err)
	}

	if !bytes.Equal(original, recovered) {
		t.Fatalf("none round-trip mismatch: got=%q want=%q", string(recovered), string(original))
	}
}

func TestNoneDecompressRequiresExactExpectedSize(t *testing.T) {
	compressor, err := Lookup(CompressionNone)
	if err != nil {
		t.Fatalf("Lookup none: %v", err)
	}

	input := []byte("identity-no-copy")
	recovered, err := compressor.Decompress(input, int64(len(input)))
	if err != nil {
		t.Fatalf("exact none decompress: %v", err)
	}
	if len(input) > 0 && &recovered[0] != &input[0] {
		t.Fatal("none decompression copied the input")
	}

	for _, expectedSize := range []int64{-1, int64(len(input) - 1), int64(len(input) + 1), MaxDecompressedBlockSize + 1} {
		_, err := compressor.Decompress(input, expectedSize)
		if !errors.Is(err, ErrCompressionSizeMismatch) {
			t.Fatalf("expected size mismatch for expected=%d, got: %v", expectedSize, err)
		}
	}

	empty, err := compressor.Decompress([]byte{}, 0)
	if err != nil {
		t.Fatalf("empty none decompress: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty none output length: got=%d", len(empty))
	}
}

func TestZstdRoundTripExact(t *testing.T) {
	compressor, err := Lookup(CompressionZstd)
	if err != nil {
		t.Fatalf("Lookup zstd: %v", err)
	}

	original := bytes.Repeat([]byte("coldkeep-zstd-phase3-"), 512)
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("zstd compress: %v", err)
	}
	recovered, err := compressor.Decompress(compressed, int64(len(original)))
	if err != nil {
		t.Fatalf("zstd decompress: %v", err)
	}

	if !bytes.Equal(original, recovered) {
		t.Fatalf("zstd round-trip mismatch")
	}
}
