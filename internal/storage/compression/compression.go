package compression

import (
	"fmt"
	"strings"
)

const (
	CompressionNone = "none"
	CompressionZstd = "zstd"

	// MaxDecompressedBlockSize is the maximum complete encoded CKBL block
	// accepted by the storage decompression boundary. Released writers produce
	// at most a 3 MiB chunk payload plus CKBL header/table overhead, which is
	// strictly below this 4 MiB format/runtime ceiling.
	MaxDecompressedBlockSize int64 = 4 << 20

	// DefaultCompressionCodec keeps compression disabled unless explicitly enabled.
	DefaultCompressionCodec = CompressionNone
	DefaultCompressionLevel = 3
)

// ErrInvalidCompressionLevel is returned when a compression level is outside
// the supported range for the selected codec.
var ErrInvalidCompressionLevel = fmt.Errorf("invalid compression level")

// Compressor provides a codec-stable compression/decompression contract.
// Decompress requires a known expected size in [0, MaxDecompressedBlockSize].
// A successful decode always returns exactly expectedSize bytes.
type Compressor interface {
	Codec() string
	Compress(input []byte) ([]byte, error)
	Decompress(input []byte, expectedSize int64) ([]byte, error)
}

func validateDecompressionExpectation(codec string, expectedSize, maxOutput int64) error {
	if maxOutput <= 0 {
		return newCompressionError(
			ErrCompressionSizeMismatch,
			0,
			codec,
			expectedSize,
			-1,
			fmt.Errorf("invalid decompression maximum: %d", maxOutput),
		)
	}
	if expectedSize < 0 || expectedSize > maxOutput {
		return newCompressionError(
			ErrCompressionSizeMismatch,
			0,
			codec,
			expectedSize,
			-1,
			fmt.Errorf("expected size outside permitted range [0,%d]", maxOutput),
		)
	}
	return nil
}

func validateDecompressedSize(codec string, actualSize, expectedSize int64) error {
	if actualSize == expectedSize {
		return nil
	}
	return newCompressionError(
		ErrCompressionSizeMismatch,
		0,
		codec,
		expectedSize,
		actualSize,
		nil,
	)
}

// Lookup returns a compressor implementation for the requested codec.
// Empty codec maps to "none" for compatibility with legacy metadata defaults.
func Lookup(codec string) (Compressor, error) {
	normalized := strings.TrimSpace(strings.ToLower(codec))
	if normalized == "" {
		normalized = CompressionNone
	}

	switch normalized {
	case CompressionNone:
		return noneCompressor{}, nil
	case CompressionZstd:
		return NewZstdCompressor(DefaultCompressionLevel)
	default:
		return nil, newCompressionError(ErrUnsupportedCompressionCodec, 0, normalized, -1, -1, nil)
	}
}
