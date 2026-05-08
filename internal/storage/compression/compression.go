package compression

import (
	"fmt"
	"strings"
)

const (
	CompressionNone = "none"
	CompressionZstd = "zstd"

	// DefaultCompressionCodec is the recommended codec for new compression-capable
	// metadata once compression activation is enabled in later Phase 3 steps.
	DefaultCompressionCodec = CompressionZstd
	DefaultCompressionLevel = 3
)

// ErrUnsupportedCompression is returned when codec lookup receives
// an unknown compression codec name.
var ErrUnsupportedCompression = fmt.Errorf("unsupported compression codec")

// ErrInvalidCompressionLevel is returned when a compression level is outside
// the supported range for the selected codec.
var ErrInvalidCompressionLevel = fmt.Errorf("invalid compression level")

// Compressor provides a codec-stable compression/decompression contract.
type Compressor interface {
	Codec() string
	Compress(input []byte) ([]byte, error)
	Decompress(input []byte, expectedSize int64) ([]byte, error)
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
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCompression, codec)
	}
}
