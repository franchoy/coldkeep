package compression

import (
	"fmt"
	"strings"
)

const (
	CompressionNone = "none"
	CompressionZstd = "zstd"

	// DefaultCompressionCodec keeps compression disabled unless explicitly enabled.
	DefaultCompressionCodec = CompressionNone
	DefaultCompressionLevel = 3
)

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
		return nil, newCompressionError(ErrUnsupportedCompressionCodec, 0, normalized, -1, -1, nil)
	}
}
