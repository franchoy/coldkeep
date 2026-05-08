package compression

import (
	"fmt"
	"strings"
)

const (
	CompressionNone = "none"
	CompressionZstd = "zstd"
)

// ErrUnsupportedCompression is returned when codec lookup receives
// an unknown compression codec name.
var ErrUnsupportedCompression = fmt.Errorf("unsupported compression codec")

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
		return zstdCompressor{}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCompression, codec)
	}
}
