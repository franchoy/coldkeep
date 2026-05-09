package compression

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const (
	zstdLevelMin = 1
	zstdLevelMax = 22
)

type zstdCompressor struct {
	level int
}

// NewZstdCompressor returns a Compressor configured for zstd at the provided
// compression level. Supported levels are 1..22.
func NewZstdCompressor(level int) (Compressor, error) {
	if level < zstdLevelMin || level > zstdLevelMax {
		return nil, fmt.Errorf("%w: zstd level=%d supported=[%d..%d]", ErrInvalidCompressionLevel, level, zstdLevelMin, zstdLevelMax)
	}
	return zstdCompressor{level: level}, nil
}

func (c zstdCompressor) Codec() string {
	return CompressionZstd
}

func (c zstdCompressor) Compress(input []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(c.level)))
	if err != nil {
		return nil, newCompressionError(ErrCompressionFailed, 0, CompressionZstd, -1, -1, err)
	}
	defer func() { _ = encoder.Close() }()

	return encoder.EncodeAll(input, make([]byte, 0, len(input))), nil
}

func (zstdCompressor) Decompress(input []byte, expectedSize int64) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, newCompressionError(ErrDecompressionFailed, 0, CompressionZstd, expectedSize, -1, err)
	}
	defer decoder.Close()

	output, err := decoder.DecodeAll(input, nil)
	if err != nil {
		return nil, newCompressionError(ErrDecompressionFailed, 0, CompressionZstd, expectedSize, -1, fmt.Errorf("invalid compressed input: %w", err))
	}

	if expectedSize >= 0 && int64(len(output)) != expectedSize {
		return nil, newCompressionError(ErrCompressionSizeMismatch, 0, CompressionZstd, expectedSize, int64(len(output)), nil)
	}

	return output, nil
}
