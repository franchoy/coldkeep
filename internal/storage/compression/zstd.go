package compression

import (
	"errors"
	"fmt"
	"strings"

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
// Repository-level configuration in v1.9 intentionally constrains defaults to
// 1..9; this constructor exposes the broader codec capability for lower-level
// and test contexts.
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
	return decompressZstdBounded(input, expectedSize, MaxDecompressedBlockSize)
}

func decompressZstdBounded(input []byte, expectedSize, maxOutput int64) ([]byte, error) {
	if err := validateDecompressionExpectation(CompressionZstd, expectedSize, maxOutput); err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(
		nil,
		zstd.WithDecoderMaxMemory(uint64(maxOutput)),
		zstd.WithDecodeAllCapLimit(true),
	)
	if err != nil {
		return nil, newCompressionError(ErrDecompressionFailed, 0, CompressionZstd, expectedSize, -1, err)
	}
	defer decoder.Close()

	output, err := decoder.DecodeAll(input, make([]byte, 0, int(expectedSize)))
	if err != nil {
		if isZstdDecompressionLimitError(err) {
			return nil, newCompressionError(ErrCompressionSizeMismatch, 0, CompressionZstd, expectedSize, -1, err)
		}
		return nil, newCompressionError(ErrDecompressionFailed, 0, CompressionZstd, expectedSize, -1, fmt.Errorf("invalid compressed input: %w", err))
	}

	if err := validateDecompressedSize(CompressionZstd, int64(len(output)), expectedSize); err != nil {
		return nil, err
	}

	return output, nil
}

func isZstdDecompressionLimitError(err error) bool {
	return errors.Is(err, zstd.ErrDecoderSizeExceeded) ||
		errors.Is(err, zstd.ErrWindowSizeExceeded) ||
		// klauspost/compress v1.18.0 has SIMD decode paths that report a
		// cap-limit breach with this non-sentinel error instead of
		// ErrDecoderSizeExceeded. The dependency is pinned, and this keeps all
		// of its bounded-output paths under Coldkeep's size-mismatch contract.
		strings.Contains(err.Error(), "output bigger than max block size")
}
