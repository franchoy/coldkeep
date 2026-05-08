package compression

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

type zstdCompressor struct{}

func (zstdCompressor) Codec() string {
	return CompressionZstd
}

func (zstdCompressor) Compress(input []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd: create writer: %w", err)
	}
	defer encoder.Close()

	return encoder.EncodeAll(input, make([]byte, 0, len(input))), nil
}

func (zstdCompressor) Decompress(input []byte, expectedSize int64) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd: create reader: %w", err)
	}
	defer decoder.Close()

	output, err := decoder.DecodeAll(input, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd: decode: %w", err)
	}

	if expectedSize >= 0 && int64(len(output)) != expectedSize {
		return nil, fmt.Errorf("zstd: decoded size mismatch: got=%d want=%d", len(output), expectedSize)
	}

	return output, nil
}
