package compression

type noneCompressor struct{}

func (noneCompressor) Codec() string {
	return CompressionNone
}

func (noneCompressor) Compress(input []byte) ([]byte, error) {
	return input, nil
}

func (noneCompressor) Decompress(input []byte, expectedSize int64) ([]byte, error) {
	if err := validateDecompressionExpectation(CompressionNone, expectedSize, MaxDecompressedBlockSize); err != nil {
		return nil, err
	}
	if err := validateDecompressedSize(CompressionNone, int64(len(input)), expectedSize); err != nil {
		return nil, err
	}
	return input, nil
}
