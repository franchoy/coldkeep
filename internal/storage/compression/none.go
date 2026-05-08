package compression

type noneCompressor struct{}

func (noneCompressor) Codec() string {
	return CompressionNone
}

func (noneCompressor) Compress(input []byte) ([]byte, error) {
	return input, nil
}

func (noneCompressor) Decompress(input []byte, _ int64) ([]byte, error) {
	return input, nil
}
