package blocks

import "context"

// Transformer defines how a chunk is transformed into a stored block
// and how a stored block is transformed back into plaintext.
type Transformer interface {
	Encode(ctx context.Context, in EncodeInput) (*TransformedBlock, error)
	Decode(ctx context.Context, in DecodeInput) ([]byte, error)
}

// TransformMetadata carries explicit transformation information through the persistence pipeline.
// It threads compression metadata, payload information, and hash data from encode stage
// through transform stage to final persistence for the current v1.9 write/read contract.
type TransformMetadata struct {
	// PayloadHash is a compatibility/observability mirror of the logical block hash
	// (lowercase-hex SHA256 of encoded plaintext block bytes before transforms).
	// storage_blocks.block_hash remains the authoritative logical identity.
	PayloadHash string

	// CompressionCodec identifies the compression stage outcome persisted per block.
	// v1.9 values are 'none' and 'zstd'. Store-if-smaller may keep 'none' even when
	// zstd is configured for the repository.
	CompressionCodec string

	// CompressionRatio is compressed_payload_size / encoded_plaintext_size
	// for the pre-encryption compression stage.
	// 1.0 indicates no effective compression (including store-if-smaller fallback),
	// values < 1.0 indicate successful size reduction.
	CompressionRatio float64
}
