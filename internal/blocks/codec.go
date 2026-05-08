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
// through transform stage to final persistence, enabling future phases to activate
// compression and validation without invasive refactoring.
type TransformMetadata struct {
	// PayloadHash is the SHA256 hex digest of the plaintext block bytes
	// (before any compression or encryption). Used for incremental validation
	// and future deduplication.
	PayloadHash string

	// CompressionCodec identifies the compression algorithm applied.
	// Current values: 'none' (no compression)
	// Future values: 'gzip', 'zstd', etc.
	CompressionCodec string

	// CompressionRatio is the ratio of stored_size to plaintext_size.
	// Typically 1.0 when no compression is used.
	// Values < 1.0 indicate compression achieved, > 1.0 indicate expansion.
	CompressionRatio float64
}
