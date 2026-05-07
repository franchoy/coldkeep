package blocks

import "context"

// PlainTransformer stores chunks as-is without any transformation.
type PlainTransformer struct{}

func (t *PlainTransformer) Encode(_ context.Context, in EncodeInput) (*TransformedBlock, error) {
	// Phase 6 Step 8: Avoid unnecessary byte copies
	// Plaintext is used once to create the payload - no mutation after encoding
	// Caller never modifies in.Plaintext after this encode call
	payload := in.Plaintext

	return &TransformedBlock{
		Descriptor: Descriptor{
			ChunkID:       in.ChunkID,
			Codec:         CodecPlain,
			FormatVersion: 1,
			PlaintextSize: int64(len(in.Plaintext)),
			StoredSize:    int64(len(payload)),
			Nonce:         nil,
		},
		Payload: payload,
	}, nil
}

// No transformation needed for plain codec, just return the payload as-is.
// Phase 6 Step 8: Avoid unnecessary byte copies during restore
// - Decode returns payload directly without copying
// - Caller (restore.go) never mutates plaintext after verification
// - Direct return saves memory allocation and copy time per chunk
func (t *PlainTransformer) Decode(_ context.Context, in DecodeInput) ([]byte, error) {
	return in.Payload, nil
}
