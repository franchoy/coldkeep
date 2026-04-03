package blocks

import "context"

// PlainTransformer stores chunks as-is without any transformation.
type PlainTransformer struct{}

func (t *PlainTransformer) Encode(_ context.Context, in EncodeInput) (*EncodedBlock, error) {
	// Copy plaintext to avoid accidental mutation
	payload := append([]byte(nil), in.Plaintext...)

	return &EncodedBlock{
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
func (t *PlainTransformer) Decode(_ context.Context, in DecodeInput) ([]byte, error) {
	// Copy payload to avoid mutation
	out := append([]byte(nil), in.Payload...)
	return out, nil
}
