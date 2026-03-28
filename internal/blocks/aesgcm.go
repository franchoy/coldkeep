package blocks

import "context"

// WARNING: AESGCMTransformer is a NON-FUNCTIONAL PLACEHOLDER.
// It does NOT encrypt data. It passes plaintext through unchanged.
// The codec field is set to "aes-gcm" but no encryption is performed.
// Do NOT use this in production or treat it as encryption-capable.
// This must be replaced with a real AES-GCM implementation before enabling.
type AESGCMTransformer struct{}

func (t *AESGCMTransformer) Encode(ctx context.Context, in EncodeInput) (*EncodedBlock, error) {
	// Copy plaintext to avoid accidental mutation
	payload := append([]byte(nil), in.Plaintext...)

	return &EncodedBlock{
		Descriptor: Descriptor{
			ChunkID:       in.ChunkID,
			Codec:         CodecAESGCM,
			FormatVersion: 1,
			PlaintextSize: int64(len(in.Plaintext)),
			StoredSize:    int64(len(payload)),
			Nonce:         nil,
		},
		Payload: payload,
	}, nil
}

// No encryption performed — plaintext is copied through unchanged.
// This is a placeholder stub; see WARNING above.
func (t *AESGCMTransformer) Decode(ctx context.Context, in DecodeInput) ([]byte, error) {
	// Copy payload to avoid mutation
	out := append([]byte(nil), in.Payload...)
	return out, nil
}
