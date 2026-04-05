package blocks

import (
	"bytes"
	"context"
	"testing"
)

func TestPlainTransformerEncodePopulatesDescriptorAndPayload(t *testing.T) {
	transformer := &PlainTransformer{}
	plaintext := []byte("plain-encode-payload")

	encoded, err := transformer.Encode(context.Background(), EncodeInput{
		ChunkID:   7,
		ChunkHash: "ignored-for-plain",
		Plaintext: plaintext,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if encoded.Descriptor.ChunkID != 7 {
		t.Fatalf("unexpected chunk id: got %d want %d", encoded.Descriptor.ChunkID, 7)
	}
	if encoded.Descriptor.Codec != CodecPlain {
		t.Fatalf("unexpected codec: got %q want %q", encoded.Descriptor.Codec, CodecPlain)
	}
	if encoded.Descriptor.FormatVersion != 1 {
		t.Fatalf("unexpected format version: got %d want 1", encoded.Descriptor.FormatVersion)
	}
	if encoded.Descriptor.PlaintextSize != int64(len(plaintext)) {
		t.Fatalf("unexpected plaintext size: got %d want %d", encoded.Descriptor.PlaintextSize, len(plaintext))
	}
	if encoded.Descriptor.StoredSize != int64(len(plaintext)) {
		t.Fatalf("unexpected stored size: got %d want %d", encoded.Descriptor.StoredSize, len(plaintext))
	}
	if encoded.Descriptor.Nonce != nil {
		t.Fatalf("expected nil nonce for plain codec, got %v", encoded.Descriptor.Nonce)
	}
	if !bytes.Equal(encoded.Payload, plaintext) {
		t.Fatalf("payload mismatch: got %q want %q", encoded.Payload, plaintext)
	}
}

func TestPlainTransformerEncodeCopiesPayload(t *testing.T) {
	transformer := &PlainTransformer{}
	plaintext := []byte("immutable-check")

	encoded, err := transformer.Encode(context.Background(), EncodeInput{Plaintext: plaintext})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	plaintext[0] = 'X'
	if encoded.Payload[0] == 'X' {
		t.Fatalf("expected encoded payload to be copied, but mutation leaked")
	}
}

func TestPlainTransformerDecodeCopiesPayload(t *testing.T) {
	transformer := &PlainTransformer{}
	payload := []byte("decode-copy-check")

	decoded, err := transformer.Decode(context.Background(), DecodeInput{Payload: payload})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	payload[0] = 'X'
	if decoded[0] == 'X' {
		t.Fatalf("expected decoded payload to be copied, but mutation leaked")
	}
}
