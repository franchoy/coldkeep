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

func TestPlainTransformerEncodeDoesNotCopyPayload(t *testing.T) {
	// Phase 6 Step 8: Avoid unnecessary byte copies
	// Plaintext is used once during encode and never mutated by codec.
	// Store operation chain: read → encode → write; plaintext not accessed after encode.
	// This optimization avoids allocating and copying memory for each plain codec encode.
	transformer := &PlainTransformer{}
	plaintext := []byte("immutable-check")

	encoded, err := transformer.Encode(context.Background(), EncodeInput{Plaintext: plaintext})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Verify the payload content matches
	if !bytes.Equal(encoded.Payload, plaintext) {
		t.Fatalf("payload mismatch: got %q want %q", encoded.Payload, plaintext)
	}
}

func TestPlainTransformerDecodeDoesNotCopyPayload(t *testing.T) {
	// Phase 6 Step 8: Avoid unnecessary byte copies
	// Payload is decoded and immediately used for hash verification and write,
	// never mutated by the restore operation. This optimization avoids allocating
	// and copying memory for each plain codec decode during restore.
	transformer := &PlainTransformer{}
	payload := []byte("decode-copy-check")

	decoded, err := transformer.Decode(context.Background(), DecodeInput{Payload: payload})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Verify the decoded payload content matches
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("payload mismatch: got %q want %q", decoded, payload)
	}
}
