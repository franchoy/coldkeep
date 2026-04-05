package blocks

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestAESGCMEncodeFailsWithInvalidKeyLength(t *testing.T) {
	transformer := &AESGCMTransformer{Key: []byte("tooshort")} // 8 bytes, not valid AES key

	_, err := transformer.Encode(context.Background(), EncodeInput{
		ChunkID:   1,
		ChunkHash: strings.Repeat("a", 64),
		Plaintext: []byte("hello"),
	})
	if err == nil || !strings.Contains(err.Error(), "create cipher") {
		t.Fatalf("expected create-cipher error contract, got: %v", err)
	}
}

func TestAESGCMDecodeFailsWithInvalidKeyLength(t *testing.T) {
	transformer := &AESGCMTransformer{Key: []byte("tooshort")} // 8 bytes, not valid AES key

	_, err := transformer.Decode(context.Background(), DecodeInput{
		ChunkHash:  strings.Repeat("a", 64),
		Descriptor: Descriptor{Nonce: make([]byte, 12)},
		Payload:    []byte("somepayload"),
	})
	if err == nil || !strings.Contains(err.Error(), "create cipher") {
		t.Fatalf("expected create-cipher error contract, got: %v", err)
	}
}

func TestAESGCMDecodeFailsWithCorruptCiphertext(t *testing.T) {
	key := bytes.Repeat([]byte{0xab}, 32) // valid 32-byte AES-256 key
	transformer := &AESGCMTransformer{Key: key}

	// Encode a real payload to get a valid nonce, then tamper with the ciphertext.
	encoded, err := transformer.Encode(context.Background(), EncodeInput{
		ChunkID:   1,
		ChunkHash: strings.Repeat("b", 64),
		Plaintext: []byte("original plaintext"),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	tampered := make([]byte, len(encoded.Payload))
	copy(tampered, encoded.Payload)
	tampered[0] ^= 0xFF // flip bits in first byte to break authentication tag

	_, err = transformer.Decode(context.Background(), DecodeInput{
		ChunkHash:  strings.Repeat("b", 64),
		Descriptor: encoded.Descriptor,
		Payload:    tampered,
	})
	if err == nil || !strings.Contains(err.Error(), "decrypt payload") {
		t.Fatalf("expected decrypt-payload error contract, got: %v", err)
	}
}

func TestAESGCMEncodeDecodeRoundtrip(t *testing.T) {
	key := bytes.Repeat([]byte{0xcd}, 32)
	transformer := &AESGCMTransformer{Key: key}
	plaintext := []byte("roundtrip test payload")

	encoded, err := transformer.Encode(context.Background(), EncodeInput{
		ChunkID:   2,
		ChunkHash: strings.Repeat("c", 64),
		Plaintext: plaintext,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := transformer.Decode(context.Background(), DecodeInput{
		ChunkHash:  strings.Repeat("c", 64),
		Descriptor: encoded.Descriptor,
		Payload:    encoded.Payload,
	})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !bytes.Equal(decoded, plaintext) {
		t.Fatalf("roundtrip mismatch: got %q, want %q", decoded, plaintext)
	}
}
