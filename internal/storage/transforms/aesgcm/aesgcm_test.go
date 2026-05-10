package aesgcm_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage/transforms"
	"github.com/franchoy/coldkeep/internal/storage/transforms/aesgcm"
)

func newTestKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

// Compile-time interface check.
var _ transforms.Transform = (*aesgcm.AESGCMTransform)(nil)

func TestAESGCMTransform_Name(t *testing.T) {
	tr := &aesgcm.AESGCMTransform{Key: newTestKey(t)}
	if tr.Name() != "aes-gcm" {
		t.Fatalf("expected name %q, got %q", "aes-gcm", tr.Name())
	}
}

func TestAESGCMTransform_RoundTrip(t *testing.T) {
	tr := &aesgcm.AESGCMTransform{Key: newTestKey(t)}
	plaintext := []byte("hello coldkeep transform pipeline")

	encoded, err := tr.Encode(plaintext)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Encoded output must be longer than plaintext (nonce + auth tag overhead).
	if len(encoded) <= len(plaintext) {
		t.Fatalf("encoded length %d should exceed plaintext length %d", len(encoded), len(plaintext))
	}

	decoded, err := tr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(decoded, plaintext) {
		t.Fatalf("recovered plaintext does not match original")
	}
}

func TestAESGCMTransform_NoncePrefix(t *testing.T) {
	tr := &aesgcm.AESGCMTransform{Key: newTestKey(t)}
	plaintext := []byte("nonce prefix wire format check")

	encoded, err := tr.Encode(plaintext)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Wire format: first 12 bytes are the nonce.
	if len(encoded) < aesgcm.NonceSize {
		t.Fatalf("encoded output shorter than nonce size")
	}
}

func TestAESGCMTransform_NondeterministicNonce(t *testing.T) {
	// Two encodes of identical plaintext must produce different outputs
	// (different random nonces), preserving ciphertext indistinguishability.
	tr := &aesgcm.AESGCMTransform{Key: newTestKey(t)}
	plaintext := []byte("same plaintext twice")

	first, err := tr.Encode(plaintext)
	if err != nil {
		t.Fatalf("Encode first: %v", err)
	}
	second, err := tr.Encode(plaintext)
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}

	if bytes.Equal(first, second) {
		t.Fatal("two encodes of same plaintext produced identical output — nonce reuse")
	}

	// But both must decode to the same plaintext.
	d1, err := tr.Decode(first)
	if err != nil {
		t.Fatalf("Decode first: %v", err)
	}
	d2, err := tr.Decode(second)
	if err != nil {
		t.Fatalf("Decode second: %v", err)
	}
	if !bytes.Equal(d1, plaintext) || !bytes.Equal(d2, plaintext) {
		t.Fatal("decoded values do not match original plaintext")
	}
}

func TestAESGCMTransform_WrongKeyFails(t *testing.T) {
	key1 := newTestKey(t)
	key2 := newTestKey(t)

	enc := &aesgcm.AESGCMTransform{Key: key1}
	dec := &aesgcm.AESGCMTransform{Key: key2}

	encoded, err := enc.Encode([]byte("secret data"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	_, err = dec.Decode(encoded)
	if err == nil {
		t.Fatal("expected decryption with wrong key to fail, but it succeeded")
	}
}

func TestAESGCMTransform_TamperedCiphertextFails(t *testing.T) {
	tr := &aesgcm.AESGCMTransform{Key: newTestKey(t)}

	encoded, err := tr.Encode([]byte("tamper test"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Flip a byte in the ciphertext region (after the nonce).
	tampered := append([]byte(nil), encoded...)
	tampered[aesgcm.NonceSize] ^= 0xFF

	_, err = tr.Decode(tampered)
	if err == nil {
		t.Fatal("expected tampered ciphertext to fail authentication, but Decode succeeded")
	}
}

func TestAESGCMTransform_TooShortPayloadFails(t *testing.T) {
	tr := &aesgcm.AESGCMTransform{Key: newTestKey(t)}

	_, err := tr.Decode(make([]byte, aesgcm.NonceSize))
	if err == nil {
		t.Fatal("expected error for payload <= nonce size, got nil")
	}
}

func TestAESGCMTransform_EmptyPlaintext(t *testing.T) {
	tr := &aesgcm.AESGCMTransform{Key: newTestKey(t)}

	encoded, err := tr.Encode([]byte{})
	if err != nil {
		t.Fatalf("Encode empty: %v", err)
	}

	decoded, err := tr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode empty: %v", err)
	}

	if len(decoded) != 0 {
		t.Fatalf("expected empty plaintext, got %d bytes", len(decoded))
	}
}
