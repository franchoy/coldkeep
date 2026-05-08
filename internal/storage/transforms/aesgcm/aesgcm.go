// Package aesgcm provides an AES-GCM encrypt/decrypt implementation of the
// transforms.Transform interface. The stored payload format is:
//
//	nonce (12 bytes) || AES-GCM ciphertext
//
// This layout is identical to the v1.8 wire format for storage_blocks rows
// with codec "aes-gcm", so repositories written by v1.8 are fully compatible.
package aesgcm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// NonceSize is the AES-GCM nonce length used by this transform.
// It matches the v1.8 packedStorageBlockAESGCMNonceSize constant.
const NonceSize = 12

// AESGCMTransform encrypts/decrypts block payloads with AES-256-GCM.
//
// Wire format (Encode output / Decode input):
//
//	[ nonce : 12 bytes ][ AES-GCM ciphertext ]
//
// The nonce is prepended to the ciphertext so each stored payload is
// self-contained; no external nonce column is required.
type AESGCMTransform struct {
	// Key must be 16, 24, or 32 bytes (AES-128, AES-192, or AES-256).
	Key []byte
}

// Name returns the stable identifier for this transform stage.
func (t *AESGCMTransform) Name() string { return "aes-gcm" }

// Encode encrypts plaintext and returns nonce || ciphertext.
// A fresh random nonce is generated for each call.
func (t *AESGCMTransform) Encode(plaintext []byte) ([]byte, error) {
	aead, err := t.newAEAD()
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := aead.Seal(nil, nonce, plaintext, nil)

	out := make([]byte, 0, NonceSize+len(ciphertext))
	out = append(out, nonce...)
	out = append(out, ciphertext...)
	return out, nil
}

// Decode decrypts a payload that starts with a 12-byte nonce prefix.
// Returns the original plaintext.
func (t *AESGCMTransform) Decode(payload []byte) (plaintext []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("aes-gcm decode panic (invalid input or corruption): %v", r)
			plaintext = nil
		}
	}()

	if len(payload) <= NonceSize {
		return nil, fmt.Errorf("aes-gcm payload too small: got %d bytes, need more than %d for nonce", len(payload), NonceSize)
	}

	aead, err := t.newAEAD()
	if err != nil {
		return nil, err
	}

	nonce := payload[:NonceSize]
	ciphertext := payload[NonceSize:]

	result, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}
	return result, nil
}

func (t *AESGCMTransform) newAEAD() (cipher.AEAD, error) {
	block, err := aes.NewCipher(t.Key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}
	return aead, nil
}
