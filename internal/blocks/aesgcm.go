package blocks

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

type AESGCMTransformer struct {
	Key []byte
}

func (t *AESGCMTransformer) Encode(ctx context.Context, in EncodeInput) (*EncodedBlock, error) {
	block, err := aes.NewCipher(t.Key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := aead.Seal(nil, nonce, in.Plaintext, nil)

	desc := Descriptor{
		ChunkID:       in.ChunkID,
		Codec:         CodecAESGCM,
		FormatVersion: 1,
		PlaintextSize: int64(len(in.Plaintext)),
		StoredSize:    int64(len(ciphertext)),
		Nonce:         nonce,
	}

	return &EncodedBlock{
		Descriptor: desc,
		Payload:    ciphertext,
	}, nil
}

func (t *AESGCMTransformer) Decode(ctx context.Context, in DecodeInput) ([]byte, error) {
	block, err := aes.NewCipher(t.Key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	plaintext, err := aead.Open(nil, in.Descriptor.Nonce, in.Payload, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}

	return plaintext, nil
}
