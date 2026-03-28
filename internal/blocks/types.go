package blocks

import (
	"fmt"
	"time"
)

type Codec string

const (
	CodecPlain  Codec = "plain"
	CodecAESGCM Codec = "aes-gcm"
)

// Descriptor represents how a chunk is physically stored in the system.
// It links logical chunk identity to its encoded representation in a container.
type Descriptor struct {
	ID            int64
	ChunkID       int64
	Codec         Codec
	FormatVersion int
	PlaintextSize int64
	StoredSize    int64
	Nonce         []byte
	ContainerID   int64
	BlockOffset   int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type EncodeInput struct {
	ChunkID   int64
	ChunkHash string
	Plaintext []byte
}

type DecodeInput struct {
	ChunkHash  string
	Descriptor Descriptor
	Payload    []byte
}

type EncodedBlock struct {
	Descriptor Descriptor
	Payload    []byte
}

// get codec transformer from codec name
func GetBlockTransformer(codec Codec) (Transformer, error) {
	switch codec {
	case CodecPlain:
		return &PlainTransformer{}, nil
	case CodecAESGCM:
		key, err := LoadEncryptionKey()
		if err != nil {
			return nil, fmt.Errorf("aes-gcm requires COLDKEEP_KEY\n\nRun:\n  coldkeep init")
		}
		return &AESGCMTransformer{Key: key}, nil
	default:
		return nil, fmt.Errorf("unknown codec: %s", codec)
	}
}
