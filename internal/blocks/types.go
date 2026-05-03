package blocks

import (
	"context"
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

// Block represents a physical stored block unit for v1.8 packed-block model.
// It is intentionally introduced early in Phase 1 and may remain unused until
// later phases wire write/read/verify paths.
type Block struct {
	ID              int64
	FormatVersion   int
	Codec           string
	PlaintextSize   int64
	StoredSize      int64
	ContainerID     int64
	ContainerOffset int64
	BlockHash       []byte
}

// ChunkSegment represents one chunk placement inside a physical block.
// Offset and Size are relative to decoded plaintext block bytes.
type ChunkSegment struct {
	ChunkID int64
	BlockID int64
	Offset  int64
	Size    int64
}

// BlockStore is the minimal storage-block retrieval boundary for v1.8+.
// It is intentionally small in Phase 1 and will be extended in later phases.
type BlockStore interface {
	GetBlock(blockID int64) (*Block, error)
}

// ChunkLocator resolves chunk placement inside a physical block.
// This abstraction is introduced early as part of the future engine boundary.
type ChunkLocator interface {
	GetChunkSegment(chunkID int64) (*ChunkSegment, error)
}

// BlockReader provides context-aware read access to decoded blocks.
// Used in Phase 3+ read path abstraction for both v1.7 and v1.8 formats.
type BlockReader interface {
	ReadBlock(ctx context.Context, blockID int64) (*EncodedBlock, error)
}

// ChunkResolver provides context-aware resolution of chunk placement inside blocks.
// Used in Phase 3+ read path abstraction for both v1.7 and v1.8 layouts.
type ChunkResolver interface {
	ResolveChunk(ctx context.Context, chunkID int64) (*ChunkSegment, error)
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

type TransformedBlock struct {
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
