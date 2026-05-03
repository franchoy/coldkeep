package blocks

import "errors"

var ErrBlockBuilderSizeOverflow = errors.New("block builder: size overflow")
var ErrBlockBuilderInvalidTargetSize = errors.New("block builder: invalid target size")
var ErrBlockBuilderZeroChunkSize = errors.New("block builder: zero-size chunk is not allowed")
var ErrBlockBuilderChunkSizeMismatch = errors.New("block builder: chunk size does not match data length")
var ErrBlockBuilderCannotFit = errors.New("block builder: chunk does not fit current block")

// PendingChunk is one chunk candidate waiting to be packed into a block.
// Hash contains the chunk identity hash over plaintext bytes.
type PendingChunk struct {
	ChunkID int64
	Hash    []byte
	Data    []byte
	Size    int64
}

// BlockBuilder incrementally accumulates pending chunks for one packed block.
// Packing is deterministic: insertion order defines entry order.
type BlockBuilder struct {
	targetSize int64
	chunks     []PendingChunk
	size       int64
}

// NewBlockBuilder creates a builder with the given target block size.
func NewBlockBuilder(targetSize int64) *BlockBuilder {
	return &BlockBuilder{targetSize: targetSize}
}

// ShouldFlushBeforeAdd applies deterministic flush rule checks before adding the
// next chunk. It returns true only for size-based reasons:
//  1. current_size + next_size > target_size
//  2. oversized next chunk must be written alone (when current block is non-empty)
//
// It does not consider timing, goroutine completion, or random ordering.
func (b *BlockBuilder) ShouldFlushBeforeAdd(nextSize int64) bool {
	if b == nil || b.Empty() || nextSize <= 0 || b.targetSize <= 0 {
		return false
	}

	if nextSize > b.targetSize {
		// Oversized chunk cannot be combined with current block.
		return true
	}

	if b.size > (1<<63-1)-nextSize {
		return true
	}

	return b.size+nextSize > b.targetSize
}

// ShouldFlushAtEnd applies deterministic end-of-operation flush rule:
// flush remaining pending chunks when operation ends.
func (b *BlockBuilder) ShouldFlushAtEnd() bool {
	return b != nil && !b.Empty()
}

// CanFit reports whether a chunk of given size can be added without splitting.
// Oversized chunks are allowed only when the builder is empty so they can be
// emitted alone after a caller flushes any current block.
func (b *BlockBuilder) CanFit(size int64) bool {
	if b == nil || size <= 0 || b.targetSize <= 0 {
		return false
	}
	if size > b.targetSize {
		return b.Empty()
	}
	if b.size > (1<<63-1)-size {
		return false
	}
	return b.size+size <= b.targetSize
}

// Add appends one pending chunk to the current block candidate.
func (b *BlockBuilder) Add(chunk PendingChunk) error {
	if b == nil {
		return ErrBlockBuilderInvalidTargetSize
	}
	if b.targetSize <= 0 {
		return ErrBlockBuilderInvalidTargetSize
	}
	if chunk.Size <= 0 {
		return ErrBlockBuilderZeroChunkSize
	}
	if int64(len(chunk.Data)) != chunk.Size {
		return ErrBlockBuilderChunkSizeMismatch
	}
	if b.size > (1<<63-1)-chunk.Size {
		return ErrBlockBuilderSizeOverflow
	}
	if !b.CanFit(chunk.Size) {
		return ErrBlockBuilderCannotFit
	}

	b.chunks = append(b.chunks, PendingChunk{
		ChunkID: chunk.ChunkID,
		Hash:    append([]byte(nil), chunk.Hash...),
		Data:    append([]byte(nil), chunk.Data...),
		Size:    chunk.Size,
	})
	b.size += chunk.Size
	return nil
}

// Empty reports whether there are no pending chunks.
func (b *BlockBuilder) Empty() bool {
	return b == nil || len(b.chunks) == 0
}

// Reset clears current pending chunks so the builder can start a new block.
func (b *BlockBuilder) Reset() {
	if b == nil {
		return
	}
	b.chunks = b.chunks[:0]
	b.size = 0
}

// AddChunk appends one chunk and updates aggregate plaintext size.
// Compatibility helper while write path migrates to PendingChunk API.
func (b *BlockBuilder) AddChunk(id uint64, data []byte) error {
	if b == nil {
		return ErrBlockBuilderInvalidTargetSize
	}
	if b.targetSize <= 0 {
		// Backward-compatible fallback for existing tests/callers:
		// when target is unset, treat it as unlimited.
		b.targetSize = 1<<63 - 1
	}
	return b.Add(PendingChunk{ChunkID: int64(id), Data: data, Size: int64(len(data))})
}

// Build constructs encoded block in-memory representation plus mandatory
// plaintext-encoded block hash.
func (b *BlockBuilder) Build() (*EncodedBlock, []byte, error) {
	entries := make([]ChunkEntry, 0, len(b.chunks))
	payload := make([]byte, 0, b.size)
	offset := uint64(0)

	for i := range b.chunks {
		chunk := b.chunks[i]
		entry := ChunkEntry{
			ChunkID: uint64(chunk.ChunkID),
			Offset:  offset,
			Size:    uint64(chunk.Size),
		}
		entries = append(entries, entry)
		offset += entry.Size
		payload = append(payload, chunk.Data...)
	}

	serialized, err := EncodePackedBlockV1(entries, payload)
	if err != nil {
		return nil, nil, err
	}

	block := &EncodedBlock{
		Header:  serialized.Header,
		Entries: append([]ChunkEntry(nil), entries...),
		Payload: payload,
	}

	return block, serialized.BlockHash, nil
}
