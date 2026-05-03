package blocks

import "errors"

var ErrBlockBuilderSizeOverflow = errors.New("block builder: size overflow")

// BlockBuilder incrementally accumulates chunk data for one v1 encoded block.
// MaxBlockSize enforcement is intentionally deferred to higher-level packing
// policy in later phases.
type BlockBuilder struct {
	chunks [][]byte
	ids    []uint64
	size   int64
}

// AddChunk appends one chunk and updates aggregate plaintext size.
func (b *BlockBuilder) AddChunk(id uint64, data []byte) error {
	if int64(len(data)) < 0 || b.size > (1<<63-1)-int64(len(data)) {
		return ErrBlockBuilderSizeOverflow
	}

	b.ids = append(b.ids, id)
	b.chunks = append(b.chunks, append([]byte(nil), data...))
	b.size += int64(len(data))
	return nil
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
			ChunkID: b.ids[i],
			Offset:  offset,
			Size:    uint64(len(chunk)),
		}
		entries = append(entries, entry)
		offset += entry.Size
		payload = append(payload, chunk...)
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
