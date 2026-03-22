package container

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	ChunkHashSize         = 32
	ChunkRecordSizeSize   = 4
	ChunkRecordHeaderSize = ChunkHashSize + ChunkRecordSizeSize
)

func BuildChunkRecord(chunk []byte) []byte {
	sum := sha256.Sum256(chunk)
	record := make([]byte, ChunkRecordHeaderSize+len(chunk))

	copy(record[0:ChunkHashSize], sum[:])
	binary.LittleEndian.PutUint32(record[ChunkHashSize:ChunkRecordHeaderSize], uint32(len(chunk)))
	copy(record[ChunkRecordHeaderSize:], chunk)

	return record
}

func ReadChunkDataAt(c Container, offset int64, expectedSize int64) ([]byte, error) {
	headerHash, err := c.ReadAt(offset, ChunkHashSize)
	if err != nil {
		return nil, fmt.Errorf("read chunk header hash: %w", err)
	}

	sizeBuf, err := c.ReadAt(offset+ChunkHashSize, ChunkRecordSizeSize)
	if err != nil {
		return nil, fmt.Errorf("read chunk header size: %w", err)
	}

	recordSize := int64(binary.LittleEndian.Uint32(sizeBuf))
	if recordSize != expectedSize {
		return nil, fmt.Errorf("chunk size mismatch at offset %d (db=%d record=%d)", offset, expectedSize, recordSize)
	}

	chunkData, err := c.ReadAt(offset+ChunkRecordHeaderSize, recordSize)
	if err != nil {
		return nil, fmt.Errorf("read chunk data: %w", err)
	}

	sum := sha256.Sum256(chunkData)
	if !bytes.Equal(sum[:], headerHash) {
		return nil, fmt.Errorf("chunk record header hash mismatch at offset %d", offset)
	}

	return chunkData, nil
}
