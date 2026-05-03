package blocks

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// CKBL in ASCII.
	BlockMagicV1 uint32 = 0x434B424C

	BlockFormatVersionV1 uint16 = 1
	BlockCodecNoneV1     uint16 = 0

	blockHeaderSizeV1 = 20
	chunkEntrySizeV1  = 24
)

var (
	ErrBlockFormatTooSmall      = errors.New("block format: payload too small for header")
	ErrBlockFormatUnsupported   = errors.New("block format: unsupported header values")
	ErrBlockFormatInvalidLayout = errors.New("block format: invalid table/payload layout")
)

// BlockHeader is the fixed-size binary block header for v1 format.
//
// Layout (20 bytes):
// - magic          uint32
// - version        uint16
// - codec          uint16
// - chunk_count    uint32
// - plaintext_size uint64
type BlockHeader struct {
	Magic         uint32
	Version       uint16
	Codec         uint16
	ChunkCount    uint32
	PlaintextSize uint64
}

// ChunkEntry is one chunk segment entry in the v1 chunk table.
//
// Layout (24 bytes):
// - chunk_id uint64
// - offset   uint64
// - size     uint64
type ChunkEntry struct {
	ChunkID uint64
	Offset  uint64
	Size    uint64
}

// PackedChunk is an encode helper input for building payload and chunk table
// deterministically from an ordered chunk sequence.
type PackedChunk struct {
	ChunkID uint64
	Data    []byte
}

// PackedBlockV1 is the in-memory representation of a decoded v1 encoded block.
type PackedBlockV1 struct {
	Header  BlockHeader
	Entries []ChunkEntry
	Payload []byte
}

// EncodedPackedBlockV1 is the result of encoding v1 block bytes.
// BlockHash is mandatory and computed from plaintext encoded block bytes.
type EncodedPackedBlockV1 struct {
	Bytes     []byte
	BlockHash []byte
	Header    BlockHeader
	Entries   []ChunkEntry
}

// HashPlaintextEncodedBlock returns SHA-256 over the full plaintext encoded block
// bytes. This is the mandatory v1.8 block hash identity.
func HashPlaintextEncodedBlock(encoded []byte) []byte {
	sum := sha256.Sum256(encoded)
	return sum[:]
}

// EncodePackedBlockV1FromChunks builds a v1 encoded block from an ordered chunk
// sequence, creating chunk table segmentation and payload deterministically.
func EncodePackedBlockV1FromChunks(chunks []PackedChunk) (*EncodedPackedBlockV1, error) {
	entries := make([]ChunkEntry, 0, len(chunks))
	payloadSize := uint64(0)

	for _, ch := range chunks {
		entry := ChunkEntry{
			ChunkID: ch.ChunkID,
			Offset:  payloadSize,
			Size:    uint64(len(ch.Data)),
		}
		entries = append(entries, entry)
		payloadSize += entry.Size
	}

	payload := make([]byte, 0, int(payloadSize))
	for _, ch := range chunks {
		payload = append(payload, ch.Data...)
	}

	return EncodePackedBlockV1(entries, payload)
}

// EncodePackedBlockV1 encodes block bytes using deterministic v1 layout:
// | HEADER | CHUNK_TABLE | PAYLOAD |
func EncodePackedBlockV1(entries []ChunkEntry, payload []byte) (*EncodedPackedBlockV1, error) {
	header := BlockHeader{
		Magic:         BlockMagicV1,
		Version:       BlockFormatVersionV1,
		Codec:         BlockCodecNoneV1,
		ChunkCount:    uint32(len(entries)),
		PlaintextSize: uint64(len(payload)),
	}

	if err := validateChunkEntries(entries, header.PlaintextSize); err != nil {
		return nil, err
	}

	tableSize := uint64(len(entries)) * chunkEntrySizeV1
	encoded := make([]byte, int(uint64(blockHeaderSizeV1)+tableSize+header.PlaintextSize))

	writeBlockHeader(encoded[:blockHeaderSizeV1], header)

	offset := blockHeaderSizeV1
	for _, entry := range entries {
		writeChunkEntry(encoded[offset:offset+chunkEntrySizeV1], entry)
		offset += chunkEntrySizeV1
	}

	copy(encoded[offset:], payload)

	return &EncodedPackedBlockV1{
		Bytes:     encoded,
		BlockHash: HashPlaintextEncodedBlock(encoded),
		Header:    header,
		Entries:   append([]ChunkEntry(nil), entries...),
	}, nil
}

// DecodePackedBlockV1 parses and validates encoded v1 block bytes.
func DecodePackedBlockV1(encoded []byte) (*PackedBlockV1, error) {
	if len(encoded) < blockHeaderSizeV1 {
		return nil, ErrBlockFormatTooSmall
	}

	header := readBlockHeader(encoded[:blockHeaderSizeV1])
	if header.Magic != BlockMagicV1 || header.Version != BlockFormatVersionV1 || header.Codec != BlockCodecNoneV1 {
		return nil, fmt.Errorf("%w: magic=%x version=%d codec=%d", ErrBlockFormatUnsupported, header.Magic, header.Version, header.Codec)
	}

	tableSize := uint64(header.ChunkCount) * chunkEntrySizeV1
	totalMin := uint64(blockHeaderSizeV1) + tableSize
	if uint64(len(encoded)) < totalMin {
		return nil, ErrBlockFormatInvalidLayout
	}

	payload := encoded[int(totalMin):]
	if uint64(len(payload)) != header.PlaintextSize {
		return nil, ErrBlockFormatInvalidLayout
	}

	entries := make([]ChunkEntry, int(header.ChunkCount))
	offset := blockHeaderSizeV1
	for i := 0; i < int(header.ChunkCount); i++ {
		entries[i] = readChunkEntry(encoded[offset : offset+chunkEntrySizeV1])
		offset += chunkEntrySizeV1
	}

	if err := validateChunkEntries(entries, header.PlaintextSize); err != nil {
		return nil, err
	}

	return &PackedBlockV1{
		Header:  header,
		Entries: entries,
		Payload: payload,
	}, nil
}

// SliceChunkFromPayload returns chunk bytes for one table entry.
func SliceChunkFromPayload(payload []byte, entry ChunkEntry) ([]byte, error) {
	end := entry.Offset + entry.Size
	if end > uint64(len(payload)) {
		return nil, ErrBlockFormatInvalidLayout
	}
	return payload[entry.Offset:end], nil
}

func validateChunkEntries(entries []ChunkEntry, payloadSize uint64) error {
	for _, entry := range entries {
		end := entry.Offset + entry.Size
		if end < entry.Offset || end > payloadSize {
			return ErrBlockFormatInvalidLayout
		}
	}
	return nil
}

func writeBlockHeader(dst []byte, h BlockHeader) {
	binary.LittleEndian.PutUint32(dst[0:4], h.Magic)
	binary.LittleEndian.PutUint16(dst[4:6], h.Version)
	binary.LittleEndian.PutUint16(dst[6:8], h.Codec)
	binary.LittleEndian.PutUint32(dst[8:12], h.ChunkCount)
	binary.LittleEndian.PutUint64(dst[12:20], h.PlaintextSize)
}

func readBlockHeader(src []byte) BlockHeader {
	return BlockHeader{
		Magic:         binary.LittleEndian.Uint32(src[0:4]),
		Version:       binary.LittleEndian.Uint16(src[4:6]),
		Codec:         binary.LittleEndian.Uint16(src[6:8]),
		ChunkCount:    binary.LittleEndian.Uint32(src[8:12]),
		PlaintextSize: binary.LittleEndian.Uint64(src[12:20]),
	}
}

func writeChunkEntry(dst []byte, e ChunkEntry) {
	binary.LittleEndian.PutUint64(dst[0:8], e.ChunkID)
	binary.LittleEndian.PutUint64(dst[8:16], e.Offset)
	binary.LittleEndian.PutUint64(dst[16:24], e.Size)
}

func readChunkEntry(src []byte) ChunkEntry {
	return ChunkEntry{
		ChunkID: binary.LittleEndian.Uint64(src[0:8]),
		Offset:  binary.LittleEndian.Uint64(src[8:16]),
		Size:    binary.LittleEndian.Uint64(src[16:24]),
	}
}
